# flink-cdc-rs 性能优化计划

## v1.2.0 版本目标 ✅

### 目标内容

- ✅ 实现 `mysqldump -> console` 数据流水线
- ✅ 支持从 MySQL dump SQL 文件解析数据变更事件
- ✅ 支持将解析后的 CDC 事件输出到 console，便于本地调试和链路验证

### 流水线架构

```
MySQL Dump SQL → [mysqldump source] → [pipeline] → [console sink]
```

### 应用场景

- 本地验证 mysqldump 解析逻辑
- 在不依赖 Kafka/MySQL sink 的情况下调试 CDC 事件结构
- 为后续 source/sink 组合式流水线能力打基础

---

## 阶段1: 内存分配优化 ✅

### 优化内容

- **架构**: 单线程同步处理路线
- **主要优化**: 使用 `tikv-jemallocator` 替换默认的 glibc malloc
- **配置**:
  ```rust
  #[global_allocator]
  static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
  ```

### 性能提升

- ✅ **性能提升: 50%**
- 处理速度: 从 **1.1w/s** 提升到 **1.6w/s**

### 优化原理

通过 perf 性能分析发现：

- **优化前**: libc.so.6 (内存分配) 占用 **37.13%** CPU
- **优化后**: libc.so.6 占用降至 **13.43%**，应用业务逻辑占用从 **34.53%** 提升到 **54.93%**

tikv-jemallocator 的优势：

- ✅ Arena 机制：每个线程独立 arena，减少锁竞争
- ✅ 更优的内存池管理：减少系统调用
- ✅ 针���小对象分配优化：适合 CDC 高频事件场景
- ✅ 更少的内存碎片：提高缓存命中率

### 当前性能瓶颈 (基于 perf 分析)

#### 应用层热点函数 (flink-cdc-rs)

1. **TableMetadata::parse** (1.39%)
   - 位置: `mysql_binlog_connector_rust::event::table_map::table_metadata`
   - 场景: MySQL binlog 表元数据解析
   - 优化方向: 已实现缓存机制

2. **迭代器 drop 操作** (0.97%)
   - 位置: `core::ptr::drop_in_place`
   - 场景: 大量临时对象销毁
   - 优化方向: 对象池重用

3. **Kafka 协议处理** (0.49%)
   - 位置: `rskafka::protocol::primitives::String_::read`
   - 场景: Kafka 协议解析
   - 优化方向: 批量处理减少协议开销

#### 主要性能开销函数

- `handle_write_event`: 写入事件处理
- `handle_update_event`: 更新事件处理 (最耗时，处理双倍数据)
- `handle_delete_event`: 删除事件处理

**核心热点**: `convert_and_parse_row` 方法

- 每行都需要执行
- 大量 `Map<String, Value>` 创建
- 每个列都要进行 JSON 序列化
- UTF8 转换和字符串分配频繁

---

## 阶段2: 生产者-消费者架构优化 📋

### 优化目标

通过分离 **binlog 读取** 和 **事件处理**，实现生产者-消费者模式，最大限度提升吞吐量。

### 架构设计

#### 当前架构（阶段1）

```
MySQL Binlog → [单线程同步处理] → Kafka (rdkafka/rskafka)
     ↓
  读取+处理在同一流程
```

#### 新架构（阶段2）

```
MySQL Binlog → [Binlog 读取线程] → Channel → [事件处理线程池] → Kafka (rskafka)
                   (生产者)           (缓冲区)      (消费者)
```

### 技术方案

#### 1. 使用 rskafka 替代 rdkafka

- **优势**: 纯 Rust 实现，更好的异步集成
- **性能**: 减少跨 FFI 边界调用
- **配置**: 通过环境变量 `RSKAFKA` 控制

```rust
// 当前实现（row.rs:134-138）
if std::env::var("RSKAFKA").is_ok() {
    self.rskafka_sink.send_messages(debeziums).await;
} else {
    self.kafka_sink.send_batch_messages(debeziums).await;
}
```

#### 2. Channel 分离设计

- **读取线程**: 专注 binlog 流式读取，最大化 I/O 吞吐
- **Channel 缓冲**: 使用 `tokio::sync::mpsc` 作为异步缓冲队列
- **处理线程池**: 并发处理事件，充分利用多核 CPU

```rust
// 设计草图（伪代码）
let (tx, rx) = tokio::sync::mpsc::channel::<BinlogEvent>(10000);

// 生产者：binlog 读取线程
tokio::spawn(async move {
    loop {
        let event = read_binlog_event().await?;
        tx.send(event).await?;
    }
});

// 消费者：事件处理线程池
for _ in 0..num_cpus::get() {
    let rx = rx.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            handle_event(event).await?;
        }
    });
}
```

### 预期收益

- ✅ **I/O 和计算解耦**: binlog 读取不被事件处理阻塞
- ✅ **多核并行利用**: 事件处理在多线程并行执行
- ✅ **背压机制**: Channel 缓冲区满时自动背压，防止 OOM
- ✅ **弹性扩展**: 可根据负载调整线程池大小

### 性能目标

- 预期提升: **2-3x** (相比阶段1的 1.6w/s)
- 目标速度: **3w-5w/s**

### 实施步骤

1. [ ] 设计 BinlogEvent 数据结构（包含所有必要信息）
2. [ ] 实现 binlog 读取生产者（基于现有代码抽取）
3. [ ] 实现 Channel 通信机制
4. [ ] 实现事件处理消费者线程池
5. [ ] 集成 rskafka 发送逻辑
6. [ ] 性能测试和调优（channel 容量、线程数等）

---

## 下一步优化方向

### 阶段3: 业务逻辑优化 📋

基于阶段2的并发架构，进一步优化单事件处理性能：

- [ ] 减少 JSON 序列化次数
- [ ] 使用 `Cow<str>` 减少字符串分配
- [ ] 预分配容器容量（避免动态扩容）
- [ ] 对象池：重用 `Map<String, Value>` 等高频对象

### 阶段4: 高级优化 📋

- [ ] SIMD 优化：UTF8 转换和 JSON 序列化
- [ ] 零拷贝解析：避免中间缓冲区
- [ ] 批量发送优化：增大 Kafka 批量大小
- [ ] 自适应调优：根据负载动态调整参数

---

## 性能测试环境

- **CPU**: Intel(R) Xeon(R) CPU E5-2660 v4 @ 2.00GHz
- **OS**: Linux 5.14.0-479.el9.x86_64
- **Rust**: edition 2024, opt-level 3, lto true
- **场景**: MySQL binlog CDC → Kafka

---

## 已知问题 / Bug 记录

### 🔴 严重问题

_暂无严重问题_

### 🟡 中等问题

#### Bug #001:

**状态**: 待修复
**影响版本**: v1.1.2
**优先级**: P2
**描述**:

**复现步骤**:

1.
2.
3.

**预期行为**:

**实际行为**:

**临时解决方案**:

**计划修复版本**: v1.1.3

---

### 🟢 低优先级问题

_暂无低优先级问题_

---

## 参考资料

- [tikv-jemallocator 文档](https://docs.rs/tikv-jemallocator/)
- [hashbrown HashMap](https://docs.rs/hashbrown/) - 已集成使用
- [rskafka](https://docs.rs/rskafka/) - 纯 Rust Kafka 客户端
- [tokio::sync::mpsc](https://docs.rs/tokio/*/tokio/sync/mpsc/index.html) - 异步 Channel
- [Perf 性能分析工具](https://perf.wiki.kernel.org/)
