# binlog-cdc

[中文](#中文) | [English](#english)

A lightweight Rust CDC toolkit inspired by Apache Flink CDC. It runs without a JVM or Flink cluster, but it is **not** an official Flink CDC port or a drop-in replacement.

## 中文

### 项目定位

`binlog-cdc` 面向 MySQL 数据同步：全量初始化工具扫描表并产生 Debezium 风格事件，实时引擎读取 MySQL Binlog 并转发变更。项目使用 Rust 单独运行，适合需要轻量部署、可控资源开销和可观测流水线的场景。其连接器和一致性能力目前少于 [Apache Flink CDC](https://github.com/apache/flink-cdc)，不要将两者视为功能等价。

### 当前支持范围

下表按**实际接通的流水线**列出能力；配置枚举或解析器中出现某种类型，不代表它已接入实时引擎。

| 组件 | 数据源 | 目标 | 状态 |
| --- | --- | --- | --- |
| `flink-cdc-rs` | MySQL Binlog | Kafka | 已实现；下文有生产观测数据 |
| `flink-cdc-rs` | MySQL Binlog | 控制台 | 已实现，适合调试 |
| `flink-cdc-rs` | Kafka 中的 Debezium 风格事件 | MySQL | 已实现；使用前须验证目标表结构和主键 |
| `flink-cdc-rs` | RocketMQ | Kafka | 已实现；需按自己的消息格式联调 |
| `flink-cdc-rs` | 控制台 | Kafka | 已实现，适合调试 |
| `flink-cdc-init` | MySQL 全量表扫描 | Kafka、文件、控制台 | 已实现；可选行数核对 |

仓库还包含 MySQL Binlog 协议解析和离线文件解析代码；它们不等于已接通的实时流水线。PostgreSQL 等数据库源、自动 Schema 演进、Flink SQL API 当前均不支持。

### 环境与构建

- Rust 工具链需支持 Edition 2024；建议使用 `Cargo.lock` 锁定依赖。
- MySQL 增量源需开启 ROW 格式 Binlog，账号具备复制读取权限；全量扫描还需要目标表的读取权限。
- 使用 Kafka 目标时，先准备 Broker 和 Topic；使用 MySQL 目标时，先创建结构兼容且带主键的目标表。
- 所有仓库内的 YAML 都是示例，账号、密码和地址必须替换为你自己的配置。

```bash
cargo build --locked --release -p flink-cdc-rs -p flink-cdc-init
```

### 快速开始：MySQL Binlog → Kafka

复制示例到被 Git 忽略的本地配置文件，至少修改源库连接、表匹配表达式、唯一 `server-id`、起始 Binlog 文件/位置、Kafka Broker 和 Topic。示例中的 `specific-offset` 是指定起点，不代表自动发现全量快照边界。

```bash
cp flink-cdc-rs/mysql-to-kafka.yaml flink-cdc-rs/flink-cdc.yaml
# 编辑 flink-cdc-rs/flink-cdc.yaml，填写自己的连接参数
./target/release/flink-cdc-rs --flink-cdc flink-cdc-rs/flink-cdc.yaml
```

输出为含 `before`、`after`、`op` 和 `source` 的 Debezium 风格 JSON。`tables` 是库表匹配表达式；并行度由 `pipeline.parallelism` 控制。调试时可改用 `flink-cdc-rs/mysql-to-console.yaml`。

### 快速开始：MySQL 全量初始化

全量初始化与增量读取是**两个独立程序**。下面的文件示例会扫描匹配表、写出事件，并在配置中启用源表行数与导出条数核对；也可改用 `flink-cdc-init/mysql-to-kafka.yaml`。

```bash
cp flink-cdc-init/mysql-to-file.yaml flink-cdc-init/flink-cdc.yaml
# 编辑连接信息、tables 和输出文件 path
./target/release/flink-cdc-init --flink-cdc flink-cdc-init/flink-cdc.yaml
```

`verify.row-count: true` 启用行数核对，`verify.fail-on-mismatch: true` 使核对失败返回错误。当前**没有自动完成全量快照与 Binlog 增量的一致性切换**；生产使用时须自行确定起始位点、处理重叠数据并核对结果。

### 快速开始：Kafka → MySQL

Kafka 源订阅 `route.source-table` 指定的 Topic；`route.sink-table` 指向已创建的 `数据库.表`。消息须符合本项目使用的 Debezium 风格结构，目标表列和主键须与事件兼容。先在测试库验证插入、更新、删除和重复消费，再用于正式数据。

```yaml
source:
  type: kafka
  name: kafka-source
  properties.bootstrap.servers: 127.0.0.1:9092
  properties.group.id: cdc-mysql-demo
sink:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: example_user
  password: example_password
route:
  - source-table: cdc-events
    sink-table: app_db.orders_copy
pipeline:
  name: kafka-to-mysql-demo
  parallelism: 1
```

将配置保存为被 Git 忽略的 `flink-cdc-rs/flink-cdc.yaml`，再运行上述 `flink-cdc-rs --flink-cdc` 命令。RocketMQ → Kafka 可使用 `source.type: rocketmq`，源配置需要 `topic`、`group`、`nameserver`，目标使用 Kafka 配置；该链路应按实际消息格式单独联调。

### 配置、恢复与监控

配置以 `source`、`sink`、可选 `route` 和 `pipeline` 组织。示例中可能出现的 `transform` 尚未接入当前实时流水线，不应据此认为转换已生效。实时引擎对未接通的 source/sink 组合会拒绝运行。当前本地 `./savepoints/savepoint.data` 只保存 **Binlog 文件名**，不是精确的文件内位置；重启可能重读，请让下游具备去重或幂等处理能力。不要假定端到端 Exactly-Once。

实时引擎在 `:9249/metrics` 暴露 Prometheus/OpenMetrics 指标：

```bash
curl http://127.0.0.1:9249/metrics
```

重点关注 `flink_mysql_cdc_total{type_name}`、`flink_mysql_binlog_event_timestamp`、`flink_sink_kafka_message_total`、`flink_channel_depth` 和 `flink_channel_usage_percent`。`write-rows`、`update-rows`、`delete-rows` 计的是 **Binlog Rows 事件对象**，不是受影响的数据库行数；不同部署版本的 Kafka 计数标签可能不同。

### 生产观测：高负载 MySQL Binlog → Kafka

以下是内部生产 Prometheus 对一条高负载实例的只读观测，**不是与 Java Flink CDC 的同条件对比压测**。为适合公开仓库，隐去内部监控地址、Pod 名和数据库地址。生产镜像标记为 `v3.6.0-1ff99ff8c3`；当前仓库代码与该历史部署可能不同。

- 时间：2026-09-16 00:00 至 2026-09-23 00:00，Asia/Shanghai，7 个完整自然日。
- 资源与速率：5 分钟步长，2017 个采样点。CPU 单位为核，内存为容器 Working Set。
- 日计数：Prometheus `increase(...[1d])` 在每日结束时计算，结果经四舍五入；这是基于采样的估计值，不是数据库精确审计计数。

| 指标 | 平均 | P95 | 最大 |
| --- | ---: | ---: | ---: |
| CPU 使用（核） | 0.435 | 1.228 | 1.558 |
| 内存 Working Set（MiB） | 265.0 | 299.3 | 312.1 |
| 全部 Binlog 事件采集（事件/秒） | 21,920 | 52,621 | 72,022 |
| Rows 事件采集（事件/秒） | 5,409 | 14,280 | 18,584 |
| Kafka 消息计数速率（条/秒） | 5,186 | 17,145 | 21,844 |

| 日期（北京时间） | 全部 Binlog 事件 | Rows 事件 | Kafka 消息计数 | `format-description` 事件（文件读取次数近似） |
| --- | ---: | ---: | ---: | ---: |
| 2026-09-16 | 1,877,812,144 | 468,469,430 | 450,078,866 | 555 |
| 2026-09-17 | 1,801,267,181 | 445,333,624 | 418,415,430 | 523 |
| 2026-09-18 | 1,933,348,087 | 476,175,345 | 455,259,162 | 565 |
| 2026-09-19 | 1,671,048,327 | 399,879,819 | 363,692,987 | 471 |
| 2026-09-20 | 1,858,986,912 | 454,747,152 | 430,411,686 | 541 |
| 2026-09-21 | 2,231,901,600 | 558,116,758 | 568,008,632 | 677 |
| 2026-09-22 | 1,888,155,041 | 469,618,609 | 449,723,026 | 540 |

7 日合计约 **132.63 亿**个 Binlog 事件、**32.72 亿**个 Rows 事件、**31.36 亿**条 Kafka 消息计数。`format-description` 通常每读取一个 Binlog 文件出现一次，但重连/重读会重复；因此表中是**文件读取次数的近似指标，不是去重文件数**。`rotate` 事件在本样本中约为其两倍，不可直接当作文件数。Kafka 生产部署的该旧指标按事件类型计数，不能单凭它证明 Broker 已确认每条消息。

CDC 从 MySQL 的源 Slave 读取 Binlog。不能直接用 PromQL 的 `time() - 事件时间戳` 与复制延迟相减：`time()` 是查询求值时刻，不是该指标实际采集时刻。我们用 `timestamp(指标)` 取得各自的 Prometheus 采集时间，先算“CDC 指标采集时间 − 最后读取的 Binlog 事件时间”，再与源 Slave 三条复制通道中**最大**的 `Seconds_Behind_Master` 比较。以下仍是 5 分钟求值步长的 2017 个配对样本：

| 延迟口径（秒） | P50 | P95 | 最大 |
| --- | ---: | ---: | ---: |
| CDC 指标实际采集时间 − 已读取 Binlog 事件时间 | 0.48 | 276.08 | 2,671.48 |
| 源 Slave 复制延迟（三通道最大值） | 0 | 266 | 2,666 |
| 两项差值（未截断负值，近似值） | 0.48 | 5.48 | 20.48 |

采集时间本身不同：CDC 指标相对求值时刻的采集滞后中位数为 **10.52 秒**，Slave 指标为 **29.25 秒**，两者实际采集时间相差中位数 **18.73 秒**。因此原先按求值时刻计算出的“额外 11 秒”主要是采样时刻偏移，不能归为 CDC 处理耗时。按 Rows 事件速率将样本分为最低和最高四分位（各 505 点）后，低峰的差值 P95 为 **0.48 秒**，高峰为 **14.48 秒**；高峰时源事件年龄 P95 为 1663.28 秒、Slave 延迟 P95 为 1650.4 秒，长尾主要与上游复制滞后同期出现。高峰的 14.48 秒仍小于约 18.73 秒的跨指标采集时间差，**不能据此断言 CDC 自身有 14.48 秒延迟，也不能证明严格零延迟**。两个指标并非同一瞬间采集，复制通道延迟还可能在采样间变化；事件时间戳也不是 Kafka Broker 确认时间或端到端业务延迟。

复核时使用相同时间窗和实例标签，主要 PromQL 如下（占位符不是真实生产标识）：

```promql
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>"}[1d]))
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>",type_name=~"write-rows|update-rows|delete-rows"}[1d]))
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>",type_name="format-description"}[1d]))
sum(increase(flink_sink_kafka_message_total{pod="<CDC_POD>"}[1d]))
timestamp(flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"}) - flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"}
time() - timestamp(flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"})
max(mysql_slave_status_seconds_behind_master{instance="<SOURCE_SLAVE_EXPORTER>"})
max(timestamp(mysql_slave_status_seconds_behind_master{instance="<SOURCE_SLAVE_EXPORTER>"}))
```

### Roadmap

- [ ] 为 Kafka Sink 完善批量发送、Broker 确认结果与失败重试的统一统计。
- [ ] 持久化精确 Binlog 文件和位置，明确重启恢复与重复投递语义。
- [ ] 建立全量快照到增量的可验证一致性切换与端到端延迟指标。
- [ ] 增加去重 Binlog 文件计数；扩展并验证更多数据源、目标和转换能力。
- [ ] 在相同数据、硬件和交付语义下与 Java Flink CDC 做可复现的性能/资源对比。

Roadmap 是计划，不代表当前已经支持；没有承诺发布时间。

### 安全提示

示例配置仅包含占位值。不要提交真实账号、密码、内部地址或运行时 `flink-cdc.yaml`；公开仓库时也不要直接推送可能包含旧凭据的 Git 历史。曾暴露的凭据应轮换。

## English

### What this project is

`binlog-cdc` is a standalone Rust toolkit for MySQL change-data capture. The snapshot utility scans tables and emits Debezium-style events; the streaming engine reads MySQL Binlog and forwards changes. It targets lightweight deployment, controlled resource use, and observable pipelines. It has fewer connectors and consistency features than [Apache Flink CDC](https://github.com/apache/flink-cdc) and is not feature-equivalent to it.

### Supported pipelines

Only **wired and runnable** pipelines are listed here. A configuration enum or parser alone does not make a pipeline supported.

| Component | Source | Sink | Status |
| --- | --- | --- | --- |
| `flink-cdc-rs` | MySQL Binlog | Kafka | Implemented; production observations below |
| `flink-cdc-rs` | MySQL Binlog | Console | Implemented; useful for debugging |
| `flink-cdc-rs` | Debezium-style events in Kafka | MySQL | Implemented; verify target schema and primary key first |
| `flink-cdc-rs` | RocketMQ | Kafka | Implemented; validate your message format |
| `flink-cdc-rs` | Console | Kafka | Implemented; useful for debugging |
| `flink-cdc-init` | Full MySQL table scan | Kafka, file, console | Implemented; optional row-count verification |

The repository also includes MySQL Binlog protocol and offline-file parsing code; these are not additional wired streaming pipelines. PostgreSQL sources, automatic schema evolution, and the Flink SQL API are not currently supported.

### Requirements and build

- Use a Rust toolchain that supports Edition 2024. Keep dependencies pinned with `Cargo.lock`.
- MySQL streaming needs ROW-format Binlog and replication-read privileges; snapshots also need SELECT access to the source tables.
- Prepare a Kafka Broker and Topic before using a Kafka sink. Create a schema-compatible target table with a primary key before using a MySQL sink.
- Repository YAML files are examples: replace all connection placeholders locally.

```bash
cargo build --locked --release -p flink-cdc-rs -p flink-cdc-init
```

### Quick start: MySQL Binlog → Kafka

Copy the sample to the Git-ignored local config, then set the source connection, table pattern, unique `server-id`, starting Binlog file/position, Kafka Brokers, and Topic. `specific-offset` specifies a starting point; it does not discover a snapshot boundary automatically.

```bash
cp flink-cdc-rs/mysql-to-kafka.yaml flink-cdc-rs/flink-cdc.yaml
# Edit flink-cdc-rs/flink-cdc.yaml with your own connection settings
./target/release/flink-cdc-rs --flink-cdc flink-cdc-rs/flink-cdc.yaml
```

Output is Debezium-style JSON with `before`, `after`, `op`, and `source`. `tables` is a database/table match expression, and `pipeline.parallelism` controls concurrency. For debugging, use `flink-cdc-rs/mysql-to-console.yaml` instead.

### Quick start: MySQL snapshot

Snapshot and Binlog streaming are **separate programs**. The file example scans matching tables and can compare source row counts with exported event counts. Use `flink-cdc-init/mysql-to-kafka.yaml` to send the snapshot to Kafka instead.

```bash
cp flink-cdc-init/mysql-to-file.yaml flink-cdc-init/flink-cdc.yaml
# Edit connection settings, tables, and output path
./target/release/flink-cdc-init --flink-cdc flink-cdc-init/flink-cdc.yaml
```

`verify.row-count: true` enables the count check; `verify.fail-on-mismatch: true` makes a mismatch fail. There is currently **no automatic, consistency-guaranteed snapshot-to-Binlog handoff**. For production use, establish the starting offset, handle overlap, and reconcile the result yourself.

### Quick start: Kafka → MySQL

The Kafka source subscribes to the Topic in `route.source-table`; `route.sink-table` names an existing `database.table`. Messages must use this project's Debezium-style shape. The target columns and primary key must be compatible. Test inserts, updates, deletes, and duplicate consumption against a non-production database first.

```yaml
source:
  type: kafka
  name: kafka-source
  properties.bootstrap.servers: 127.0.0.1:9092
  properties.group.id: cdc-mysql-demo
sink:
  type: mysql
  hostname: 127.0.0.1
  port: 3306
  username: example_user
  password: example_password
route:
  - source-table: cdc-events
    sink-table: app_db.orders_copy
pipeline:
  name: kafka-to-mysql-demo
  parallelism: 1
```

Save this as the ignored `flink-cdc-rs/flink-cdc.yaml` and run the same `flink-cdc-rs --flink-cdc` command. For RocketMQ → Kafka, use `source.type: rocketmq` with `topic`, `group`, and `nameserver`, plus the Kafka sink settings; validate the message contract for your workload.

### Configuration, recovery, and monitoring

Configuration uses `source`, `sink`, optional `route`, and `pipeline` sections. A `transform` section may appear in examples, but it is not wired into the current streaming pipeline; do not assume it takes effect. The streaming engine rejects source/sink combinations that have not been wired. The local `./savepoints/savepoint.data` currently stores only the **Binlog filename**, not its exact byte position. Restarts can replay data; make downstream processing idempotent or deduplicate. Do not assume end-to-end Exactly-Once delivery.

The streaming engine exposes Prometheus/OpenMetrics on `:9249/metrics`:

```bash
curl http://127.0.0.1:9249/metrics
```

Key metrics are `flink_mysql_cdc_total{type_name}`, `flink_mysql_binlog_event_timestamp`, `flink_sink_kafka_message_total`, `flink_channel_depth`, and `flink_channel_usage_percent`. The `write-rows`, `update-rows`, and `delete-rows` counters count **Binlog Rows event objects**, not affected database rows. Kafka counter labels differ between deployed versions.

### Production observations: high-load MySQL Binlog → Kafka

These are read-only observations from an internal production Prometheus instance, **not a controlled Java-vs-Rust Flink CDC benchmark**. Internal monitoring URLs, Pod names, and database addresses are omitted for a public repository. The observed production image tag was `v3.6.0-1ff99ff8c3`; current repository code may differ from that historical deployment.

- Window: 2026-09-16 00:00 to 2026-09-23 00:00, Asia/Shanghai; seven complete calendar days.
- Resources and rates: five-minute steps, 2017 samples. CPU is in cores; memory is container Working Set.
- Daily counts: Prometheus `increase(...[1d])` evaluated at each day boundary and rounded. They are sampling-based estimates, not exact database audit counts.

| Metric | Mean | P95 | Maximum |
| --- | ---: | ---: | ---: |
| CPU usage (cores) | 0.435 | 1.228 | 1.558 |
| Memory Working Set (MiB) | 265.0 | 299.3 | 312.1 |
| All captured Binlog events (events/s) | 21,920 | 52,621 | 72,022 |
| Captured Rows events (events/s) | 5,409 | 14,280 | 18,584 |
| Kafka message counter rate (messages/s) | 5,186 | 17,145 | 21,844 |

| Date (Asia/Shanghai) | All Binlog events | Rows events | Kafka message counter | `format-description` events (file-read proxy) |
| --- | ---: | ---: | ---: | ---: |
| 2026-09-16 | 1,877,812,144 | 468,469,430 | 450,078,866 | 555 |
| 2026-09-17 | 1,801,267,181 | 445,333,624 | 418,415,430 | 523 |
| 2026-09-18 | 1,933,348,087 | 476,175,345 | 455,259,162 | 565 |
| 2026-09-19 | 1,671,048,327 | 399,879,819 | 363,692,987 | 471 |
| 2026-09-20 | 1,858,986,912 | 454,747,152 | 430,411,686 | 541 |
| 2026-09-21 | 2,231,901,600 | 558,116,758 | 568,008,632 | 677 |
| 2026-09-22 | 1,888,155,041 | 469,618,609 | 449,723,026 | 540 |

Seven-day totals are approximately **13.26 billion** Binlog events, **3.27 billion** Rows events, and **3.14 billion** Kafka message-counter increments. A `format-description` event normally occurs once per Binlog file read, but reconnects/re-reads can count a file again. It is a **file-read proxy, not a distinct-file count**. Observed `rotate` counts were about twice as high, so they are not treated as file counts. The older production Kafka metric counts by event type and alone cannot prove that every message was acknowledged by the Broker.

The CDC source reads Binlog from a MySQL Slave. Subtracting replication lag from `time() - event_timestamp` would confuse the PromQL evaluation time with the metric's actual scrape time. We use `timestamp(metric)` for each series, calculate "CDC metric scrape time − last read Binlog event time", and compare it with the **maximum** `Seconds_Behind_Master` across the Slave's three replication channels. There are 2017 paired samples on a five-minute evaluation grid:

| Lag definition (seconds) | P50 | P95 | Maximum |
| --- | ---: | ---: | ---: |
| CDC metric scrape time − timestamp of last read Binlog event | 0.48 | 276.08 | 2,671.48 |
| Source Slave replication lag (maximum channel) | 0 | 266 | 2,666 |
| Difference between the two (unclamped approximation) | 0.48 | 5.48 | 20.48 |

The CDC and Slave metrics were collected at different times: their median age relative to the query evaluation time was **10.52 s** and **29.25 s**, respectively, for a median **18.73 s** separation between scrape times. The previously computed "extra 11 s" mostly reflected that sampling offset and must not be attributed to CDC processing. Splitting the samples by Rows-event rate into the lowest and highest quartiles (505 samples each), the residual P95 was **0.48 s** at low load and **14.48 s** at high load. At high load, source event age P95 was 1663.28 s and Slave lag P95 was 1650.4 s, so most of the long tail coincided with upstream replication lag. The 14.48 s high-load residual is still below the approximately 18.73 s cross-metric scrape gap; it **neither establishes 14.48 s of CDC processing delay nor proves zero delay**. These metrics are not collected simultaneously, replication lag may change between scrapes, and the event timestamp is neither a Kafka Broker acknowledgement time nor an end-to-end business latency measure.

Use the same window and instance selectors to reproduce the measurements. Placeholders below are not production identifiers:

```promql
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>"}[1d]))
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>",type_name=~"write-rows|update-rows|delete-rows"}[1d]))
sum(increase(flink_mysql_cdc_total{pod="<CDC_POD>",type_name="format-description"}[1d]))
sum(increase(flink_sink_kafka_message_total{pod="<CDC_POD>"}[1d]))
timestamp(flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"}) - flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"}
time() - timestamp(flink_mysql_binlog_event_timestamp{pod="<CDC_POD>"})
max(mysql_slave_status_seconds_behind_master{instance="<SOURCE_SLAVE_EXPORTER>"})
max(timestamp(mysql_slave_status_seconds_behind_master{instance="<SOURCE_SLAVE_EXPORTER>"}))
```

### Roadmap

- [ ] Complete Kafka sink batching, Broker-acknowledgement visibility, and consistent failure/retry metrics.
- [ ] Persist both Binlog filename and exact position; define replay and recovery semantics.
- [ ] Build a verifiable, consistent snapshot-to-stream handoff and an end-to-end latency metric.
- [ ] Add a distinct-file counter; expand and validate more sources, sinks, and transformations.
- [ ] Benchmark against Java Flink CDC with identical data, hardware, and delivery semantics.

Roadmap items are plans, not currently shipped features or release-date commitments.

### Security

Example configs contain placeholders only. Never commit real credentials, internal addresses, or runtime `flink-cdc.yaml` files. Do not publish old Git history that may contain secrets; rotate any previously exposed credentials.
