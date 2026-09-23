# Flink CDC Pipeline Tokio MPSC 替换设计

## 目标

保持“单 Source Reader、多 Sink Worker、Channel 通信”的现有架构，将 pipeline 中的同步 `crossbeam_channel` 替换为异步 `tokio::sync::mpsc`，避免在 `tokio::spawn` 中执行阻塞式 `recv()`。

## 数据流

1. Pipeline 按 `parallelism` 创建 N 组有界 Tokio MPSC channel。
2. Source 保持单任务读取，根据消息 key 的哈希结果选择唯一 sender。
3. Source 通过 `sender.send(record).await` 投递；channel 满时异步等待，形成背压但不阻塞 Tokio worker。
4. 每个 receiver 只归属一个 Sink Worker，通过 `receiver.recv().await` 串行消费。
5. Sink Worker 等待当前 `producer.produce(record).await` 完成后再消费下一条消息。

## 顺序保证

- 同一个 key 始终进入同一个 channel。
- Tokio MPSC 保证单 sender 的 FIFO 顺序。
- 一个 channel 只有一个 Sink Worker，不并发处理同一个 key。
- 同一个 key 在 Sink 侧仍映射到同一个 Kafka partition。
- 不要求不同 key 之间保持全局顺序。

## 变更边界

- 修改 pipeline channel 的 Sender/Receiver 类型和创建方式。
- 将 MySQL、RocketMQ Source 的同步 `send` 改为异步 `send().await`，并沿调用链传播 async。
- 将 `RskafkaSink` 持有 receiver 的方式改为所有权消费，worker 使用 `recv().await`。
- 保留现有 key 哈希、Kafka partition、消息格式和 Kafka 写失败处理策略。
- 不修改 Kafka topic、批次参数、压缩参数和业务数据结构。

## 生命周期

- Source 结束并释放所有 sender 后，receiver 得到 `None`，Sink Worker 正常退出。
- Pipeline 等待所有 Sink Worker 结束，避免任务泄漏。

## 验证

- channel 创建数量与 `parallelism` 一致。
- 同 key 消息进入同一 worker，并按投递顺序处理。
- channel 满时 Source 异步等待，不阻塞 Tokio runtime。
- 所有 sender 释放后 Sink Worker 能退出。
- MySQL→Kafka、RocketMQ→Kafka 两条 pipeline 能通过编译和相关测试。
