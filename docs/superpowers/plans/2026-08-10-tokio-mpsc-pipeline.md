# Tokio MPSC Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace blocking crossbeam pipeline channels with bounded Tokio MPSC channels while preserving per-key Kafka write order.

**Architecture:** One source task hashes each key onto one of N bounded channels. Each channel is owned by exactly one sink task, which receives and writes records serially; channel send and receive both await without blocking Tokio worker threads.

**Tech Stack:** Rust 2024, Tokio `mpsc`, rskafka, Cargo test

## Global Constraints

- Preserve the existing key hash and Kafka partition calculation.
- Preserve message formats, topic selection, compression, batching, and Kafka error policy.
- Do not require ordering between different keys.
- Preserve the user's existing `pipeline/mod.rs` import edit.

---

### Task 1: Create and verify bounded Tokio channels

**Files:**
- Modify: `flink-cdc-rs/src/pipeline/mod.rs`
- Test: `flink-cdc-rs/src/pipeline/mod.rs`

**Interfaces:**
- Produces: `bounded_channels<T>(parallelism: u32, capacity: u32) -> (Vec<Sender<T>>, Vec<Receiver<T>>)`
- Produces: `channels(&CdcConfig) -> (Vec<Sender<PipelineRecord>>, Vec<Receiver<PipelineRecord>>)`

- [x] **Step 1: Add a test that asserts channel count, FIFO delivery, and closure**

```rust
#[tokio::test]
async fn bounded_tokio_channels_preserve_fifo_and_close() {
    let (senders, mut receivers) = bounded_channels::<u32>(2, 2);
    assert_eq!(senders.len(), 2);
    assert_eq!(receivers.len(), 2);
    senders[0].send(1).await.unwrap();
    senders[0].send(2).await.unwrap();
    assert_eq!(receivers[0].recv().await, Some(1));
    assert_eq!(receivers[0].recv().await, Some(2));
    drop(senders);
    assert_eq!(receivers[0].recv().await, None);
}
```

- [x] **Step 2: Run the test and verify it fails because the helper does not exist**

Run: `cargo test -p flink-cdc-rs bounded_tokio_channels_preserve_fifo_and_close`

- [x] **Step 3: Implement the generic bounded channel helper**

```rust
fn bounded_channels<T>(parallelism: u32, capacity: u32) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    (0..parallelism)
        .map(|_| tokio::sync::mpsc::channel(capacity as usize))
        .unzip()
}
```

Make `channels(&CdcConfig)` delegate to this helper.

- [x] **Step 4: Run the focused test and verify it passes**

Run: `cargo test -p flink-cdc-rs bounded_tokio_channels_preserve_fifo_and_close`

### Task 2: Convert source delivery to async Tokio send

**Files:**
- Modify: `flink-cdc-rs/src/source/mysql.rs`
- Modify: `flink-cdc-rs/src/source/rocketmq.rs`

**Interfaces:**
- Consumes: `Vec<tokio::sync::mpsc::Sender<PipelineRecord>>`
- Produces: async row handlers and `send(...).await` delivery with bounded backpressure

- [x] **Step 1: Run compile-check and capture the crossbeam/Tokio type failures**

Run: `cargo check -p flink-cdc-rs`

- [x] **Step 2: Replace MySQL sender types and propagate async through row handlers**

For both `MysqlDebezium` and `MysqlBinlogEvent`, change channel fields and constructor parameters to Tokio sender. Convert the row handlers and send helpers to `async fn`, call each helper with `.await`, and send using:

```rust
if let Err(err) = self.channels[index].send(record).await {
    warn!("send mysql record to channel error:{:?}", err);
}
```

- [x] **Step 3: Replace RocketMQ sender types and await delivery**

Change `RocketMQSource` and `RocketMQDebeziumHandler` to Tokio senders. Make the handler helper async and call it from `MessageHandler::handle` with `.await`.

- [x] **Step 4: Run compile-check and verify source code compiles up to sink receiver ownership errors**

Run: `cargo check -p flink-cdc-rs`

### Task 3: Convert Rskafka sink workers to async receive

**Files:**
- Modify: `flink-cdc-rs/src/sink/kafka.rs`
- Test: `flink-cdc-rs/src/sink/kafka.rs`

**Interfaces:**
- Consumes: `Vec<tokio::sync::mpsc::Receiver<PipelineRecord>>`
- Produces: `RskafkaSink::start(self) -> Vec<JoinHandle<()>>`

- [x] **Step 1: Add a worker lifecycle test**

```rust
#[tokio::test]
async fn rskafka_workers_exit_after_all_senders_are_dropped() {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    let sink = RskafkaSink {
        partition_producers: HashMap::new(),
        topic: "test".to_string(),
        channels: vec![receiver],
    };
    let handle = sink.start().into_iter().next().unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker did not exit")
        .expect("worker panicked");
}
```

- [x] **Step 2: Run the lifecycle test and verify it fails with the old receiver/start API**

Run: `cargo test -p flink-cdc-rs rskafka_workers_exit_after_all_senders_are_dropped`

- [x] **Step 3: Make the sink own receivers and await `recv()`**

Change receiver types to Tokio MPSC. Make `start` consume `self`, move each receiver into one task, and use:

```rust
while let Some(message) = receiver.recv().await {
    sink.send_message(message).await;
}
```

Change `write(&self)` to `write(self)` because Tokio receivers are not cloneable.

- [x] **Step 4: Run the lifecycle test and verify it passes**

Run: `cargo test -p flink-cdc-rs rskafka_workers_exit_after_all_senders_are_dropped`

### Task 4: Integration verification

**Files:**
- Modify only if required by compiler: files listed in Tasks 1–3

**Interfaces:**
- Verifies the MySQL→Kafka and RocketMQ→Kafka pipelines compile with the same key routing behavior.

- [x] **Step 1: Format changed Rust files**

Run: `cargo fmt --all -- --check`

If it reports formatting differences, run `cargo fmt --all`, then rerun the check.

- [x] **Step 2: Run the flink-cdc-rs test suite**

Run: `cargo test -p flink-cdc-rs`

- [x] **Step 3: Run compile-check**

Run: `cargo check -p flink-cdc-rs`

- [x] **Step 4: Inspect the final diff**

Run: `git diff --check && git diff --stat && git status --short`

## Execution Notes

- Focused Tokio channel tests: 2 passed, 0 failed.
- `cargo check -p flink-cdc-rs`: passed with existing warnings.
- `cargo test -p flink-cdc-rs`: 18 passed, 6 failed. Five failures install the global tracing subscriber more than once; one failure is caused by the existing sample YAML missing `properties.group.id`.
- Changed Rust files pass standalone `rustfmt --check`; workspace-wide formatting also reports unrelated pre-existing files.
