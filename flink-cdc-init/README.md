# flink-cdc-init

`flink-cdc-init` 用来做 MySQL 全量初始化导出：

1. 按照 `source.tables` 匹配目标表
2. 全量扫描表数据
3. 转成 Debezium JSON `insert` 事件
4. 写入配置里的 Kafka / 文件 / 控制台

配置文件格式和 `flink-cdc-rs` 保持一致，直接使用 `source/sink/pipeline` 三段 YAML。

额外支持：

1. `sink.type: kafka | file | console`
2. `verify.row-count: true` 时，对比源表 `COUNT(*)` 和实际导出条数
3. `verify.fail-on-mismatch: true` 时，条数不一致直接返回失败

启动方式：

```bash
cargo run -p flink-cdc-init -- --flink-cdc ./flink-cdc-init/mysql-to-kafka.yaml
```

Kafka 初始化配置示例：

```bash
cargo run -p flink-cdc-init -- --flink-cdc ./flink-cdc-init/mysql-to-kafka.yaml
```

导出到文件示例：

```bash
cargo run -p flink-cdc-init -- --flink-cdc ./flink-cdc-init/mysql-to-file.yaml
```
