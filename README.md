# binlog-cdc

`binlog-cdc` 是 Rust 实现的轻量级 CDC 工具。`flink-cdc-rs` 读取 MySQL Binlog 等实时源并写入 Kafka 等目标；`flink-cdc-init` 单独执行 MySQL 全量扫描。无需 JVM 或 Flink 集群。

当前版本：[v1.0.0](docs/releases/v1.0.0.md)。发布页提供 Linux x86_64 二进制，Docker Hub 镜像格式为 `liuxuzxx/flink-cdc-rs:v1.0.0-<10 位提交号>`。

## 使用：MySQL Binlog → Kafka

这是主要使用场景。准备 MySQL 和 Kafka 后，在仓库根目录执行：

```bash
cargo build --locked --release -p flink-cdc-rs
cp flink-cdc-rs/mysql-to-kafka.yaml flink-cdc-rs/flink-cdc.yaml
```

编辑 `flink-cdc-rs/flink-cdc.yaml`。一个完整的最小示例：

```yaml
source:
  type: mysql
  name: mysql-source
  hostname: 127.0.0.1
  port: 3306
  username: example_user
  password: example_password
  tables: 'app_db.orders_[0-9]+'
  server-id: 5710-5716
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: mysql-bin.000001
  scan.startup.specific-offset.pos: 4
sink:
  type: kafka
  name: kafka-sink
  properties.bootstrap.servers: 127.0.0.1:9092
  properties.compression.type: lz4
  topic: cdc-events
pipeline:
  name: mysql-to-kafka
  parallelism: 2
  channel.capacity: 10
```

把示例值替换为实际连接信息：

| 配置 | 如何填写 |
| --- | --- |
| `source.hostname`、`port`、`username`、`password` | MySQL 连接信息；账号需要读取 Binlog 的复制权限。 |
| `source.tables` | 要同步的 `库.表` 正则表达式。 |
| `source.server-id` | 此 CDC 实例独占的 MySQL 复制客户端 ID；不能与其他客户端冲突。 |
| `scan.startup.specific-offset.file`、`pos` | 从指定 Binlog 文件和位置开始；先确认该文件仍在 MySQL 上。 |
| `properties.bootstrap.servers`、`topic` | Kafka Broker 与已准备好的 Topic；容器运行时地址须在容器内可达。 |
| `pipeline.parallelism`、`channel.capacity` | 并行任务数和通道容量。 |

MySQL 须开启 ROW 格式 Binlog。启动后，程序将变更写成含 `before`、`after`、`op`、`source` 等字段的 Debezium 风格 JSON：

```bash
./target/release/flink-cdc-rs --flink-cdc flink-cdc-rs/flink-cdc.yaml
curl http://127.0.0.1:9249/metrics
```

也可以下载 [Release 二进制](https://github.com/liuxuzxx/binlog-cdc/releases)，解压后用相同的 `--flink-cdc` 参数启动。Docker 镜像需挂载自己的配置文件：

```bash
IMAGE="liuxuzxx/flink-cdc-rs:v1.0.0-$(git rev-parse --short=10 'v1.0.0^{commit}')"
docker run --rm -p 9249:9249 \
  -v "$PWD/flink-cdc-rs/flink-cdc.yaml:/app/flink-cdc.yaml:ro" \
  "$IMAGE" --flink-cdc /app/flink-cdc.yaml
```

`flink-cdc.yaml` 已被 Git 忽略。当前增量程序的本地 savepoint 只保存 Binlog 文件名，重启可能重读；下游应具备去重或幂等能力。全量初始化和增量同步是两个独立程序，尚未自动完成一致性切换。

## 支持的 Source → Sink

| 程序 | Source → Sink | 示例 |
| --- | --- | --- |
| `flink-cdc-rs` | MySQL Binlog → Kafka | 上面的完整示例 |
| `flink-cdc-rs` | MySQL Binlog → 控制台 | [mysql-to-console.yaml](flink-cdc-rs/mysql-to-console.yaml) |
| `flink-cdc-rs` | Kafka → MySQL | 下方简例 |
| `flink-cdc-rs` | RocketMQ → Kafka | 下方简例 |
| `flink-cdc-rs` | 控制台 → Kafka | [console-to-kafka.yaml](flink-cdc-rs/console-to-kafka.yaml) |
| `flink-cdc-init` | MySQL 全量扫描 → Kafka / 文件 / 控制台 | [mysql-to-kafka.yaml](flink-cdc-init/mysql-to-kafka.yaml)、[mysql-to-file.yaml](flink-cdc-init/mysql-to-file.yaml) |

Kafka → MySQL：Kafka 消息需采用本项目的 Debezium 风格结构，目标表须预先建好并配置兼容主键。以下配置保存为 `flink-cdc-rs/flink-cdc.yaml` 后，仍用上面的启动命令：

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
  name: kafka-to-mysql
  parallelism: 1
```

RocketMQ → Kafka：使用上面 MySQL → Kafka 示例的 `sink` 和 `pipeline`，将 `source` 换为：

```yaml
source:
  type: rocketmq
  topic: source-events
  group: cdc-forwarder
  nameserver: 127.0.0.1:9876
```

MySQL 全量初始化示例：`cargo build --locked --release -p flink-cdc-init`，复制 `flink-cdc-init/mysql-to-kafka.yaml` 或 `mysql-to-file.yaml`，填写连接信息，再执行 `./target/release/flink-cdc-init --flink-cdc <配置路径>`。可用 `verify.row-count` 做源表行数与导出条数核对。

## 生产观测：少量资源处理高吞吐 Binlog

2026-09-16 至 2026-09-22，单实例 MySQL Binlog → Kafka 的生产 Prometheus 观测；这是历史部署 `v3.6.0-1ff99ff8c3` 的数据，不代表 v1.0.0 的压测结果。CPU 为核数，内存为容器 Working Set。

| 指标 | 平均 | P95 | 峰值 |
| --- | ---: | ---: | ---: |
| Kafka 消息计数 TPS | 5,186 | 17,145 | 21,844 |
| Binlog 事件 TPS | 21,920 | 52,621 | 72,022 |
| CPU（核） | 0.435 | 1.228 | 1.558 |
| 内存（MiB） | 265.0 | 299.3 | 312.1 |

| 日期 | Binlog 文件读取次数（约） | Binlog 事件数 | Kafka 消息计数 |
| --- | ---: | ---: | ---: |
| 09-16 | 555 | 1,877,812,144 | 450,078,866 |
| 09-17 | 523 | 1,801,267,181 | 418,415,430 |
| 09-18 | 565 | 1,933,348,087 | 455,259,162 |
| 09-19 | 471 | 1,671,048,327 | 363,692,987 |
| 09-20 | 541 | 1,858,986,912 | 430,411,686 |
| 09-21 | 677 | 2,231,901,600 | 568,008,632 |
| 09-22 | 540 | 1,888,155,041 | 449,723,026 |

文件读取次数取自 `format-description` 事件，重读会重复；计数来自 Prometheus `increase` 估计值，Kafka 消息计数不等于 Broker 确认数。

## 版本记录与发布

| 版本 | 变更 |
| --- | --- |
| [v1.0.0](docs/releases/v1.0.0.md) | 首次公开发布；统一版本号、提供 Docker Hub 镜像和 GitHub Release 二进制自动发布。 |

在 218 上提交版本变更后，执行 `./scripts/release.sh v1.0.0`。脚本构建并推送 `liuxuzxx/flink-cdc-rs:v1.0.0-<10 位提交号>`，再将发布提交和 Git 标签推送到 `liuxuzxx/binlog-cdc`；GitHub Actions 根据同一标签构建 Linux x86_64 二进制并上传到 [Releases](https://github.com/liuxuzxx/binlog-cdc/releases)。运行前需在 218 完成 Docker Hub 登录，并确保 Git 可以向这个 GitHub 仓库推送。后续版本更新两个 crate 版本号、新增 `docs/releases/vX.Y.Z.md` 并更新此表。
