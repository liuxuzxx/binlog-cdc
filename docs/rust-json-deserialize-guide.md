# Rust 反序列化如何选型：从固定 Struct 到动态配置

很多人第一次在 Rust 中读取 YAML，都会觉得这件事非常简单：配置里有什么字段，就在 `struct` 里定义什么字段，然后调用一次 `serde_yaml::from_str`。

这个判断没有错。

真正麻烦的是，配置通常不会永远停留在最初的样子。随着业务演进，我们会陆续遇到这些情况：

- 有些字段可以不填写；
- 有些字段缺失时需要使用默认值；
- YAML 字段名不符合 Rust 的命名习惯；
- 一个配置对象中又包含多个子对象；
- 同一个位置会因为 `type` 不同而出现完全不同的字段；
- 甚至在解析之前，我们根本不知道数据会是什么结构。

这篇文章不直接从最复杂的写法开始，而是让一份配置逐步“长大”：从几个固定字段开始，一直走到 Flink CDC 风格的多态配置。每增加一种复杂度，就选择一种恰好够用的反序列化方式。

文中的示例使用 `serde` 和 `serde_yaml`：

```toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
```

基本的读取代码始终只有两步：先读取文本，再反序列化。

```rust
let content = std::fs::read_to_string("application.yaml")?;
let config: AppConfig = serde_yaml::from_str(&content)?;
```

真正需要选择的，不是这一行 API，而是用什么 Rust 类型表达 YAML 的结构。

---

## 第一阶段：字段固定，直接映射成 Struct

先从最简单的配置开始：字段数量固定，每个字段也都是 `String`、`i32`、`i64`、`bool` 这样的简单类型。

```yaml
name: binlog-reader
port: 8080
worker_count: 4
checkpoint: 128000
enabled: true
```

这种配置不需要任何高级技巧，按照字段逐一建立结构体即可：

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AppConfig {
    name: String,
    port: i32,
    worker_count: i32,
    checkpoint: i64,
    enabled: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string("application.yaml")?;
    let config: AppConfig = serde_yaml::from_str(&yaml)?;

    println!("{config:#?}");
    Ok(())
}
```

Serde 会按照字段名和字段类型完成映射：

```text
AppConfig {
    name: "binlog-reader",
    port: 8080,
    worker_count: 4,
    checkpoint: 128000,
    enabled: true,
}
```

此时有两个重要特征：

1. YAML 中的字段是确定的；
2. 每个字段的类型也是确定的。

只要这两个前提成立，固定 `struct` 就是最清晰、最安全的选择。不要因为以后“也许会变化”，过早使用 `HashMap` 或 `serde_yaml::Value`。

### 类型不一致会怎样

Serde 的类型检查很严格。假设 Rust 中的 `checkpoint` 是 `i64`，YAML 却写成普通字符串：

```yaml
checkpoint: "latest"
```

反序列化会直接失败。Serde 不会猜测这个字符串应该如何转换。

反过来，如果结构体声明的是 `String`，下面两个值也不是一回事：

```yaml
server_id: 100
server_id_text: "100"
```

前者是整数，后者才是字符串。强类型检查会让错误尽早停留在配置加载阶段，这正是使用固定结构体的价值。

---

## 第二阶段：可选字段、默认值与字段重命名

真实配置很快就会遇到第二层复杂度：有些字段可以不写，有些字段不写时应该自动使用默认值，字段名也未必符合 Rust 的命名规范。

例如：

```yaml
sourceName: mysql-source
propertiesName: binlog
properties.source.name: mysql.orders
retryTimes: 3
description: 主库订单表
```

这里出现了三类情况：

- `description` 可以缺失；
- `timeoutSeconds` 没有配置时，希望默认使用 30；
- `sourceName`、`propertiesName` 使用驼峰命名，`properties.source.name` 中还包含点号。

### 用 Option 表示“可以缺失”

如果一个字段确实可以不存在，就使用 `Option<T>`：

```rust
#[derive(Debug, Deserialize)]
struct SourceProperties {
    description: Option<String>,
}
```

YAML 中存在该字段时得到 `Some(value)`，不存在时得到 `None`。

`Option` 表达的是业务语义上的“可选”，不应该为了避免反序列化报错而把所有字段都包成 `Option`。例如 `sourceName` 是系统运行必需的信息，就应该继续使用 `String`；缺少时，让配置加载直接失败。

### 用 default 处理默认值

如果字段缺失时需要一个确定的值，可以使用 `#[serde(default)]`：

```rust
#[derive(Debug, Deserialize)]
struct RetryConfig {
    #[serde(default)]
    retry_times: i32,
}
```

这里会使用 `i32::default()`，也就是 `0`。如果业务默认值不是类型默认值，可以提供函数：

```rust
fn default_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
struct RetryConfig {
    #[serde(default = "default_timeout_seconds")]
    timeout_seconds: u64,
}
```

这三种写法的语义并不相同：

| Rust 定义 | YAML 缺少字段时 | 适用场景 |
|---|---|---|
| `name: String` | 反序列化失败 | 必填字段 |
| `name: Option<String>` | 得到 `None` | 业务允许缺失 |
| `#[serde(default)] name: String` | 得到空字符串 | 缺失时使用类型默认值 |
| `#[serde(default = "...")]` | 使用业务默认值 | 具有明确默认规则 |

### 用 rename 处理特殊字段名

Rust 字段一般使用 `snake_case`，但外部配置可能使用驼峰、连字符，甚至包含点号。

针对单个字段，可以使用 `rename`：

```rust
#[derive(Debug, Deserialize)]
struct SourceProperties {
    #[serde(rename = "properties.source.name")]
    properties_source_name: String,

    #[serde(rename = "server-id")]
    server_id: String,
}
```

针对整个结构体的驼峰字段，可以使用 `rename_all`：

```rust
fn default_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourceProperties {
    source_name: String,       // sourceName
    properties_name: String,   // propertiesName

    #[serde(rename = "properties.source.name")]
    properties_source_name: String,

    retry_times: i32,          // retryTimes

    #[serde(default = "default_timeout_seconds")]
    timeout_seconds: u64,      // timeoutSeconds，不填写时为 30

    description: Option<String>,
}
```

常见命名策略包括：

```rust
#[serde(rename_all = "camelCase")]
#[serde(rename_all = "kebab-case")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
```

选择原则很简单：Rust 内部保持统一的 `snake_case`，外部字段如何命名，交给 Serde 适配。

如果需要兼容旧字段名，还可以使用 `alias`：

```rust
#[derive(Debug, Deserialize)]
struct AppConfig {
    #[serde(alias = "workerCount")]
    worker_count: i32,
}
```

它可以同时接收 `worker_count` 和历史字段 `workerCount`，适合配置迁移期间使用。

---

## 第三阶段：Struct 中继续包含 Struct

当配置项越来越多，把所有字段平铺在一个结构体中会变得难以维护。YAML 通常会自然地分成 `source`、`sink` 和 `pipeline` 等区块：

```yaml
source:
  hostname: 127.0.0.1
  port: 3306
  username: example_user

sink:
  bootstrap_servers: 127.0.0.1:9092
  topic: order-events

pipeline:
  parallelism: 6
  capacity: 1000
```

YAML 中的嵌套对象，可以直接映射成嵌套结构体：

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct CdcConfig {
    source: SourceConfig,
    sink: SinkConfig,
    pipeline: PipelineConfig,
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    hostname: String,
    port: u32,
    username: example_user
}

#[derive(Debug, Deserialize)]
struct SinkConfig {
    bootstrap_servers: String,
    topic: String,
}

#[derive(Debug, Deserialize)]
struct PipelineConfig {
    parallelism: u32,
    capacity: usize,
}
```

这一阶段看起来只是“结构体套结构体”，但它解决了一个重要问题：不同职责的配置开始拥有自己的类型边界。

- `SourceConfig` 只关心数据源；
- `SinkConfig` 只关心输出端；
- `PipelineConfig` 只关心流水线参数。

如果整个 `pipeline` 区块都允许缺失，可以把它声明成：

```rust
pipeline: Option<PipelineConfig>
```

如果区块允许缺失，但希望缺失时创建默认配置，可以让 `PipelineConfig` 实现 `Default`，再使用 `#[serde(default)]`。

到这里为止，不管结构嵌套多少层，只要每个位置的类型都是确定的，普通 `struct` 依然足够。

---

## 第四阶段：Type 不同，字段集合也不同

真正棘手的情况，是同一个配置位置可能代表不同类型的对象。

Flink CDC 风格的数据源配置就是一个典型例子：

```yaml
source:
  type: mysql
  name: mysql-source
  hostname: 127.0.0.1
  port: 3306
  username: example_user
  password: example_password
  tables: mydb.order_*
  server-id: "5400-5404"
  scan.startup.mode: specific-offset
```

如果 `type` 换成 Kafka，字段会完全不同：

```yaml
source:
  type: kafka
  name: kafka-source
  properties.bootstrap.servers: 127.0.0.1:9092
  properties.group.id: cdc-reader
  topic: order-events
```

再换成文件数据源，可能只剩下：

```yaml
source:
  type: mysqldump
  name: dump-source
  filepath: /data/orders.sql
```

它们都叫 `source`，但除 `type`、`name` 外，参数集合几乎没有共同点。

### 最直接的写法：一个超级 Struct

最容易想到的办法，是把所有类型的字段都放进同一个结构体。公共必填字段保持普通类型，只有部分类型需要的字段全部写成 `Option<T>`：

```rust
#[derive(Debug, Deserialize)]
struct SourceConfig {
    r#type: String,
    name: String,

    // MySQL 专用
    hostname: Option<String>,
    port: Option<u32>,
    username: example_user
    password: example_password
    tables: Option<String>,

    #[serde(rename = "server-id")]
    server_id: Option<String>,

    #[serde(rename = "scan.startup.mode")]
    startup_mode: Option<String>,

    // Kafka 专用
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_servers: Option<String>,

    #[serde(rename = "properties.group.id")]
    group_id: Option<String>,

    topic: Option<String>,

    // mysqldump 专用
    filepath: Option<String>,
}
```

业务代码再根据 `type` 自己判断：

```rust
match config.r#type.as_str() {
    "mysql" => {
        let hostname = config
            .hostname
            .as_deref()
            .expect("mysql source requires hostname");
        let port = config.port.unwrap_or(3306);
        connect_mysql(hostname, port);
    }
    "kafka" => {
        let servers = config
            .bootstrap_servers
            .as_deref()
            .expect("kafka source requires bootstrap servers");
        subscribe_kafka(servers);
    }
    _ => panic!("unsupported source type"),
}
```

这个方案能运行，而且在类型很少时也不难理解。问题会随着类型和字段数量增长逐渐暴露。

### 问题一：必填字段变成了可选字段

`hostname` 对 MySQL 来说是必填字段，对 Kafka 来说则根本不存在。超级结构体只能把它声明成 `Option<String>`。

结果是：缺少 `hostname` 的 MySQL 配置可以顺利通过反序列化，直到业务代码调用 `expect` 时才报错。错误从配置加载阶段被推迟到了运行阶段。

### 问题二：非法组合可以被表示

下面这个 Rust 对象完全可以通过编译：

```rust
SourceConfig {
    r#type: "mysql".to_string(),
    name: "orders".to_string(),
    hostname: None,
    bootstrap_servers: Some("127.0.0.1:9092".to_string()),
    // 其余字段省略
}
```

它声称自己是 MySQL，却没有 MySQL 地址，反而带着 Kafka 参数。类型系统无法阻止这个对象出现。

### 问题三：Type 只是一个字符串

`"mysql"`、`"kafka"` 都是普通字符串。拼写错误只能在运行时发现，每个使用配置的地方也都要重复编写字符串分支。

### 问题四：结构体会持续膨胀

增加 RocketMQ、PostgreSQL、Elasticsearch 后，结构体可能拥有几十个字段，而任意一个实例中的大部分字段永远都是 `None`。

这时问题已经不再是“Serde 注解怎么写”，而是“应该用什么类型表达多种互斥结构”。Rust 对这个问题有一个更准确的答案：`enum`。

---

## 第五阶段：使用 Enum + Tag 表达多态配置

Flink CDC 配置已经明确提供了 `type` 字段。Serde 可以直接根据这个字段选择枚举变体：

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Source {
    #[serde(rename = "mysql")]
    Mysql(MysqlSource),

    #[serde(rename = "kafka")]
    Kafka(KafkaSource),

    #[serde(rename = "mysqldump")]
    MysqlDump(MysqlDumpSource),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MysqlSource {
    name: String,
    hostname: String,
    port: u32,
    username: example_user
    password: example_password
    tables: String,

    #[serde(rename = "server-id")]
    server_id: String,

    #[serde(rename = "scan.startup.mode")]
    startup_mode: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaSource {
    name: String,

    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_servers: String,

    #[serde(rename = "properties.group.id")]
    group_id: String,

    topic: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MysqlDumpSource {
    name: String,
    filepath: String,
}
```

当 Serde 读到：

```yaml
type: mysql
```

它会选择 `Source::Mysql`，然后按照 `MysqlSource` 解析剩余字段。此时 `hostname` 又恢复成真正的必填字段，Kafka 配置也不再出现在 MySQL 类型中。

业务代码同样变得明确：

```rust
match source {
    Source::Mysql(mysql) => {
        connect_mysql(&mysql.hostname, mysql.port);
    }
    Source::Kafka(kafka) => {
        subscribe_kafka(&kafka.bootstrap_servers);
    }
    Source::MysqlDump(dump) => {
        read_dump_file(&dump.filepath);
    }
}
```

新增一种 Source 后，如果某个 `match` 没有处理新变体，编译器会直接指出遗漏。分支判断从运行时字符串比较回到了类型系统。

### 一个容易忽略的严格模式

Serde 默认会忽略结构体中没有定义的 YAML 字段。因此，使用枚举后，下面的 `properties.bootstrap.servers` 默认不会进入 `MysqlSource`，但也未必会报错：

```yaml
source:
  type: mysql
  hostname: 127.0.0.1
  properties.bootstrap.servers: 127.0.0.1:9092
```

如果配置需要严格校验，可以像上面的示例一样，在具体结构体上添加：

```rust
#[serde(deny_unknown_fields)]
```

这样，字段拼写错误或者不同类型的字段混用都会在加载配置时暴露。

是否开启严格模式取决于业务：

- 自己维护、要求尽早发现错误的配置，建议开启；
- 需要向前兼容、允许新旧版本字段共存的协议，需要谨慎开启。

---

## 第六阶段：Tag 不止一种写法

前面的 `#[serde(tag = "type")]` 称为内部标签。Serde 一共支持四种枚举表示方式。

### 1. 内部标签：Tag 与属性处于同一层

```rust
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Source {
    #[serde(rename = "mysql")]
    Mysql(MysqlSource),
    #[serde(rename = "kafka")]
    Kafka(KafkaSource),
}
```

对应 YAML：

```yaml
type: mysql
hostname: 127.0.0.1
port: 3306
```

它最符合 Flink CDC 这类配置的结构，也是本文场景的首选。

### 2. 相邻标签：Tag 与内容分开

```rust
#[derive(Deserialize)]
#[serde(tag = "type", content = "config")]
enum Source {
    Mysql(MysqlSource),
    Kafka(KafkaSource),
}
```

对应 YAML：

```yaml
type: Mysql
config:
  hostname: 127.0.0.1
  port: 3306
```

当协议本身已经把类型和内容分成两个字段时，使用相邻标签最自然。

### 3. 外部标签：变体名就是外层 Key

这是 Serde 枚举的默认形式：

```rust
#[derive(Deserialize)]
enum Event {
    Click { button: String },
    Scroll { distance: i32 },
}
```

对应 YAML：

```yaml
Click:
  button: submit
```

它适合“信封式”数据，但不符合 Flink CDC 的平铺风格。

### 4. 无标签：按照数据形状依次尝试

```rust
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigValue {
    Number(i64),
    Text(String),
    Range { start: i64, end: i64 },
}
```

对应数据可以是：

```yaml
100
```

也可以是：

```yaml
start: 100
end: 200
```

`untagged` 很灵活，但 Serde 需要按照变体顺序逐个尝试。当多个变体结构相似时，不仅解析成本更高，失败信息也不如明确的 Tag 清晰。

选择原则是：有可靠的类型字段就使用 Tag；只有协议无法提供类型字段时，才使用 `untagged`。

---

## 第七阶段：结构需要复用，但 YAML 不想增加层级

有时 Rust 类型希望复用已有结构，而 YAML 又要求所有字段保持平铺。这时可以使用 `flatten`。

例如，binlog 文件回放不仅需要文件路径，还需要一套 MySQL 连接信息：

```rust
#[derive(Debug, Deserialize)]
struct MysqlConnection {
    hostname: String,
    port: u32,
    username: example_user
    password: example_password
}

#[derive(Debug, Deserialize)]
struct MysqlBinlogFile {
    filepath: String,

    #[serde(flatten)]
    mysql: MysqlConnection,
}
```

YAML 不需要增加 `mysql:` 这一层：

```yaml
filepath: /data/mysql-bin.000730
hostname: 127.0.0.1
port: 3306
username: example_user
password: example_password
```

反序列化后，连接字段会被放进 `mysql: MysqlConnection`。这样既保持了 YAML 的兼容性，又让 Rust 代码拥有清晰的结构边界。

`flatten` 还可以把未知字段收集到 `HashMap`：

```rust
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct ConnectorConfig {
    name: String,

    #[serde(flatten)]
    properties: HashMap<String, serde_yaml::Value>,
}
```

这种写法适合“固定公共字段 + 一组透传属性”的插件配置。不过一旦进入 `HashMap`，其中字段就不再拥有编译期类型约束，应当把动态范围控制在必要的边界内。

需要注意，Serde 不支持把 `flatten` 与 `deny_unknown_fields` 组合成同一种严格校验策略：一个要接收额外字段，一个要拒绝额外字段，两者的目标本身就是相反的。

---

## 第八阶段：结构完全未知时使用 Value

如果数据结构在解析前完全未知，可以使用 `serde_yaml::Value`：

```rust
use serde_yaml::Value;

let value: Value = serde_yaml::from_str(yaml)?;

let source_type = value
    .get("source")
    .and_then(|source| source.get("type"))
    .and_then(Value::as_str);
```

它适合这些场景：

- 配置查看器或通用编辑器；
- 插件属性需要原样透传；
- 只读取未知文档中的少量字段；
- 需要先检查某个字段，再决定第二阶段解析类型。

代价也很明显：

- 字段拼错时编译器无法发现；
- 每次取值都要处理 `Option`；
- 类型错误会被推迟到业务代码；
- 重构字段时无法依靠编译器完成全局检查。

因此，`Value` 不应该因为“写起来快”就成为业务配置的默认方案。更稳妥的做法是：能确定的外层继续使用 `struct`，只有确实动态的局部才使用 `Value`。

例如：

```rust
#[derive(Debug, Deserialize)]
struct PluginConfig {
    name: String,
    plugin_type: String,
    properties: serde_yaml::Value,
}
```

---

## 第九阶段：注解无法表达时，手写 Deserialize

极少数情况下，选择类型的规则并不是简单地读取 `type`，而是需要组合多个字段、兼容历史格式，或者根据字段值执行转换。这时可以手写 `Deserialize`。

```rust
use serde::{Deserialize, Deserializer};
use serde::de::Error;
use serde_yaml::Value;

enum VersionedConfig {
    V1(V1Config),
    V2(V2Config),
}

impl<'de> Deserialize<'de> for VersionedConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let version = value
            .get("version")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("missing version"))?;

        match version {
            "1" => serde_yaml::from_value(value)
                .map(VersionedConfig::V1)
                .map_err(D::Error::custom),
            "2" => serde_yaml::from_value(value)
                .map(VersionedConfig::V2)
                .map_err(D::Error::custom),
            other => Err(D::Error::custom(format!(
                "unsupported version: {other}"
            ))),
        }
    }
}
```

手写反序列化拥有最大的控制力，也意味着更多代码、更多错误分支和更高的测试成本。

在决定手写之前，建议依次确认：

1. 普通 `struct` 能不能表达？
2. `rename`、`default`、`flatten` 能不能解决？
3. `enum + tag` 能不能完成分派？
4. `untagged` 是否已经足够？

只有这些方式都无法准确表达协议时，才值得接管完整的反序列化过程。

---

## 最后：根据配置的复杂度选择类型

回顾整条演进路线：

```text
固定简单字段
    ↓
可选字段、默认值和特殊命名
    ↓
嵌套 Struct
    ↓
多种类型塞进超级 Struct
    ↓
发现必填约束、非法组合和字符串分派问题
    ↓
使用 Enum + Tag 建模
    ↓
按需使用 Flatten、Untagged、Value 或手写 Deserialize
```

最终可以归纳成这张选型表：

| 配置特征 | 推荐方案 |
|---|---|
| 字段与类型完全固定 | `struct` + `#[derive(Deserialize)]` |
| 字段允许缺失 | `Option<T>` |
| 缺失时使用默认值 | `#[serde(default)]` 或自定义默认函数 |
| YAML 与 Rust 字段名不同 | `rename` / `rename_all` / `alias` |
| 一个对象中包含多个固定对象 | 嵌套 `struct` |
| 根据 `type` 决定字段集合 | `enum + tag` |
| Rust 中需要复用结构，YAML 仍需平铺 | `flatten` |
| 没有类型字段，只能按结构判断 | `untagged` |
| 局部或整体结构无法预先确定 | `serde_yaml::Value` |
| 分派和兼容规则无法通过注解表达 | 手写 `Deserialize` |

Serde 的高级能力很多，但选型原则并不复杂：**配置有多确定，Rust 类型就应该有多确定。**

字段固定时，普通 `struct` 已经是最好的答案；出现多种互斥结构时，让 `enum` 接管分支；只有真正动态的部分，才退到 `Value` 或手写解析。

反序列化的目标不只是“把 YAML 读进来”，而是让数据一旦解析成功，就已经满足程序对结构和类型的基本要求。能在配置加载阶段发现的问题，就不要留给业务运行阶段。

---

*文中的 Flink CDC 配置和类型设计取自 `flink-cdc-rs` 工程，示例基于 Serde 1.x 与 serde_yaml 0.9。*
