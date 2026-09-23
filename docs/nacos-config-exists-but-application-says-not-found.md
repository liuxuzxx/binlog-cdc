# Nacos 明明有配置，Spring Boot 为什么一直说不存在？一次零宽空格引发的排障

服务启动失败时，日志已经把答案写得很“明确”：

```text
APPLICATION FAILED TO START
Config data resource ... dataId='service-a.yml' ... does not exist
```

看到这里，大多数人的第一反应都是去 Nacos 控制台找配置：DataId 是否写错，Group 是否选错，Namespace 是否切错。

但这次故障恰恰不在这些地方。

Nacos 中的配置真实存在；容器内直接请求也能得到 `HTTP 200` 和非空正文。最终导致服务持续重启的，是配置文件里一行肉眼几乎看不见的字符：

```text
U+200B ZERO WIDTH SPACE
```

它的中文名称是“零宽空格”。屏幕上看起来是一行空白，YAML 解析器却把它当成了有效内容。

这篇文章完整还原本次排查过程：我们如何从服务重启、镜像回退一路排查到 Nacos，如何逐层排除网络、路由和配置作用域，最后又如何从原始 ZIP 中锁定这个不可见字符。为避免暴露实际环境信息，文中统一将故障服务称为“服务A”。

---

## 一、故障表象：升级后服务持续重启

问题最初表现为：下午升级后，服务一直处于重启状态。

排查这类问题时，第一步不能急着猜代码，而是先回答两个问题：

1. 容器是应用主动退出，还是被 Kubernetes 杀掉？
2. 重启是升级后才出现，还是升级后才被注意到？

我们通过 Loki 对服务A的启动时间线进行了核对。多个 Pod 连续出现 `APPLICATION FAILED TO START`，随后被 Kubernetes 重新拉起，形成持续重启。

这个现象只能说明应用没有完成 Spring Boot 启动，无法直接证明是镜像、资源、探针还是外部依赖导致。真正有价值的信息，仍然要从每次启动失败前的异常链中寻找。

---

## 二、从启动失败日志提取精确坐标

查询服务A的日志后，真正的启动失败信息出现了：

```text
APPLICATION FAILED TO START

Config data resource 'NacosConfigDataResource{...
namespace='example_namespace',
group='DEFAULT_GROUP',
dataId='service-a.yml',
optional=false ...}'
via location 'nacos:service-a.yml' does not exist
```

日志已经给出了应用实际使用的完整坐标：

| 项目 | 运行时取值 |
|---|---|
| Nacos 地址 | `<nacos-service>:8848` |
| Namespace | `example_namespace` |
| Group | `DEFAULT_GROUP` |
| DataId | `service-a.yml` |
| 是否允许缺失 | `false` |

源码中的导入配置也与日志一致：

```yaml
spring:
  cloud:
    nacos:
      config:
        server-addr: ${NACOS_SERVER:...}
        namespace: ${PROFILES_ACTIVE:example_dev}
        group: ${NACOS_GROUP:DEFAULT_GROUP}
  config:
    import:
      - nacos:service-common.yml
      - nacos:service-a.yml
      - nacos:service-database.yml
```

因此，下一步不再泛泛地问“Nacos 配置有没有”，而是验证这组精确坐标。

---

## 三、回退旧镜像：问题仍然存在

为了判断故障是否由升级镜像引入，现场将服务A回退到旧版本。

回退后，新的 Pod 仍然重复出现相同错误：

```text
service-a.yml does not exist
```

日志中至少出现了多个不同版本的 ReplicaSet：

```text
service-a-<new-revision>-*
service-a-<old-revision>-*
```

不同镜像版本得到相同错误，说明问题不应继续归因于本次业务代码变更。排查范围进一步收敛到外部配置和配置加载过程。

---

## 四、使用精确 URL 验证 Nacos 配置

Nacos 配置可以通过 OpenAPI 精确读取。这里的 `tenant` 对应 Namespace ID：

```bash
curl -i --connect-timeout 5 \
  'http://<nacos-service>:8848/nacos/v1/cs/configs?dataId=service-a.yml&group=DEFAULT_GROUP&tenant=example_namespace'
```

容器内执行结果为：

```text
HTTP/1.1 200
Config-Type: yaml
Content-MD5: <config-content-md5>
Last-Modified: <last-modified-time>
Content-Type: text/plain;charset=UTF-8
```

同时还使用 Nacos 单节点 IP 进行了验证：

```bash
curl -i --connect-timeout 5 \
  'http://<nacos-node-ip>:8848/nacos/v1/cs/configs?dataId=service-a.yml&group=DEFAULT_GROUP&tenant=example_namespace'
```

两个地址均返回 `HTTP 200`、相同的 MD5 和非空配置正文。现场也确认 Nacos 只有一个节点。

至此可以排除：

- Nacos 网络不通；
- Kubernetes Service 指向错误；
- Nacos多节点配置不一致；
- Namespace、Group 或 DataId 拼写错误；
- 配置正文为空。

但一个矛盾随之出现：既然配置可以正常读取，为什么应用仍然说它不存在？

---

## 五、关键转折：“does not exist”不一定真的不存在

继续查看 Spring Cloud Alibaba 的 Nacos ConfigData 加载逻辑，可以看到它大致执行了以下步骤：

```java
try {
    String content = configService.getConfig(dataId, group, timeout);
    return parseNacosData(content, suffix);
} catch (Exception exception) {
    if (!resource.isOptional()) {
        throw new ConfigDataResourceNotFoundException(resource, exception);
    }
}
```

这里最值得注意的不是 `getConfig()`，而是 `catch (Exception)`。

配置拉取、内容解析等步骤中的异常，都可能被包装为 `ConfigDataResourceNotFoundException`。Spring Boot 最终展示给用户的失败分析便是：

```text
Config data resource ... does not exist
```

所以，这条提示并不能严格证明“配置不存在”。它也可能表示：

- YAML 无法解析；
- 配置中存在重复键；
- 缩进或冒号错误；
- 出现非法字符；
- 配置加载器内部发生其他异常。

更麻烦的是，现场还存在 Log4j 配置异常：

```text
Unknown object "Async"
Unable to locate appender "ASYNC_NAMING"
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"
```

这使得原本应该输出的底层异常没有完整呈现，只剩下最外层、最容易误导人的“配置不存在”。

既然网络和配置坐标都已排除，下一步必须检查原始配置文件本身。

---

## 六、从运维原始 ZIP 开始校验

运维从 Nacos 导出了原始文件：

```text
nacos-export.zip
```

整个过程中没有经过人工解压、复制或重新保存，避免编辑器自动修复或改变不可见字符。

ZIP 中只有一个文件：

```text
DEFAULT_GROUP/service-a.yml
```

基础检查结果如下：

| 检查项 | 结果 |
|---|---|
| 配置正文大小 | 3158 字节 |
| 行数 | 131 行 |
| MD5 | 与 Nacos HTTP 响应头一致 |
| 编码 | UTF-8 |
| UTF-8 BOM | 无 |
| NUL 字节 | 无 |
| Tab 缩进 | 无 |
| 换行符 | LF |

文件 MD5 与 Nacos HTTP 响应头完全一致，证明检查的就是 Nacos 当前实际保存的内容，而不是另一个副本。

接下来使用带重复键检查的 YAML Loader 进行严格解析，结果为：

```text
yaml_parse ERROR
error_line 130
error_column 1
error_type ScannerError
error_problem could not find expected ':'
```

错误落在第 130 行第 1 列，但第 130 行本身看起来是正常的键。真正可疑的是它前面的第 129 行：

```text
line_129_length=1
has_colon=False
```

这行肉眼看起来是空行，却被程序判断为包含一个字符。

---

## 七、根因：第129行存在 U+200B 零宽空格

对第 129 行按 Unicode Code Point 检查后，结果为：

```text
line_129_repr = '\u200b'
codepoint = U+200B
```

`U+200B` 是零宽空格。它有几个很危险的特点：

- 在普通编辑器和 Nacos控制台中几乎不可见；
- 看起来像一行空白；
- 它不属于 YAML 认可的普通空白字符；
- YAML 解析器会把它视为一个实际的标量内容；
- 下一行出现新的顶层键时，解析器仍在等待上一行的冒号，因此错误位置落到了第 130 行。

这也解释了为什么错误信息是：

```text
could not find expected ':'
```

我们没有直接修改文件，而是在内存中模拟删除第 129 行，再次执行严格解析：

```text
remove_suspicious_line 129
yaml_parse OK
document_count 1
```

删除这一行后，整份 YAML 可以正常解析，并且没有发现后续语法错误或重复键。

至此，根因形成了完整证据闭环：

```text
Nacos 配置存在且非空
        ↓
Spring Nacos 客户端成功定位到 dataId
        ↓
YAML 第129行包含 U+200B
        ↓
配置解析抛出 ScannerError
        ↓
异常被包装成 ConfigDataResourceNotFoundException
        ↓
日志显示 service-a.yml does not exist
        ↓
Spring Boot 启动失败，Pod 持续重启
```

---

## 八、修复方式

修复本身很简单：

1. 打开 Nacos 中的 `service-a.yml`；
2. 删除第 129 行的 `U+200B`；
3. 最稳妥的方式是将第 128 行末尾到第 130 行开头之间的空行整体删除，再手工回车；
4. 重新发布配置；
5. 再次导出并执行 YAML 严格校验；
6. 重启或观察正在重启的服务A；
7. 确认日志出现 `Started`，且不再出现 `APPLICATION FAILED TO START`。

不要通过下面这种方式绕过：

```yaml
spring:
  config:
    import:
      - optional:nacos:service-a.yml
```

`service-a.yml` 是服务运行所需的正式配置。将其改为可选，只会让服务带着缺失配置继续启动，可能把启动期的显式失败变成运行期的隐蔽错误。

---

## 九、以后再遇到“配置不存在”，可以这样排查

### 第一步：从日志中抄下精确坐标

不要只在 Nacos 中搜索文件名，要同时确认：

```text
serverAddr + namespace + group + dataId
```

### 第二步：在应用容器内直接请求

```bash
curl -i --connect-timeout 5 \
  'http://<nacos-address>/nacos/v1/cs/configs?dataId=<dataId>&group=<group>&tenant=<namespaceId>'
```

这样可以一次性验证容器网络、DNS、Service 路由和配置作用域。

### 第三步：不要只看 HTTP 200

至少同时检查：

- 响应正文是否非空；
- `Content-MD5` 是否与导出文件一致；
- `Last-Modified` 是否符合预期；
- 配置类型是否正确。

只统计正文、不打印敏感配置，可以使用：

```bash
curl -sS '<config-url>' | python3 -c '
import sys, hashlib
b = sys.stdin.buffer.read()
print("bytes=", len(b),
      "nonWhitespace=", len(b"".join(b.split())),
      "md5=", hashlib.md5(b).hexdigest())
'
```

### 第四步：导出原始文件，不要复制粘贴

直接从 Nacos 导出 ZIP，可以保留原始字节，避免编辑器替换换行、编码或不可见字符。

### 第五步：做严格 YAML 校验

如果环境中安装了 `yq`：

```bash
yq eval '.' service-a.yml >/dev/null \
  && echo YAML_OK \
  || echo YAML_INVALID
```

### 第六步：检查不可见字符

下面的 Python 脚本可以定位零宽字符：

```python
from pathlib import Path

text = Path("service-a.yml").read_text(encoding="utf-8-sig")

for line_no, line in enumerate(text.splitlines(), 1):
    for column, char in enumerate(line, 1):
        if char in {"\u200b", "\u200c", "\u200d", "\ufeff"}:
            print(
                f"line={line_no}, column={column}, "
                f"codepoint=U+{ord(char):04X}"
            )
```

这类检查不会输出整份配置，适合处理可能包含数据库账号、密钥等敏感信息的配置文件。

---

## 十、这次排障真正值得复用的经验

这次问题的技术修复只是删除一个字符，但排查过程比修复本身更有价值。

第一，不要把最外层错误信息直接当成根因。`does not exist` 是异常包装后的结果，不一定意味着远端资源真的不存在。

第二，回退旧版本仍然失败，是一个非常有价值的差分实验。它快速把问题从“代码变更”收敛到了“外部配置和运行环境”。

第三，验证 Nacos 不能只看控制台。必须使用应用真实的 Namespace、Group、DataId，并从容器网络内直接请求。

第四，肉眼检查配置远远不够。零宽空格、BOM、Tab、特殊换行和重复键，都需要工具按字节和语法验证。

第五，日志系统本身也是排障基础设施。当前 Log4j Appender 配置异常遮蔽了底层解析异常，显著增加了定位成本。修复业务配置后，也应该单独修复日志配置，保证类似异常以后能够输出完整 Cause Chain。

当服务告诉你“Nacos 配置不存在”，而你明明能在控制台里看到它时，不妨先问一句：

> 它是真的不存在，还是存在，但无法被正确解析？

很多时候，答案就藏在一个看不见的字符里。
