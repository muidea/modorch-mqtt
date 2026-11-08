# 安全与运维接入（实验版本）

本轮实现 F5 中的 Auth、Admin、Metrics，并加入 TLS 和按 IP 的连接准入限制。未配置 Storage 时，回执为 `MemoryOnly`，进程退出丢失状态。可选 SQLite 持久化扩展另见 [storage.md](storage.md)；本节的原始验证记录保留当时的内存实现范围。

## 显式装配

Facade 默认 Feature 为空，基础 Core/内存功能始终提供。按需启用 `auth`、`tls`、`admin`、`metrics`；Feature 只开放实现和类型，不会自动注册或启用 Module。Core 不依赖扩展实现 crate，只依赖 `modorch-mqtt-api` 的 DTO。

```toml
modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt", features = ["auth", "tls", "admin", "metrics"] }
```

可编译的完整装配见 [secure-modorch-server](../examples/secure-modorch-server/src/main.rs)。宿主分别注册 PasswordAuthFactory、Core Bundle、AdminFactory、MetricsFactory，并显式选择四个组件。Core 的 `auth.component` 声明对 Provider 的 required 依赖；Admin/Metrics 声明依赖 Core，不形成环。Bundle 仍然只注册 Core。

| 配置点 | 必要约束 |
|---|---|
| `MqttBrokerConfig.auth` | Provider 组件 ID、正超时（≤60s）；不能与匿名模式同时选择 |
| `MqttBrokerConfig.tls` | PEM 证书链/私钥路径，各≤1MiB；Cargo `tls` Feature 必须开启 |
| 非回环监听 | 必须同时有 Auth、TLS，禁止匿名 |
| `MqttBrokerConfig.admin` | Core 管理订阅 ID、可信 Admin 组件来源列表；缺省不安装端点 |
| `AdminConfig.allowed_callers` | 可信业务 Handler 组件精确列表；默认空，全部拒绝 |
| 所有订阅 ID | Application 全局唯一；默认 Core `[1,2,3]`，示例 Core Admin `4`、Auth `1000`、Admin `2000` |

## 密码、ACL 与会话归属

`PasswordAuthConfig.users` 使用 Argon2id v19 PHC 字符串，不接受明文密码配置；成本限制为 m=19456..65536 KiB、t=2..4、p=1..4，输出至少32字节。密码验证运行在宿主 `spawn_cpu`，受宿主配额约束，Handler 等待 `wait_finished`。配置最多1024用户和1024内部来源，各用户最多256条发布规则、256条订阅规则、256个 ClientID 前缀；配置过滤器最多1024字节。任务/邮箱满额和错误/超时均拒绝，不回退为匿名。凭据 DTO 与用户配置的 Debug 脱敏；会话、观察和审计不保存凭据。

用户以 `domain + subject` 构成稳定 PrincipalKey，`username` 可与 subject 分离。Core 独立执行已有 Session owner 的相等校验；Provider 即使错误地允许跨身份，也不能清除/恢复/接管旧会话。Provider 还需允许对应 ClientID 前缀。恢复订阅逐条重新授权，异步授权期间的 Session 修改使版本 CAS 失败，避免用旧快照接管新状态。匿名域不能被 Auth 自动认领。

发布规则检查 Topic、原发布 QoS、Retain 权限；订阅检查请求过滤器的完整语言被某一配置规则包含，不能把 `secure/#` 扩大成 `#`。规则不推断多条 ACL 的并集；根通配不覆盖 `$`，空层按 MQTT 规则处理。没有匹配规则默认拒绝。Core 对 `$modorch` 的保留命名空间限制独立执行。

CONNECT Will 在成功 CONNACK、抢占旧连接和安装之前授权，保存绑定主体、Topic、QoS、Retain、用途与策略版本的 Grant。失败不会影响旧连接；已授权 Will 在连接过期/异常断开后仍按原 Grant 执行。策略可通过 PasswordAuthHandle 原子替换，新revision拒绝旧Principal的后续授权；已安装Will沿用接纳时Grant，TTL或策略替换不能撤销它。

Principal TTL 默认1h，可配置1s..24h，到期关闭空闲或活动连接。TLS 握手、CONNECT、认证和恢复订阅授权共享同一个绝对握手期限（默认5s），逐包 ACL 另受 Provider 超时、身份有效期及当前写期限约束。Argon2 在调试构建/高负载时可能触发此期限，部署时应按实测 CPU 成本和接入负载配置。

内部发布需要独立 `InternalPolicy`，来源为框架 ComponentRef 的 Display 字符串，如 `Module:business`。Provider 仅接受 `trusted_brokers`（默认 `mqtt-broker`）的 Handler 转述，业务方不能靠 DTO/Header 伪造来源。内部发布默认无规则即拒绝；这不替代宿主对业务 Handler 自身入口的鉴权。详见 [EventHub 接入](eventhub.md)。

## TLS 与连接准入

TLS 使用 rustls/ring 的显式 Provider，不修改进程全局加密配置；不开启 early data。证书加载通过宿主 blocking-I/O Scheduler 完成并等待任务退出，错误不包含 PEM 内容。TLS 握手在完成连接/内存/IP 准入后执行，受取消和握手绝对期限控制。每连接额外预留256KiB逻辑内存，TLS写缓冲限64KiB；逻辑预留不是全进程 RSS 硬上限。不提供双向证书身份认证或证书热更新。

默认每 IP 同时32连接、60s内100次准入，最多1024个 IP 桶，每桶记账192字节。桶满拒绝新 IP；过期且无活动连接的桶回收。此计数是固定窗口，统计被准入的 TCP 尝试；使用 socket peer IP，不解析代理 Header。NAT 后的客户端共享额度。全局连接/内存/任务上限仍同时生效，限流拒绝不重启 Core。

```bash
# 显式演示模式：固定 device/demo-password，只监听回环随机端口
cargo run -p secure-modorch-server --locked -- --demo --once
# 实际装配：必须自行提供证书、私钥和 PHC 文件
cargo run -p secure-modorch-server --locked -- cert-chain.pem private-key.pem password.phc 0.0.0.0:8883
```

示例账号固定为 `device`，ClientID 必须以 `device-` 开头，仅允许 `devices/#`。演示/测试使用仓库中公开的 localhost 证书与私钥（`crates/modorch-mqtt-core/tests/fixtures`），只供测试；正常启动不会默认加载它们。测试证书覆盖 localhost/127.0.0.1，客户端仍验证证书。示例不创建管理 HTTP 入口。

## Admin

业务 Handler 向 `ADMIN_TOPIC`（`$modorch/mqtt/admin/v1`）的 Admin 组件发送直接 `Request<AdminRequest, AdminResult>`。Admin 验证框架来源、精确 Destination 和消息类型，再向 Core 的 `ADMIN_CONTROL_TOPIC` 转发。Core 另验 Admin 来源及当前代际；外部根、Event/Command 和伪造 Header 均不能操作。

支持 ListClients、GetSession、ListSubscriptions、BrokerStats、DisconnectClient。页长1..256，使用排他的词典序游标，结果是实时视图，不是跨页一致快照。响应不含密码或消息 payload。断连须带 GetSession 观察到的 `expected_connection_id`；超时后重复旧请求不能断开新连接。断连遵循异常断开 Will 语义。

`AdminHandle.audit()` 读取默认1024条（可配1..10000）内存审计环，跨同 Factory 的 Module 重建保留。记录时间、框架来源、固定操作名及成功标志，不含 ClientID/Topic/凭据/payload。失败与不确定执行均为 success=false，不能推断操作已回滚；等待超时语义继承 EventHub。审计不是磁盘日志，进程退出丢失。

## Metrics

`MetricsFactory` 使用一个宿主受管周期任务，通过 `external()` 读取 Core 状态；没有逐消息 Hub 广播，也不依赖业务订阅。任务取消后等待真实退出，配额准入失败参与整体启动补偿。

`MetricsHandle.snapshot()` 返回最近成功采样的时间戳、BrokerStatus 与累计失败数；首次成功前为 None。`prometheus()` 输出36条固定名字的数值，不含 ClientID/Topic 标签，首次采样前为空。连接/会话/订阅、可靠 Pending/Inflight/字节、QoS0队列、内存、收发消息/明文I/O字节、拒绝/超时/慢写、重连、Will、观察/幂等与限流均可查看。另含接收端 QoS2 交换条数/字节和存储提交成功/失败数、耗时微秒总和与最大值；计时从取得事务 gate 后到确认/失败，不包含排队时间，也不是纯 SQLite fsync 时间。I/O 字节不含 TLS 或内核开销。宿主选择并保护自己的 HTTP exporter；本库不默认监听额外端口。

## 当前验证与后续范围

执行记录及 loopback 性能基线见 [安全与运维验证](validation/2026-11-07-f5-security-ops/README.md)。独立 Paho 客户端脚本验证证书、密码、QoS1、ACL 拒绝和检查式退出。性能脚本为 Python 标准库实现，记录1/10/100/500客户端、QoS0/1、128字节、自订阅 PUB→SUB 延迟及采样 RSS；不包含 TLS/Auth、磁盘恢复和生产负载门禁。

F5 已补充 SQLite FULL 有界全量快照、提交查询幂等、离线订阅预监听恢复及 SIGKILL/磁盘满故障验证，见 [Storage 验证](validation/2026-11-07-f5-storage/README.md)。F5 的生产性能、真实断电/跨平台恢复和发布门禁仍未验收；F6 双向 QoS2 已实现并有独立客户端/故障验证，见 [QoS2 说明](qos2.md)；完整3.1.1符合性、F7 MQTT5、WebSocket、Bridge 仍未完成。`storage` Feature 提供真实 Provider 实现，未提供 `mqtt5` 空 Feature。


## TLS/Auth/SQLite与QoS2测量

先构建对应release示例，再运行可选组合；默认TCP/QoS0/1参数继续兼容：

```bash
cargo build --release -p standalone-broker -p secure-modorch-server --locked
python3 scripts/benchmark.py --clients 1 10 --messages 20 --qos 0 1 2 \
  --compositions tcp tls-auth tls-auth-storage --output /tmp/mqtt-baseline.json
```

每个场景新建临时SQLite库并在检查式停止后清理，使用明确的loopback demo证书/账号，保持32连接/IP限制。报告含连接建立时间（TLS/密码认证包含在内）、稳态PUB→SUB延迟、双向完整交换吞吐、RSS原始采样、消息延迟原始样本、二进制摘要、配置和SQLite文件大小。QoS2等待上行PUBCOMP及下行PUBREL并发送PUBCOMP后才继续；PUB→SUB延迟采样点在消息体到达，不等待下行释放。采样器每10ms读取Linux `/proc`，操作系统调度可能使间隔更长；RSS不是Core逻辑预算。

停止计时在全部交换与客户端清理完成之后，不能代表高负载退出。启动/订阅不计入稳态吞吐，Argon2认证并非每条消息重复；ACL/持久提交包含在运行路径内。此处是实验测量方法，生产阈值、长运行、慢消费者、恢复峰值与SLO审查仍待执行；按[发布门禁](release.md)保留未批准状态。

动态策略/证书、持久审计、独立HTTP管理与Bridge装配见[可选功能](optional-features.md)。这些入口仍由宿主显式注册和选择。
