# TOML 配置接入

Facade 的 `toml` Feature 默认关闭。它提供 `MqttTomlConfig`，将单独的 `[mqtt.*]` 文档转换为当前 Core 配置和显式组件选择。该接口不读取文件、不创建 Runtime/Application/Hub、不注册扩展 Factory、不启用组件或开放端口；宿主负责这些步骤。完整 MQTT3.1.1 符合性仍为 Partial，配置 `protocols = ["3.1.1"]` 不表示协议认证通过。

## 独立 Broker

```bash
cargo run -p standalone-broker --locked -- --config examples/standalone-broker/mqtt.toml
# 启动并执行检查式退出
cargo run -p standalone-broker --locked -- --config examples/standalone-broker/mqtt.toml --once
```

[开发配置](../examples/standalone-broker/mqtt.toml)显式允许匿名回环，默认端口1883可改为0。原有位置地址与 `--benchmark` 继续可用，`--config` 与二者互斥，避免静默覆盖文件限额。`--once` 可用于配置启动检查；禁用 Core 时不装配 MQTT 组件，直接退出。

独立示例仅注册 Core，Auth/Admin/Metrics引用会在 build 时因未注册组件失败；Storage还要求宿主注入 capability，独立示例不提供该注入；TLS要求宿主启用 `tls` Cargo Feature。需要 Auth/Storage/Admin/Metrics 时按下面的已有宿主方式装配，不会因配置文件出现这些名字而自动启用内置实现。

## 已有宿主

```toml
# 宿主 Cargo.toml，仍与本项目使用相同 Modorch git rev
modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt", features = ["toml"] }
```

宿主在启动前有界读取UTF-8文件，然后执行：

```rust,ignore
// input 为已读取的独立配置；config_dir 是配置文件所在目录。
// None 仅适用于未配置 Storage；持久模式传入宿主生成的随机 StoreAccessKey。
let plan = modorch_mqtt::MqttTomlConfig::parse(&input, config_dir, storage_access)?;
// 在此注册 TOML 引用的 Auth/Storage/Admin/Metrics Factory。
if let Some(mqtt) = plan.register(&mut builder)? {
    for id in mqtt.module_ids() {
        selected.enable_module(id);
    }
    // 保存 mqtt.handle() 所需的只读句柄，然后使用宿主原有 builder/selection 启动。
}
```

实际可编译与运行的完整扩展装配见 [config.rs](../crates/modorch-mqtt/tests/config.rs) 中 `explicit_host_factories_match_toml_refs_tls_and_prelisten_storage_recovery`。运行示例见 [standalone-broker](../examples/standalone-broker/src/main.rs)。`core_config()` 可只读检查，`into_core_config()` 用于自定义 Factory/监督策略；消费配置前须自行保存 `module_ids()`，后者包含所有声明启用的外部组件引用。

Provider注册、Cargo Feature、Core Provider引用和Selection仍分别处理。`auth/storage/admin/metrics` Feature 仅导出本项目内置实现；引用宿主的其他兼容 Provider 不要求编译这些内置 Feature。TLS 实现位于 Core，配置启用 TLS 则必须编译 `tls`。重复组件 ID、Core 控制订阅 ID 与 Admin Core端点冲突会被拒绝；扩展自身的订阅 ID 和宿主其他组件订阅 ID 仍由宿主统一分配，框架在 build/start 中继续校验。

## 配置合同

只接受一个 `[mqtt.*]` 文档，不混入宿主其他顶层配置。必须提供 `[mqtt.core]`；`enabled` 默认false，其余Core值默认与 Rust API 一致。省略安全配置不会默认开放匿名访问。启用 Core 后执行同一 `MqttBrokerConfig::validate()`，包括公网必须Auth+TLS、预算、超时、额度和溢出校验。Core禁用时不应用Core限额；仍检查文档结构、固定策略及扩展声明，禁止在Core禁用时单独启用扩展。

| 区域 | 支持字段及约束 |
|---|---|
| `mqtt.core` | 全部标量资源字段与 `control_subscription_ids`，名称同 `MqttBrokerConfig`；Duration使用 `handshake_timeout_ms`、`write_timeout_ms`、`connection_attempt_window_ms` 整数毫秒 |
| 固定Core策略 | `protocols`支持 `["3.1.1"]` 或 `["3.1.1","5.0"]`（后者要求 `mqtt5` Feature）；`connection_idle_ms=0`表示遵循MQTT KeepAlive；`reliable_overflow_policy="reject_publish"` |
| `mqtt.security` | `anonymous_access`默认false；`allow_public_listener_without_tls=false`、`reserved_internal_prefix="$modorch/"`、`session_owner_policy="same_principal"`不能更改 |
| `mqtt.eventhub` | 仅支持 `inbound_mode="best_effort"`、`inbound_topic_mode="original"`、`control_prefix="$modorch/mqtt/control/"`、`allow_implicit_outbound=false` |
| `mqtt.auth` | `enabled=true`时必须给 `component`；`timeout_ms`默认5000，1..60000；具体账号/ACL在宿主Provider配置中 |
| `mqtt.tls` | `enabled=true`时必须给非空 `certificate_chain`、`private_key`；相对路径以传入的配置目录为基准，绝对路径保持；Core setup通过受管IO加载PEM |
| `mqtt.storage` | 禁用仅允许 `enabled=false`、`mode="memory"`；启用必须 `mode="external"`、`component`和宿主传入非零 capability；可选上限与期限见下文 |
| `mqtt.admin` | `enabled=true`必须给 `component`、`control_subscription_id`；Core仅信任该Module转发；操作员白名单/审计额度由宿主的Admin配置决定 |
| `mqtt.metrics` | `enabled=true`必须给 `component`；采样周期、超时和HTTP出口属于宿主配置 |

Auth/TLS/Storage/Admin/Metrics/WebSocket默认禁用。禁用区块提供额外参数会报错，避免遗漏 `enabled=true` 后参数静默失效。未知字段、重复键/表、错误类型、不支持的策略和 Bridge 声明均报错，不降级成其他模式。不支持在线重载、环境变量插值、include合并、覆盖优先级或TOML内的密码/PHC/StoreAccessKey。

Storage可选字段为 `max_snapshot_bytes`（默认4MiB）、`recovery_page_bytes`（4096）、`timeout_ms`（2000）、`recovery_timeout_ms`（30000）。快照/恢复页/预算的具体边界沿用[存储说明](storage.md)。`recovery_page_max_records`与原拟议的 `recovery_page_max_bytes` 不属于当前按字节分页协议，不能混用。数据库路径、`disk_budget_bytes`、Provider订阅ID及实际后端配置由宿主设置；TOML接入不绕过物理容量门禁或预监听恢复。

例如，宿主另行注册 `my-auth` 与 `my-store`，共享随机 capability，并配置双方相同的快照上限：

```toml
[mqtt.core]
enabled = true
listener = "127.0.0.1:1883"
[mqtt.auth]
enabled = true
component = "my-auth"
timeout_ms = 5000
[mqtt.storage]
enabled = true
mode = "external"
component = "my-store"
max_snapshot_bytes = 4194304
recovery_page_bytes = 4096
timeout_ms = 2000
recovery_timeout_ms = 30000
```

## 诊断与验证

输入限64KiB，超限在解析前拒绝；宿主也应限制文件读取，独立示例最多读取上限加1字节。语法/结构错误只返回行号与字节列，语义错误返回静态原因，不转发TOML库附带原文的错误；配置计划的Debug也不打印配置内容或capability。该保护针对解析接口，宿主不能再把原文追加到日志。

`./scripts/check.sh`执行严格解析、限额/策略、脱敏、注册不启用、真实TCP QoS1和完整扩展装配测试，并运行 `scripts/verify-toml.py` 的独立子进程验证：拒绝配置/UTF-8/超限/参数冲突、QoS2 Retained双向握手、SIGINT检查式退出和端口归还。`cargo test -p modorch-mqtt --no-default-features --features toml --test config --locked` 另核对仅启用解析器而无TLS内置实现的边界。证据见[验证结论摘要](validation/README.md#implementation)。

`[mqtt.websocket]` 支持 `enabled=true` 与 `path="/mqtt"`，要求 `websocket` Feature；禁用时不得指定路径。TLS 与 WebSocket 组合为 WSS。Broker 的资源配置文件不提供任意在线重载；Auth/TLS 可由宿主调用显式更新 API，详见[可选功能](optional-features.md)。
