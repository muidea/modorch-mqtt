# modorch-mqtt

基于 Modorch-RS 0.0.3 的可嵌入式 MQTT Broker 库。当前版本为 **0.1.0-alpha.1 / MQTT 3.1.1 QoS0/1/2 实验实现**，已实现 F0–F3 子集、F4 EventHub 集成及 F5 的认证/运维与 SQLite 持久化实验扩展，以及 F6 双向 QoS2 状态机和可选 MQTT5/WebSocket/Bridge 功能；尚不声明完整 MQTT 3.1.1 符合性。

当前实现、最近测试结论及归档范围见[最终验证与收口记录](docs/validation/README.md)。完整规范与生产发布仍待独立验收。

宿主拥有 Tokio、Application、EventHub 和退出机制。Bundle 只注册 Factory，宿主显式启用组件；Core 不创建第二个 Application/Hub，监听和每个连接均由宿主 TaskScheduler 监督。

```rust
use modorch::{ApplicationBuilder, ComponentSelection, ShutdownOptions};
use modorch_mqtt::{MqttBrokerBundle, MqttBrokerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = ApplicationBuilder::new("existing-host");
    let mqtt = MqttBrokerBundle::new(MqttBrokerConfig::loopback_development())
        .register(&mut builder)?;
    let mut selection = ComponentSelection::new();
    for id in mqtt.module_ids() {
        selection.enable_module(id);
    }
    let app = builder.selection(selection).build()?.start().await?;
    println!("{}", mqtt.handle().local_addr().unwrap());
    tokio::signal::ctrl_c().await?;
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    Ok(())
}
```

依赖 `modorch` 时须使用与本项目完全相同的 Git 来源和 revision：

```toml
[dependencies]
modorch = { git = "https://github.com/muidea/modorch-rs", rev = "336ba20196044e1b81fb2ca297a18b5e258a5800" }
modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

Rust >=1.85 / Edition 2024。首次构建需要获取依赖，之后可使用 `--locked --offline`。

```bash
cargo run -p standalone-broker --locked -- 127.0.0.1:1883
cargo run -p existing-modorch-server --locked -- --once
```

| 当前支持 | 范围 |
|---|---|
| MQTT TCP | CONNECT/CONNACK、PING、DISCONNECT；增量解码及包大小/超时限制 |
| Pub/Sub | QoS0/1/2、SUBSCRIBE/UNSUBSCRIBE、重叠订阅按最高匹配 QoS 去重 |
| 内存 Session | Clean Session、Session Present、ClientID 接管、离线 QoS1/2、Inflight 重连 DUP/Packet ID 保留及 PUBREL 阶段恢复 |
| Retained / Will | 保留值替换/删除/回放；异常断开、KeepAlive、接管触发 Will；正常 DISCONNECT 清除 |
| 框架生命周期 | 全体 ready 后 listen；激活失败补偿；任务配额拒绝；检查式停机；Core 重建保留内存状态 |
| 资源限制 | 连接/Session/订阅/报文/积压条数及字节/Will/全局记账预算；满额时整次可靠发布拒绝 |
| 安全扩展 | 可选密码/ACL Provider、稳定会话归属、Will 预授权、TLS、公网监听安全校验、按 IP 握手限流 |
| 运维扩展 | 可选 Admin 的有界分页/统计/连接断开/内存审计；Metrics 周期采样与固定标签 Prometheus 文本 |
| 持久化扩展 | 可选 SQLite FULL 事务、writer fencing/CAS、提交查询幂等、预监听分页恢复及持久成功回执 |
| 内部交互 | 同宿主 Hub 的 MqttInbound 事件、显式发布 Request、分级接纳回执、来源隔离的幂等与状态查询 |

## 扩展与运行边界

`Default` 要求宿主显式选择 Auth Provider；回环开发可使用 `loopback_development()`。非回环监听必须同时配置 Auth 和 TLS。Facade 的 `auth/tls/admin/metrics/storage/toml/mqtt5/websocket/bridge/bootstrap/audit/http` Feature 均默认关闭；编译后仍须注册、配置并显式启用 Module。

- 认证、ACL、TLS、Admin、Metrics 和动态策略/证书更新见[安全与运维接入](docs/security-and-operations.md)。
- SQLite 持久化接入见[持久化说明](docs/storage.md)。可靠状态和内部幂等账本先提交磁盘，再返回 `DurableCommitted`；未配置时为 `MemoryOnly`，均不代表客户端已接收或业务 exactly-once。当前写 format5，兼容读 format1–4；旧 format2 多 Release 拒绝歧义升级，禁止原地降级。这是有界全量快照实现，尚无生产吞吐或硬件断电验收。
- 双向 QoS2、重复报文处理和持久恢复见[QoS2 协议说明](docs/qos2.md)。停机 Will 允许在重启时收尾。
- MQTT5、WebSocket/WSS、网络 Bridge、Tokio 宿主适配、持久审计与 HTTP 管理见[可选功能](docs/optional-features.md)。Bridge 每方向一个在途，源 Session 丢失后须等待旧目标工作完成才重建订阅；新订阅前的发布需由源业务保留或重发。
- Core TOML 严格拒绝未知/不支持字段，默认禁用 Core，不自动注册 Provider；详见[TOML 配置](docs/toml.md)。

```bash
cargo run -p durable-broker --locked -- /tmp/mqtt-broker.sqlite 127.0.0.1:1883
cargo run -p secure-modorch-server --locked -- --demo --once
cargo run -p standalone-broker --locked -- --config examples/standalone-broker/mqtt.toml
```

## 开发与验证

```bash
./scripts/check.sh
# 可选的独立客户端验证：在虚拟环境中安装 scripts/requirements-interop.txt
cargo build -p standalone-broker -p secure-modorch-server --locked
python3 scripts/verify-paho.py
python3 scripts/verify-paho-tls.py
# MQTT5 / WebSocket 验证须先构建对应 Feature
cargo build -p standalone-broker --locked --features modorch-mqtt/mqtt5,modorch-mqtt/websocket
python3 scripts/verify-paho5.py
```

[项目开发 Skill](.agents/skills/modorch-mqtt-development/SKILL.md) 当前为 1.0.5，绑定 Modorch 0.0.3 / Skill Bundle 1.0.7。Linux CI 定义见 [check.yml](.github/workflows/check.yml)；定义存在不代表远端已执行。

本地测试提供限定场景的 `TargetedEvidence`。完整 MQTT3.1.1/5.0 符合性与生产发布仍为 **Partial**，178 项发布义务尚无独立评审批准；`--allow-partial` 仅用于开发清单检查。发布流程见[发布证据说明](docs/release.md)，待验收项见[剩余需求清单](docs/remaining-work.md)。短 loopback 测量不能替代生产负载、长期 RSS、高负载退出/恢复 SLO 或硬件断电验证。

## 文档与证据

- 接入与实现：[开始使用](docs/getting-started.md)、[架构](docs/architecture.md)、[API](docs/api.md)、[EventHub](docs/eventhub.md)。
- 配置与运行：[配置边界](docs/configuration.md)、[可靠性](docs/reliability.md)、[运维](docs/operations.md)。
- 需求与映射：[完整设计](docs/modorch_mqtt_broker_full_design_0.0.3.md)、[契约与测试映射](contracts/README.md)、[MQTT3.1.1 场景](docs/mqtt311-conformance.md)、[报文与状态审计](docs/mqtt311-audit.md)。设计包含后续目标，当前能力以实现说明和剩余清单为准。
- 持续收口：[F6/F7 待收口计划](docs/f6-f7-closure-plan.md)，跟踪语义审查、验证补齐和独立验收的任务、交付物及完成标准。
- 验证结论：[最终验证与收口记录](docs/validation/README.md)。归档只保留最终结论，过程日志、数据库、探针和未批准包不随源码归档；严格验收仍须真实同轮材料。

[协议完整性收口](docs/protocol-integrity.md)记录QoS跨段义务、MQTT5断开/Will与取消订阅修复及规范表回归；完整F6/F7独立验收仍Partial。

组合宿主可在注册前通过 `MqttBrokerBundle::with_failure_policy(FailurePolicy::FailFast)` 显式选择组件监督；省略保留默认Restart，连接级错误仍隔离。完整边界见[API说明](docs/api.md#宿主监督策略)。
