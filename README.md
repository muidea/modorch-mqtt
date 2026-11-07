# modorch-mqtt

基于 Modorch-RS 0.0.3 的可嵌入式 MQTT Broker 库。当前版本为 **0.1.0-alpha.1 / MQTT 3.1.1 QoS0/1 实验子集**，对应设计 F0–F3；尚不声明完整 MQTT 3.1.1 符合性。

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
| Pub/Sub | QoS0/1、SUBSCRIBE/UNSUBSCRIBE、重叠订阅按最高匹配 QoS 去重 |
| 内存 Session | Clean Session、Session Present、ClientID 接管、离线 QoS1、Inflight 重连 DUP/Packet ID 保留 |
| Retained / Will | 保留值替换/删除/回放；异常断开、KeepAlive、接管触发 Will；正常 DISCONNECT 清除 |
| 框架生命周期 | 全体 ready 后 listen；激活失败补偿；任务配额拒绝；检查式停机；Core 重建保留内存状态 |
| 资源限制 | 连接/Session/订阅/报文/积压条数及字节/Will/全局记账预算；满额时整次可靠发布拒绝 |
| 内部交互 | 通过宿主 Hub 的类型化状态 Request；业务消息入站/出站桥接在 F4 实现 |

当前仅支持显式匿名的回环地址开发模式。`Default` 不允许匿名；必须显式选择 `loopback_development()` 或设置 `anonymous_access`。凭据认证、TLS、公网监听、持久 Storage、QoS2、MQTT5、WebSocket、Admin/Bridge/Metrics 扩展尚未实现；相关能力不会以空 Feature 或成功回执伪装支持。QoS2 订阅条目返回失败，QoS2 PUBLISH/Will 被拒绝。

查看[配置与运行边界](docs/configuration.md)、[可靠性说明](docs/reliability.md)、[契约与测试映射](contracts/README.md)和[本轮验证记录](docs/validation/2026-11-07-f0-f3/README.md)。完整设计仍维护后续阶段的目标，不能作为当前能力清单。

```bash
./scripts/check.sh
# 可选的第二种独立客户端验证：在虚拟环境中安装 scripts/requirements-interop.txt 后执行
cargo build -p standalone-broker --locked
python scripts/verify-paho.py
```
