# 业务模块与 MQTT 双向接入

当前实现使用宿主已有的 Application/EventHub。MQTT 客户端之间的投递由 Core 处理；业务观察通过类型化事件，业务出站通过显式 Request。

| 接口 | Topic / Destination | 类型及边界 |
|---|---|---|
| 网络入站观察 | 原 MQTT Topic / Broadcast | `MqttInbound`，接纳后 best effort，不等待业务完成 |
| 内部发布 | `$modorch/mqtt/control/publish` / Core Component | `MqttPublishRequest` → `MqttPublishResult` |
| 发布结果查询 | `$modorch/mqtt/control/publish/query` / 同一 Core | `MqttPublishQuery` → `MqttPublishQueryResult` |
| Broker 状态 | `$modorch/mqtt/control/status` / Core | `()` → `BrokerStatus` |

业务 Module 的 HandlerRegistration 可以订阅如 `demo/device/+` 的过滤器，再用 `message.payload::<MqttInbound>()` 读取 DTO。DTO 包含原 Topic、Bytes、QoS、retain、Packet ID、接收时间及 Core 标注的 broker/generation/connection_id/ClientID。Packet ID 只属于该连接，不能全局去重。该 DTO 对可信进程内代码公开可构造，不是身份认证凭据。

入站观察为每条消息使用 `external()` 建立独立根，原生 EventHub source/causation 不继承 Listener。所有权检查和 MQTT 接纳在 Core 状态边界执行，观察在锁外同步尝试准入；与之后的停机/重建并发时，事件中的 generation 标明原接纳代际。已经排进业务邮箱的事件不会被重建撤回。当前 Will 和内部发布不产生网络入站 DTO。

在业务 Handler 内发送出站 Request：

```rust
use modorch::{ComponentRef, Destination, FrameworkResult, HandlerContext, MessageOptions};
use modorch_mqtt::{MqttPublishRequest, MqttPublishResult, Qos, PUBLISH_TOPIC};
use std::time::Duration;

async fn send_command(ctx: &HandlerContext) -> FrameworkResult<MqttPublishResult> {
    let request = MqttPublishRequest {
        request_id: "device-001-command-42".into(),
        topic: "demo/device/001/command".into(),
        payload: b"start".as_slice().into(),
        qos: Qos::AtLeastOnce,
        retain: false,
    };
    let cost = 256 + request.request_id.len() + request.topic.len() + request.payload.len();
    let result = ctx.event_hub.request::<_, MqttPublishResult>(
        PUBLISH_TOPIC,
        Destination::Component(ComponentRef::module("mqtt-broker")),
        request,
        MessageOptions { payload_bytes: Some(cost), ..Default::default() },
    )?.wait(Duration::from_secs(1)).await?;
    Ok(result.as_ref().clone())
}
```

这里有两个结果层：外层 `FrameworkResult` 表示 Hub 准入/执行/等待是否成功；内层 `MqttPublishResult` 表示 Broker 是否原子接纳。成功回执 `accepted=true` 的 `durability` 在内存模式为 `MemoryOnly`、启用 Storage 且磁盘提交确认后为 `DurableCommitted`；二者均不保证订阅客户端已经收到或确认，QoS0 目标仍可因拥塞丢弃。内部发布支持 QoS0/1/2；非法/保留 Topic、超限报文和资源不足返回类型化拒绝。QoS2 网络重复报文不会再次产生观察事件，观察仍为 best effort。

出站必须具有框架填入的 `MessageEnvelope.source`，并使用 Core 的精确组件 Destination。当前 Modorch 只有 Handler 子调用具有该来源；生命周期钩子或 Scheduler task 可先发到业务 Handler，再由 Handler 发起发布。外部 `app.event_hub().external()` 直接请求 Core 会返回 `UntrustedSource`。Header 的 source 字符串不参与判断。宿主必须为业务 Handler 自身的公开入口制定鉴权规则。启用 Auth 时，内部发布还须通过 Provider 的 `InternalPolicy`（如来源 `Module:business`）；Core 以框架来源进行转述，Provider 只信任显式配置的 Core 来源。没有策略默认拒绝，拒绝不创建账本记录。具体装配见 [安全与运维接入](security-and-operations.md)。

不要把 `command()` 的邮箱接纳当作 BrokerAccepted；Core 对发布 Event/Command 均不执行状态变更。Core 默认只订阅三个控制 Topic（管理端点须另行配置），不订阅通配业务事件；普通业务 DTO、网络入站事件和伪造的 Header 不会自动外发。客户端订阅自己发布的 Topic 仍正常收到 MQTT 回送。

幂等与结果不确定时的处理：

1. 在首次发送前生成稳定 `request_id`，重试时保持 ID 和完整内容不变。
2. `wait()` 返回 `Indeterminate` 不能说明未提交。由同一业务组件向 `PUBLISH_QUERY_TOPIC` 发送 `MqttPublishQuery { request_id }`，等待 `MqttPublishQueryResult`。
3. 查询 `Ok(Some(Ok(receipt)))` 表示已接纳；`Ok(Some(Err(reason)))` 为已记录的路由拒绝；相同 ID 的重试只返回该终态。
4. `Ok(None)` 表示账本暂无记录，原请求仍可能排队。保持相同 ID/内容重试受幂等保护。换 ID 重试已拒绝操作应是明确的新业务决策。
5. 同源同 ID 改内容返回 `IdConflict`；不同组件来源独立。同一组件跨 Core 重建仍属于同一幂等空间。

账本只保留完成预留后的路由终态，不记录前置语法/来源/记录容量拒绝。默认最多 10000 条，逐条计入全局内存，不淘汰旧记录；满额 `LedgerFull`，旧记录仍可查询/重试。配置需覆盖预期实验运行量。没有 TTL 或在线清空。内存模式的新 Factory/进程不延续幂等保证；配置 Storage 后账本随事务持久化并跨进程恢复。同源同 ID 的既有终态在重新授权前返回，不重新匹配目标或执行发布；新 ID 仍受当前策略校验。详见 [持久化接入](storage.md)。

Core 的 `control_subscription_ids` 默认 `[1,2,3]`。Modorch 的订阅 ID 是全 Application 唯一，宿主应与业务和其他 Broker 一起统一分配。示例业务使用 `[100,101]`。

运行完整宿主示例：

```bash
cargo run -p existing-modorch-server --locked
# 另一终端用 MQTT 客户端订阅 demo/business/ready，可收到启动时的 retained 业务消息。
# 发布到 demo/device/001，宿主会打印类型化入站观察。
# 仅运行启动/接纳/检查式退出验证：
cargo run -p existing-modorch-server --locked -- --once
```

源码见 [existing-modorch-server](../examples/existing-modorch-server/src/main.rs)。[集成测试](../crates/modorch-mqtt-core/src/tests/eventhub.rs)以真实 TCP 客户端及两个业务模块覆盖双向接入、幂等、查询、无回环、重建和 Hub 背压；历史验证结论见 [验证结论摘要](validation/README.md#implementation)。
