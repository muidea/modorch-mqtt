# MQTT 3.1.1 QoS2（实验实现）

当前支持网络发布、订阅、内部发布、Will 和 Retained 的 QoS2。核心双向状态机及恢复已实现，完整 MQTT 3.1.1 符合性矩阵、生产性能和跨平台门禁仍未验收，不据此声明完整协议认证。显式选装方式保持兼容，无需开启新的 QoS2 Feature。ClientID 上限现在至少23字节，旧配置不足23时拒绝启动。

## 接收端：PUBLISH 到 PUBCOMP

采用 [MQTT 3.1.1 §4.3.3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) 的 Method B。第一次 PUBLISH 先为接收记录和所有可靠目标/Retained 预留容量，原子路由并保存以 Packet ID 为键的接收状态，再回复 PUBREC。此时 Broker 已承担后续交付，不等待 PUBREL 才匹配目标。

| 输入/状态 | 行为 |
|---|---|
| 新 PUBLISH / ID 未占用 | 校验身份/ACL和容量，路由一次，保存 AwaitPubRel，回复 PUBREC |
| QoS2 PUBLISH / 相同 ID 已占用 | 忽略载荷，只重发 PUBREC；不重新路由、授权、修改 Retained 或生成观察 |
| PUBREL / AwaitPubRel | 删除接收记录，回复 PUBCOMP，不再路由 |
| 重复或未知 PUBREL | 回复 PUBCOMP，不建立新状态 |
| 完成后的新 PUBLISH / 相同 ID | 作为新交换处理 |

重复以 Packet ID 识别，即使内容变化，也不覆盖原来已接纳的消息（MQTT-4.3.3-2）；这里与业务 `request_id` 的异内容冲突语义不同。复用未完成的 QoS2 ID 发送 QoS1 是协议错误，只关闭该连接。连接身份仍须有效，断线重连仍须完成 owner 和会话授权；已接纳交换的重传不撤销原来的发布承诺。

`max_inbound_qos2_per_client` 默认128，范围1..65535；`max_inbound_qos2_bytes_per_client` 默认16MiB，每条按 `256 + topic.len() + payload.len()` 计入该额度和全局预算。接收记录保存原消息，便于持久恢复和额度核对。条数/字节或可靠目标空间不足时，在路由/PUBREC前拒绝，连接关闭，不部分交付。已有 ID 的重传在额度满时仍可完成。记录直到 PUBREL 或会话按协议清理，不自动过期或淘汰。

## 发送端：PUBLISH 到 PUBCOMP

下行沿用有界可靠 Pending/Inflight 队列和 Packet ID 分配器，目标 QoS 仍取匹配订阅最大 QoS 与原发布 QoS 的较小值。

| 当前阶段 | 输入 | 行为 |
|---|---|---|
| 等待 PUBREC | PUBREC | 保存 Release 阶段，发送 PUBREL |
| Release | 重复 PUBREC | 重发 PUBREL |
| Release 且已选择过 PUBREL 发送 | PUBCOMP | 删除交付，释放 Packet ID 和积压空间 |
| 等待 PUBREC | PUBACK / 过早 PUBCOMP | 不完成 QoS2，不释放 ID |
| 未确认 PUBLISH 后重连 | — | 使用原 ID，DUP=1 重发 PUBLISH |
| Release 后重连 | — | 使用原 ID，只重发 PUBREL |

PUBREL 的固定 flags 始终为0010（`0x62`），不使用 PUBLISH 的 DUP 位。ACK 包严格校验固定 flags、非零 ID、精确长度；未知 PUBREC/PUBCOMP 不会删除其他交付。上下行 ID 空间独立，可以同时使用相同数值。

按 MQTT-4.6.0-4，每个有效 PUBREC（含重复）在持久确认后按到达顺序进入连接的有界控制 FIFO。写端阻塞时不能改按原 PUBLISH 顺序选取或合并重复回复；控制队列满则暂停消费新包。可靠队列把对应交付移到末尾，持久保存每个未完成 ID 最近一次 PUBREL 的顺序，未确认 PUBLISH 的相对顺序不变。

PUBREC 不释放可靠消息额度，PUBCOMP 才释放。PUBREL 控制帧优先完成已有交换，不因 PUBLISH 窗口已满而死锁；没有活动连接内的定时重传，恢复重发发生在持久会话重连。保留消息以目标实际 QoS 走相同握手，Will 仍在 CONNECT 前执行原发布 QoS/Retain ACL。

## 恢复与失败

Clean Session=false 保留双向交换；true 清理会话状态。内存模式保留范围为同 Factory 的 Core 监督重建。配置 [Storage](storage.md) 后，接收记录、发送阶段、Packet ID/ever_sent 和相关配额一并持久化。

持久模式下，接收记录及第一次路由确认后才发 PUBREC；清除接收记录确认后才发 PUBCOMP；下行阶段推进确认后才发 PUBREL；完成状态未确认时不会继续接纳下一项并假装成功。任何提交不确定都复用 Store 原 operation_id 查询，无法证实时关闭本代并恢复最后 committed image。测试逐个延迟/拒绝这四类提交，核对无提前回包/下行和重启状态。

当前快照格式为 format3，沿用 format2 字段结构并约定可靠队列中 Release 条目的顺序。新版读取旧 format1 的 QoS0/1 image；旧 format2 持久 Session 若至多一个未完成 Release，可升级。若有多个，旧数据未记录 PUBREC 到达顺序，新版返回 `legacy QoS2 release order` 并拒绝监听，保留原数据库；需在旧版本先排空这些交换，再停机升级。恢复及 normalization 确认后写入 format3；旧版拒绝 format3，不能直接降级。非法阶段/ID/配额或较小新预算使启动失败，不清空原数据库。数据库 Provider 协议不变。

BrokerStatus/Metrics 新增 `incoming_qos2`、`incoming_qos2_bytes`，和原有下行积压分开统计，Prometheus文本共36个固定名字，无 ClientID/Topic 标签。指标不持久化。

QoS2保证单个有效会话中协议交换的去重，不提供跨系统 exactly-once：下行目标请求较低 QoS 时遵守较低等级，业务观察仍为 best effort，订阅方在应用处理与协议 ACK 之间的事务由其负责。Clean Session 清除或创建新 Application 的内存模式不能延续旧交换。

[本轮验证](validation/2026-11-07-f6-qos2/README.md)记录原始命令和输出；[契约映射](../contracts/README.md)列出用例。完整设计尚余 MQTT5、WebSocket/Bridge、完整3.1.1符合性与生产/跨平台/发布门禁。

PUBREL 顺序修复及规范场景清单见[符合性推进记录](mqtt311-conformance.md)和[本次验证](validation/2026-11-07-f6-conformance/README.md)。
