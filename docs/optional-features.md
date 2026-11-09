# 可选功能接入与交付边界

本轮实现见 [验证结论摘要](validation/README.md#implementation)。所有功能默认关闭；Cargo Feature、配置、Factory 注册和 Selection 分别显式处理。这里记录可运行的实验能力，完整规范符合性和生产发布仍需独立验收。

后续组合边界修复见[验证结论摘要](validation/README.md#closed-issues)。

[验证结论摘要](validation/README.md#verification)修复R-17：共享组优先在线成员，全离线时将QoS1/2接纳到一个未过期持久成员的可靠积压。QoS0不离线积压；容量不足整次拒绝，不提前确认或改写Retained；所有者在接纳时固定，重连不重新分配已开始交换。订阅标识与RAP沿用目标语义，format5保存恢复不变。唯一成员、多个离线成员、在线优先、普通/共享合并、清理/过期、预算拒绝和SQLite均有定向回归。原审计反例保持原样。

## 普通 Tokio 宿主

Facade `bootstrap` 导出 `MqttBrokerHost`。宿主提供 Tokio Runtime；`start(config).await` 显式创建一个 Application，`start_with(builder, config, selection)` 复用传入的 Builder 并加入 Core 的显式选择。已有扩展仍须注册并选择。`broker()`、`application()` 返回当前应用能力；退出必须等待 `shutdown_checked(options).await`。失败保留实际 Build/Startup 错误、清理结果与 retry handle。Drop 不启动清理任务，不接管进程信号。

## MQTT 5

编译 `mqtt5` 并设置 `MqttBrokerConfig.mqtt5=true`，同一监听接受 3.1.1 与 5.0。TOML 写 `protocols=["3.1.1","5.0"]`；省略仍只启用 3.1.1。实现包括：

- Clean Start、Session Expiry、分配 ClientID、连接协商和对应的 CONNACK/SUBACK/UNSUBACK Reason Code；完整 QoS0/1/2 沿用 Core 所有权和持久事务。
- 入站 Receive Maximum使用`max_inbound_qos2_per_client`，QoS1/2共用连接额度，完整写出PUBACK/PUBCOMP才释放（PUBREC不释放）；超额发布在授权/路由/持久化前拒绝，控制FIFO后发送DISCONNECT 0x93。QoS0不占额度，重连重置网络额度；旧接收记录仍受Session持久额度约束，未知/重复PUBREL不增加额度，排队的旧PUBCOMP不能释放重用ID的新交换。网络记录单独计入全局预算，无持久格式变化。
- 客户端 Receive Maximum、Maximum Packet Size；超过下行大小的消息按目标丢弃，计入 `dropped_oversized`，不会永久阻塞该 Session。控制响应超过限制时关闭连接。
- Message Expiry、Payload Format、Content Type、Response Topic、Correlation Data、有序 User Properties；Topic Alias 解析后入库，别名只在当前连接内有效，入站上限32，出站发送完整 Topic。
- Subscription Identifier、No Local、Retain As Published、Retain Handling；`$share/group/filter` 在 MQTT5 下按组选择一个在线成员，组名禁止空值及通配。3.1.1 的 `$share` 继续是字面前缀。共享订阅不回放 Retained。
- Will Delay、Will Message Expiry、Disconnect with Will；延迟在 Session 到期时截断，到期前恢复 Session 取消尚未触发的延迟 Will。已到期或已冻结目标、等待可靠容量的 Will 独立保留，恢复/接管不取消，释放容量后继续投递；SQLite 重启保留相同边界，见[验证结论摘要](validation/README.md#closed-issues)。

原始属性在第三方解码前检查长度、方向、单值属性重复、保留位、UTF-8 和非法取值；CONNECT 密码保留二进制。用户名密码可接已有 Auth Provider。Enhanced Authentication/AUTH 不是当前支持的认证机制，不接受声明 Authentication Method 的连接；不声明完整 MQTT5 符合性。

`MqttMessage`、`MqttPublishRequest` 和 `MqttInbound` 新增 `properties: MqttProperties`。Rust 结构体字面量须显式补 `properties: Default::default()`；旧序列化数据缺字段时使用默认值。`expires_at_ms` 是绝对 Unix 毫秒，重试不能重算以延长过期时间。Subscription Identifiers 由 Broker 按目标填充，内部发布请求不能自行指定。

User Properties最多128个普通条目，另为`modorch.bridge.origin`与`modorch.bridge.hops`各保留一个条目；重复保留键拒绝，普通键仍可重复。Core与Bridge通过API常量共享保留键，所有条目继续计入报文大小与内存预算。Session Expiry=0只在断开后结束Session；旧连接仍在线时，Clean Start=false接管保留原订阅及可靠交换。

MQTT5 Session/订阅选项、消息属性与延迟 Will 写入 format5。恢复仍先校验、归一化、提交，再 listen。在线 Session 崩溃恢复的过期起点采用最后快照的秒级时间，离线 Session 保留原绝对期限；停机时间计入过期，保守误差最多一个快照时间粒度。运行中 MQTT5 快照时间每秒变化，因此持久模式有至多每秒一次的维护检查点，须计入容量测量。时钟语义依赖宿主可靠的墙上时钟。

持久Session只能使用原协议恢复。跨3.1.1/5.0且不清理的CONNECT返回拒绝，原Session、订阅和QoS所有权保持；切换协议须显式Clean Session/Clean Start新建Session，Session Present=0。过期Session按正常维护清理后可新建。

format5持久保存Retained发布来源ClientID或内部发布标记，来源字节纳入预算。No Local同时过滤实时消息与Retained回放，包含QoS0/1/2、Will和3.1.1发布来源；过滤按ClientID，与连接或Session代际无关。读取format1–4时来源标为LegacyUnknown，普通订阅仍回放，No Local订阅保守跳过未知来源消息；重新发布后采用新来源。不能从旧格式重建已经丢失的发布身份，不能原地降级。

混合监听恢复按消息实际属性选择最小可表示的协议编码，空属性允许3.1.1原始包长边界；真实源属性仍验证并计入包长，目标新增的Subscription Identifiers按下行限额处理。这不改变已协商MQTT5下行超限丢弃策略。

## WebSocket

编译 `websocket`，设置 `config.websocket=Some(WebSocketConfig { path:"/mqtt".into() })`。TOML：

```toml
[mqtt.websocket]
enabled = true
path = "/mqtt"
```

入口严格匹配路径和 `mqtt` 子协议，只接受 Binary 数据，支持 MQTT 报文跨消息分片及合并，处理 Ping/Pong、Mask 和 WebSocket 分片。升级与 TLS 共用绝对握手期限，帧、消息、写缓存及连接预算均有界。配置 TLS 即为 WSS。每个 Core 当前只有一个监听；启用 WebSocket 后该监听执行 HTTP Upgrade。多个独立 Core 必须使用不同组件名和订阅 ID，各有独立 Session 状态。

## 网络 Bridge

编译 Facade `bridge`，使用 `bridge::{BridgeFactory,BridgeConfig,BridgeLink,BridgeEndpoint}`。Factory 名称是稳定的 Module/客户端身份；注册后显式选择，并声明现有 Core 依赖。每个 Link 表示一个方向，源/目标均为显式 MQTT5 TCP 地址和独立凭据；双向配置两条 Link。Core 不依赖具体 Bridge crate，也不自动外发业务 Event。

源/目标 Prefix 必须是非空、无通配且以 `/` 结尾的字面前缀。Bridge 订阅 `source_prefix#`，将剩余后缀接到目标 Prefix。两侧 Broker 按各自账号执行 ACL；只给对应订阅/发布范围的权限。业务进程的出站仍走带可信 Handler source 的 `MqttPublishRequest`，不会把任意 Event 当成可出网消息。

每方向两个 MQTT 客户端、容量2的请求通道、一个在途转发；源 Receive Maximum 为1。目标 PUBACK 成功后才提交源 PUBACK。远端断网时持有一个有界消息，其余可靠积压留在源 Broker；源须配置持久 Session，跨进程恢复能力还取决于源 Broker 的 Storage。目标拒绝按配置间隔重试；连接失败独立退避，不阻塞其他 Link；订阅被ACL/额度拒绝时关闭原连接、按同一退避重新认证和订阅。尚未开始发送的 Message Expiry 会扣除等待时间。

目标重连返回Session Present=false时，由Bridge重新提交仍持有的Transfer；Session仍存在时沿用客户端的原Packet ID交换并设置DUP，避免另建一份在途。每侧未完成poll跨调度保留，另一侧进度不取消其握手/写入；已解析的PUBACK优先于重连处理，ACK后立即断线也能释放源所有权。映射后的消息仍受目标报文/策略上限约束；目标拒绝时保持源所有权并按既定策略重试，不静默删除属性或提前确认源。

保留 User Properties 中 `modorch.bridge.origin` 和 `modorch.bridge.hops`，过滤回到同一 Origin、重复/非法元数据和超 Hop 消息。它们是路由环路控制，不是身份凭据。重新启动前后保持 Bridge 名称、Link 顺序及映射稳定，避免遗留会话订阅。

源Session代际与Transfer绑定。源CONNACK的Session Present=0会隔离所有旧Session的ACK；旧目标工作继续，完成后跳过旧源确认，再订阅新Session。Session Present=1保留原代际，DUP消息沿用原目标交换。断线时再次清理客户端请求队列，避免此前清理后排入的ACK跨重连发送；源重连未确定Session前不排ACK。异常额外可靠投递使连接重试，保持源所有权，不计作QoS0丢弃。

这保持每方向一个在途；新Session订阅等待旧工作完成，新订阅建立前的发布不能依赖已经丢失的Session积压，需由源业务保留/重发。修复不会重建源Broker已经丢失的消息。限定回归与公开API复测见[验证结论摘要](validation/README.md#closed-issues)。

目标接纳与源确认之间崩溃可能重复投递，语义为至少一次；QoS0 在 Link 忙时允许丢弃。计数通过 `BridgeHandle::status()` 读取。停机取消并等待全部受管 Link 释放套接字，不宣称断网时已排空全部源积压。Bridge 当前使用 TCP；需要链路加密时由宿主显式部署可信 TLS 代理。

## 策略与证书更新

`PasswordAuthFactory::handle()` 返回应用内 `PasswordAuthHandle`；`replace(new_config)` 完整校验并原子替换策略，名称、Domain、订阅 ID 不变，revision 严格递增。每个授权决策使用一个完整策略快照。 密码Provider固定Handler并发上限8、4个密码请求准入及1个物理Argon2任务；密码等待不占满轻量授权容量，超额返回Unavailable。额度持有至实际CPU退出，Running请求超时不能释放；操作开始时固定revision，Core仍逐连接等待授权。旧 Principal 的后续授权拒绝；已接纳可靠交换和预授权 Will 保留原接纳语义。

`BrokerHandle::reload_tls(tls).await` 使用当前 Owner 的 blocking-I/O Scheduler 有界读取 PEM。成功只更新后续握手；现有 TLS 连接继续工作。失败或代际过期保留原证书，不把文件校验失败升级为组件故障。Factory 的配置不是任意热重载接口，其他资源参数仍重新装配。

## 持久审计与 HTTP

Facade `audit` 启用 `AdminFactory::with_audit_log(SqliteAuditConfig)`。使用独立数据库、SQLite FULL/DELETE、排他 writer、物理页与行数上限。管理操作先提交 intent，再转发，最后写 outcome；拒绝/非法请求也持久记录。失败关闭管理准入；有 intent 无 outcome 表示结果未知。日志不记录 ClientID/Topic/令牌，滚动保留窗口会删除旧行，不提供无限历史。备份、保留与审计数据库路径由宿主负责。

Facade `http` 导出独立 `HttpFactory`；显式配置回环监听、32..256字节 Bearer token、并发和绝对期限。Admin 的 `allowed_callers` 必须允许该 HTTP Module；Core 继续只信任 Admin 的转发。远端管理使用宿主的可信 TLS 反向代理。

| 方法/路径 | 参数与行为 |
|---|---|
| GET `/v1/stats` | BrokerStatus JSON |
| GET `/v1/clients` | `after`、`limit`，有界分页 |
| GET `/v1/session` | `client_id` |
| GET `/v1/subscriptions` | `client_id`、`after`、`limit` |
| POST `/v1/disconnect` | `client_id`、`connection_id`；代际连接 fencing |
| GET `/metrics` | 显式绑定 MetricsHandle 的 Prometheus 文本 |

查询参数按 URL 编码；单请求后关闭连接，不接受请求体、Transfer-Encoding、重复/未知参数。Header 限8192字节/32项，分页1..256，JSON编码不分配中间Value树，响应上限1MiB，超限返回413并要求减小分页。503 可能意味着未执行或结果不确定；条件断连应携原 connection_id 核对，不能推断回滚。TOML 不含 Bearer/Bridge 凭据、Provider内部策略或审计数据库配置，由宿主构建这些 Factory。


Topic Alias恢复区分入站线帧与展开后的存储消息：只要合法完整源帧或别名消息帧/绑定帧都可在max_packet_bytes内表示，即可恢复；完整Topic和真实属性仍保存、校验并完整计费。别名只在原连接内有效。MQTT5下行按完整Topic及目标/监听上限在编码前判限，超限丢弃计数；3.1.1连接层超限拒绝策略不变。原8191字节反例、真实属性8192字节边界与SQLite双向QoS2/Retained验证见[验证结论摘要](validation/README.md#closed-issues)。

协议处理补充：MQTT5合法RL=1 DISCONNECT按省略Property Length处理，只有Normal Disconnection取消Will；其他合法客户端原因仍按延迟规则触发Will，Server专用原因按异常输入关闭。重复UNSUBSCRIBE按顺序生成0/0x11原因，已接纳交换保留。原格式5及Feature/API边界不变。规范表、正文索引和验证见[协议完整性](protocol-integrity.md)。
