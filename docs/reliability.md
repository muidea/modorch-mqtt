# 当前可靠性与协议边界

本轮是 MQTT 3.1.1 QoS0/1/2 实验实现；双向 QoS2 状态机已接入，完整 MQTT 3.1.1 符合性矩阵尚未验收。可显式启用 SQLite 持久化，没有跨系统 exactly-once。下文内存行为适用于未配置 Storage 的实例，持久化差异见文末。

QoS1/2 入站先固定匹配目标并计算交付 QoS，为全部可靠目标和 Retained 变更预留容量，再在共享 MemoryStore 的同一临界区原子提交，之后才允许 PUBACK。任一目标积压或全局预算不足，整次发布在接纳前失败，发布连接关闭且不返回 PUBACK；其他目标和 Retained 都不部分更新。其他 Topic/独立发布继续处理。QoS0 目标独立 best effort，可在满队列时丢弃并计数。

每个 Session 的 Pending 与 Inflight 使用同一条数/字节账本，在线转离线不重置。Writer 队列满不会删除已接纳 QoS1 消息。PUBACK 回收对应 Packet ID 和容量；重连保留 Packet ID、按原顺序以 DUP 重发未确认消息。不在活动连接内按定时器重发未确认 QoS1，重发发生于持久会话重连。发布方因 ACK 丢失重传可能再次路由，符合 QoS1 允许重复的范围；不使用跨连接 Packet ID 做全局去重。

同一 Factory 持有共享状态。Core 监督重建排空旧任务，推进 generation，再复用全部 Session/订阅/Retained/Inflight/待发 Will；旧 generation 和 connection ID 无权修改新状态。Module teardown 只释放该代资源。停机后本地只读 Handle 仍可持有状态，释放 Application/Factory/Handle 的最后句柄后回收。新 Application、进程重启及断电不保留内存状态。

Clean Session=true 的 Session 在断开时删除；false 的 Session 保留，首期不自动过期。允许同一匿名域使用同 ClientID 接管；检查新 CONNECT 的资源和保留命名空间必须先于旧连接失效，失败不抢断旧连接。启用 Auth 后，Core 强制校验稳定 owner，认证 Provider 不能绕过；恢复订阅逐条重新授权，异步授权期间使用 Session 版本 CAS 拒绝竞态。Principal 到期关闭连接，包括空闲连接以及等待鉴权/存储的连接；已接纳可靠消息仍按原会话保证保留。

Will 在接受 CONNECT 前验证 Topic、QoS0/1/2、保留命名空间及资源预算；启用 Auth 时还验证发布/Retain 权限，绑定完整发布参数、Principal 及策略版本并保存 Grant。已授权 Will 在断开后按此授权执行，不因连接身份自然到期重新拒绝；策略热更新与撤销尚未提供。异常关闭、KeepAlive 超时、同 ClientID 接管触发；正常 DISCONNECT 移除。已触发 Will 固定目标快照，目标无可靠容量时保留有界待发工作并重试，受已接受 Will 记录额度约束。显式 Clean Session 清理使旧 Session epoch 失效后，不向其旧记录投递。没有订阅者且非 Retained 的 Will 发布后即完成。

宿主停机先取消任务，各连接实际退出时由 RAII 处理 Session/Will 并归还内存和连接许可，quiesce 等待真实资源退出。整体停机可能使在线 clean-session 订阅者先退出，所以不保证每个停机 Will 立即送达所有原连接；仍有效的离线持久 Session、Retained 或待发记录保留于当前内存状态。不要将检查式停机理解成持久化保存，销毁当前 Application 后这些保证结束。

F4 的 `MqttInbound` 在网络 PUBLISH 完成内存原子接纳后，以原 MQTT Topic 向同一 Hub 广播，使用 `external()` 新建根并检查 Core generation/连接所有权。不等待业务处理；无观察者、Topic 超 Hub 本地预算、Hub 字节/邮箱满和部分 fanout 拒绝只计数，不回滚 MQTT 路由或改变 PUBACK。事件标明接纳时的代际和连接；接纳后的关闭与独立观察可并发，已进入 Hub 的旧代事件不会因重建自动撤回。Will 与内部发布当前不生成网络入站观察事件。

内部发布必须由业务 Handler 使用 `ctx.event_hub.request` 发往 Core 的显式控制 Topic，并等待 `MqttPublishResult`。只接受框架生成的组件来源和直接组件 Destination；外部入口、生命周期钩子或独立 Scheduler task 没有 Handler 来源，不能直接发布。Event/Command 不执行发布，Command 的 Admission 仅表示邮箱接纳。业务 Handler 的入口鉴权仍由宿主负责。启用 Auth 时，内部发布额外按框架来源及 `InternalPolicy` 进行 ACL 校验；没有策略默认拒绝。Auth 拒绝不写入接纳账本。

内存模式回执承诺 `MemoryOnly`：全部可靠目标及 Retained 状态已原子接纳；QoS0 目标仍允许丢弃，回执不承诺客户端收到/确认。幂等键为 `(framework source ComponentRef, request_id)`，完整请求内容必须相同。终态记录跨 Core 重建保留，重试只返回原结果，不重新匹配目标、恢复已删除 Retained 或新增交付；路由拒绝也记录，资源释放后重试原 ID 仍返回原拒绝。校验或记录预留前的拒绝不占记录。

幂等账本固定有界且不淘汰，满时拒绝新 ID，旧 ID 仍可查询/重试。这一实验策略需要按业务量配置 `max_publish_records` 和全局内存预算；内存模式没有 TTL、在线清空或跨进程幂等保证。`RequestHandle.wait` 返回 `Indeterminate` 时须由同一来源查询或保持相同内容及 ID 重试；查询 `None` 不表示并发排队的原请求永远不会执行。新 Application/进程会丢失账本，不应跨进程崩溃推断安全重试。详见 [EventHub 接入](eventhub.md)。

## 连接期限与持久提交

同一个受管连接任务持续检查 CONNECT（包括调度排队/TLS）、KeepAlive、身份到期和当前整帧写入期限；鉴权、等待存储 gate 或提交回执期间同样有效。KeepAlive=0 不启用该计时器，Principal 的长有效期不会被截为24小时。写入期限从选定当前帧开始，到整帧完成才清除，输入 PING 或部分写进度不能重置它。

期限到达或任务收到取消时先关闭 TCP/TLS，并使连接令牌失效。尚未获得 gate 的等待可以取消；获得事务后必须确认提交结果再退出，不用 timeout 丢弃已开始的 durable promise。任务、连接许可及内存预留在实际完成前持续占用；因此网络关闭不等于资源已经归还。迟到的鉴权或 gate 结果不能继续接纳消息/会话。已接纳 QoS2 在确认提交后仍可恢复，重传原 Packet ID 不重复路由；超时关闭不发送迟到成功 ACK。

这些期限依赖执行器能够获得调度和短同步临界区正常执行，不是实时系统保证。等待后端时不并发解析新网络包；接收内核中尚未解析的 PING 不会延长期限。Will 的持久收尾也需等待事务 gate/提交，网络关闭与 Will 到达不能视为同一时刻。故障测试及限定范围见[验证结论摘要](validation/README.md#verification)。

## 启用 Storage 时

Core 通过独立 SQLite Module 的定向 Request 提交完整有界状态 image。事务 gate 串行协调协议、下行选择、Will 与内部发布，不跨 await 持有同步状态锁。CONNACK/SUBACK/PUBACK、下行数据和入站观察均等待相关提交确认；下行 Packet ID 与已发送标志先提交再写，重启按原 ID/DUP 重发。QoS0 交付仍为 best effort，不能把持久内部回执解释为 QoS0 下行队列可恢复。

提交丢回执时查询同 operation_id/digest，未知才重试同操作；仍不确定则关闭本代入口和连接，禁止 ACK。恢复只信最后 committed image，检查 epoch/version、分页偏移/进度/结束标记、摘要、身份引用和配额。离线订阅在 listen 前全部恢复；预算变小导致不满足时拒绝启动，不删已承诺数据。持久会话 owner、Will Grant 和同源业务幂等结果跨新 Application 恢复，成功内部回执为 `DurableCommitted`。

宿主停止时取消先于新 CPU/I/O 任务准入，Core 等待已有连接真实退出，不在 quiesce/teardown 新建关键磁盘提交。停止前已保存的 armed Will 在下次启动时收尾，并在监听前提交；因此停止导致的 Will 可延迟到重启发布。运行期正常 DISCONNECT 仍先持久清除 Will。SQLite 使用 FULL + DELETE journal；SIGKILL 恢复验证不等于已验证真实硬件断电或任意文件系统。详见 [持久化接入](storage.md)及 [验证结论摘要](validation/README.md#implementation)。

## QoS2 的协议边界

接收端采用 MQTT 3.1.1 §4.3.3 Method B：第一次 PUBLISH 原子路由并保存接收状态后返回 PUBREC，后续同 Packet ID 的 QoS2 PUBLISH 只重发 PUBREC，不处理其内容、不重新授权或观察。PUBREL 只清除接收状态并回复 PUBCOMP；重复/未知 PUBREL 仍回复 PUBCOMP。同 ID 在完成后可用于新的消息，跨方向的 Packet ID 空间独立。

发送端在原有可靠队列中增加 PUBREL 阶段。PUBREC 不释放消息或 Packet ID；重复 PUBREC 重发固定 flags=0010 的 PUBREL，PUBCOMP 才完成。重连时未收 PUBREC 的消息按原 ID、DUP=1 重发 PUBLISH；已收 PUBREC 的消息只重发 PUBREL。PUBACK 或过早的 PUBCOMP 不会删除 QoS2 消息。

接收端账本另受条数/字节和全局预算限制，满额在路由与 PUBREC 前拒绝。Clean Session=false 保留双向状态；true 清理。内存模式跨 Core 监督重建，Storage 模式跨进程恢复。协议成功 ACK/阶段推进仍等待持久提交；任一步存储失败均不能靠 ACK 掩盖。QoS2 约束单个持续会话中的 MQTT 交换，不保证业务事件或低 QoS 订阅者的跨系统 exactly-once。详见 [qos2.md](qos2.md)。


## 出站重试与慢消费者的限定故障证据

[验证结论摘要](validation/README.md#protocol)验证PUBLISH只写出前缀后被本地写超时关闭：内存持久Session仍保留QoS1/2所有权，新连接按原PacketID和DUP重试，直至相应ACK完成。QoS0不享有重连重传保证。此项使用确定性duplex流，不代表真实TCP/TLS或SQLite所有中断时序均已验证。

真实TCP慢消费者在AwaitPubComp阶段继续占用窗口和可靠积压，容量不足时新的可靠发布（包括Retained替换）整体拒绝，不发送成功PUBREC，不向其他目标部分投递，也不建立已成功接纳的入站QoS2记录。容量释放后同ID重试可正常接纳；其后重复PUBLISH不再次路由。有限状态正确性不等于生产背压/RSS/SLO验收。

## 宿主选择组件监督

Bundle支持with_failure_policy显式透传Core既有策略，默认仍Restart（3次/50ms）。统一FailFast的宿主按组件故障停止整个Application，保留失败并checked drain；非法客户端不会因此升级为组件故障。策略不改变既有发布幂等、未知结果或真实资源排空语义，见[API说明](api.md#宿主监督策略)。
