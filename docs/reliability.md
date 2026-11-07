# 当前可靠性与协议边界

本轮是 MQTT 3.1.1 QoS0/1 实验子集。QoS2 未实现，所以不声明完整 MQTT 3.1.1 支持。没有磁盘持久化，也没有跨系统 exactly-once。

QoS1 入站先固定匹配目标并计算交付 QoS，为全部可靠目标和 Retained 变更预留容量，再在共享 MemoryStore 的同一临界区原子提交，之后才允许 PUBACK。任一目标积压或全局预算不足，整次发布在接纳前失败，发布连接关闭且不返回 PUBACK；其他目标和 Retained 都不部分更新。其他 Topic/独立发布继续处理。QoS0 目标独立 best effort，可在满队列时丢弃并计数。

每个 Session 的 Pending 与 Inflight 使用同一条数/字节账本，在线转离线不重置。Writer 队列满不会删除已接纳 QoS1 消息。PUBACK 回收对应 Packet ID 和容量；重连保留 Packet ID、按原顺序以 DUP 重发未确认消息。不在活动连接内按定时器重发未确认 QoS1，重发发生于持久会话重连。发布方因 ACK 丢失重传可能再次路由，符合 QoS1 允许重复的范围；不使用跨连接 Packet ID 做全局去重。

同一 Factory 持有共享状态。Core 监督重建排空旧任务，推进 generation，再复用全部 Session/订阅/Retained/Inflight/待发 Will；旧 generation 和 connection ID 无权修改新状态。Module teardown 只释放该代资源。停机后本地只读 Handle 仍可持有状态，释放 Application/Factory/Handle 的最后句柄后回收。新 Application、进程重启及断电不保留内存状态。

Clean Session=true 的 Session 在断开时删除；false 的 Session 保留，首期不自动过期。允许同一匿名域使用同 ClientID 接管；检查新 CONNECT 的资源和保留命名空间必须先于旧连接失效，失败不抢断旧连接。凭据/身份归属功能留待 Auth 阶段，不把匿名 Session 当安全隔离边界。

Will 在接受 CONNECT 前验证 Topic、QoS0/1、保留命名空间及资源预算。异常关闭、KeepAlive 超时、同 ClientID 接管触发；正常 DISCONNECT 移除。已触发 Will 固定目标快照，目标无可靠容量时保留有界待发工作并重试，受已接受 Will 记录额度约束。显式 Clean Session 清理使旧 Session epoch 失效后，不向其旧记录投递。没有订阅者且非 Retained 的 Will 发布后即完成。

宿主停机先取消任务，各连接实际退出时由 RAII 处理 Session/Will 并归还内存和连接许可，quiesce 等待真实资源退出。整体停机可能使在线 clean-session 订阅者先退出，所以不保证每个停机 Will 立即送达所有原连接；仍有效的离线持久 Session、Retained 或待发记录保留于当前内存状态。不要将检查式停机理解成持久化保存，销毁当前 Application 后这些保证结束。

EventHub 状态 Request 是内部控制接口，不是 MQTT 消息交付 ACK。F4 的 MqttInbound/显式 MqttPublishRequest 尚未实现；外部 PUBLISH 不自动操纵内部 EventHub Topic，也不把事件邮箱 Admission 转成 PUBACK。
