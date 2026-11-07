# 当前配置与运行边界

本轮通过 Rust 的 `MqttBrokerConfig` 配置；完整设计中的 TOML、Feature、Auth/Storage 等扩展仍是后续目标。本库不读取进程环境或宿主配置文件。

默认监听 `127.0.0.1:1883`，端口 0 可用于测试。`loopback_development()` 显式允许匿名；普通 `Default` 在启动前校验失败。只允许回环监听，带用户名/密码的连接拒绝，避免误认为已经验证凭据。所有匿名客户端处于同一信任域，ClientID 不提供身份隔离；`$modorch` 命名空间不能通过网络发布、订阅或设置为 Will。

Factory 先校验配置；Application build 不创建对象或监听。setup 最多 bind 尚未 listen 的 TcpSocket，所有组件 ready 后在 activate_ingress 调用 listen。局部 Listener 可能早于全 Application Ready 开始接入，与 Modorch 0.0.3 一致。

| 字段 | 默认值 | 语义 |
|---|---|---|
| max_connections / max_sessions | 500 / 2000 | 含握手的受管连接数 / 含离线的 Session 数 |
| max_packet_bytes / max_client_id_bytes | 1048576 / 128 | 完整报文（含头）/ ClientID 编码字节 |
| handshake_timeout / write_timeout | 5s / 10s | CONNECT 接受期限 / 单个报文写完期限；配置最大 24h |
| max_subscriptions_per_client | 256 | 单 Session 过滤器数及单 SUB/UNSUB 包条目数；单包额外限制至 1000 条 |
| per_client_outbound_queue | 128 | QoS0 和控制回包各自队列长度 |
| max_inflight_per_client | 16 | 当前连接发出且待 PUBACK 的 QoS1 上限 |
| max_offline_messages_per_client | 2048 | 在线/离线可靠 Pending + Inflight 总数，范围 1..65535 |
| max_pending_bytes_per_client | 16777216 | 可靠消息 payload + Topic + 元数据的逻辑记账字节上限 |
| max_retained_topics / max_will_records | 10000 / 500 | Retained Topic 数 / 已安装及已触发待发 Will 数 |
| memory_budget_bytes | 268435456 | 全局记账及预留上限，不等于操作系统 RSS 上限 |

每条消息保守记账 `192 + topic.len() + payload.len()`；即使 Bytes 共享也逐目标收费。Session、订阅、Will、临时路由目标及保留回放都有独立额度。连接在任务准入前预留 `3 * max_packet_bytes + per_client_outbound_queue * min(max_packet_bytes, 1024) + 16384` 字节，覆盖接收、编解码、发送、控制队列和元数据；Will 另外预留最多 `max_sessions * (96 + max_client_id_bytes)` 的目标元数据。

全局预算不足时实际连接数可能低于 max_connections。Scheduler 默认总任务上限 1024，还需为宿主其他 Module 留额度；每 Listener/连接各占一个任务，任务准入失败关闭新 socket 并计数。预算保证针对本库的逻辑记账和已声明预留，标准库容器、分配器、Tokio/框架和内核 socket 的额外开销需以 RSS 压测评估，不宣称精确限制全进程内存。

配置在注册后不可变；修改需重新装配 Application。本轮基线使用线性匹配和短同步临界区，没有在锁内等待 I/O；尚无生产容量/SLO 宣称。对于大量 Session/订阅，后续需用性能测量决定 Trie/分片方案。
