# 当前配置与运行边界

本轮通过 Rust 的 `MqttBrokerConfig` 配置；支持显式 Auth/TLS/Admin/Storage 配置；完整设计中的 TOML 配置文件仍是后续目标。本库不读取进程环境或宿主配置文件。

默认监听 `127.0.0.1:1883`，端口 0 可用于测试。普通 `Default` 需要配置 Auth；`loopback_development()` 显式允许匿名且只能回环监听，此模式拒绝带凭据的连接。匿名客户端处于同一信任域，ClientID 不提供身份隔离；启用 Auth 后，稳定 PrincipalKey、ClientID 使用权限与 Core owner 校验共同控制会话恢复/接管/清除。非回环监听必须 Auth + TLS；匿名与 Auth 不能同时选择。`$modorch` 命名空间始终不能通过网络发布、订阅或设置为 Will。

`auth` 指定 Provider 组件 ID 和 0..60s 范围内的正超时，Core 声明 required 依赖；缺少显式选择的 Provider 启动失败。`tls` 指定证书链/私钥 PEM 路径，各限 1MiB，须启用 Cargo `tls` Feature。`admin` 只在显式配置时安装 Core 管理端点，并需来源白名单。Auth/Admin/Metrics 的装配及安全边界见 [security-and-operations.md](security-and-operations.md)。

Factory 先校验配置；Application build 不创建对象或监听。setup 最多 bind 尚未 listen 的 TcpSocket，所有组件 ready 后在 activate_ingress 调用 listen。局部 Listener 可能早于全 Application Ready 开始接入，与 Modorch 0.0.3 一致。

| 字段 | 默认值 | 语义 |
|---|---|---|
| max_connections_per_ip | 32 | 同一 peer IP 的在途握手与在线连接合计 |
| max_connection_attempts_per_ip / connection_attempt_window | 100 / 60s | 每 IP 固定窗口准入次数，窗口最大 1h |
| max_tracked_peer_ips | 1024 | 含未过期离线 peer 的桶总数；满额拒绝新 IP，每桶记账 192 字节 |
| max_connections / max_sessions | 500 / 2000 | 含握手的受管连接数 / 含离线的 Session 数 |
| max_packet_bytes / max_client_id_bytes | 1048576 / 128 | 完整报文（含头）/ ClientID 编码字节；ClientID 上限须为23..65535 |
| handshake_timeout / write_timeout | 5s / 10s | TLS + MQTT + Auth 共用的 CONNECT 绝对接受期限 / 单个报文写完期限；配置最大 24h |
| max_subscriptions_per_client | 256 | 单 Session 过滤器数及单 SUB/UNSUB 包条目数；单包额外限制至 1000 条 |
| per_client_outbound_queue | 128 | QoS0 和控制回包各自队列长度 |
| max_inflight_per_client | 16 | 当前连接已发送的可靠 PUBLISH 上限，QoS2 到 PUBCOMP 才释放；PUBREL控制帧优先收尾 |
| max_offline_messages_per_client | 2048 | 在线/离线可靠 Pending + Inflight 总数，范围 1..65535 |
| max_pending_bytes_per_client | 16777216 | 可靠消息 payload + Topic + 元数据的逻辑记账字节上限 |
| max_inbound_qos2_per_client | 128 | 单Session已接纳且待PUBREL的QoS2接收交换数，1..65535；MQTT5同时以此宣告连接级QoS1/2 Receive Maximum，完整写出PUBACK/PUBCOMP才归还网络额度；独立于下行积压 |
| max_inbound_qos2_bytes_per_client | 16777216 | 接收交换的逻辑字节总额，每条 `256 + topic.len() + payload.len()`，同时计入全局预算 |
| max_retained_topics / max_will_records | 10000 / 500 | Retained Topic 数 / 已安装及已触发待发 Will 数 |
| max_publish_records | 10000 | 内部发布终态幂等记录总数；不淘汰，满额拒绝新 ID；Storage 模式跨进程恢复 |
| control_subscription_ids | [1, 2, 3] | status / publish / publish-query 的全局订阅 ID；宿主须统一分配，多个 Broker 也不能重复 |
| memory_budget_bytes | 268435456 | 全局记账及预留上限，不等于操作系统 RSS 上限 |

每条消息保守记账 `192 + topic.len() + payload.len()`；即使 Bytes 共享也逐目标收费。Session、订阅、Will、临时路由目标及保留回放都有独立额度。连接在任务准入前预留 `3 * max_packet_bytes + per_client_outbound_queue * min(max_packet_bytes, 1024) + 16384` 字节，覆盖接收、编解码、发送、控制队列和元数据；启用 TLS 的连接额外预留 256KiB；TLS 写缓冲限制 64KiB；IP 桶和 Auth 快照/Will Grant 分别记账。Will 另外预留最多 `max_sessions * (96 + max_client_id_bytes)` 的目标元数据。

全局预算不足时实际连接数可能低于 max_connections。Scheduler 默认总任务上限1024、每组件任务上限256（同组件所有执行类别共享），还需为宿主其他 Module 留额度；每 Listener/连接各占一个任务；需要超过255个连接时，宿主还须通过 `ApplicationBuilder::execution_resources` 调整 `max_tasks_per_owner`。任务准入失败关闭新 socket 并计数。预算保证针对本库的逻辑记账和已声明预留，标准库容器、分配器、Tokio/框架和内核 socket 的额外开销需以 RSS 压测评估，不宣称精确限制全进程内存。

资源配置变更需重新装配 Application；Auth 策略与 TLS 凭据提供显式原子更新 API，见[可选功能](optional-features.md)。本轮基线使用线性匹配和短同步临界区，没有在锁内等待 I/O；已提供 1/10/100/500 客户端 loopback 基线脚本 `scripts/benchmark.py`，尚无生产容量/SLO 宣称。对于大量 Session/订阅，后续需用性能测量决定 Trie/分片方案。

F4 入站观察使用宿主 Hub 的 `max_topic_bytes`、邮箱容量及字节限制。Core 显式申报 DTO 的 payload cost；合法 MQTT Topic 超过 Hub 本地预算时，只跳过观察并计数，客户端路由与 PUBACK 保持独立。建议宿主配置 `HubLimits` 的 `max_message_bytes`、`max_in_flight_bytes` 和逐目标字节预算；Hub 对任意业务 DTO 的字节成本由发送者如实申报。

内部发布的完整 MQTT 包同样受 `max_packet_bytes` 约束，request_id 固定限 1..128 UTF-8 字节且不得含 NUL。幂等记录保守记账 `192 + topic.len() + payload.len() + 256 + 2 * request_id.len() + source.id.len()`，另有原子路由、交付和 Retained 的独立预算。失败于语法校验、记录数量/全局记录预留的请求不创建记录；已完成记录预留后的路由成功与拒绝均保留终态。

`storage: Some(StorageProviderConfig { .. })` 声明 Provider 的 required 依赖，仍需宿主显式注册/选择。Core 与 Provider 使用相同随机 `StoreAccessKey` 和快照上限（1KiB..64MiB）；恢复页1..1MiB，单请求超时≤60s、总恢复期限≤300s。`4 * max_snapshot_bytes` 必须小于全局内存预算，实际事务还须容纳现存状态和连接。SQLite Provider 的 `disk_budget_bytes` 至少为 `3 * (max_snapshot_bytes + 1MiB)`、最大1TiB；目录须预先存在。提交请求的快照与封装、恢复页响应都必须适配宿主 Hub 字节准入预算；默认额度不自动扩大。完整装配、数据库占用和停止行为见 [storage.md](storage.md)。

连接期限在鉴权、存储 gate 和持久提交等待期间仍会检查；`handshake_timeout` 包含任务排队、TLS 与 CONNECT，`write_timeout` 限制一整帧，不因输入报文或部分写入而续期。KeepAlive=0 不启用 KeepAlive 期限；身份按实际有效期检查。到期先关网，已开始的事务继续确认结果，连接许可/内存在实际退出前不会提前归还。高延迟后端可能延长资源占用及 Will 发布，见[可靠性边界](reliability.md)。


Core现提供默认关闭的Facade `toml` Feature；`MqttTomlConfig`支持全量Core限额、毫秒期限、固定策略和宿主Provider组件引用。`standalone-broker --config FILE [--once]`支持独立文件启动；完整字段、默认禁用、64KiB输入界限、路径与脱敏规则见[TOML配置](toml.md)。Provider工厂、能力密钥、数据库磁盘额度及策略仍由宿主管理，文件不会自动启用扩展。

MQTT5、WebSocket、Bridge、bootstrap、HTTP 与持久审计的显式配置见[可选功能接入](optional-features.md)。Core 现在至少需要两个受管任务额度（accept 与维护），每个连接还需额外额度；扩展任务另行计入。
