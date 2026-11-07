# F0–F3 实验实现契约

适用本仓库 `0.1.0-alpha.1`，依赖 Modorch-RS `0.0.3 / 336ba20196044e1b81fb2ca297a18b5e258a5800`。完整设计列出全部未来 MUST；本表只声明当前子集的可观察行为。[执行记录](../docs/validation/2026-11-07-f0-f3/README.md)须与源码/测试一起阅读，测试存在不等于已运行。

| 设计条款 | 当前范围 | 测试（除注明外位于 Core `src/tests.rs`） |
|---|---|---|
| MQTT-ARCH-001 / ARCH-002 | 一个宿主 Application/Hub；API/Core/Facade 单向依赖；Bundle 不启动，必须显式选择 | registration_is_explicit_and_status_uses_the_hosts_hub；existing-modorch-server --once |
| MQTT-LIFE-001 | 所有 ready 通过后 listen，连接/监听均为受管任务 | all_ready_before_listen_and_listener_spawn_quota_compensates_bound_socket；cancel_start_while_ready_still_completes_owned_compensation |
| MQTT-LIFE-002 | 后续入口失败、Listener/连接任务限额均无端口或任务泄漏 | later_activation_failure_and_listener_quota_release_the_port；all_ready_before_listen_and_listener_spawn_quota_compensates_bound_socket |
| MQTT-LIFE-003 / STATE-004 | 旧 generation/connection ID 拒绝；Core 重建及一次激活失败补偿保留 Factory 内存 | restart_preserves_memory_sessions_retained_inflight_and_fences_old_generation |
| MQTT-TOPIC-001 | Topic/Filter 空层、`+/#`、`$` 匹配；合法长 Topic 不受 Hub 1024 字节预算影响 | codec::tests::mqtt_topic_edges；long_topics_qos0_self_echo_and_reserved_namespaces |
| MQTT-ROUTE-001 | 每 Session 订阅独立，重叠按最高匹配 QoS、受原发布 QoS 限制 | qos1_overlap_retained_delete_and_unsubscribe；rumqttc 集成测试 |
| MQTT-QOS-001（子集） | QoS0/1、Packet ID/PUBACK、离线积压和重连 DUP | offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup；saturation_rejects_entire_publish_without_retained_or_partial_delivery |
| MQTT-QOS-002 | 未实现 QoS2，所以拒绝并标识实验子集；不声明完整 3.1.1 | malformed_client_and_qos2_close_only_that_connection；qos1_overlap_retained_delete_and_unsubscribe |
| MQTT-STATE-001（内存） | Clean Session、Session Present、Retained 回放/删除、Will、无进程重启持久化 | offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup；qos1_overlap_retained_delete_and_unsubscribe；will_on_abrupt_close_and_takeover_but_not_disconnect |
| MQTT-SEC-001 / SEC-003（匿名子集） | 显式匿名回环配置；PUBLISH/SUBSCRIBE/Will 的保留命名空间拒绝发生在接管前；畸形连接不升级为组件故障 | configuration_rejects_unsafe_and_overflowing_limits；long_topics_qos0_self_echo_and_reserved_namespaces；malformed_client_and_qos2_close_only_that_connection |
| MQTT-BOUND-001 / BOUND-002（内存） | 连接/任务/记账内存与可靠积压有界；整次可靠发布接纳或拒绝；已接纳消息不淘汰 | application_isolation_and_memory_quota_release；saturation_rejects_entire_publish_without_retained_or_partial_delivery；codec::tests::fragmented_length_and_malformed_frames_are_bounded |
| MQTT-COMPAT-001 | 固定 Git rev、统一 Facade 来源、MSRV 1.85 | Cargo.lock；framework-preflight.log；msrv.log |

KeepAlive/握手超时与分片输入由 `handshake_timeout_keepalive_will_and_fragmented_packets` 验证。跨 Application 状态隔离由 `application_isolation_and_memory_quota_release` 验证。独立客户端由 `crates/modorch-mqtt/tests/rumqttc.rs` 和 `scripts/verify-paho.py` 验证；后者单独运行，不混入 Cargo 的 Passed 数量。

F4 的 MQTT-EVENT-001、F5 的 Auth/SEC-002、持久 Store/STATE-002/STATE-003、磁盘提交幂等与恢复、F6 QoS2、F7 MQTT5、生产性能和跨平台门禁都尚未实现/验收。F0 中面向 Storage Module 的 prelisten Request 恢复用例随 F5 实现，本轮仅验证共享内存的代际恢复；不将其标为持久恢复 Passed。
