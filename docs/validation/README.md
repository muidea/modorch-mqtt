# 最终验证与收口记录

更新日期：2026-11-12。适用版本：`0.1.0-alpha.1`，Rust 最低 1.85，Modorch-RS 0.0.3，固定 rev `336ba20196044e1b81fb2ca297a18b5e258a5800`。

按用户要求，仓库归档仅保留本最终记录；各轮临时验证目录、原始日志、测试数据库、探针、摘要清单和未批准评审包已清理。以下分别标明历史基线与最新限定回归，不能代替原始材料或当前输入的独立批准。逐次报告和临时日志仍不归档。

当前完整 F6/F7 与生产发布仍为 **Partial**，真实独立批准均为 0。后续任务统一见[F6/F7 待收口计划](../f6-f7-closure-plan.md)及[剩余需求](../remaining-work.md)。

<a id="implementation"></a>

## 最终实现范围

| 阶段 | 当前实现 |
|---|---|
| F0–F3 | 可运行的 QoS0/1、连接/会话、订阅/路由、Retained、Will 和资源预算实验版本 |
| F4 | 宿主 Application/EventHub 集成，显式注册和选择、可信定向发布、幂等查询及受管生命周期 |
| F5 | Auth/ACL、TLS、Admin、Metrics 与 SQLite 持久化实验实现；提交确认后 ACK/下行，持久恢复完成后 listen |
| F6 | 双向 QoS2、Packet ID 所有权、重连/持久恢复和有限规范/故障矩阵；完整 MQTT 3.1.1 验收未完成 |
| F7 | MQTT5 属性/原因码、会话与消息过期、流控/包长、别名、订阅选项/标识、共享订阅、延迟 Will；完整 MQTT5 验收未完成 |
| 可选扩展 | WebSocket/WSS、网络 Bridge、bootstrap、动态策略/证书、持久审计和 HTTP 管理等实验功能 |

实际接入、资源限制及失败语义以[契约](../../contracts/README.md)、[可选功能](../optional-features.md)、[持久化](../storage.md)和[安全运维](../security-and-operations.md)为准。写 format5、兼容读取 format1–4；Enhanced Authentication / AUTH 明确不支持。

<a id="closed-issues"></a>

## 已完成修复与验证收口

下表保留最终处理结论，不代表完整协议或生产认证。历史修复前失败不改称通过；原始执行文件已按归档要求移除。

| 编号 | 最终处理 |
|---|---|
| R-01 | 零过期 MQTT5 在线会话可接管，真正断开后清理，Clean Start 仍重建 |
| R-02 | Bridge 目标 Session 丢失/恢复时按对应状态继续交换与重发 |
| R-03 | Bridge 普通属性与防环属性分别限额，全部计入包长和内存 |
| R-04 | 幂等记录恢复校验真实属性并按实际内容计费 |
| R-05 | 不清理的跨协议会话恢复拒绝，保留原所有权；显式清理后可切换 |
| R-06 | 恢复按可表示的源线帧核对包长，目标附加订阅标识不误占源预算 |
| R-07 | Retained 保存来源并执行 No Local；旧格式来源未知时保守处理 |
| R-08 | 合法 Topic Alias 展开后可恢复，绑定帧、真实属性与预算仍严格校验 |
| R-09 | Bridge Transfer 绑定源 Session 代际，隔离旧 ACK，完成旧工作后重建订阅 |
| R-10 | 已到期或等待可靠容量的延迟 Will 不被恢复 Session 错误删除 |
| R-11 / R-16 | MQTT5 合法短格式及空 DISCONNECT 正确处理 |
| R-12 / R-13 | 非零合法客户端 DISCONNECT 保留 Will；拒绝客户端使用 Server 专用原因码 |
| R-14 | 重复 UNSUBSCRIBE 按输入顺序逐项处理并返回正确原因 |
| R-15 | 恢复旧 PUBREL 不占用新连接 PUBLISH 额度，旧 PUBCOMP 不额外增额 |
| R-17 | 共享组优先在线成员；全离线时为未过期持久成员接纳可靠积压并固定所有者 |
| R-18 | Receive Maximum 在 ACK 完整写出前保持占用，拒绝超额发布且不部分修改状态 |
| R-19 | Client PUBACK/PUBREC 拒绝仅 Server 可发送的 0x10，保留原交换和持久恢复 |
| T-01–T-03 | 独立评审导入与严格判定通道、文件别名及已知评审等内容副本排除已实现 |
| T-04 | 修正规范完整段落中的 Server 角色与限定测试映射，保留真实语义审查要求 |
| V-02 | 慢消费者恢复核对已观察报文的原 ID、DUP 和载荷；错误输入能使测量失败 |
| V-03 | 修复密码工作与 Session/ACL 串行排队耦合；最终限定 debug/release 并发负载成功，容量边界继续保留 |
| V-04 | 物理认证任务执行中的取消、强制退出、监督重建及实际额度归还有受控验证 |

V-01 中已复现的独立连接 PING 被无关 Store 提交阻塞及故障夹具时序问题已处理；缺阶段日志的历史失败仍无法全部唯一归因，保留为证据限制，不以重复通过关闭。

<a id="verification"></a>

## 最近功能验证结论

2026-11-10 的开发样例收口已通过完整 `scripts/check.sh`：226 项 Rust、53 项脚本测试，以及 fmt/check/all-features Clippy/Rustdoc、契约、TLS/Auth/Storage 与 SIGKILL 恢复检查。新增 4 项受管任务测试和真实 SIGINT/SIGTERM、MQTT 心跳、退出回执、端口重绑检查；Rust1.85 all-targets/all-features、最小 Feature 编译和 8 项 TOML 配置测试通过。正式指南见 [magicDataHub 集成](../magicdatahub-integration.md)。随后默认 Feature lint 收口只调整 Core 的条件 lint 与 TLS 可选字段初始化，不改变协议行为；默认 Feature Clippy 已进入门禁。当前源码再次通过完整检查、MSRV/最小 Feature 与宿主回归，并在新的隔离镜像验证；常驻服务未替换。未重新执行全部独立客户端扩展互操作/生产负载，完整符合性与发布仍 Partial。

2026-11-12 新增两版实现侧验收 Profile 契约及 4 项反例、F67-04 矩阵台账校验及 4 项反例，脚本测试由 53 增至 61；新增重叠订阅最高匹配 QoS、最小(发布,授予) QoS 用例后 Rust 测试为 233 项。`cargo test --workspace --all-features --locked` 为 **233 passed / 0 failed / 0 ignored**，`check-conformance.py --results` 41/41 场景 TargetedEvidence 通过、规范仍 Partial，`check-protocol-review.py --results` 缺失证据 0。`scripts/check-acceptance-profile.py` 与 `scripts/check-matrix-inventory.py` 已通过；完整 `scripts/check.sh` 已重跑并 exit 0，包含 **233 Rust / 61 脚本测试**、fmt/check/all-features Clippy/Rustdoc、契约门禁、示例/TOML/Storage/SIGKILL 检查。该变化固定实现侧范围并防止自评/计数/Feature 漂移，F67-01 独立审查仍为 NotRun。

### 2026-11-12 F67-02 批次复核（实现侧，非独立批准）

**范围**：MQTT3.1.1 `MQTT-3.3.5-1`（重叠订阅按最高匹配 QoS 投递）与 `MQTT-3.8.4-6`（下行 QoS 取发布 QoS 与最高授予的最小值）。

**实现核对**：`crates/modorch-mqtt-core/src/state.rs:638` 对每个匹配订阅先取 `sub.qos.min(message.qos)`，再以 `max` 合并，对应最高匹配 QoS；随后按该 QoS 生成单一交付。代码路径与两条规范文本方向一致。

**证据**：`overlapping_subscriptions_deliver_one_copy_at_highest_matching_qos` 覆盖最高授予 2、发布 QoS2；`overlapping_subscription_delivery_qos_is_min_of_publish_and_max_grant` 覆盖最高授予 2/1/0 与发布 QoS0/1/2 的最小值组合；已有 `qos1_overlap_retained_delete_and_unsubscribe` 覆盖保留消息/取消订阅路径。

**本轮发现并修正**：`MQTT-3.8.4-6` 原先未映射新增的最小 QoS 用例，`MQTT-ROUTE-001` 也漏映射两个新增重叠用例；已补齐精确测试映射，`check-protocol-review.py --results` 仍通过。

**结论**：在上述限定范围内证据充分，未发现运行时缺陷；但这只是实现侧批次复核，**不关闭 F67-02 整体**，也不构成独立批准。F67-02 仍待其余 435 个输入及 339 个表格行的逐项审查。

### 2026-11-12 F67-02 批次 02（实现侧，非独立批准）

**范围**：MQTT3.1.1 `MQTT-3.8.4-1`—`-6`（SUBACK 响应与 Packet Identifier、重复过滤器替换及 Retained 重放、多过滤器合并响应、逐过滤器返回码、下行 QoS 最小值）。

**实现核对**：`State::subscribe` 按输入顺序逐项处理并汇总同一 SUBACK；重复过滤器经 `subscribe_one` 替换，`replaced` 路径保留已开始交换并重放匹配 Retained；逐项 code 由实际 QoS/失败生成，下行 QoS 使用 `min(publish, granted)`。

**证据**：`mixed_subscribe_codes_follow_filter_order_and_duplicate_filters_replace_in_sequence` 使用同一 SUBSCRIBE 中的 `mix/#`(0)、`$modorch/#`(1)、重复 `mix/#`(2)、`extra`(1)，核对 SUBACK ID `0x1234` 与 codes `[0,0x80,2,1]`；替换后 Retained 重放 QoS 为 `[0,2]`，后续 live `mix/topic` 为 QoS2、`extra` 为 QoS1，且替换过滤器无重复 live 交付。

**本轮发现并修正**：`MQTT-3.8.4-6` 原先未映射该混合订阅/替换用例，导致最小 QoS 证据链不完整；已补齐映射，`check-protocol-review.py --results` 通过。

**结论**：6 个输入在限定范围内证据充分，未发现运行时缺陷；不关闭 F67-02 整体，也不构成独立批准。F67-02 累计 8/437 输入完成实现侧复核，其余 429 个输入及 339 个表格行仍待。

### 2026-11-12 F67-02 批次 03（实现侧，非独立批准）

**范围**：MQTT3.1.1 `MQTT-3.3.1-4`—`-12`（PUBLISH QoS 非法值、Retained 存储/替换/删除/非 Retained 发布、向新订阅回放的 RETAIN 标志）。

**实现核对**：PUBLISH QoS=3 在 codec/连接层拒绝并只关闭违规连接；Retained 值按 QoS 覆盖，零字节 Retained 删除，非 Retained 发布不替换也不删除旧值；实时交付 RETAIN=0，新订阅回放 RETAIN=1。

**证据**：
- `malformed_client_and_invalid_qos_close_only_that_connection`：QoS=3 关闭违规连接。
- `retained_replacement_qos0_delete_and_nonretained_publish_obey_wire_flags`：QoS2/0/1 Retained 替换，late 订阅 header `0x31/0x33`、实时 header `0x30/0x32/0x34`，零字节 Retained 删除，非 Retained 空/非空发布不改变旧值。

**结论**：9 个输入在限定范围内证据充分，未发现运行时缺陷；不关闭 F67-02 整体，也不构成独立批准。F67-02 累计 17/437 输入完成实现侧复核，其余 420 个输入及 339 个表格行仍待。

### 2026-11-12 F67-02 批次 04（实现侧，非独立批准）

**范围**：MQTT3.1.1 `MQTT-3.10.1-1`、`MQTT-3.10.3-1/2`、`MQTT-3.10.4-1`—`-6`（UNSUBSCRIBE 固定头、UTF-8/非空过滤器、精确删除、停止新投递、完成已开始 QoS1/2、UNSUBACK 与多过滤器合并），以及 `MQTT-3.14.1-1`、`MQTT-3.14.4-3`（DISCONNECT 固定头、丢弃 Will）。

**实现核对**：
- UNSUBSCRIBE 只接受固定头 `0xA2`、非空 UTF-8 过滤器；精确匹配删除，未知过滤器仍返回 UNSUBACK。
- 删除后停止新路由，但已开始的 QoS1/2 交付继续；多过滤器合并为单个 UNSUBACK，Packet Identifier 保持。
- 正常 DISCONNECT 丢弃当前连接的 Will；`3.14.4-1/2` 是 ClientOnly，不属于 Server 验收范围。

**证据**：`malformed_packets_and_second_connect_close_only_the_offending_connection`、`malformed_utf8_in_each_connect_and_filter_field_is_rejected_but_password_is_binary`、`replacement_subscription_replays_retained_and_unsubscribe_is_exact`、`unsubscribe_keeps_started_qos1_and_qos2_deliveries_but_stops_new_routing`、`will_on_abrupt_close_and_takeover_but_not_disconnect`。

**结论**：11 个输入在限定范围内证据充分，未发现运行时缺陷；不关闭 F67-02 整体，也不构成独立批准。F67-02 累计 28/437 输入完成实现侧复核，其余 409 个输入及 339 个表格行仍待。

### 2026-11-12 F67-03 批次复核（实现侧，非独立批准）

**范围**：MQTT5 `MQTT-3.3.2-1`/`-2`（PUBLISH Topic UTF-8、禁止通配符）、`MQTT-3.3.2-4`（Payload Format Indicator 原样转发）、`MQTT-3.3.2-5/6`（Message Expiry 到期删除与转发递减）、`MQTT-3.3.2-7/8`（Topic Alias 不跨连接、禁止 0）、`MQTT-3.3.2-15/16/17/18/20`（Response Topic、Correlation Data、User Property 顺序、Content Type 原样转发）、`MQTT-3.3.4-2`（重叠订阅最高匹配 QoS）、`MQTT-3.3.4-3`/`-4`（单副本携带全部匹配 Subscription Identifier）、`MQTT-3.8.3-1`—`-5`（过滤器 UTF-8、非空载荷、No Local、Shared No Local、保留位）、`MQTT-3.8.4-1`—`-7`（SUBACK、逐过滤器原因码、重复过滤器替换、多过滤器合并响应）、`MQTT-3.8.4-8`（下行 QoS 取发布 QoS 与最高授予的最小值）。

**实现核对**：与 F6 共用 `crates/modorch-mqtt-core/src/state.rs:638` 的 `max(sub.qos.min(message.qos))` 路径；单副本聚合所有匹配订阅的 identifier，符合 `MQTT-3.3.4-3/-4` 的方向。

**证据**：
- `mqtt5_overlapping_subscriptions_use_max_qos_and_report_matching_identifiers`：最高授予 2/1/0 时，分别收到 QoS2（标识 `[7,9]`）、QoS1（标识 `[10,11]`）和 QoS0（标识 `[12]`）单副本。
- `mqtt5_multi_filter_subscribe_reports_codes_and_replaces_duplicate`：单个 SUBSCRIBE 携带重复 `multi/#` 与 `$modorch/blocked`，单个 SUBACK 返回逐项 `[0,2,0x87]`；重复过滤器最终按 QoS2 替换并只投递 QoS2 单副本。
- `mqtt5_invalid_subscribe_payload_and_options_close_only_offender`：空载荷、非法 UTF-8、非法 `a/#/b`、QoS3、保留位、Retain Handling=3、Shared No Local 均只关闭违规连接，健康连接 PING 仍可用。
- `mqtt5_invalid_publish_topic_closes_only_offender`：空 Topic、`a/#`、`a/+`、非法 UTF-8 Topic 均只关闭违规连接，健康连接 PING 仍可用。
- `mqtt5_payload_format_indicator_one_is_forwarded`：Payload Format Indicator=1 的合法 UTF-8 载荷转发后仍为 1；`mqtt5_publish_all_qos_preserves_binary_metadata_user_order_and_outgoing_flags` 同时核对 Response Topic、Correlation Data、User Property 顺序和 Content Type 原样转发。
- `mqtt5_positive_ttl_phase_restart_matrix_expires_only_unsent_messages_and_retained`：真实墙钟等待后 Retained 重放的 Message Expiry 由 4 递减为 1—2，到期阶段删除 queued/publish/release 与 Retained。
- `mqtt5_alias_rejects_invalid_unbound_reconnected_oversize_and_unfunded_messages_atomically`：Topic Alias 不跨连接携带，Alias=0 与未绑定别名被拒，原 Retained 保持不变。

**本轮发现**：既有 MQTT5 重叠测试只覆盖相同 QoS 与两个标识，没有覆盖不同授予下的最高/最小 QoS 组合；新用例补上该有限等价类，并映射至 `MQTT-3.3.4-2/3/4` 与 `MQTT-3.8.4-8`。测试使用两个匹配标识；手工三标识原始帧由 rumqttc 0.25.1 解码时仅返回两个标识，因此未把三标识作为通过证据，也没有据此判定产品缺陷。

**结论**：在上述限定范围内证据充分，未发现运行时缺陷；不关闭 F67-03 整体，也不构成独立批准。批次 01 共覆盖 28/607 个输入；F67-03 仍待其余 579 个输入及 505 个表格行的逐项审查。

**本轮发现并修正**：`MQTT-3.3.2-7/8` 原先未映射现成的 `mqtt5_alias_rejects_invalid_unbound_reconnected_oversize_and_unfunded_messages_atomically`；已补齐映射。

**边界说明**：实现把显式 Payload Format Indicator=0 归一化为缺失；MQTT5 §3.3.2.3.2 规定 0 等价于不发送，本批未将其判为代码缺陷，也未把该归一化作为“原样 0”证据。

### 2026-11-12 F67-03 批次 02：ACK 原因码与流控（实现侧，非独立批准）

**范围**：MQTT5 `MQTT-3.4.2-1/2/3`（PUBACK 原因码、Reason String/User Property 的大小限制）、`MQTT-3.5.2-1/2/3`（PUBREC 对应项）、`MQTT-4.9.0-1/2/3`（Receive Maximum、发送配额、配额耗尽时仍处理其他控制包）。

**实现核对**：
- PUBACK/PUBREC 只由固定 reason code 与最小属性构造；实现没有发送 Reason String 或 ACK User Property 的路径，因此“不得因这些属性超过对端 Maximum Packet Size”的约束不会被违反。
- 存储矩阵覆盖 Client 合法/Server-only reason code、错误阶段与重连后的所有权。
- 流控按 `max_inbound_qos2_per_client` 与 PUBCOMP 释放配额；重连不重复消耗/归还，配额耗尽时 PUBLISH QoS>0 被拒绝而 PING/其他控制包仍处理。

**证据**：`mqtt5_ack_and_disconnect_reason_tables_enforce_packet_kind_and_sender_role`、`mqtt5_outbound_control_frames_match_independent_literal_layouts`、`mqtt5_client_ack_legal_reason_matrix_completes_once_across_sqlite_restarts`、`client_ack_server_only_reason_matrix_closes_without_mutating_sqlite_sender_ownership`、`mqtt5_receive_maximum_rejects_before_retained_routing_and_isolates_the_connection`、`mqtt5_reconnected_pubrel_does_not_consume_fresh_receive_quota_or_replenish_it_twice`、`mqtt5_sqlite_recovered_pubrel_has_fresh_connection_quota`。

**结论**：9 个输入在限定范围内证据充分；Reason String/User Property 两项属于“无发送路径”的适用性结论，不是代码缺陷。不关闭 F67-03 整体，也不构成独立批准。F67-03 累计 37/607 输入完成实现侧复核，其余 570 输入及 505 表格行仍待。

### 2026-11-13 F67-02 批次 05：连接、CONNACK、KeepAlive 与 Session（实现侧，非独立批准）

**范围**：MQTT3.1.1 `MQTT-3.1.0-1/2`、`MQTT-3.1.2-1`—`6`、`MQTT-3.1.2-8`—`24`（含 `3.1.2-23` ClientOnly）、`MQTT-3.1.3-1`—`11`、`MQTT-3.1.4-1`—`5`、`MQTT-3.2.0-1`、`MQTT-3.2.2-1`—`6`、`MQTT-3.12.4-1`、`MQTT-4.1.0-1/2`，共 51 个输入。

**实现核对**：
- `connection.rs` 首包仅接受有效 CONNECT；协议名非 `MQTT` 或保留位/Flags/Will/User/Password 结构违规直接关闭且不发 CONNACK；协议级别非 4 发送 `[0x20, 2, 0, 1]` 并关闭；重复 CONNECT 作为协议违规断连。
- `state.rs::attach` 校验 ClientID 长度（配置不少于 23 字节）与 UTF-8；空 ClientID 在 `CleanSession=0` 返回 CONNACK `0x02` 并关闭，`CleanSession=1` 分配唯一不冲突 ClientID；`CleanSession=1` 丢弃旧会话且断开后删除，`CleanSession=0` 按是否已有持久会话设置 `Session Present` 0/1；在线同 ClientID 接管取消旧连接并触发其 Will；拒绝的 CONNECT 不处理后续流水线报文，未分类内部错误不伪造 CONNACK。
- `connection.rs` 以 `1.5 * keep_alive` 更新网络超时并在收到任意完整控制包时刷新（`keep_alive=0` 禁用）；超时按异常断连关闭并触发 Will；`PINGREQ` 直接返回 `[0xd0, 0]`，不被无关连接的 Store 提交阻塞。

**证据**：`malformed_connect_closes_without_connack`、`malformed_packets_and_second_connect_close_only_the_offending_connection`、`rejected_connect_does_not_process_pipelined_publication`、`all_256_connect_flags_validate_payload_presence_and_wire_rejection`、`connack_reserved_flags_are_zero_for_new_resumed_clean_and_rejected_sessions`、`outgoing_connack_new_resumed_clean_and_rejected_frames_preserve_existing_session`、`connect_failure_codes_distinguish_rejected_id_capacity_and_authorization`、`clean_session_discards_both_qos2_directions_and_subscriptions_but_keeps_retained`、`persistent_offline_session_queues_both_reliable_qos_levels_until_reconnect`、`assigned_client_id_never_takes_over_an_existing_session`、`required_client_identifiers_and_opaque_unicode_topics_are_preserved`、`client_id_budget_cannot_exclude_mandatory_23_byte_identifiers`、`handshake_timeout_keepalive_will_and_fragmented_packets`、`ping_on_idle_connection_does_not_wait_for_another_connections_store_commit`、`connect_failures_use_table31_reasons_and_unknown_failures_have_no_connack`，及新增 `keepalive_resets_on_control_packet_and_zero_stays_open`（验证 `keep_alive=0` 越过 `handshake_timeout` 且非零 KeepAlive 在收到控制包后重置计时并在静默后触发 Will）。

**本轮发现并修正**：`MQTT-3.1.2-24`、`MQTT-3.12.4-1` 与 `MQTT-3.2.0-1` 补充了 KeepAlive 重置与离线重连先收 CONNACK 的精确测试映射；未发现运行时代码缺陷。

**结论**：51 个输入在限定范围内证据充分（其中 `MQTT-3.1.2-23` 为 ClientOnly）；不关闭 F67-02 整体，也不构成独立批准。F67-02 累计 79/437 输入完成实现侧复核，其余 358 个输入及 339 个表格行仍待。

### 2026-11-13 F67-03 批次 03：MQTT5 CONNACK、KeepAlive、Assigned ClientID 与 Session（实现侧，非独立批准）

**范围**：MQTT5 `MQTT-3.1.2-20`—`23`（含 `3.1.2-20/21` ClientOnly）、`MQTT-3.1.3-6/7/8`、`MQTT-3.2.0-1/2`、`MQTT-3.2.2-1`—`7`、`MQTT-3.2.2-9`—`22`（含 `3.2.2-4/5` ClientOnly）、`MQTT-3.12.4-1`、`MQTT-4.1.0-1/2`，共 33 个输入。

**实现核对**：
- `mqtt5.rs::Codec::response` 构造 CONNACK：保留位 7–1 固定为 0，仅在成功时附带 `Receive Maximum (0x21)`、`Topic Alias Maximum=32 (0x22)`、`Maximum Packet Size (0x27)` 及空 ClientID 时的 `Assigned Client Identifier (0x12)`；无 `Server Keep Alive`、`Reason String`、`User Property` 发送路径，KeepAlive 沿用客户端 CONNECT 值；失败原因码映射 `0x84/0x85/0x86/0x87/0x88` 保持 `Session Present=0` 且随后关闭连接；`wire.max_packet()` 防止发出超过客户端 `Maximum Packet Size` 的 CONNACK。
- `state.rs` 在 `Clean Start=1` 时丢弃旧会话并置 `Session Present=0`；`Clean Start=0` 时依据持久会话或在线零过期会话接管规则设置 `Session Present`；零过期会话在断开后立即清理，`Session Expiry > 0` 按到期间隔保留并由 `process_wills` 到期清理。

**证据**：`mqtt5_connack_omits_optional_diagnostics_and_defaults_allow_qos2_retained_with_exact_packet_limit`、`mqtt5_outbound_control_frames_match_independent_literal_layouts`、`mqtt5_keepalive_uses_connect_value_and_publishes_will_on_timeout`、`mqtt5_online_zero_expiry_takeover_preserves_subscriptions_and_both_qos2_directions`、`mqtt5_delayed_will_cancel_resume_session_expiry_and_message_expiry`、`mqtt5_negotiated_packet_limit_discards_oversized_delivery_without_blocking_session`、`mqtt5_sqlite_restart_preserves_options_metadata_and_qos2_ownership`，及新增 `mqtt5_zero_length_client_id_assigns_unique_identifier_and_resumes_persistent_session`（验证空 ClientID 返回唯一 `0x12` 属性、不接管现有 `m2` 会话、持久分配后可重连恢复）和 `mqtt5_topic_alias_above_connack_maximum_is_rejected`（验证超过 CONNACK 通告 `Topic Alias Maximum=32` 的别名 33 被拒绝）。

**本轮发现并修正**：原 `MQTT-3.1.3-6/7` 与 `MQTT-3.2.2-16` 映射的测试未覆盖空 ClientID 分配路径，且 `MQTT-3.2.2-17` 缺少超过 `Topic Alias Maximum` 上限的显式负例；已补齐两个测试用例与精确映射，未发现运行时代码缺陷。

**结论**：33 个输入在限定范围内证据充分（含 4 项 ClientOnly）；Reason String / User Property / Server Keep Alive 属于无发送路径的适用性结论。不关闭 F67-03 整体，也不构成独立批准。F67-03 累计 70/607 输入完成实现侧复核，其余 537 个输入及 505 个表格行仍待。

### 2026-11-13 F67-02 批次 06—07：QoS 0/1/2 流、Packet ID、有序主题、UTF-8、Topic 匹配与其余 3.1.1 编号条款（实现侧，非独立批准）

**批次 06 范围（30 项去重输入）**：MQTT3.1.1 `MQTT-2.3.1-1`—`7`（含 `2.3.1-2` ClientOnly）、`MQTT-3.3.1-1`—`3`、`MQTT-3.3.2-1`—`3`、`MQTT-3.3.4-1`、`MQTT-3.3.5-2`、`MQTT-3.6.1-1`、`MQTT-4.3.1-1`、`MQTT-4.3.2-1/2`、`MQTT-4.3.3-1/2`、`MQTT-4.4.0-1`、`MQTT-4.5.0-1/2`（含 `4.5.0-2` ClientOnly）、`MQTT-4.6.0-1`—`6`。

**批次 07 范围（32 项去重输入）**：MQTT3.1.1 其余全部编号条款——`MQTT-1.5.3-1`—`3`、`MQTT-2.2.2-1/2`、`MQTT-3.8.1-1`、`MQTT-3.8.3-1`—`3`、`MQTT-3-8.3-4`、`MQTT-3.9.3-1/2`、`MQTT-3.14.4-1/2`（ClientOnly）、`MQTT-4.7.1-1`—`3`、`MQTT-4.7.2-1`、`MQTT-4.7.3-1`—`4`、`MQTT-4.8.0-1/2`、`MQTT-6.0.0-1`—`4`（`OptionalWebSocket`，另有真实 WebSocket 分片用例覆盖二进制帧与子协议协商）、`MQTT-7.0.0-1`（`ConditionalBridge`）、`MQTT-7.0.0-2`、`MQTT-7.1.1-1`、`MQTT-7.1.2-1`（ClientOnly）。

**实现核对**：
- `state.rs` / `state/qos2.rs` / `connection.rs`：PUBLISH/PUBREL 严格按 QoS 0/1/2 状态机推进，Packet ID 在 1..=65535 跳过未完成 QoS1/2 项；QoS 1 `ack` 与 QoS 2 Method B 去重保护确保不重复投递；ACL/暂态/容量拒绝回滚并关闭违例连接（符合 `3.3.5-2` 与 `4.8.0-1/2`）；`session.pending` 与 `session.qos0` 双端队列及 `VecDeque` ACK FIFO 保持有序主题与重连重发顺序。
- `codec.rs`：非零保留位（`PUBREL=0x62` 等）、非零 Packet ID、UTF-8 与非规范化精确字节匹配、Topic 长度与空字符拒绝均在解码/校验层拦截并关闭违规连接。

**证据与本轮修正**：
- 修正 `contracts/protocol-review.json` 中 `MQTT-3.6.1-1` 原经 `mqtt311.json` 场景捆绑带入 3 项不相关重叠订阅测试的误映射，改为直接映射 `qos2_control_flags_identifiers_and_complete_lengths_are_validated` 与 `all_256_fixed_headers_preserve_healthy_connections_and_expected_wire_responses`。
- 在 `crates/modorch-mqtt-core/src/tests/protocol.rs` 新增 `ordered_topic_preserves_qos0_qos1_qos2_and_reconnect_resend_order`，显式验证同主题同发送者在 QoS 0、QoS 1（含 `CleanSession=0` 重连 `DUP=1` 同原 Packet ID 顺序重发）与 QoS 2 下的顺序投递，并更新 `contracts/protocol-review.json` 与 `contracts/mqtt311.json` 中 `MQTT-4.6.0-1` 与 `MQTT-4.6.0-6` 映射。

**结论与计数校准**：批次 01–04 实际去重包含 27 项输入（此前按 `2+6+9+11=28` 计入时将 `MQTT-3.8.4-6` 跨批次重计 1 次），加上批次 05（51 项）、批次 06（30 项）与批次 07（32 项），F67-02 累计 **140/437** 去重输入完成实现侧复核——MQTT 3.1.1 全部 **140/140** `Numbered` 规范条款已完成实现侧逐项核对；其余 **297** 输入（`Table` 56、`Section` 219、`Unnumbered` 22）及 **339** 表格行仍待，真实独立批准仍为 0，**不关闭 F67-02 整体**。

### 2026-11-13 F67-03 批次 04—05：MQTT5 QoS/PUBREL/PUBCOMP/Ordered Topic 与 SUBACK/UNSUBSCRIBE/DISCONNECT/Topic/Shared/Error/WebSocket（实现侧，非独立批准）

**批次 04 范围（42 项去重输入）**：MQTT5 `MQTT-2.2.1-2`—`6`（含 `2.2.1-3` ClientOnly）、`MQTT-2.2.2-1`、`MQTT-3.6.1-1`、`MQTT-3.6.2-1`—`3`、`MQTT-3.7.2-1`—`3`、`MQTT-4.3.1-1`、`MQTT-4.3.2-1`—`5`、`MQTT-4.3.3-1`—`13`、`MQTT-4.4.0-1/2`、`MQTT-4.5.0-1/2`（含 `4.5.0-2` ClientOnly）、`MQTT-4.6.0-1`—`6`（其中 `4.6.0-1`—`4` 在 MQTT5 原文中为明确限定 Client 的 `ClientOnly` 条款）。

**批次 05 范围（66 项去重输入）**：MQTT5 `MQTT-1.5.4-1`—`3`、`MQTT-1.5.5-1`、`MQTT-1.5.7-1`、`MQTT-2.1.3-1`、`MQTT-3.8.1-1`、`MQTT-3.9.2-1/2`、`MQTT-3.9.3-1/2`、`MQTT-3.10.1-1`、`MQTT-3.10.3-1/2`、`MQTT-3.10.4-1`—`6`、`MQTT-3.11.2-1/2`、`MQTT-3.11.3-1/2`、`MQTT-3.14.0-1`、`MQTT-3.14.1-1`、`MQTT-3.14.2-1`—`4`、`MQTT-3.14.4-1`—`3`（含 `3.14.4-1/2` ClientOnly）、`MQTT-3.15.1-1`、`MQTT-3.15.2-1`—`3`（`UnsupportedConditional`）、`MQTT-4.7.0-1`、`MQTT-4.7.1-1/2`、`MQTT-4.7.2-1`、`MQTT-4.7.3-1`—`4`、`MQTT-4.8.2-1`—`6`、`MQTT-4.12.0-1`—`7`（含 `4.12.0-2`—`5` `UnsupportedConditional`）、`MQTT-4.12.1-1/2`（`UnsupportedConditional`）、`MQTT-4.13.1-1`、`MQTT-4.13.2-1`、`MQTT-6.0.0-1`—`4`。

**实现核对**：
- `mqtt5.rs` 与 `mqtt5/validation.rs`：PUBREL/PUBCOMP/SUBACK/UNSUBACK/DISCONNECT 不附加可选 Reason String 或 User Property，确保不因诊断属性超过对端 `Maximum Packet Size`；服务端不发送 Enhanced Auth `AUTH`（`4.12.0-6/7`）并直接关闭包含未支持 `Authentication Method` 或非法保留位/格式错的连接（符合 `4.13.1-1` 允许不回 DISCONNECT 直接关闭网络连接）；`Shared Subscriptions`（`$share/<ShareName>/<filter>`）语法校验、同组成员 QoS2 所有权跨重连与负响应丢弃（`4.8.2-1`—`6`）均由现有矩阵和协议路径保证。
- `validation.rs` 对 SUBSCRIBE/UNSUBSCRIBE 均检查 Packet ID 非零、属性有效性、过滤器 UTF-8/格式及载荷非空；`connection.rs` 对已开始的 QoS1/2 在 UNSUBSCRIBE 后继续完成但不新增匹配路由。

**证据与本轮发现并修正**：
- 扩展 `crates/modorch-mqtt-core/src/tests/mqtt5_protocol.rs` 中 `mqtt5_invalid_subscribe_payload_and_options_close_only_offender` 加入 `0xA2` 空载荷、零 Packet ID、非法 UTF-8 及非法 `#` 位置 UNSUBSCRIBE 负例。
- 新增 `mqtt5_preserves_utf8_bom_non_normalized_topics_and_ordered_qos_delivery`，真实 TCP 验证 Topic 与 User Property 中 `U+FEFF`（`0xEF 0xBB 0xBF`）不剥离、NFC `"é"` 与 NFD `"e\u{301}"` 不规范化、以及 Ordered Topic 在 QoS 0/1（含 `CleanStart=0` 重连重发顺序与原 Packet ID）/QoS 2 下的顺序投递。
- 补齐 `contracts/protocol-review.json` 中 `MQTT-1.5.4-3`、`3.9.3-1/2`、`3.10.3-1/2`、`3.10.4-1`—`6`、`3.11.3-1/2`、`4.6.0-5/6`、`4.7.3-4` 的直接测试映射。

**结论**：批次 04—05 共 108 项去重输入在限定范围内证据充分，未发现运行时缺陷；增强认证 9 项编号条目维持 `UnsupportedConditional`，`3.6.2/3.7.2/3.9.2/3.11.2/3.14.2` 诊断属性尺寸限制属于无发送路径的适用性结论。F67-03 累计 **178/607** 去重输入完成实现侧复核（占 250 项 `Numbered` 中的 178 项），其余 **429** 输入（`MQTT-3.1.*`/`3.3.*` 剩余 72 项 `Numbered`、`Table` 58、`Section` 292、`Unnumbered` 7）及 **505** 表格行仍待，真实独立批准仍为 0。

### 2026-11-16 F67-03 批次 06—07 与 F67-02 批次 08：F6/F7 全部受控规范输入（Numbered / Table / Unnumbered / Section）实现侧复核收口（非独立批准）

**F67-03 批次 06 范围（72 项去重 `Numbered` 输入）**：MQTT5 `MQTT-3.1.0-1/2`（含 `3.1.0-1` ClientOnly）、`MQTT-3.1.2-1`—`19`、`24`—`30`（含 `3.1.2-30` ClientOnly）、`MQTT-3.1.3-1`—`5`、`9`—`12`、`MQTT-3.1.4-1`—`6`、`MQTT-3.3.1-1`—`13`、`MQTT-3.3.2-3`、`9`—`14`、`19`（含 `3.3.2-9/10` ClientOnly）、`MQTT-3.3.4-1`、`5`—`10`、`MQTT-4.2-1`。

**F67-02 批次 08 与 F67-03 批次 07 范围（全部非编号、表格与节条目）**：
- F6（MQTT 3.1.1）：`Unnumbered` 22 项（`UN-001`—`UN-022`：RFC2119 Definition、QoS1/2 分步骤、Retained/CONNACK 标志、`4.6` ClientContext、`7.1.1/7.1.2` Aggregate）、`Table` 56 项（42 `TargetedEvidence` + 14 `Informative`，覆盖 **339** 个表格行检查）、`Section` 219 项（`SECTION-029` 独立双向 Packet ID 矩阵 + 218 `Context` 节且无遗漏 `[MQTT-...]` 编号）。
- F7（MQTT 5.0）：`Unnumbered` 7 项（`UN-001`—`UN-007`：RFC2119 Definition、62 字符 1–23 字节 ClientID、Will/PUBLISH UTF-8 PFI=1、Connect Reason Code、`7.1.1/7.1.2` Aggregate）、`Table` 58 项（46 `TargetedEvidence` + 10 `Informative` + 2 `UnsupportedConditional`，覆盖 **505** 个表格行检查）、`Section` 292 项（`SECTION-028/106/114/252` 4 项 `TargetedEvidence` + 288 `Context` 节且无遗漏 `[MQTT-...]` 编号）。

**本轮发现的真实缺陷（`F67-G001`）与修正**：
- **MQTT5 会话接管未发送 `DISCONNECT 0x8E`（`MQTT-3.1.4-3`）**：原 `state.rs::attach` 取消旧连接 `cancel` 时，`connection.rs::run` 对所有 `local_cancel` 直接 `socket.close()`，导致被顶替的 MQTT 5.0 在线客户端只收到 TCP EOF 而未收到规范要求的 `DISCONNECT (Reason Code 0x8E: Session taken over)`。已在 `state.rs` 新增 `Connection::taken_over` 与 `State::takeover_flag`，并在 `connection.rs` 中仅针对被顶替的 MQTT 5.0 连接在 `250ms` 有界窗口内先写出 `[0xE0, 1, 0x8E]` 再关闭连接（不改变 MQTT 3.1.1、管理断连或关闭停机的行为，也不在半帧未写完时注入脏帧）。
- **MQTT5 不支持协议版本响应码（`MQTT-3.1.2-2`）**：在 `wire.rs` 新增 `Protocol::unsupported_protocol_connack`，当启用 MQTT5 的监听器收到协议级别 `> 5`（如版本 `6`）时返回有效 MQTT5 `CONNACK 0x84`（Unsupported Protocol Version）并关闭，保留版本 `<= 4` / 非 MQTT5 监听器的原有 `0x01` 兼容行为。
- **验证与精确映射补齐**：在 `crates/modorch-mqtt-core/src/tests/mqtt5_protocol.rs` 新增 `mqtt5_takeover_sends_disconnect_0x8e_and_enforces_connect_and_response_topic_rules`，实测验证 MQTT5 接管发出 `(0xE0, [0x8E])`、触发原连接 Will、版本 `6` 返回 `(0x20, [0, 0x84, 0])`、被拒 CONNECT 后续流水线 PUBLISH 不处理、`Response Topic` 通配符 `+`/`#` 拒绝及 62 个 ASCII 字符 1–23 字节 ClientID 全支持；并在 `contracts/protocol-review.json` 补齐 `MQTT-3.1.2-2`、`3.1.3-5`、`3.1.4-3/6`、`3.3.1-9`—`13`、`3.3.2-12/14`、`UN-002/003/005` 映射。

**结论**：F67-02（**437/437** 输入及 **339** 表格行）与 F67-03（**607/607** 输入及 **505** 表格行，11 项 Enhanced Auth `UnsupportedConditional` 维持排除边界）已全部完成实现侧复核；`F67-G001` 已修复并通过回归，**F67-04/05** 亦同步完成实现侧定界与修复补测（18 个 state/fault 矩阵 / 222 组合覆盖全部边界，无需新增矩阵）。

### 2026-11-16 F67-06：客户端互操作、wss 传输与 Feature / MSRV 组合核对（实现侧，非独立批准）

- **客户端与真实传输组合**：在当前源码上实测运行 Paho MQTT 2.1.0 全部三项专项——`verify-paho.py`（MQTT 3.1.1 TCP QoS 0/1/2 + Retained + SIGINT 检查式退出）、`verify-paho5.py`（MQTT 5.0 TCP 与 WebSocket QoS 0/1/2 + 属性 + Topic Alias + SubID + 非零 DISCONNECT Will + 连续 UNSUBSCRIBE）与 `verify-paho-tls.py`（MQTTS TLS 1.2+ + Argon2id Auth + 通配符 ACL 拒绝 + 错误密码隔离）；同时在 `crates/modorch-mqtt-core/src/tests/websocket.rs` 新增 `websocket_over_tls_mqtt5_qos2_and_plaintext_isolation`，补齐声明的 `websocket-tls`（`wss://`）+ MQTT 5.0 QoS 2 与明文拒绝运行证据。
- **Feature 子集与 MSRV 1.85.0**：验证 `modorch-mqtt-core` 全部 8 种 Feature 组合（`""`、`mqtt5`、`tls`、`websocket`、`mqtt5,tls`、`mqtt5,websocket`、`tls,websocket`、`mqtt5,tls,websocket`）与 `modorch-mqtt` 两版 Profile 精确 Feature 子集（F6 无 `mqtt5`/`websocket`/`bridge`，F7 无 `bridge`）编译通过；运行 `modorch-mqtt-core --no-default-features --lib`（117 项通过）与 F6 Profile 精确 Feature 集 `modorch-mqtt` 测试（16 项通过，0 失败）；运行 Rust `1.85.0` 全 Workspace `--all-targets --all-features` MSRV 编译检查通过。
- **平台与排除边界**：以上运行证据均为本机 `linux-x86_64`；`macos`/`windows` 继续按 `acceptance-profile.json` 保持 `NotRun` 显式边界。**真实独立批准仍为 0**，待 `F67-07`/`F67-08`。

### 2026-11-16 F67-07 第 1 轮独立评审与缺口收口（批准对当前输入失效）

- **第 1 轮评审**：由项目负责人认可的 AI 评审者在新会话中执行（未参与实现），针对旧输入 `source_digest 8c42ef1f…` 审毕全部 1,064 个对象和 5,250 个 check：Approved 1,028（mqtt311 437/437、mqtt5 572/607、矩阵 18/18、`mqtt311:acceptance`），Rejected 36（mqtt5），NotRun 0；严格门禁退出 1。评审材料保存在 Git 忽略目录 `validation/2026-11-16-f67-review/`。
- **缺口收口（实现方）**：登记为 `F67-G002`—`G012`，详见[待收口计划](../f6-f7-closure-plan.md)。`F67-G006` 是代码缺陷（入站显式 Payload Format Indicator=0 在转发时被删除，违反 MQTT-3.3.2-4“unaltered”），已修复：`MqttProperties` 新增向后兼容字段 `explicit_binary_payload`。其余 10 项为验证缺口，评审探针显示实现行为本来正确，本轮在 `tests/mqtt5_protocol.rs` 新增 11 个测试并补强 2 个现有测试，`contracts/protocol-review.json` 补充映射了 25 个 TargetedEvidence 对象，并在 11 个 UnsupportedConditional 对象的 `scope` 中写明拒绝证据测试名。`scripts/verify-paho5.py` 写明需要启用 `mqtt5`/`websocket` Feature 构建的二进制（评审观察 O-6）。
- **批准状态**：以上修改改变了受控输入，第 1 轮的全部批准对当前输入失效。当前输入的**真实独立批准为 0**，需要按新生成的未批准交接包进行第 2 轮复审。

### 2026-11-16 F67-07 第 2 轮独立复审：严格门禁通过

- **评审者与范围**：与第 1 轮是同一评审身份 `claude-opus-5.5-fresh-session-2026-11-16`（AI 评审，新会话，未参与实现；采用 AI 评审由项目负责人认可）。评审对象为未批准交接包 `validation/2026-11-16-f67-gap-closure/protocol-bundle.json`，绑定 `source_digest 46ae2010b6087737882367b0f893df27ca91a39760d6bffd2b0b862d49736a16` 和日志 `workspace.log`（sha256 `d53f665e…67f6`）。受控输入基于 HEAD `ea95ad6` 加上未提交的工作树改动。
- **结果**：1,064 / 1,064 个对象全部 Approved：mqtt311 437/437、mqtt5 607/607、矩阵 18/18，`mqtt311:acceptance` 与 `mqtt5:acceptance` 均为 Passed，Failed 0，NotRun 0。第 1 轮的 `F67-G002`—`G012` 经复核全部闭环，无新增缺口。
- **严格门禁**：`check-protocol-review.py --bundle validation/2026-11-16-f67-review-r2/protocol-bundle.reviewed.json --require-complete` 退出码 0，`validation_status = Passed`、`overall_status = Passed`。实现方收到评审结果后自行重跑了该命令，结果相同。评审材料保存在 Git 忽略目录 `validation/2026-11-16-f67-review-r2/`（`protocol-bundle.reviewed.json` sha256 前缀 `7d892c93…`）。
- **批准边界**：批准只覆盖 `contracts/acceptance-profile.json` 声明的范围：Server 角色；TCP/TLS/WebSocket/WebSocket-TLS；内存/SQLite 存储；`linux-x86_64`；不含 Bridge；Enhanced Authentication 只证实了拒绝行为，不代表已认证支持。不覆盖 macOS/Windows、硬件掉电、生产 SLO，也不覆盖 178 项发布义务（`check-release.py` 另行评审，当前批准仍为 0）。离线门禁不认证评审者身份，独立性由负责人对评审方式的认可来保证。
- **有效性条件**：本批准绑定上述 `source_digest`。今后任何受控输入（`crates/`、`contracts/`、`docs/`（`docs/validation/` 除外）、`scripts/`、`examples/`、`Cargo.*`、`README.md` 等）发生变化，批准即失效，需要重新生成交接包并复审。本节写在 `docs/validation/` 下，不属于受控输入，所以不会使批准失效。
- **评审观察（不阻塞）**：O-6，`check.sh` 默认构建的 `standalone-broker` 未启用 MQTT5/WebSocket，运行 `verify-paho5.py` 前需要按脚本说明带 Feature 构建。

此前一轮为“同号双向 QoS2 故障交叉与正 TTL 收口”。清理前记录的历史结果为：

| 检查 | 历史结论 |
|---|---|
| 工作区 Rust 测试 | 219 项通过，Core 179 项、Auth 11 项，无忽略测试 |
| Python/工具测试 | 53 项通过 |
| Rust 1.85 / 最小 Feature | 兼容检查通过 |
| Paho 互操作 | MQTT 3.1.1 TCP/TLS、MQTT5 TCP/WebSocket 专项通过 |
| 开发全检查 | fmt/check/clippy、契约精确映射、Rustdoc/doctest、示例及相关冒烟检查通过 |
| 协议/发布严格检查 | 结构与限定执行映射通过；因独立批准缺失，严格判定返回不完整，整体 Partial |

该轮未发现需要修改生产状态机的新缺陷，新增 50 个组合在已有实现上通过。部分写清理时 Session Expiry 设置错误和 ignored 子进程夹具问题已纠正；它们是验证夹具问题，不能登记为未修复的产品缺陷。

<a id="protocol"></a>

## 协议覆盖基线与最新组合

| 项目 | MQTT 3.1.1 / F6 | MQTT5 / F7 |
|---|---|---|
| 受控规范输入 | 437 | 607 |
| 表格行检查 | 339 | 505 |
| TargetedEvidence 限定映射 | 190 | 281 |
| 真实独立批准 | 0 | 0 |

合计 18 个有限矩阵、222 个组合、1,064 个评审对象：1,044 个规范输入 + 18 个矩阵 + 两版 acceptance。844 个表格行检查位于对象内部。当前精确要求以[协议契约](../../contracts/protocol-review.json)为准，不用计数推断未实现功能。

最新新增 50 个组合已完成：

| 场景 | 组合数 | 验证边界 |
|---|---|---|
| 同 Packet ID 双向 QoS2 提交故障与恢复 | 24 | TCP/SQLite，提交前失败、丢回执、提交后 Query 不可用 |
| 同号双向 QoS2 部分 ACK 写中断 | 16 | 单字节内存传输，Owner 取消/EOF；不等同真实网络或崩溃 |
| 同号双向 QoS2 双 SIGKILL 恢复 | 4 | 当前 Unix 宿主，每组合两次进程 SIGKILL；不等同硬件断电 |
| MQTT5 正 TTL 跨阶段、到期与重启 | 6 | 真实 4 秒 TTL、选定阶段和重启顺序；不等同全部时钟/TTL 边界 |

<a id="capacity"></a>

## 容量与历史异常结论

已有本机 TLS/Auth/SQLite/QoS2 持续负载、慢消费者、活跃交换中退出及原库恢复测量；最终限定配置下 debug/release 各两次四客户端并发负载成功，另有两批共 1,984 项存储扩压通过的历史结论。

较早未优化配置的失败不能由后续成功抵销；缺少阶段信息的旧超时/EOF 不统一倒推根因。本机短测、选定构建 Profile 和进程 SIGKILL 都不构成生产 SLO、长期 RSS、任意负载或硬件断电批准。

<a id="limitations"></a>

## 仍待收口

- F6/F7：F67-07 已在 `source_digest 46ae2010…` 上通过严格协议门禁（见上文第 2 轮记录）；受控输入一旦变化就需要重新复审。F67-08 中受控文档（`docs/f6-f7-closure-plan.md`、`docs/remaining-work.md`）的状态同步会改变受控输入，需要与重新复审一并安排。
- F7：11 项 Enhanced Authentication 条件输入保持不支持的适用性边界；新增支持需要另列开发任务。
- 生产 SLO、长期 RSS、代表性负载和退出/恢复时限按用户选择保留待验收。
- 外部宿主 Profile、magicCommon、远端 CI、跨平台/文件系统、硬件断电及独立安全/性能/接入/发布仍待真实执行。
- 178 项发布义务真实批准仍为 0，协议检查通过不自动批准生产发布。

<a id="cleanup"></a>

## 归档清理结论

此前已删除 `target/` 及两处 Python 缓存，共 6,773 个可再生文件，约 2 GiB。按用户本次要求，进一步清理 48 个逐轮验证目录及其过程产物，原有 2,037 个验证文件精简为本文件；包含 39 个测试 SQLite 数据库、日志、探针、重复摘要与未批准计划。

保留产品源码、测试实现、测试证书、契约及规范正文索引、设计、接入文档、项目 skill、锁文件和待收口计划。文档历史引用统一指向本结论摘要；原始材料已不在仓库，不能据此重新校验历史日志或沿用旧批准。该轮未打包、提交或推送；后续本地提交为 MQTT `59dff8d`、magicDataHub `41eecf2`，仍未形成 registry/发布或远端 CI 证据。

<a id="artifact-policy"></a>

## 后续记录规则

仓库持续维护本最终记录和待收口清单。后续验证使用 Git 忽略的 `validation/<轮次>/` 等本地目录；原始材料在核对期间保留，需要严格门禁时必须提供真实的同轮日志与独立评审。归档前将结论、范围、失败处理及遗留项更新至本页，再移除过程产物。CI 制品按 CI 自身保留策略管理，不回填大量临时文件到源码归档。

归档清理不降低协议/发布门禁要求。最终记录只是结论摘要，不能充当原始执行材料、评审报告或真实独立批准；原材料清理后如需验证对应当前输入，须重新执行并生成有效评审包。

本次整理已实际通过：交付物/本地链接检查、178 项发布目录校验、协议清单结构检查、53 项 Python/工具回归、项目 skill 合规与链接检查、Git 差异检查，以及最终记录锚点/忽略规则核对。166 个受保护的源码、测试、契约和 skill 等文件摘要保持不变；未重新生成构建或 Python 缓存。上述检查未重新执行 Rust、客户端互操作、负载或完整协议验收，独立批准仍为 0。
