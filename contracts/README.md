# QoS0/1/2、安全运维与持久化实验实现契约

适用本仓库 `0.1.0-alpha.1`，依赖 Modorch-RS `0.0.3 / 336ba20196044e1b81fb2ca297a18b5e258a5800`。完整设计列出全部未来 MUST；本表只声明当前子集的可观察行为。[验证结论摘要](../docs/validation/README.md#implementation)须与源码/测试一起阅读，测试存在不等于已运行。

| 设计条款 | 当前范围 | 测试（除注明外位于 Core `src/tests.rs`） |
|---|---|---|
| MQTT-ARCH-001 / ARCH-002 | 一个宿主 Application/Hub；API/Core/Facade 单向依赖；Bundle 不启动，必须显式选择 | registration_is_explicit_and_status_uses_the_hosts_hub；existing-modorch-server --once |
| MQTT-LIFE-001 | 所有 ready 通过后 listen，连接/监听均为受管任务 | all_ready_before_listen_and_listener_spawn_quota_compensates_bound_socket；cancel_start_while_ready_still_completes_owned_compensation |
| MQTT-LIFE-002 | 后续入口失败、Listener/连接任务限额均无端口或任务泄漏 | later_activation_failure_and_listener_quota_release_the_port；all_ready_before_listen_and_listener_spawn_quota_compensates_bound_socket |
| MQTT-LIFE-003 / STATE-004 | 旧 generation/connection ID 拒绝；Core 重建及一次激活失败补偿保留 Factory 内存 | restart_preserves_memory_sessions_retained_inflight_and_fences_old_generation |
| MQTT-TOPIC-001 | Topic/Filter 空层、`+/#`、`$` 匹配；合法长 Topic 不受 Hub 1024 字节预算影响 | codec::tests::mqtt_topic_edges；long_topics_qos0_self_echo_and_reserved_namespaces |
| MQTT-ROUTE-001 | 每 Session 订阅独立，重叠按最高匹配 QoS、受原发布 QoS 限制 | qos1_overlap_retained_delete_and_unsubscribe；rumqttc 集成测试 |
| MQTT-QOS-001（子集） | QoS0/1、Packet ID/PUBACK、离线积压和重连 DUP | offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup；saturation_rejects_entire_publish_without_retained_or_partial_delivery |
| MQTT-QOS-002 | 双向 QoS2 FSM 和持久重连已实现；完整3.1.1矩阵待验收 | 见文末 F6 映射 |
| MQTT-STATE-001（内存） | Clean Session、Session Present、Retained 回放/删除、Will、无进程重启持久化 | offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup；qos1_overlap_retained_delete_and_unsubscribe；will_on_abrupt_close_and_takeover_but_not_disconnect |
| MQTT-SEC-001 / SEC-003（匿名子集） | 显式匿名回环配置；PUBLISH/SUBSCRIBE/Will 的保留命名空间拒绝发生在接管前；畸形连接不升级为组件故障 | configuration_rejects_unsafe_and_overflowing_limits；long_topics_qos0_self_echo_and_reserved_namespaces；malformed_client_and_invalid_qos_close_only_that_connection |
| MQTT-BOUND-001 / BOUND-002（内存） | 连接/任务/记账内存与可靠积压有界；整次可靠发布接纳或拒绝；已接纳消息不淘汰 | application_isolation_and_memory_quota_release；saturation_rejects_entire_publish_without_retained_or_partial_delivery；codec::tests::fragmented_length_and_malformed_frames_are_bounded |
| MQTT-EVENT-001 | 原 Topic 的类型化入站观察；出站仅显式、可信组件来源的直接 Request；无默认回环 | `src/tests/eventhub.rs`：inbound_fans_out_typed_events_and_self_subscription_has_no_bridge_loop；outbound_receipts_idempotency_source_isolation_and_frozen_targets；control_rejects_spoofed_source_invalid_dtos_and_non_requests_without_restart |
| MQTT-BOUND-001 / EVENT-001（F4） | Hub 无匹配/满邮箱/部分 fanout/Topic 及字节预算拒绝不影响 MQTT；内部发布账本有界、记账且不淘汰 | observer_backpressure_partial_fanout_long_topics_and_no_match_do_not_change_puback；observation_declares_payload_cost_and_hub_byte_rejection_preserves_mqtt_routing；ledger_is_bounded_and_atomic_routing_rejections_are_queryable；ledger_reservations_share_global_budget_and_do_not_mutate_on_exhaustion |
| MQTT-STATE-004 / MSG-REQ-004（F4） | 幂等结果跨 Core 重建保留；来源隔离；不确定结果由同源查询恢复；旧 generation 拒绝 | publish_ledger_survives_reconstruction_and_stale_handler_cannot_mutate；caller_indeterminate_after_commit_resolves_with_same_source_query_and_retry |
| MQTT-COMPAT-001 | 固定 Git rev、统一 Facade 来源、MSRV 1.85 | Cargo.lock；framework-preflight.log；msrv.log |

KeepAlive/握手超时与分片输入由 `handshake_timeout_keepalive_will_and_fragmented_packets` 验证。跨 Application 状态隔离由 `application_isolation_and_memory_quota_release` 验证。独立客户端由 `crates/modorch-mqtt/tests/rumqttc.rs` 和 `scripts/verify-paho.py` 验证；后者单独运行，不混入 Cargo 的 Passed 数量。

F5 的持久 Store/STATE-002/STATE-003 已实现下表的受限全量 image 方案。F6 双向 QoS2 已实现下表范围；完整3.1.1符合性、F7 MQTT5 实验功能已接入，完整规范、生产性能和跨平台门禁仍未验收。Auth、TLS、Admin、Metrics 的历史结论见 [验证结论摘要](../docs/validation/README.md#implementation)。F0 的 prelisten Storage Request 恢复门禁在本轮 Storage 测试中补充；F4 与安全/运维历史记录仍保留原内存范围。

## F5 安全与运维映射

本表只覆盖已实现的内存 QoS0/1 版本，不能替代完整 F5 的持久存储门禁。

| 条款/能力 | 实现边界 | 测试文件及关键用例 |
|---|---|---|
| MQTT-SEC-001 / SEC-002 | Argon2id/ACL、Core owner 强制校验、恢复订阅重新授权、CAS防竞态、期限失败关闭 | Core `src/tests/auth.rs` 的授权/归属/期限用例；Auth `tests/security.rs` 的身份隔离与过期用例 |
| MQTT-SEC-003 | Will 参数/Retain/策略绑定，授权失败先于接管；保存 Grant | Auth `will_authorization_precedes_takeover_and_subscription_acl_cannot_be_widened`；Core `authorization_timeout_and_mismatched_grant_never_commit_or_restart_core` |
| 内部来源 ACL | 配置可信 Core 才能转述业务来源，无策略拒绝，无凭据泄漏 | Auth `internal_publications_require_component_policy_and_provider_refuses_forged_origin` |
| TLS / 公网准入 | 未信任证书、明文和握手超时只影响连接；取消/加载失败归还资源；公网必须Auth+TLS | Core `src/tests/tls.rs` 的3个用例 |
| MQTT-BOUND-001 | 按IP并发、固定窗口与有界桶，握手前准入，内存/任务配额继续生效 | Core `peer_ip_handshake_admission_and_backoff_are_bounded_before_authentication` |
| Admin | 独立选择；两级来源限制、分页、元数据审计、按连接ID断连 | Extensions `administration_is_source_protected_paged_audited_and_disconnect_is_connection_fenced` |
| Metrics | 真实IO计数、固定名字无敏感标签、受管任务取消和配额补偿 | Extensions `metrics_pull_real_counters_without_client_or_topic_labels_and_stop_cleanly`；`metrics_task_quota_failure_compensates_the_already_active_broker` |
| 独立扩展 | 注册不启用、不隐式开放网络入口 | Extensions `optional_factories_do_not_enable_themselves_or_export_listeners`；secure-modorch-server --demo --once |
| 性能测量方法 | release loopback 1/10/100/500 clients × QoS0/1，可复现配置与PUB→SUB统计 | `scripts/benchmark.py`，结果不代表生产容量验收 |


## F5 Storage 映射

[验证结论摘要](../docs/validation/README.md#implementation)汇总历史执行结论，原始过程材料已清理；实现边界见 [storage.md](../docs/storage.md)。下列用例除注明外均位于 Core `src/tests/storage.rs`。

| 条款/能力 | 实现边界 | 测试 |
|---|---|---|
| MQTT-STATE-002 | 独立 Provider 的定向 Request 恢复/校验/normalization 完成后才 listen，失败无端口 | corrupt_recovery_page_and_reduced_budget_never_open_listener_or_discard_committed_data |
| MQTT-STATE-003 | 有界字节分页完整 image；离线订阅先于重连恢复、Retained、Inflight ID/DUP | sqlite_restart_restores_offline_routes_retained_and_inflight_before_subscriber_reconnect；scripts/verify-storage.py |
| MQTT-STATE-004 / writer fencing | 单库独占、epoch/CAS/SHA256、Complete/Abort fencing；无空库回退 | Provider `src/lib.rs`：sqlite_epoch_cas_commit_query_and_recovery_abort_are_fenced_and_idempotent |
| MQTT-QOS-001（QoS1持久子集） | 磁盘确认前无 PUBACK/下行，丢回执查询同操作 | durable_commit_barrier_blocks_puback_and_downstream_delivery_and_resolves_lost_reply |
| MQTT-BOUND-002 / commit failure | 失败停止本代、无 ACK/下行，恢复仅最后 committed image | failed_store_commit_sends_no_ack_or_delivery_and_restart_uses_last_committed_image |
| MQTT-SEC-002 / SEC-003（持久子集） | 跨重启 owner 与 Will Grant 恢复，不允许跨身份抢占/清除 | durable_owner_and_authorized_will_survive_restart_without_credential_storage |
| MQTT-EVENT-001 / 业务幂等 | DurableCommitted/原终态跨新 Application，同源隔离、内容冲突和私有 Store capability | durable_internal_idempotency_and_original_receipt_survive_a_new_application |
| MQTT-BOUND-001（持久子集） | image/内存/磁盘页限、摘要、独占身份，DiskFull 不改变 committed version | Provider sqlite_capacity_integrity_and_exclusive_database_identity_are_enforced |
| 数据库接入 | 拒绝不相关库与非4096字节页库，不改写原文件 | Provider sqlite_refuses_unrelated_databases_and_nonstandard_pages_without_rewriting_them |
| MQTT-LIFE-001/002（持久组合） | 真实SIGKILL后预监听恢复，SIGINT检查式退出和端口归还 | scripts/verify-storage.py |

停止时不新增关键持久提交，已提交的 armed Will 在下一次启动收尾；不保证停机 Will 立即落盘外发。SQLite FULL 的保证依赖本地文件系统/设备正确实现 fsync；尚无真实硬件断电、存储生产吞吐和跨平台门禁证据。快照已在 F6 扩展为包含 QoS2 的 format3（旧 format2 多 Release 的升级限制见 storage.md），完整 MQTT3.1.1/F5 仍未通过全部门禁。


## F6 QoS2 映射

历史验证结论见 [验证结论摘要](../docs/validation/README.md#protocol)。协议解释见 [qos2.md](../docs/qos2.md)。本表只声明已运行覆盖的行为，完整协议符合性和发布状态仍为 Partial。

| 契约 | 实现和测试 |
|---|---|
| MQTT-QOS-001/002 接收端 | `src/tests/qos2.rs`：qos2_incoming_duplicate_release_and_identifier_reuse_route_exactly_once；qos2_packet_id_dedup_ignores_changed_payload_and_clean_session_resets_exchange |
| MQTT-QOS-002 发送端 | qos2_outgoing_reconnect_resumes_publish_or_pubrel_and_wrong_acks_cannot_free_id；qos2_reliable_window_preserves_publish_order_and_releases_capacity_only_on_pubcomp |
| MQTT-BOUND-001/002 | qos2_receiver_count_and_byte_limits_reject_before_pubrec_and_resume_after_release |
| MQTT-STATE-002/003/004 | 内存代际用例 qos2_memory_generation_recovery_preserves_receiver_and_sender_phase；Storage 三个 qos2_* 用例覆盖新Application恢复、四阶段提交屏障和失败不推进wire；scripts/verify-storage.py 覆盖真实SIGKILL |
| MQTT-STATE-003 快照兼容 | `state/snapshot.rs` legacy_snapshot_upgrades_and_invalid_qos2_phase_or_quotas_fail_without_replacing_state |
| MQTT-SEC-001/003 | Auth qos2_publish_acl_uses_original_qos_and_retries_do_not_create_another_exchange；qos2_will_and_retained_replay_use_full_receiver_handshake；Paho TLS/密码/QoS2 |
| MQTT-EVENT-001 | qos2_internal_publication_and_network_duplicate_preserve_single_business_observation |
| 编解码 | qos2_control_flags_identifiers_and_complete_lengths_are_validated |
| 独立客户端 | rumqttc_qos0_qos1_qos2_retained_self_delivery_and_graceful_stop；scripts/verify-paho.py；scripts/verify-paho-tls.py |

## F6 规范场景与 PUBREL 顺序补充

当前受控清单为 [mqtt311.json](mqtt311.json)，说明见[符合性范围](../docs/mqtt311-conformance.md)，最新执行见[验证结论摘要](../docs/validation/README.md#protocol)。条款编号来自官方 OASIS 3.1.1 OS 文本，保留源 URL/SHA256；140个编号不等于全部规范要求。13组场景关联62个编号，78个未映射编号保留 NotRun；`TargetedEvidence` 只代表所述场景已执行，不能将整条规范或完整 F6 标为 Passed。

`check.sh` 现在通过 `scripts/check-conformance.py` 核对每个指定测试的实际 `test ... ok` 行；缺失、ignored、失败或只有测试总数均不能通过。门禁自身有4个正反向测试。PUBREL按PUBREC（包括重复）进入有界FIFO，确认屏障保持；format3持久化恢复顺序，旧format2多个Release拒绝歧义升级。ClientID配置下限为23字节，覆盖1..23字节必需ID、BOM/Unicode不归一化和独立连接失败隔离。

## F6 发布/订阅和会话协议收口补充

最新执行见[验证结论摘要](../docs/validation/README.md#protocol)。当前[mqtt311.json](mqtt311.json)扩展至31组限定场景，关联128个规范编号；12个未映射编号仍NotRun，并逐项说明客户端职责（7项）、可选WebSocket（4项）和条件性Bridge（1项）。适用性信息必须精确覆盖未映射集合且有依据；日志门禁不据此把缺失证据变成通过，自身增至6项正反向测试。

新增协议证据覆盖九种发布/订阅QoS组合、混合SUBACK及重复过滤器替换、Retained替换/删除/标志、UNSUB后未完成QoS1/2、CleanSession双向QoS2清理/Retained独立、离线新QoS2、UTF8多字段及65535字节Topic/Filter、Will六组合、PacketID回绕与双向命名空间。CONNECT失败分类限定标准码：存储不可用不再兜底为ID拒绝；无法归类时无CONNACK关闭。

128个编号仅有指定轨迹的TargetedEvidence，不代表完整条款或完整F6认证；未编号MUST、表格/FSM全面审计和更广异常链路仍待收口。前文数字为各轮历史时点。


## F6 Topic共享向量与F8开发门禁

当前规范清单增至32组限定场景，编号覆盖仍为128个TargetedEvidence、12个NotRun。`topic-vectors.json`是上游固定rev的39组Rust独立预期向量的规范化JSON导出，来源/摘要随文件保存。`tests::topic_vectors::*`分别核对Core/Facade、真实Hub订阅与Handler、真实MQTT验证与投递；magicCommon端尚未执行。

`scripts/check-project.py`阻止缺失设计要求的开发文档/Skill/CI入口和失效本地链接，`check.sh`运行该检查及正反向门禁测试。`check-conformance.py --require-complete`拒绝将Partial转为完整规范发布。MQTT-DOC-001当前文档/Skill子集得到补充，完整MUST与独立Agent/平台/性能发布门禁仍未完成。实际记录见[验证结论摘要](../docs/validation/README.md#verification)。


## F6 正文候选、报文表与ACK转换

[mqtt311-audit.json](mqtt311-audit.json)维护固定官方来源的9段未编号关键词候选、8张相关结构表、7个非规范例表/图及发送端ACK转换范围。9段候选中3段是MQTT-4.3.2-1续文，不能重复计为新义务；总体符合性聚合项保留NotRun。`audit-mqtt311.py`从本地官方HTML核验来源摘要和正文段落清单，不抓取网络。

[mqtt311-packets.json](mqtt311-packets.json)是独立预期，驱动256首字节、256 CONNECT flags、长度边界/ID和12项QoS2发送ACK转换；真实TCP同时验证隔离、Will和持久会话。Codec提前拒绝非法头部/长度，decode只接收恰好一个完整帧。规范日志清单现35组场景，编号覆盖仍128个TargetedEvidence/12个NotRun；新审计清单的所有限定测试也必须真实通过。完整F6/F8仍Partial，细节见[审计说明](../docs/mqtt311-audit.md)与[验证结论摘要](../docs/validation/README.md#verification)。


## F6 连接期限与事务排空

本轮实测见[验证结论摘要](../docs/validation/README.md#verification)。MQTT-3.1.2-24的后端等待轨迹映射到`mqtt311.json`的两组新增场景；`mqtt311-audit.json`中的`FSM-CONNECTION-DEADLINES`只标限定证据。

| 能力 | 实现及用例 |
|---|---|
| MQTT-BOUND-001 / MQTT-SEC-001 | `tests::auth::keepalive_and_principal_expiry_close_during_authorization_and_ignore_late_grants`：Publish/Subscribe等待中关闭，迟到授权不能写入；长有效期不截断为24小时 |
| MQTT-QOS-002 / 持久提交 | `tests::storage::keepalive_closes_socket_during_commit_then_drains_without_poison_or_qos2_reroute`：先关闭网络，提交结果确定后原ID重传不重复路由；TLS另有真实连接测试 |
| MQTT-STATE-001 / 连接fencing | CONNECT等待gate超时不attach，已开始attach提交则排空并清理；已连接发布等待gate时超时不接纳 |
| MQTT-BOUND-001 / 写端期限 | `connection::tests::stalled_frame_write_expires_despite_incoming_ping_traffic`：一字节duplex制造整帧背压，输入PING不能重置期限 |
| MQTT-LIFE-001 / 资源 | `tests::storage::shutdown_closes_socket_and_waits_for_committed_receipt_before_releasing_resources`：检查式停止先关网，等已有回执后才归还许可/内存，重启可恢复 |

CONNECT/写入/身份期限是本地策略，不能当作新增MQTT规范义务。完整FSM、故障总矩阵及完整发布仍Partial。


## Core TOML配置与装配

MQTT-CONFIG-001（配置入口限定合同）：Facade `toml`默认关闭，文档限64KiB、Core默认禁用，未知/不支持/歧义/不安全设置拒绝；解析错误不回显原文，Provider capability由宿主注入，注册与选择分离。Core参数使用同一Rust validate，Provider内部配置仍由宿主负责。

精确测试位于Facade `tests/config.rs`：configuration_and_registration_never_enable_components_implicitly；strict_schema_rejects_unknown_unsupported_ambiguous_and_unsafe_settings；every_core_limit_and_millisecond_duration_is_mapped_and_defaults_match_rust；parser_errors_debug_and_size_limit_do_not_echo_source_or_secrets；provider_references_require_host_capability_and_do_not_register_factories；tls_paths_use_explicit_base_without_reading_files_and_require_feature；enabled_toml_profile_uses_host_hub_and_real_tcp_qos1_then_checked_shutdown；explicit_host_factories_match_toml_refs_tls_and_prelisten_storage_recovery。子进程协议/CLI验证为 `scripts/verify-toml.py`；[验证结论摘要](../docs/validation/README.md#implementation)不代表MQTT规范矩阵增加或完整发布通过。


## MUST机器映射与发布流程

[project-must.json](project-must.json)覆盖设计§15全部21项及CONFIG-001；精确测试和源文件映射只声明TargetedEvidence。[mqtt311-text.json](mqtt311-text.json)保存117段正文/列表项，覆盖140编号，需与跨段续文和表格审计共同审核。[release-policy.json](release-policy.json)列出严格发布目标及10类报告必需检查。

`check.sh`逐项核验22项MUST的测试日志。`check-release.py`将128个适用规范编号、18个审计项、22个项目合同和10个发布域共178项纳入当前源码绑定的独立评审；UN-008为推导项。删项、陈旧输入/评审、篡改日志、自评或未完整执行的报告均不能批准。详见[发布流程](../docs/release.md)和[验证结论摘要](../docs/validation/README.md#protocol)。当前完整发布仍Partial；原有场景门禁继续限定证据语义。


## F6服务端输出与慢消费者故障矩阵

[mqtt311-outbound.json](mqtt311-outbound.json)维护独立线帧预期：16个控制响应、14个PUBLISH标志/字段向量、12个RL边界，加CONNACK新建/恢复/Clean及4种实际拒绝上下文。`tests::outbound::*`的5项测试在编码层及真实TCP比较完整字节，期望不经过生产Codec生成或解码。

`connection::tests::partial_publish_write_timeout_preserves_qos1_and_qos2_for_dup_reconnect`用一字节duplex确定性模拟半个PUBLISH写超时，核对保留消息、原ID/DUP及重新握手完成；不是持久/TLS/内核故障全矩阵。`tests::qos2::qos2_slow_consumer_pubcomp_backlog_rejects_atomically_and_same_id_retry_can_commit`在真实TCP窗口1/积压2下验证原子拒绝、Retained不变、无接收去重幽灵记录及容量释放后的同ID重试。

三组新场景使受控场景数成为40；编号覆盖仍128项TargetedEvidence/12项范围说明，限定审计增至17项。新增两个审计入口，发布评审总数178。完整FSM、长期RSS/生产慢消费者/SLO及独立发布审查仍未通过。

## 可选功能合同

MQTT5、WebSocket、Bridge、bootstrap、动态策略/证书、HTTP 与持久审计的实际范围见[可选功能](../docs/optional-features.md)。当前写 format5，读取1/2/3/4；历史 format3 记录保持其原验证时点。独立字节、真实 MQTT5/Paho、恢复和退出测试只构成专项证据，不升级现有发布批准。

本轮新增6个精确测试映射，覆盖在线零过期Session接管、目标Session丢失/恢复的Bridge单交换重发、128普通属性加防环元数据端到端传递、属性限额和幂等恢复预算/校验。`mqtt311.json`及`mqtt311-audit.json`中的可选WebSocket/Bridge范围说明已同步当前实验实现；它们仍在既定TCP/TLS/SQLite发布Profile之外，不据此批准完整符合性。执行材料见[验证结论摘要](../docs/validation/README.md#closed-issues)。

Topic Alias恢复修复增加3个精确可选测试映射：合法别名展开及绑定帧边界、SQLite Retained/双向QoS2与真实属性恢复、非法/重连失效/超线帧和预算不足的原子拒绝。7组可选场景现有31个映射；41组3.1.1限定场景中的混合监听恢复关联同一SQLite测试。format5不变，完整规范/独立发布审查仍Partial，材料见[验证结论摘要](../docs/validation/README.md#closed-issues)。

源会话修复为OPT-BRIDGE增加2个精确映射，覆盖源Session代际/旧ACK隔离、延后新订阅、同/不同ID、连续源丢失、两端同时丢失、源恢复及重连期间目标完成确认。当前7组可选场景共33个映射；3.1.1仍为41组限定场景，发布审查仍未批准。材料见[验证结论摘要](../docs/validation/README.md#closed-issues)。

## 协议完整性增补

[mqtt311-text.json](mqtt311-text.json)为4.3.2/4.3.3的四个孤立编号绑定15条前置义务和角色上下文；新增16组合接收端恢复测试进入mqtt311.json。发布目录仍178项，但评审对象包含补齐的正文，旧摘要不能复用。

[mqtt5-text.json](mqtt5-text.json)是独立OASIS来源的216段/250编号正文索引；`scripts/audit-mqtt5.py`以本地固定来源核对，不执行网络下载，也不批准完整符合性。可选功能契约增加6项精确回归，覆盖原因码/属性表、Will断开/重启及重复取消订阅。依据与范围见[协议完整性](../docs/protocol-integrity.md)。


## F6/F7完整核对输入与有限模型

[protocol-review.json](protocol-review.json)绑定390个编号、29个未编号候选和114个完整表格/图形布局。来源补充分别为[mqtt311-review-inputs.json](mqtt311-review-inputs.json)及[mqtt5-review-inputs.json](mqtt5-review-inputs.json)；`audit-protocol.py`以显式本地HTML校验固定来源和每个单元格/跨行跨列信息，不下载或批准语义。

`check-protocol-review.py --results CARGO_LOG`核验逐条摘要、角色、范围、实现路径与唯一精确通过日志；有限状态/故障矩阵另核验维度和组合数。该检查已接入check.sh。表格语义实审和独立批准不足仍保留NotRun，完整规范保持Partial；`--require-complete`不能用于绕过缺失评审。


## 完整章节与真实评审导入

完整正文新增3.1.1的219个章节和MQTT5的292个章节，合计1,044项评审输入。关键词之外的行为说明也进入摘要；MQTT5 SECTION-252明确包含唯一共享成员的离线行为。共享订阅新增12组合选择矩阵及内存/TCP/SQLite/预算回归，精确映射加入optional-functional.json。

`check-protocol-review.py --prepare`生成未批准计划，`--bundle PATH --require-complete`导入真实独立材料，检查动态源码/原始日志、身份分离、完整义务/矩阵/Profile及材料摘要。合成单元测试验证可通过路径，当前实际独立批准仍为0。字段、命令与边界见[评审流程](../docs/protocol-review.md)。

[验证结论摘要](../docs/validation/README.md#closed-issues)修正MQTT5两段含Server语义的ClientOnly分类，补12个部分ACK写入组合及精确TCP/重连/重复完成映射。7个有限矩阵共70组合，1053个对象仍须当前输入的真实独立实审；不是完整符合性批准。
