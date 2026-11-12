# F6/F7 待收口计划与跟踪清单

更新日期：2026-11-12。F6 为 MQTT 3.1.1 完整协议验收，F7 为 MQTT 5.0 完整协议验收，当前均为 **Partial**。

本页将当前核对结果整理为后续执行任务。当前没有新增已确认的运行时代码缺陷；未完成语义审查、验证覆盖或独立验收，不直接等于功能未实现。最新宿主样例/通用恢复验证更新见文末；完整语义审查和新的独立批准仍未完成。

## 依据与当前基线

- 需求依据：[完整设计](modorch_mqtt_broker_full_design_0.0.3.md) §11–§15、§30–§59。
- 当前状态：[剩余需求与验收边界](remaining-work.md)。历史执行结论汇总于最终记录，逐轮过程材料已按用户要求清理。
- 执行清单：[协议审查契约](../contracts/protocol-review.json)，保留规范原文、角色、适用条件、实现位置及限定测试映射。
- 评审格式与门禁：[完整协议评审材料](protocol-review.md)。本页跟踪任务，不替代机器可校验的契约和评审包。
- 最近功能验证：[最终验证结论](validation/README.md#verification)。下列基线引用清理前历史执行摘要；最新限定回归另列文末。摘要不能替代原始执行材料。

| 基线项目 | F6 / MQTT 3.1.1 | F7 / MQTT 5.0 |
|---|---|---|
| 受控规范审查输入 | 437 | 607 |
| 表格行检查 | 339 | 505 |
| TargetedEvidence 限定映射 | 190 | 281 |
| 真实独立批准 | 0 | 0 |
| 完整协议状态 | Partial | Partial |

两版协议合计 1,044 个规范输入、18 个有限状态/故障矩阵、222 个组合，加上两版 acceptance，共 1,064 个评审对象。844 个表格行检查属于对象内部检查，不额外增加对象数。限定映射数量不用于计算“剩余未实现功能数”。

最近一轮 219 项 Rust 测试、53 项工具测试、MSRV 1.85、最小 Feature 及 Paho 专项通过，没有忽略测试。已完成的 50 个新增组合为：

| 已完成验证 | 组合数 | 保留的证据边界 |
|---|---|---|
| 同 Packet ID 双向 QoS2 提交故障与恢复 | 24 | 提交前失败、丢回执、提交后 Query 不可用；真实 TCP/SQLite |
| 同号双向 QoS2 部分 ACK 写中断 | 16 | 单字节内存传输，Owner 取消/对端 EOF；不扩大为真实网络部分写或崩溃证据 |
| 同号双向 QoS2 双 SIGKILL 恢复 | 4 | 当前 Unix 宿主，每组合两次 SIGKILL；不等同硬件断电 |
| MQTT5 正 TTL 跨阶段、到期与重启 | 6 | 真实 4 秒 TTL、选定阶段及重启顺序；不代表全部时间边界 |

上述内容作为后续回归基线，不再登记为尚未实现的优先场景。R-19（ACK 原因码角色）和 V-04（物理认证任务生命周期）保持关闭；出现新反例时再关联重新核对。

## 收口范围与原则

先完成当前声明能力范围内的协议验收。F7 的 Enhanced Authentication / AUTH 仍明确不支持，对应 11 项 UnsupportedConditional；保留适用性说明和拒绝行为证据。若后续决定启用该能力，再新增实现与验收任务，不在本计划中默认扩展范围。

“其他状态/故障/客户端组合”目前尚未形成逐条审定的缺口列表。先按规范义务梳理必需组合、等价类和排除理由，再补测试；不要求无边界地穷举全部笛卡尔积，也不把当前未列明的组合描述成已确认缺陷。组合选择及覆盖充分性最终需要独立评审。

每条任务分别跟踪实现、验证和独立评审状态。测试通过只能说明对应断言通过，不能自动升级完整符合性；修复者不能通过更换身份名称给自己独立批准。

## 待收口任务

任务编号保持稳定；后续按编号引用和更新。下表状态指本计划的交付物，不否定已有实现或历史专项结果。

| 编号 | 待收口项 | 当前状态 | 交付物 | 完成标准 |
|---|---|---|---|---|
| F67-01 | 固定两版验收 Profile | 实现侧 v1 已固化到 [acceptance-profile.json](../contracts/acceptance-profile.json)；协议、角色、传输、存储、Feature、客户端、平台证据及排除边界已机器校验；独立审查仍 NotRun，工具宿主声明不能代替全部库验收 Profile | 协议、角色、传输、存储、Feature、宿主/平台及排除条件清单 | 两版范围明确，条件能力与现有声明一致，范围变化可追溯；工具宿主声明不能代替全部库验收 Profile |
| F67-02 | F6 完整语义审查 | 待完成；批次 01—04 已完成 `MQTT-3.3.1-4`—`-12`、`MQTT-3.3.5-1`、`MQTT-3.8.4-1`—`-6`、`MQTT-3.10.1/3.10.3/3.10.4`、`MQTT-3.14.1/3.14.4` 共 28/437 输入实现侧复核；其余 409 输入及 339 表格行仍待；独立批准 NotRun | 437 输入及其中 339 表格行的逐项核对记录 | 完整正文、关联上下文、角色、适用性及断言充分性均有结论；缺口逐条登记 |
| F67-03 | F7 完整语义审查 | 待完成；批次 01/02 已复核 `MQTT-3.3.2-1/2/4/5/6/7/8/15/16/17/18/20`、`MQTT-3.3.4-2/3/4`、`MQTT-3.4.2-1/2/3`、`MQTT-3.5.2-1/2/3`、`MQTT-3.8.3-1`—`-5`、`MQTT-3.8.4-1`—`-8`、`MQTT-4.9.0-1/2/3` 共 37/607 输入；其余 570 输入及 505 表格行仍待；独立批准 NotRun | 607 输入及其中 505 表格行的逐项核对记录 | 同 F67-02；属性、原因码发送角色及条件能力均结合完整上下文核对 |
| F67-04 | 剩余状态/故障组合定界 | 18 个既有矩阵已形成覆盖基线；2026-11-12 已登记新增组合定界草案（已复核批次不新增矩阵，连接/会话等待审查）；最终新增/排除仍待 F67-02/03 完成 | 现有 18 矩阵的覆盖对照、必需新增用例及排除理由 | 每个新增用例关联义务和缺失断言；已覆盖项不重复登记；无笼统“其他组合”待办 |
| F67-05 | 按实际缺口修复与补测 | 待 F67-02～04 识别具体项 | 缺陷反例、代码修复、验证补齐及更新后的精确映射 | 已确认缺陷修复并回归；适用验证缺口有充分证据；没有未处理失败 |
| F67-06 | 客户端与功能组合验收 | 已有限定证据，待完整核对 | 客户端版本、传输、协议、Feature、恢复方式的执行矩阵及原始材料 | Profile 内必需组合已执行或有充分的不适用依据；Paho/rumqttc 旧材料仅在适用范围内复用 |
| F67-07 | 当前输入的真实独立实审 | 待执行，批准 0 | 绑定当前源码、规范、日志摘要的独立评审记录 | 所有要求的对象/checks 完整；两版 acceptance 分别审查；不存在自评或失效批准 |
| F67-08 | 严格门禁与文档结项 | 待前置任务完成 | 严格报告、最终证据索引及同步后的状态文档 | 当前评审包严格通过，F6/F7 状态有真实依据，生产发布边界继续单独标明 |

### F67-04 现有 18 矩阵覆盖对照（基线）

下表只登记 `contracts/protocol-review.json` 中既有矩阵及其精确测试，用于 F67-04 的“已覆盖项不重复登记”基线；合计 **18 个矩阵 / 222 个组合**。`scripts/check-matrix-inventory.py` 校验行数、顺序、ID、维度、组合数和精确测试名，并有 4 项缺失/重复/乱序/陈旧反例；它只保护基线台账，不批准新增或排除项。必需新增用例、缺失断言和排除理由必须在 F67-02/F67-03 语义审查后逐项登记；在此之前不把“其他组合”写成笼统待办，也不以本表关闭 F67-04。

| # | 矩阵 | 维度 | 组合 | 证据测试 |
|---|---|---|---:|---|
| 1 | `SENDER-ACK` | states=4 / acks=3 | 12 | `state::qos2::tests::sender_ack_transition_matrix_preserves_phase_and_identifier_ownership` |
| 2 | `RECEIVER-OWNERSHIP` | states=2 / inputs=4 / memory_or_snapshot=2 | 16 | `state::qos2::tests::receiver_transition_matrix_keeps_dedup_retained_and_reuse_across_snapshot_recovery` |
| 3 | `SQLITE-PHASE-FAILURE` | protocols=2 / commit_boundaries=4 | 8 | `tests::storage::qos2_failed_phase_commits_never_advance_the_wire_or_discard_committed_exchanges` |
| 4 | `CONNECTION-QUOTA-RESET` | memory_or_sqlite=2 | 2 | `tests::mqtt5_protocol::mqtt5_reconnected_pubrel_does_not_consume_fresh_receive_quota_or_replenish_it_twice`, `tests::storage::mqtt5_sqlite_recovered_pubrel_has_fresh_connection_quota` |
| 5 | `mqtt5-shared-offline-selection` | online=2 / expired=2 / qos=3 | 12 | `state::shared_regressions::shared_offline_selection_matrix_excludes_expired_sessions_and_qos0` |
| 6 | `RECEIVER-WIRE-RESTART` | protocols=2 / reconnect_or_sqlite_restart=2 / duplicate_flag=2 | 8 | `tests::storage::qos2_receiver_duplicate_content_retained_and_id_reuse_survive_sqlite_restart` |
| 7 | `RECEIVE-WINDOW-WRITE` | protocols=2 / acknowledgement_phases=3 / next_publish_qos=2 | 12 | `connection::receive_tests::partial_ack_write_enforces_mqtt5_receive_maximum_without_limiting_mqtt311`, `connection::receive_tests::receive_quota_requires_pubcomp_and_resets_on_reconnect_with_old_receiver_records`, `connection::receive_tests::queued_duplicate_pubcomp_cannot_release_reused_identifier_receive_quota` |
| 8 | `PARTIAL-ACK-INTERRUPT-RESUME` | protocols=2 / acknowledgement_phases=3 / interruption_kinds=2 | 12 | `connection::receive_tests::interrupted_partial_ack_reconnect_preserves_receiver_ownership_and_reclaims_connection_budget` |
| 9 | `SENDER-ACK-SQLITE-RESTART` | protocols=2 / sender_phases=2 / ack_faults=3 | 12 | `tests::storage::qos2_sender_wrong_ack_phase_matrix_preserves_ownership_across_sqlite_restart` |
| 10 | `MQTT5-NEGATIVE-PUBREC-PHASE-RESTART` | sender_phases=2 / failure_reason_codes=7 | 14 | `tests::storage::mqtt5_negative_pubrec_reason_matrix_after_sqlite_restart_only_releases_publish_phase` |
| 11 | `CLIENT-ACK-ROLE-REJECT-RESTART` | protocols=2 / sender_phases=3 / extra_field_forms=2 | 12 | `tests::storage::client_ack_server_only_reason_matrix_closes_without_mutating_sqlite_sender_ownership` |
| 12 | `MQTT5-CLIENT-ACK-LEGAL-RESTART` | ack_kinds=2 / legal_client_reason_codes=8 / property_length_forms=2 | 32 | `tests::storage::mqtt5_client_ack_legal_reason_matrix_completes_once_across_sqlite_restarts` |
| 13 | `QOS2-DUPLEX-SAME-ID-RESTART` | protocols=2 / sender_phases=2 / completion_orders=2 | 8 | `tests::storage::qos2_same_identifier_bidirectional_matrix_keeps_sqlite_ownership_independent` |
| 14 | `MQTT5-ZERO-EXPIRY-RETAINED-QOS-RESTART` | qos_levels=3 / retain_flags=2 / payload_forms=2 | 12 | `tests::storage::mqtt5_zero_expiry_retained_qos_matrix_preserves_handshake_across_sqlite_restarts` |
| 15 | `DUPLEX-SAME-ID-COMMIT-FAULT-RESTART` | protocols=2 / sender_phases=2 / mutation_directions=2 / commit_fault_modes=3 | 24 | `tests::storage::qos2_duplex_same_id_commit_fault_matrix_recovers_each_direction_independently` |
| 16 | `DUPLEX-SAME-ID-PARTIAL-ACK-INTERRUPT` | protocols=2 / sender_phases=2 / partial_ack_kinds=2 / interruption_modes=2 | 16 | `connection::receive_tests::duplex_same_id_partial_ack_interrupt_matrix_preserves_outbound_and_receiver_state` |
| 17 | `DUPLEX-SAME-ID-UNIX-SIGKILL` | protocols=2 / sender_phases=2 | 4 | `tests::storage::qos2_duplex_same_id_sigkill_matrix_restores_both_directions_and_completion` |
| 18 | `MQTT5-POSITIVE-TTL-PHASE-RESTART` | delivery_phases=3 / expiry_restart_orders=2 | 6 | `tests::storage::mqtt5_positive_ttl_phase_restart_matrix_expires_only_unsent_messages_and_retained` |

### F67-04 新增组合定界草案（2026-11-12）

以下只记录已完成批次的初步决定，**不是 F67-04 最终清单**，也不改变 18 矩阵/222 组合基线。完整新增/排除项仍须在 F67-02/F67-03 全部输入审查后审定。

| 候选域 | 已复核证据 | 草案决定 | 待办 |
|---|---|---|---|
| 重叠订阅 QoS / Subscription Identifier | F6/F7 新增重叠 QoS、多标识、SUBACK 用例 | 属于协议场景证据，不新增 state/fault 矩阵 | 独立审查 |
| SUBSCRIBE/UNSUBSCRIBE 非法输入与替换 | 非法载荷/选项、重复替换、多过滤器 UNSUBACK 用例 | 属于输入/响应场景，不新增矩阵 | 独立审查 |
| Retained / Will | Retained QoS/删除/回放、Will 丢弃/触发、SQLite 恢复用例 | 由现有 retained/TTL/存储矩阵覆盖，不重复新增 | 检查与 MQTT5 交叉 |
| ACK 原因码与 Receive Maximum | 3.4.2/3.5.2/4.9 批次及 7 个现有 ACK/配额矩阵 | 现有矩阵已覆盖，不新增 | 独立审查 |
| Message Expiry / Topic Alias | 正 TTL 矩阵、Alias 负例与 SQLite 恢复 | 现有矩阵+场景已覆盖，不新增 | 检查组合边界 |
| 连接/会话、KeepAlive、CONNECT/CONNACK | 尚未形成本计划批次 | 暂列为候选，未决定新增/排除 | F67-02/F67-03 下一批 |
| TLS/持久化故障、WebSocket、Bridge、Admin HTTP | 当前验收 Profile 外或仅条件能力 | 排除出当前 F6/F7 矩阵范围 | 保持独立专项 |

**当前结论**：已复核批次未发现必须新增的 state/fault 矩阵；18 矩阵/222 组合不变。连接/会话等候选仍待审查，不能把本草案当作 F67-04 完成或完整符合性证据。

执行顺序为 F67-01 → F67-02/F67-03 → F67-04 → F67-05/F67-06 → F67-07 → F67-08。语义审查可分批推进，发现可复现缺陷即可修复；最终批准必须针对稳定后的当前输入。

## 分批核对方法

F6 建议按连接与会话、报文编码与校验、QoS/Packet ID、订阅/Topic/Retained、Will/断开、重传/持久恢复分批。F7 在相应基础上核对属性与原因码、流控/包长、Topic Alias、Session/Message Expiry、Will Delay、订阅选项/共享订阅及条件功能。该分组用于安排工作，不能代替契约中全部 ID 的核对。

每批从契约提取准确 ID，保留完整正文和跨段关联；将结论分为“证据充分”“验证缺口”“代码缺陷”“范围不适用”。证据充分仍须独立实审；范围不适用须说明角色/能力条件和依据，不能用来绕过适用的 TargetedEvidence 或矩阵用例。

优先处理可能导致消息丢失、重复路由、所有权错误、恢复错误及违规输入被接纳的问题。代码缺陷先取得失败反例，再修复并回归；验证缺口增加能区分正确与错误行为的断言。必要时分别核对内存状态、真实线帧、SQLite、重启及后续健康消息，按义务选择，不机械复制全部维度。

## 具体缺口登记模板

当前库内尚未新增经过逐项审定的 F67 具体缺口记录。magicDataHub 工具已有验证缺口 `MQTT-GAP-001`，Retained v2 已在选定范围实现；其七个官方编号仍为待审草案，不是库运行时缺陷，也不关闭 F67-02/05/07。工具后续工作见宿主仓库的 `tools/mqtt-broker-validation/remaining-work.md`。下表保留库内空模板，后续发现后填入；每条记录使用稳定编号 `F67-G001` 起递增，不复用已关闭编号。

| 字段 | 填写要求 |
|---|---|
| 缺口编号 / 关联任务 | 如 F67-G001 / F67-02；未登记前不代表存在此缺口 |
| 协议 / 规范 ID / 表格行或矩阵 case | 使用契约中的精确 ID，必要时列出相关章节与跨段义务 |
| 适用 Profile / 前置条件 | 角色、协议、Feature、传输、存储、客户端及阶段 |
| 预期行为与依据 | 完整义务和可观察结果；不只引用孤立编号 |
| 当前实现与证据 | 源码位置、已有测试名、原始日志路径、证据边界 |
| 分类与影响 | 验证缺口、代码缺陷或范围不适用；说明影响及处理优先级 |
| 处理方案 / 缺失断言 | 最小修复或新增验证内容，说明现有证据为何不足 |
| 完成条件 | 明确什么结果才算实现与验证完成 |
| 实现状态 / 验证状态 / 评审状态 | 分别记录，不以“测试通过”代替“独立批准” |
| 负责人 / 更新日期 / 轮次 | 实际执行后填写；不虚构负责人或评审者 |
| 关闭依据 | 修复前后材料、真实命令与退出码、精确测试名、摘要及独立评审引用 |

## 每轮执行与证据更新

1. 选择任务及准确规范 ID，记录本轮范围、输入版本和实际负责人。新增发现登记到缺口记录，避免只写“继续完善”。
2. 在 Git 忽略的 `validation/日期-主题/` 等本地目录收集本轮材料。核对期间保留原始失败与成功日志，不用后续成功覆盖失败；归档时仅将最终结论写入 `docs/validation/README.md`，过程产物不加入归档。
3. 按改动执行必要验证。功能代码收口按项目 skill 执行 `./scripts/check.sh`、MSRV/最小 Feature 及相关客户端专项；仅文档整理执行文档/链接与差异检查，不冒称功能重跑。
4. 同步相关契约的精确测试映射；记录原始日志、命令、退出码、测试断言和材料摘要。历史结论仅作摘要；原材料已清理的场景需要当前证据时须重新执行，不能改称本轮执行或沿用旧批准。
5. 最终源码、契约和文档稳定后，按下述流程生成新的未批准评审包。受控输入新增、删除或修改会使旧包失效，不手改摘要沿用旧批准。
6. 取得真实独立评审后运行严格门禁；缺项继续保留 Partial，并记录下一轮明确任务。更新本页任务状态、具体缺口、轮次记录和剩余清单入口。

以下命令为后续执行模板，本次文档整理未执行。`CURRENT` 替换为新的实际目录，`IMPLEMENTER_ID` 使用真实执行身份；先取得未拼接、未重复的本轮 workspace 原始日志。评审包输出路径必须不存在。

```bash
python3 scripts/check-protocol-review.py --prepare --producer IMPLEMENTER_ID \
  --results validation/CURRENT/workspace.log \
  --bundle validation/CURRENT/protocol-bundle.json
```

由真实独立评审者按[材料格式](protocol-review.md)提供完整 checks、断言、精确测试和原始执行证据，并填入 bundle 的引用后执行：

```bash
python3 scripts/check-protocol-review.py \
  --bundle validation/CURRENT/protocol-bundle.json --require-complete
```

两版 acceptance 分别包含 `state-and-fault-coverage`、`independent-client-interoperability`、`persistence-and-recovery`、`remaining-conditional-features`。矩阵每个 case 和表格每个要求的行检查均须处理；表头等确不适用的行按工具规则提供具体理由和材料。严格门禁缺项退出不是可忽略的批准步骤；离线工具也不认证评审者真实身份，仍需真实独立审查。

## F6/F7 完成条件

- 当前 Profile 中全部适用规范义务有充分实现与验证依据，完整正文、关联语义和表格行检查均有结论。
- 剩余组合已明确界定，必需用例完成；发现的代码缺陷和验证缺口均有关闭依据，没有未处理的 Failed / NotRun。
- 不适用项有真实依据；不支持的条件能力保持明确声明，不被改写为已支持或已验收。
- 当前 1,064 个对象（后续以工具动态生成的数量为准）的必需检查全部获得有效处理及真实独立批准，两版 acceptance 分别完成。
- 绑定当前输入的严格协议门禁通过，设计、剩余清单、契约及证据索引状态一致。

## 单独保留的边界

| 项目 | 后续处理 |
|---|---|
| 生产 SLO、长期 RSS、代表性负载、退出/恢复时限 | 按用户既有选择保留待验收；另列目标和测量，不以本机短测替代 |
| 外部宿主 Profile、magicCommon、远端 CI、跨平台/文件系统、硬件断电及独立安全/性能/接入/发布材料 | 需要真实执行与材料；按各自发布要求跟踪，不自动并入全部 F6/F7 代码开发 |
| 178 项发布义务 | 当前真实独立批准 0；按[发布流程](release.md)单独执行，协议门禁通过不等于生产发布批准 |
| V-01 无阶段日志的历史失败 | 保留归因限制；不以重复通过关闭，也不倒推成已知根因。若出现新反例，另建有完整阶段证据的记录 |

## 轮次记录

| 日期 | 本轮内容 | 状态变化 | 材料 |
|---|---|---|---|
| 2026-11-09 | 整理当前核对结果，建立任务编号、缺口模板和结项标准 | F6/F7 保持 Partial；没有新增功能执行或独立批准 | 本页；最近功能结论见[最终记录](validation/README.md#verification) |
| 2026-11-09 | 按用户要求归档仅保留最终记录，清理逐轮过程产物 | 历史摘要不作为原始证据或批准；后续验证使用忽略目录 | [最终记录与保留规则](validation/README.md#artifact-policy) |
| 2026-11-10 | 开发样例退出、正式宿主指南及 magicDataHub 通用双向 QoS2 四分支/10 种错误校准 | 样例与 MQTT 指南收口；226 Rust/53 脚本测试、MSRV/最小 Feature/TOML 专项通过。F6/F7、工具完整 M0/M1 和独立批准仍 Partial | [集成指南](magicdatahub-integration.md)；工具归 magicDataHub/tools/mqtt-broker-validation，逐次产物不归档 |
| 2026-11-10 | 默认 Feature Clippy 收口；宿主工具新增版本化 Profile/绑定 schema 与配置/证据核验 | lint 门禁关闭；两个宿主 Profile 声明可追溯，配置已核验与 live 未核验分开；其他 M0 数据契约、官方映射与独立批准仍待 | [剩余项](remaining-work.md)；magicDataHub 工具正式入口 |
| 2026-11-10 | 同步工具执行契约、首批上游审计及 Retained v2 的限定收口；重新核对剩余项 | 工具十二份 schema/30种事件、七个官方ID待审草案；最近工具97项/宿主15项测试及56 PASS/1 BLOCKED黑盒基线，均不升级F6/F7或独立批准。本次为清单核对，未重跑功能e2e | magicDataHub 的 `tools/mqtt-broker-validation/remaining-work.md` 与 `docs/validation.md` |
| 2026-11-10 | magicDataHub连接v2原文映射、完整交换及父进程证据重建 | MQTT-GAP-002在选定范围修复，四个连接编号新增为待审草案；105项工具测试通过，不关闭F67语义/充分性及独立批准 | 宿主工具的`normative-map.md`、`remaining-work.md`和`tests/test_connection.py` |
| 2026-11-10 | magicDataHub QoS2 v2四分支原文映射与父进程原始证据重建 | MQTT-GAP-003在选定范围修复，112项工具测试通过；新增三个待审编号，完整发送方/接收方列表未导入，不升级F6/F7或独立批准 | 宿主工具的`qos2_evidence.py`、`normative-map.md`及`remaining-work.md` |
| 2026-11-10 | magicDataHub Topic v2固定向量、健康/目标对照与父进程原始字节重建 | MQTT-GAP-004在选定范围修复，122项工具测试通过；新增四个待审编号，不等于完整匹配/非法输入或独立批准 | 宿主工具的`topic_vectors.py`、`topic_evidence.py`及`normative-map.md` |
| 2026-11-10 | 宿主工具导入QoS2两个完整复合列表及12个MUST原子，v3补完成后ID复用/读取PUBREC前重复PUBLISH | 128项工具测试及参考专项通过；父编号inventory-only、原子证据Partial，完整语义/充分性与独立批准仍待 | magicDataHub的`tools/mqtt-broker-validation/qos2-obligations.md`及`compound_source.py` |
| 2026-11-10 | 宿主工具FLOW v1补双消息在途、重复PUBREC/PUBREL及完成后ID复用，来源v6为11项原子增加补充绑定 | 140项工具/15项宿主及60组黑盒（59 PASS/1 BLOCKED/0 FAIL/0 ERROR/0 NOT_TESTED）通过；必需门禁退出0；串行发送合法，缺窗口前提BLOCKED；原子Partial/pending，独立批准0 | magicDataHub的`tools/mqtt-broker-validation/qos2-obligations.md`及`qos2_flow_evidence.py` |
| 2026-11-10 | 宿主工具FANOUT v1补双订阅者的重复输入去重/独立交换/观察，来源v7追加五项接收方补充绑定 | 153项工具、15项宿主及63组黑盒（62 PASS/1 BLOCKED/0 FAIL/0 ERROR/0 NOT_TESTED）通过；门禁退出0；相同跨连接ID合法，不能借用ACK/交付；完整语义/充分性、独立批准及更大扇出仍待 | magicDataHub的`tools/mqtt-broker-validation/qos2_fanout_evidence.py`及`qos2-obligations.md` |
| 2026-11-10 | 宿主DUPLEX v1补同连接双向同号的两个正常完成顺序、双向健康/输入ID复用及独立命名空间证据；来源v8追加三个原子绑定 | 167项工具、15项宿主及66组黑盒（65 PASS/1 BLOCKED/0 FAIL/0 ERROR/0 NOT_TESTED）通过，必需门禁退出0；库已有故障专项不重复登记为未实现，本轮未重跑该专项；完整语义/充分性及独立批准仍待 | magicDataHub的`tools/mqtt-broker-validation/qos2_duplex_evidence.py`和`qos2-obligations.md` |
| 2026-11-11 | 宿主工具MIXED v1补九行输入×请求QoS、实际授予/逐级ACK/交付及十八连接原始帧重建；重叠订阅行转MQTT-T07诊断 | 181项工具、15项宿主及68 PASS/1 BLOCKED/0 FAIL/0 ERROR/0 NOT_TESTED通过，必需门禁退出0；内部范围增至9个、事件56种，完整语义/充分性及独立批准仍待 | magicDataHub的`tools/mqtt-broker-validation/mixed_qos_evidence.py`、`mixed-qos-obligations.md`和`docs/validation.md` |
| 2026-11-12 | 固化两版实现侧验收 Profile；登记 F67-04 既有 18 矩阵覆盖基线；同步 MIXED/T07 当前事实和脚本计数 | F67-01 实现侧收口；F67-04 已覆盖项登记完成并加机器校验、新增/排除仍待语义审查；验收Profile与矩阵台账各4项脚本反例；新增重叠订阅最高匹配QoS及最小(发布,授予)QoS用例，Rust测试233项；工具新增非门禁重叠诊断及3项测试，工具测试184项；完整 `scripts/check.sh` 重跑 exit 0；独立审查仍 NotRun；F6/F7 整体仍 Partial | [acceptance-profile.json](../contracts/acceptance-profile.json)、`scripts/check-acceptance-profile.py`、本页矩阵基线 |
| 2026-11-12 | F67-02 批次 01：复核 `MQTT-3.3.5-1` 与 `MQTT-3.8.4-6`，核对 `state.rs` 最高匹配/最小 QoS 路径及新增测试 | 2/437 输入完成实现侧复核；发现 `MQTT-3.8.4-6` 与 `MQTT-ROUTE-001` 漏映射新增用例并修正；未发现运行时缺陷；独立审查仍 NotRun，F67-02 整体未关闭 | [验证记录](validation/README.md#verification)、`contracts/protocol-review.json` |
| 2026-11-12 | F67-02 批次 02：复核 `MQTT-3.8.4-1`—`-6`，核对 SUBACK 顺序/ID、重复过滤器替换、Retained 重放与最小 QoS | 6/437 输入完成实现侧复核；发现 `MQTT-3.8.4-6` 漏映射混合订阅/替换用例并修正；累计 8/437；未发现运行时缺陷；独立审查仍 NotRun，F67-02 整体未关闭 | [验证记录](validation/README.md#verification)、`contracts/protocol-review.json` |
| 2026-11-12 | F67-02 批次 03：复核 `MQTT-3.3.1-4`—`-12`，核对非法 QoS、Retained 替换/删除/非 Retained 发布与回放 RETAIN 标志 | 9/437 输入完成实现侧复核；累计 17/437；未发现运行时缺陷；独立审查仍 NotRun，F67-02 整体未关闭 | [验证记录](validation/README.md#verification)、`crates/modorch-mqtt-core/src/tests/protocol.rs` |
| 2026-11-12 | F67-02 批次 04：复核 UNSUBSCRIBE 固定头/过滤器/精确删除/多过滤器 UNSUBACK 与 DISCONNECT Will 丢弃 | 11/437 输入完成实现侧复核；累计 28/437；未发现运行时缺陷；独立审查仍 NotRun，F67-02 整体未关闭 | [验证记录](validation/README.md#verification)、`crates/modorch-mqtt-core/src/tests/protocol.rs`、`crates/modorch-mqtt-core/src/tests/conformance.rs` |
| 2026-11-12 | F67-04 新增组合定界草案：汇总已复核批次，列出候选域与暂定新增/排除决定 | 已复核批次未发现必须新增的 state/fault 矩阵，18/222 基线不变；连接/会话等待审查；F67-04 整体仍未关闭 | 本页“F67-04 新增组合定界草案” |
| 2026-11-12 | F67-03 批次 01：复核 `MQTT-3.3.2-1/2/4/5/6/7/8/15/16/17/18/20` 等，新增 Topic/属性转发、重叠 QoS、多过滤器 SUBACK 与非法 SUBSCRIBE 用例 | 28/607 输入完成实现侧复核；补上 Message Expiry 递减/Topic Alias 负例及 PUBLISH 属性证据；发现 `MQTT-3.3.2-7/8` 漏映射并修正；显式 PFI=0 归一化按等价语义保留；独立审查仍 NotRun | [验证记录](validation/README.md#verification)、`crates/modorch-mqtt-core/src/tests/mqtt5_protocol.rs` |
| 2026-11-12 | F67-03 批次 02：复核 `MQTT-3.4.2-1/2/3`、`MQTT-3.5.2-1/2/3`、`MQTT-4.9.0-1/2/3` 的 ACK 原因码/属性大小与流控配额 | 9/607 输入完成实现侧复核，累计 37/607；Reason String/User Property 无发送路径按适用性结论处理；未发现运行时缺陷；独立审查仍 NotRun | [验证记录](validation/README.md#verification)、`crates/modorch-mqtt-core/src/tests/mqtt5_protocol.rs`、`crates/modorch-mqtt-core/src/tests/storage.rs` |
