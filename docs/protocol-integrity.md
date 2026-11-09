# 协议完整性核对与收口

当前最终结论见[验证摘要](validation/README.md#protocol)，后续执行见[待收口计划](f6-f7-closure-plan.md)。下文保留协议机制与历史实施说明；逐轮过程文件已清理，历史链接只指向汇总结论，不作为原始执行证据。

本页维护 MQTT 3.1.1 与 MQTT5 的规范正文、状态转换和实际协议证据之间的关系。最新R-17、完整章节输入和评审导入见[验证结论摘要](validation/README.md#verification)。以下协议收口记录见[验证结论摘要](validation/README.md#protocol)。本地限定回归与独立完整符合性审批分开记录，完整 F6/F7 仍为 Partial。

## MQTT 3.1.1 跨段义务

[正文索引](../contracts/mqtt311-text.json)仍有117段、140个编号。官方 §4.3.2/§4.3.3 将四个条款编号单独放在段落中，真正义务位于它们之前的段落或列表。索引现在为这些编号保存原始角色上下文 `preceding_context` 及全部15条 `preceding_normative`，不再把孤立编号交给评审者作为完整正文。

| 条款与角色 | 本次绑定的完整前置义务 | 可执行核验 |
|---|---|---|
| 4.3.2-1 Sender，3条 | 分配空闲ID；新QoS1 PUBLISH的ID/QoS/DUP；PUBACK前保留未确认所有权 | `state::qos2::tests::packet_identifier_wrap_skips_live_qos1_and_qos2_ids_and_reuses_only_completed_ids`；`tests::protocol::publish_qos_matrix_keeps_payload_and_sets_outgoing_dup_independently`；`tests::offline_qos1_and_inflight_reconnect_keep_packet_id_and_dup` |
| 4.3.2-2 Receiver，2条 | 接受所有权后按原ID PUBACK；PUBACK后同ID的新PUBLISH独立处理，不按DUP去重 | `tests::protocol::publish_qos_matrix_keeps_payload_and_sets_outgoing_dup_independently`；新增 `tests::protocol::qos1_packet_id_reuse_after_puback_is_new_even_with_dup_and_reconnect`（3.1.1与MQTT5） |
| 4.3.3-1 Sender，6条 | 分配空闲ID；新QoS2 PUBLISH；PUBREC前保留PUBLISH；PUBREL沿用原ID；PUBCOMP前保留PUBREL；进入PUBREL后不再重发PUBLISH | `state::qos2::tests::sender_ack_transition_matrix_preserves_phase_and_identifier_ownership`；`tests::qos2::qos2_outgoing_reconnect_resumes_publish_or_pubrel_and_wrong_acks_cannot_free_id`；`tests::storage::qos2_snapshot_restores_both_directions_without_rerouting_and_resumes_pubrel` |
| 4.3.3-2 Receiver，4条 | 接纳后原ID PUBREC；PUBREL前重复PUBLISH只回复不重复路由；PUBREL回复原ID PUBCOMP；PUBCOMP后相同ID视为新消息 | 新增 `state::qos2::tests::receiver_transition_matrix_keeps_dedup_retained_and_reuse_across_snapshot_recovery`，加原真实TCP重复、变更载荷、重连和复用测试 |

接收端新增矩阵为2种所有权状态 × 4类输入 × 内存/快照恢复，共16个组合；验证去重、Retained不被重复覆盖、QoS1冲突及PUBCOMP后ID复用。此前发送端4状态×3种确认的12项矩阵继续保留。连接、订阅、Will及网络/存储故障证据见[现有审计](mqtt311-audit.md)，上述有限矩阵不等同所有状态和故障的笛卡尔积。

`audit-mqtt311.py`用固定SHA256的官方HTML核对全部前置正文；正文抽取覆盖嵌套 `p/li`，忽略附录。发布目录进一步拒绝缺失、截短或角色错误的QoS义务列表。发布评审对象现在包含这些正文，因此旧评审摘要不能复用于新输入。规范编号和178项发布义务数量保持不变。

## MQTT5 本轮修复

| 项目 | 规范依据 | 修复后的行为 |
|---|---|---|
| R-11 短DISCONNECT | §3.14.2.2.1：Remaining Length小于2时Property Length按0处理 | RL=1的合法输入在严格校验后，仅对第三方解码器的临时副本补零长度；原线帧限额和计费不变。正常DISCONNECT不再被误判为异常断开并触发Will |
| R-12 非零原因与Will | MQTT-3.1.2-8/10：仅Reason Code=0取消Will | 所有合法非零客户端原因保留Will；继续按原Will Delay/Session Expiry及背压规则处理 |
| R-13 原因码方向 | §3.14.2.1 Table3-10的Sent by列 | Server专用原因不能作为合法客户端DISCONNECT；异常断开不清除Will |
| R-14 重复UNSUBSCRIBE | MQTT-3.10.4-6、3.11.3-1/2 | 逐个删除并计算原因，同一过滤器第一次返回0，随后返回0x11；顺序与输入一致，已开始的QoS2交换继续完成 |

编码层穷举5类确认/断开报文 × 256个原因码 × 2种属性长度形式，共2560项期望，区分客户端/服务端发送角色。属性校验覆盖Table2-4的27种属性 × CONNECT/Will/PUBLISH/四类ACK/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT共10个上下文，并核对单次允许性、重复次数及字段各截断位置。此处的认证属性只检查结构；Enhanced Authentication仍未实现。

真实TCP覆盖全部14个合法客户端DISCONNECT原因的两种长度形式、15个Server专用原因、Will的触发/取消及后续屏障消息；SQLite另核对短正常断开与非零断开的Will在重启后的区别。Paho脚本增加TCP/WebSocket非零DISCONNECT Will与重复取消订阅的互操作验证。具体通过状态以本轮原始日志为准。

## MQTT5 规范来源与剩余审查

新增[MQTT5正文索引](../contracts/mqtt5-text.json)，来自[OASIS MQTT5 OS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)，SHA256为 `16f24e2b021d86b3bbb2b39c8b5ee134b3d39f69320ba61d3c07a38af74e696c`。主文216段关联250个不同编号；`audit-mqtt5.py --source-html PATH`逐字核验受控索引，附录不参与正文索引。编号抽取不等于未编号MUST、表格、角色和功能适用性的完整语义审查。

MQTT5完整逐义务角色/适用性审查、其余条款到测试的精确映射、全部状态/故障/客户端组合仍未验收；本轮未用新增索引声称完成这些工作。MQTT3.1.1的完整FSM审查和178项独立批准也仍未完成。完整发布门禁继续严格阻断缺失的真实材料，不能由本轮修复者自评批准。


## 连接配额、空断开与完整核对输入

新回归确认并修复R-15：MQTT5恢复Session时，重发旧PUBREL不占用新连接的PUBLISH发送配额；原PUBCOMP也不为新连接额外增加额度。`quota_used`只在内存中随连接初始化，不进入format5。TCP验证旧PUBREL、新PUBLISH、旧PUBCOMP、仍未确认的新PUBLISH和第三条消息的顺序；SQLite另验证恢复后的同一规则。依据MQTT-4.9.0-1/2/3及§4.9关于配额不属于Session State的上下文。

R-16修复第三方顶层解析器对Remaining Length=0 DISCONNECT的误拒绝；与R-11的RL=1合并，在严格校验真实帧后补临时Reason Code/Property Length，不改变线帧限额。真实TCP及SQLite覆盖空Normal Disconnection取消Will并保留Session，非零原因仍触发原有Will流程。

[完整文字输入](../contracts/mqtt311-review-inputs.json)覆盖3.1.1的22段未编号MUST/SHALL/REQUIRED候选及56个完整表格/图形布局（339行）；[MQTT5输入](../contracts/mqtt5-review-inputs.json)覆盖7段候选及58个表格（505行）。保留逐单元格文本及rowspan/colspan、前后段落和固定来源摘要。MQTT5的ClientID合法字符前置义务及原文损坏的CONNACK原因编号现在作为候选单列，不再仅依赖250个规范编号。

[核对清单](../contracts/protocol-review.json)为两种协议的390个编号、29段未编号候选及114个表格建立533条独立记录；每项绑定完整原文摘要、适用角色、范围、实现位置和限定测试或明确NotRun。有限模型另列发送端12组合、接收端16组合、双协议SQLite四提交点共8组合及两种配额恢复场景。`scripts/check-protocol-review.py`已接入check.sh，拒绝遗漏、重复、正文失效、缺少执行证据及自评批准；`--require-complete`保持阻断Partial。

完整逐表行语义、全部状态/故障/客户端组合与独立评审仍未完成。该清单是可逐项执行的审查输入和限定映射，不是533条义务全部通过的结论。新执行材料见[验证结论摘要](validation/README.md#verification)。


本轮进一步为所有规范布局表建立限定测试映射：3.1.1为188条TargetedEvidence，MQTT5为270条，其余按客户端、可选/不支持条件、定义、上下文、示例及整体符合性归类。固定头、字段长度/ID、CONNECT、出站控制帧、属性/原因码各自绑定对应断言；MQTT5的AUTH明确不支持，示例与规范字段规则分别记录。每项表格范围均保留逐行语义/独立批准未完成的说明；已发出的部分原因码通过不能认证所有可选原因。所有533项独立批准仍为0，结构及限定日志检查通过也不能升级完整符合性。


## 当前修复与验收状态

R-17 离线共享订阅积压、R-18 入站 Receive Maximum、R-19 ACK 原因码发送角色均已有修复和限定回归；具体行为见[最终修复记录](validation/README.md#closed-issues)。

T-01 已提供真实独立评审导入及严格完整判定通道，T-02/T-03 排除评审文件的路径别名和已知等内容副本，T-04 修正完整段落中的 Server 角色及限定映射。工具合成通过路径不批准当前项目，也不认证评审身份真实性。

当前输入为 MQTT 3.1.1 的 437 项、MQTT5 的 607 项，限定映射为 190/281；18 个有限矩阵共 222 个组合，计 1,064 个对象与对象内部 844 个表格行检查。完整正文、跨段、逐义务/逐行语义、剩余必需组合和独立验收仍待完成，真实批准为 0，F6/F7 保持 Partial。

V-02 恢复 DUP/原 ID 反向断言、V-04 物理认证生命周期验证已收口；V-01 缺阶段历史失败仍保留归因限制。过程材料已按用户要求清理，如需当前输入的真实执行证据和独立批准，须按[评审流程](protocol-review.md)重新生成；最终摘要不能代替日志。
