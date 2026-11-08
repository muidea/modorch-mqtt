# 剩余需求与验收边界

对应[设计](modorch_mqtt_broker_full_design_0.0.3.md) §11–§15、§30–§37。各主要功能路径已提供实验实现，具体范围见[可选功能](optional-features.md)。R-01–R-04已有[修复验证](validation/2026-11-08-remaining-fixes/README.md)；R-05–R-07已有[组合边界收口](validation/2026-11-08-followup-fixes/README.md)。最新[Topic Alias修复验证](validation/2026-11-08-alias-fixes/README.md)收口R-08和D-01；[别名修复后复核](validation/2026-11-08-post-alias-audit/README.md)确认的R-09已由[源会话修复](validation/2026-11-08-source-session-fixes/README.md)收口；完整规范及生产发布仍为 Partial。

## 最新源会话边界收口

| 编号 | 优先级 | 修复结果 | 回归范围 |
|---|---|---|---|
| R-09 | P1 | Transfer绑定源Session代际；新Session不会收到旧ACK，旧目标工作完成后才订阅新Session；断线清理排队ACK | 同/不同ID、连续源会话丢失、两端同时丢失、源恢复DUP、重连待CONNACK时目标完成确认、后续消息与检查式退出 |

每方向继续只持有一个Transfer。源会话丢失后，新订阅等待旧目标工作完成；恢复原Session则沿用原订阅和ID。新订阅建立前的发布不能依赖已经丢失的Session形成可靠积压，需由源业务保留/重发。此策略不恢复源Broker已经丢失的消息，也不承诺跨Broker exactly-once。异常额外可靠投递通过断线保留在源端，不作为QoS0静默丢弃。

原R-09审计目录保留；修复后的公开API探针分别验证源Session丢失和正常恢复，目标顺序包含新Session的消息后才推进屏障消息。R-02的目标Session重发语义保持，新增组合回归不替代完整故障矩阵。

## 最新核对项收口

| 编号 | 优先级 | 修复结果 | 回归范围 |
|---|---|---|---|
| R-08 | P1 | 网络消息恢复按合法完整Topic或Topic Alias源线帧判限，绑定帧也须合法；完整Topic/属性仍保存并计费 | 原8191字节QoS1 Retained反例、8192字节真实属性边界、SQLite双向QoS2和Retained恢复、format4/5、预算/别名/超限拒绝及下行限额 |
| D-01 | P3 | 项目modorch-mqtt-development Skill更新至1.0.4，更新format5/读1–4、跨协议策略、来源迁移和别名恢复说明 | 链接/版本校验通过，feedback预览及本地归档同步完成，status为synced；未推送远端 |

[原反例核对](validation/2026-11-08-post-format5-audit/README.md)保持原样；本轮不新增持久字段，仍写format5，已落盘的合法网络别名消息可恢复。内部发布、Will及已知Internal Retained保持原有严格界限。恢复成功不保证超下行限额报文可交付：MQTT5按完整Topic及目标/监听上限丢弃计数，3.1.1保留原连接层超限拒绝策略。显式提高监听上限后可验证完整交付。

已确认R-01–R-09与D-01均有对应修复和限定回归；这不等于全部功能组合已经验收。Payload Format Indicator=0省略已按规范等价条款排除，不登记为缺陷。R-09收口证据见上表，外部验收与既定边界列于下表。

## 本轮组合边界修复

| 编号 | 优先级 | 修复结果 | 回归范围 |
|---|---|---|---|
| R-05 | P1 | 不清理的跨协议Session恢复在修改/撤销原所有权前拒绝；显式Clean Session/Clean Start可切换协议新建 | 双向拒绝、有/无非默认订阅选项、原连接继续可用、双向QoS2/ID保持、SQLite重启及清理重建 |
| R-06 | P1 | 恢复按消息属性选择可表示的最小协议编码；目标新增订阅标识不占源包长，真实属性仍验证并计费 | 两种协议的上限/差一字节、Retained/可靠积压/入站QoS2/Will恢复、真实属性与幂等账本回归 |
| R-07 | P2 | Retained保存并按发布ClientID执行No Local；format5保存来源且纳入预算 | QoS0/1/2、同ID清理重连/SQLite重启、其他ClientID与No Local关闭、3.1.1/Will/内部来源、format1–4迁移和非法账本/预算不足拒绝 |

旧格式缺失来源的Retained迁为LegacyUnknown，普通订阅继续回放，No Local保守跳过，重新发布后恢复精确来源判断。format5禁止原地降级；旧版本已写出的协议/选项不一致快照仍拒绝启动并保留原库，不自动猜测修复。历史反例目录保持原样，修复后通过日志单独归档。

## 本轮已收口的功能问题

| 编号 | 优先级 | 修复结果 | 对应回归 |
|---|---|---|---|
| R-01 | P1 | 零过期MQTT5会话在线时仍可接管；真正断开后清理，Clean Start仍重建 | TCP保留订阅标识、QoS1原ID/DUP、双向QoS2所有权；离线清理和Clean Start |
| R-02 | P1 | 目标Session丢失后重提持有消息，Session恢复时复用原交换；双向poll不互相取消，ACK后EOF仍收尾 | 实际Bridge+受控目标端，两种Session状态、ACK前源所有权、后续消息推进及检查式退出 |
| R-03 | P2 | 128个普通属性与两个唯一防环属性分别限额；全部纳入包长/内存预算 | 126/127/128普通属性、已有/新增防环元数据穿过两个真实Broker；普通超额/重复保留键/字节超限拒绝 |
| R-04 | P2 | 恢复幂等记录使用真实属性校验并计费，拒绝内部记录自行携带订阅标识 | 多记录SQLite重启账本预算相同、原请求幂等、异属性冲突、预算不足/包长/非法属性恢复拒绝 |

MQTT3.1.1契约及审计说明中WebSocket/Bridge“未实现”的过期描述已同步为发布Profile范围排除。完整WebSocket/Bridge客户端符合性仍未批准，专项测试不改变规范适用性和正式门禁状态。

## 尚未完成的验收与既定边界

| 范围 | 当前实现 | 剩余验收/明确边界 |
|---|---|---|
| F6 MQTT3.1.1 | 双向QoS2、持久恢复、独立出站字节与FSM专项回归 | 完整规范逐义务实审、跨段语义及故障/互操作总矩阵；178项发布审查未获批准 |
| F7 MQTT5 | 属性/Reason Codes、Clean Start/Session Expiry、Receive Maximum/Maximum Packet Size、Message Expiry、别名、订阅选项/标识、共享订阅、延迟Will和持久恢复 | 实验能力，完整MQTT5独立规范审计/客户端组合矩阵待验收；Enhanced Authentication不属于已支持认证机制 |
| 入口与桥接 | TCP/TLS/WebSocket/WSS入口；独立网络Bridge的显式映射、ACL账号、防环、有界重试与源端积压 | 每Core一个监听；Bridge当前为MQTT5 TCP客户端，需要加密时显式部署可信TLS代理；跨Broker至少一次，进程崩溃可能重复 |
| 宿主与运维 | Tokio适配、Core TOML、动态Auth策略/TLS证书、分页Admin、Metrics、持久审计和受保护HTTP | 其他Provider由宿主按既有协议实现；不承诺任意后端兼容或通用配置热重载 |
| 存储与迁移 | SQLite FULL/DELETE、format5、兼容1/2/3/4、预监听恢复、SIGKILL和页限故障注入 | format2多个Release仍拒绝歧义升级；禁止原地降级；停止Will允许重启时收尾，立即停机持久发布需另行设计 |
| Topic共享向量 | 39组JSON驱动Core/Facade/Hub/TCP；MQTT5专有共享订阅已实现 | magicCommon消费同一JSON的跨实现执行证据未提供 |
| 负载与恢复 | 现有可复现TCP/TLS/Auth/SQLite/QoS2测量及慢消费者故障回归 | 代表性生产负载、长期RSS、高负载退出/恢复SLO；需要实际运行和阈值选定 |
| 平台与发布 | Linux本地检查、MSRV、最小Feature、开发CI定义与严格发布判定工具 | 远端CI、跨平台/文件系统、真实硬件断电、独立Agent和安全/性能评审均须真实材料 |

F0–F6的历史验证目录保持原样。MQTT5/WebSocket/Bridge等新场景由 `contracts/optional-functional.json` 和 `scripts/check-feature-evidence.py` 要求精确执行日志；这些映射是限定证据，不是完整F7批准。

此前`2026-11-08-functional`与`2026-11-08-remaining-audit`保留各自原始输入及反例，不改写为修复后通过。上轮修复输入、执行及未批准发布计划存于`2026-11-08-alias-fixes`；原核对材料存于`2026-11-08-post-alias-audit`；最新修复、执行及未批准发布计划存于`2026-11-08-source-session-fixes`。

默认 `scripts/check-release.py` 严格阻断缺失评审，`--allow-partial` 只检查开发清单；`check-conformance.py --require-complete` 继续阻止把专项场景标成完整 MQTT3.1.1 发布。正式验收完成前，以上外部执行项保持 NotRun。
