# MQTT 3.1.1 规范场景清单

当前实现为 QoS0/1/2 实验版本，完整 F6 与生产发布均为 **Partial**。本页描述已建立的可执行场景门禁，不声明完整协议认证。

## 来源与范围

[受控清单](../contracts/mqtt311.json)引用 [OASIS MQTT 3.1.1 OS](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)，记录2026-11-07下载源的SHA256。HTML正文可识别140个不同编号，官方原文中的 `MQTT-3-8.3-4` 拼写保留。编号集合用于追踪，不能替代对未编号 MUST、Server/Client 义务和可选功能适用性的人工审计。

当前41组场景关联128个编号；其余12个编号保留NotRun并附适用性说明（7项客户端职责、4项可选WebSocket、1项条件性Bridge）。关联测试仅验证所述轨迹，不授予完整条款符合性。每组场景写明限定范围和精确Rust测试名：

| 场景 | 核验行为 |
|---|---|
| connect-rejection | 不支持协议、空持久ClientID：CONNACK失败码后关闭，不处理其后流水PUBLISH |
| malformed-connect | 保留标志、Will标志和缺失/多余载荷字段：直接关闭，无CONNACK |
| invalid-packets | 首包/二次CONNECT、非法flags/ID/UTF8/Topic/订阅载荷；其他连接继续PING |
| subscription-replacement | 替换订阅QoS并重发Retained；UNSUB按过滤器精确匹配；未知过滤器仍ACK |
| response-order | PUBACK/PUBREC按输入顺序；逆序和重复PUBREC按到达顺序逐个回复PUBREL |
| ordered-recovery | 真实SQLite重启保留PUBREL顺序；剩余PUBLISH仍按原顺序、ID/DUP重发 |
| identifier-and-unicode | 1..23字节字母数字ClientID、配置下限；保留BOM、大小写、空层、Unicode原编码 |
| qos2-receiver | Method B去重、PUBREL与ID复用；相同ID的不同载荷不重复路由 |
| qos2-sender | 两个重连阶段、错误ACK不释放ID、可靠窗口和顺序 |
| qos2-will-retained | QoS2 retained Will和迟到订阅的完整握手 |
| keepalive-and-fragmentation | 握手与keepalive超时、Will、碎片报文与PING响应 |
| will-lifetime | 异常关闭/接管触发Will，正常DISCONNECT清除 |
| qos1-reconnect | 离线可靠消息和原ID/DUP重发 |
| mixed-subscription-codes | 混合成功/拒绝和重复过滤器，SUBACK原ID/逐项顺序及最终QoS |
| publish-qos-flags | 九种发布/订阅QoS组合、二进制载荷、入站DUP不传播、QoS0不ACK/无ID |
| retained-lifetime | QoS2→0→1替换、live/replay标志、非Retain不修改旧值、空Retain删除 |
| unsubscribe-inflight | 取消订阅后已开始的QoS1/2继续完成，新消息不再路由 |
| clean-session-lifetime | 接管清理双向QoS2/订阅但保留Retained；精确SessionPresent |
| network-topic-matching | 16组真实TCP Topic匹配；非法多过滤器报文不部分修改订阅 |
| maximum-topic-bytes | 足够包预算下65535字节UTF8 Topic/Filter及保留回放 |
| utf8-field-validation | ClientID/WillTopic/用户名/过滤器拒绝非法UTF8；密码按二进制解析 |
| will-qos-retain-matrix | QoS0/1/2×retain0/1六组Will，二进制载荷和迟到订阅 |
| packet-id-wrap | 65535回绕跳过未完成ID、正确完成才复用；双向相同ID互不干扰 |
| offline-qos1-qos2 | 持久会话离线后新QoS1/2仍入队，重连原顺序交付并完成 |
| connect-failure-codes | 错误协议名、超长ID、Will授权拒绝、Session满的不同响应 |
| assigned-client-id | 空ClientID生成的唯一身份不接管既有会话 |
| invalid-publish-qos | QoS3仅关闭违规连接 |
| overlap-and-ack-flags | 重叠订阅请求QoS0/1/2、发布QoS2时按最高匹配QoS单副本交付；发布QoS0/1/2时按最小(发布,最高授予)交付；PUBREL及确认报文固定标志/长度 |
| publication-denial | QoS2发布ACL和可靠目标满额时不部分接纳 |
| connect-unknown-failure | 存储不可用对应CONNACK3，未知错误不发错误CONNACK2 |
| standard-client-and-stream | 独立rumqttc标准协议互通、TCP碎片/合并帧 |
| shared-topic-vectors | 39组同源JSON向量核对Core/Facade/真实Hub订阅与Handler投递及真实MQTT；3.1.1下$share为字面Topic |
| control-packet-tables | 全256首字节、全256 CONNECT flags与精确CONNACK reserved bits |
| packet-length-and-id-tables | 长度边界、完整帧、ID0/1/65535、头部提前拒绝与Will/持久会话 |
| qos2-sender-ack-transition-table | QoS2发送端4状态×3ACK的12项所有权/阶段转换 |
| keepalive-during-authorization | Publish/Subscribe等待授权时KeepAlive生效；附身份到期及迟到授权不接纳验证 |
| keepalive-during-durable-work | TCP/TLS等待gate/提交期间KeepAlive生效且持久事务安全排空；附CONNECT、写入期限和停机策略验证 |
| server-outbound-wire-layouts | 独立字节预期核对九类Server输出、ID端值、PUBLISH标志/二进制/UTF8及RL边界，真实TCP验证live/retained/重连 |
| partial-publish-write-reconnect | 一字节duplex让QoS1/2 PUBLISH半途写超时，重连按原ID/DUP重试并完整确认 |
| qos2-pubcomp-slow-consumer-atomic-admission | PUBCOMP阶段占满窗口和积压，新QoS2发布无PUBREC/部分扇出/Retained变更，释放容量后原ID重试仅路由一次 |

CONNECT/写入/身份期限是本地策略，不当作新增MQTT规范义务。后端等待期间不并发解析PING，Will持久收尾需等待gate。修复和实测见[验证结论摘要](validation/README.md#verification)。

## 执行与证据语义

`./scripts/check.sh` 执行全工作区测试，再调用 `scripts/check-conformance.py`。后者仅接受精确 `test <完整名字> ... ok` 行；缺失、ignored、失败、编译失败或只有测试总数时拒绝通过。门禁自身的9个测试覆盖这些负例，不能通过删掉未知条款把总体升级为Passed。

单独复核本轮实际生成的测试日志，以下 `CURRENT` 为本地忽略目录占位符：

```bash
python3 scripts/check-conformance.py \
  --results validation/CURRENT/check.log \
  --output /tmp/modorch-mqtt-conformance.json
```

输出记录清单和日志的SHA256。场景的 `Passed` 只确认指定测试已成功执行；关联条款标 `TargetedEvidence`，未映射编号保留 `NotRun`，场景校验器的完整符合性保持 `Partial`，逐义务完整批准另由[发布门禁](release.md)判定。日志与源码版本的关联由验证记录中的输入摘要提供；校验器不会从旧日志推断当前源码正确性。

## 本次实现变化与兼容性

PUBREL之前从可靠队列按原PUBLISH顺序挑选，多个PUBREC在写端阻塞时可能重排，重复回复也可能合并。现在每个有效PUBREC直接生成回复并进入现有有界控制FIFO，持久确认后才允许发送；满队列暂停消费输入。对应交付移到可靠队列末尾，保存每个未完成ID最近一次PUBREL顺序；其他未确认PUBLISH的相对顺序不变。确定性状态测试不依赖操作系统恰好产生socket背压，TCP流水测试另验证实际接线。

当前写入format3快照；旧format2若一个持久Session含多个Release，无法重建其历史确认顺序，升级拒绝监听并保留数据。需先用旧版本排空相关交换；其他format1/2兼容范围见[Storage](storage.md)。`max_client_id_bytes`现在限定23..65535，低于23的历史配置需修正。

本轮补充CONNECT失败分类：只在确认ClientID被拒绝时使用CONNACK2；存储不可用使用3；无法匹配标准原因码的错误直接关闭且不发CONNACK（MQTT-3.2.2-6）。网络场景验证ID拒绝/Will授权拒绝/Session满，独立错误分类测试验证存储和未知错误的完整线帧。

完整规范矩阵、未编号要求与更广互操作和异常链路仍待完成。已给未映射编号逐项说明角色/可选能力；这些说明不代替执行证据，也不将NotRun转成Passed。MQTT5、可选WebSocket/Bridge、生产性能/恢复SLO、真实断电、跨平台及独立Agent发布门禁继续保留待办。当前历史验证结论见[验证结论摘要](validation/README.md#protocol)。


## 共享向量与完整规范阻断模式

[topic-vectors.json](../contracts/topic-vectors.json)由固定上游rev的独立Rust预期向量规范化导出，保存来源文件路径、SHA256和39个原始ID。上游没有JSON fixture；本轮没有修改上游，也没有执行magicCommon消费端。四个检查层共享同一JSON中的预期值，不能用某一匹配器的输出生成另一层的预期。

真实TCP测试使用同一发布者的可靠队列marker作为匹配/不匹配的交付屏障；非法Topic/Filter核对关闭违规连接。Hub测试通过实际subscribe/publish和Handler观测路由结果，不访问框架私有索引。测试专用独立Hub不改变产品复用宿主Hub的约束。

给上述复核命令增加 `--require-complete` 会在总体Partial时返回非零。该模式是完整规范发布的阻断条件，不是全部设计的发布批准器；独立Agent、生产安全/容量、平台和其他MUST由[统一发布门禁](release.md)核对，真实执行/审查尚未完成。


正文未编号候选、表格和状态转换现单独维护于[审计说明](mqtt311-audit.md)与[mqtt311-audit.json](../contracts/mqtt311-audit.json)。9段候选经人工区分上下文、跨段编号续文及服务端/客户端义务；8张规范相关表有限定测试映射，7个非规范例表/图单列Informative；QoS2发送端ACK矩阵有12项转换测试。完整语义/FSM审计仍未完成。日志门禁同时验证这些映射，拒绝缺失、重复或与ignored矛盾的结果；当前 `scripts/tests` 共61项脚本测试。报文审计历史执行见[验证结论摘要](validation/README.md#verification)。


## 逐义务发布审查

[正文索引](../contracts/mqtt311-text.json)保留117段编号正文/列表项，并与固定来源逐字核对；需同时阅读未编号续文和表格清单。[发布门禁](release.md)要求当前源码摘要、逐义务独立评审和分域原始报告；全部满足时存在真正的Passed路径。当前178项独立评审均为NotRun，工具中的合成通过用例不构成项目通过。当前历史验证结论见[验证结论摘要](validation/README.md#protocol)。

## 协议正文与接收端矩阵补齐

[协议完整性收口](protocol-integrity.md)为四个孤立QoS编号绑定15条前置义务及Sender/Receiver上下文，发布目录拒绝缺失或截短的义务文本。新增16组合接收端内存/快照恢复矩阵，加入既有qos2-receiver场景；41组场景、140编号和178项独立发布审查数量不变。最新执行见[验证结论摘要](validation/README.md#protocol)，完整规范仍Partial。
