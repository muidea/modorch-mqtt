# MQTT3.1.1 段落、报文表与状态转换审计

本轮补充[编号场景清单](mqtt311-conformance.md)，不声明完整3.1.1认证。受控材料为 [mqtt311-audit.json](../contracts/mqtt311-audit.json) 和 [mqtt311-packets.json](../contracts/mqtt311-packets.json)，实际执行见[本轮验证](validation/2026-11-08-f6-packet-audit/README.md)。

## 规范来源与未编号候选

来源为 OASIS MQTT3.1.1 OS，SHA256为 `fc1f493558339850740a57ea19c4b2bdf39db6589edf9a69a984afbec098aa9b`，与140个编号的来源相同。`audit-mqtt311.py` 读取显式指定的本地HTML，校验摘要，提取正文 p/h1–h6 中包含大写 MUST/SHALL/REQUIRED、且本段没有 MQTT-* 编号的文本。它在首个真实 AppendixHeading1 前停止，排除非规范附录B中的重复条款；目录中的Appendix文字不会提前截断。

该规则提取出9段候选，人工对照上下文后分类如下。候选数量不等于新增规范义务数量，也不证明包含隐含于表格、跨段列表和其他措辞中的所有要求。

| ID | 位置 | 核对结果与证据范围 |
|---|---|---|
| UN-001 | §1.2 | RFC2119关键词解释，定义上下文 |
| UN-002 | §3.1.4 | 可选认证/策略检查失败时必须关闭；拒绝码及流水消息隔离测试 |
| UN-003 | §3.2.2.1 | CONNACK reserved bits7..1须为0；新会话、恢复、Clean与拒绝响应的精确帧测试 |
| UN-004..006 | §4.3.2 | 三段Sender规则属于后置编号MQTT-4.3.2-1的续文；映射已有ID分配/新DUP=0/未确认重连测试 |
| UN-007 | §4.6 | 引出客户端顺序规则；服务端适用性另由MQTT-4.6.0-6承接 |
| UN-008 | §7.1.1 | Server总体符合性聚合要求，仍NotRun；限定测试不能使其通过 |
| UN-009 | §7.1.2 | Client总体符合性；可选Bridge使用MQTT5客户端，排除于本3.1.1 TCP/TLS/SQLite Profile；独立客户端/Bridge符合性仍未批准 |

复核源材料时运行：

```bash
python3 scripts/audit-mqtt311.py --source-html /path/to/mqtt-v3.1.1-os.html
```

脚本不联网。替换为实际本地文件路径；摘要、正文/附录边界或段落清单改变均失败。check.sh的Python测试覆盖这些负例；本轮另以已校验的官方HTML实际执行。CI默认不下载HTML，不能将受控清单的存在解释为本次重新审过所有语义。

## 报文结构表与提前拒绝

独立JSON预期覆盖Table2.1/2.2的全部16种类型×16种低位组合。编解码测试验证合法方向和标志，真实TCP测试确认响应与违规连接隔离。CONNECT另外覆盖全部256种Connect Flags；合法凭据布局在匿名模式下返回授权拒绝，与格式错误关闭分别核对。

收到首字节即可确定保留类型、服务端专用方向或非法固定标志时，Core立即关闭；无需等待对端声称的包体。解出Remaining Length后，进一步核对以下结构限制：

| 客户端报文 | Remaining Length限制 |
|---|---|
| CONNECT | 至少12；包括MQTT/level/flags/keepalive与ClientID长度前缀，后续字段仍由Codec检查 |
| PUBLISH QoS0 | 至少3：Topic长度前缀与至少1字节Topic |
| PUBLISH QoS1/2 | 至少5：额外2字节PacketID |
| PUBACK/PUBREC/PUBREL/PUBCOMP | 恰好2 |
| SUBSCRIBE | 至少6：ID、非空Filter和请求QoS |
| UNSUBSCRIBE | 至少5：ID与非空Filter |
| PINGREQ/DISCONNECT | 恰好0 |

Table2.4的8个长度边界通过前缀验证，不分配最大报文的包体；超四字节、非最短编码及声明大小超预算均拒绝。Table2.5覆盖每种接收端ID报文的0/1/65535，QoS0二进制载荷不被当成ID。Table2.6、3.1、3.2、3.4另关联载荷/CONNACK/QoS/回复测试；明确标为non-normative的表和流程图单列Informative，不能充当执行通过。

私有decode入口现在只接受一个完整帧，截断和额外尾字节都失败。TCP仍允许分片及合并多个报文：连接层先提取一个完整帧，再逐帧decode，不要求TCP读边界与报文对齐。头部错误断连的Will、持久Session订阅和未完成QoS2接收交换另有真实TCP验证。

## QoS2发送端ACK矩阵

JSON列出4个实现状态×3种ACK的12项转换，逐项核对消息所有权、PacketID和PUBREL输出：

| 当前状态 | PUBACK | PUBREC | PUBCOMP |
|---|---|---|---|
| Empty | 不变 | 不变 | 不变 |
| Queued，尚未发送 | 不变 | 不变 | 不变 |
| AwaitPubRec | 不变 | AwaitPubComp，发送PUBREL | 不变 |
| AwaitPubComp | 不变 | 不变，重发PUBREL | Empty，释放ID/消息 |

这里包含实现对未知/错阶段ACK的防御行为，不能据此授权客户端发送非法协议序列。重连、重复PUBREC顺序及持久化提交屏障沿用既有测试；该矩阵不覆盖所有网络、故障和持久化时序组合。

## 后端等待与连接期限的限定FSM证据

`FSM-CONNECTION-DEADLINES`覆盖等待外部鉴权、存储gate或已提交回执时的连接关闭、迟到结果隔离与事务排空；TCP/TLS、CONNECT、整帧背压和检查式停止均有故障注入。相关规范义务是KeepAlive；CONNECT/写入/身份期限作为本地策略验证。此项不证明所有调度、平台、故障组合或完整FSM，`FSM-COMPLETE`仍为NotRun。最新运行见[连接期限验证](validation/2026-11-08-f6-deadlines/README.md)。

## 门禁与剩余工作

`check-conformance.py`读取编号场景与本审计清单，按精确、唯一的实际 `test ... ok` 行核对全部TargetedEvidence；缺失、重复、与ignored矛盾或失败证据均拒绝。报告单列audit_coverage；Context/Informative/ClientOnly与聚合NotRun不会升级为Passed。`--require-complete`仍在总体Partial时返回非零。

本轮推进了正文候选与结构表审计。完整跨段语义、所有出站布局及CONNECT/Session/Will/订阅/QoS的全部状态转换、故障和互操作总矩阵仍需继续，不能将完整F6或全部MUST标为完成。后续阶段见[剩余需求](remaining-work.md)。


## 服务端输出与部分写入/慢消费者验证

[出站JSON](../contracts/mqtt311-outbound.json)提供与生产Codec独立的完整字节预期，覆盖九类已实现Server输出。CONNACK核对新会话/恢复/Clean、协议版本/空持久ID/容量/Will拒绝；控制包核对ID1/256/65535、固定标志、RL和混合SUBACK顺序。PUBLISH核对QoS0/1/2、RETAIN/DUP合法组合、空/二进制载荷、UTF8字节长度及RL127/128/16383/16384。真实TCP同时检查live/retained、错误ACK、原ID/DUP重连及PUBREL阶段恢复。PING屏障用于发现额外包和尾随字节。

一字节duplex另让PUBLISH实际只写出前缀，等待本地写期限中断，再通过新的协议流恢复原可靠消息和ID/DUP；避免依赖内核socket缓冲大小。QoS2慢消费者真实TCP用例停在PUBCOMP并占满可靠积压，验证新发布在PUBREC前整体拒绝、健康目标无部分交付、Retained不变及同ID成功重试。该行为遵循项目原子接纳政策，不是宣称协议强制所有Broker采用相同策略。

新项`FSM-SERVER-OUTBOUND`和`FSM-QOS2-SLOW-CONSUMER`都只标TargetedEvidence；旧`FSM-COMPLETE`继续NotRun。40组限定场景、17项限定审计和178项未批准发布评审的当前证据见[本轮验证](validation/2026-11-08-f6-wire-fsm/README.md)。这些验证未覆盖所有故障位置、持久/TLS写中断、长期RSS或代表性负载。
