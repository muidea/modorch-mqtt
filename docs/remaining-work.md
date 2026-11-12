# 剩余需求与验收边界

更新日期：2026-11-12。当前各主要功能路径已有实验实现，完整协议与生产发布仍为 **Partial**。本页只保留当前结论；历轮验证过程材料已按用户要求清理，汇总结论见[最终验证与收口记录](validation/README.md)。

需求依据为[完整设计](modorch_mqtt_broker_full_design_0.0.3.md) §11–§15、§30–§59；当前能力见[可选功能](optional-features.md)及[契约](../contracts/README.md)。F6/F7 后续执行按[待收口计划与跟踪清单](f6-f7-closure-plan.md)推进，任务编号、交付物、具体缺口模板及完成标准在该计划维护。

## 当前待收口项

| 项目 | 当前状态与剩余工作 | 跟踪入口 |
|---|---|---|
| F6 / MQTT 3.1.1 | 437 个规范输入、339 表格行、190 个限定映射；完整正文/跨段/逐义务/逐行语义、剩余必需组合和独立验收待完成 | F67-01、02、04–08 |
| F7 / MQTT5 | 607 个规范输入、505 表格行、281 个限定映射；完整语义、剩余必需组合和独立验收待完成 | F67-01、03–08 |
| Enhanced Authentication / AUTH | 当前明确不支持，11 项 UnsupportedConditional；不是自动新增的必做功能，启用时另立开发任务 | F67-01、03、06 |
| 生产 SLO | 按用户选择保留；长期 RSS、代表性连接/吞吐/P99、退出/恢复时限及宿主构建 Profile 需另测 | 独立容量验收 |
| 外部发布验收 | magicCommon、远端 CI、跨平台/文件系统、硬件断电及独立安全/性能/接入/发布材料待真实执行 | [发布流程](release.md) |
| magicDataHub 工具 M0/M1 | 23用例/9内部范围、十二份schema/56种事件，连接/Retained/Topic、恢复v3、FLOW、双订阅者FANOUT、双向同号DUPLEX和混合QoS MIXED的选定证据已具备；完整映射/证据模型、Oracle充分性、资产闭包与干净CI仍待 | magicDataHub 的 `tools/mqtt-broker-validation/remaining-work.md` |
| V-01 历史归因 | 无阶段日志的历史失败不能全部唯一重建；保留为归因限制，出现新反例时记录完整现场，不靠重复通过关闭 | [最终异常结论](validation/README.md#capacity) |

当前尚未整理出经过逐项审定的全部剩余可执行用例；“未完成审查”不直接等于“功能未实现”。先按规范义务识别实现缺陷、验证缺口和范围不适用项，再逐一收口。

## 已完成的基线

默认 Feature Clippy 已收口：只在未编译 MQTT5 时为两个有意单次执行的队列循环保留窄 lint 说明；无 TLS 时不再生成无效 struct update。协议状态机行为未变，默认 Feature `-D warnings` 已加入 check.sh，完整 all-features 与默认检查、MSRV/最小 Feature/TOML 及宿主回归通过，宿主源码身份按验证后的 Core 摘要同步。

2026-11-10 补齐开发样例运行失败、SIGINT/SIGTERM、StopIncomplete 同应用重试与启动补偿；真实受管任务和信号/端口释放验证已接入门禁。[正式集成指南](magicdatahub-integration.md)已落库。该轮完整 check.sh 的 226 项 Rust、53 项脚本测试通过，Rust1.85 all-targets/all-features、最小 Feature 编译与 TOML 配置专项通过。此次不重新声明 Paho 全部扩展专项；下段是此前历史基线。

2026-11-12 固化两版实现侧验收范围：[acceptance-profile.json](../contracts/acceptance-profile.json) 明确 F6 的 TCP/TLS/SQLite、F7 的 MQTT5 TCP/WebSocket/SQLite、共同宿主/Feature/客户端/平台证据边界及排除项；`scripts/check-acceptance-profile.py` 核对契约计数、Cargo Feature、宿主 revision 和 `independent_review=NotRun`，4 项脚本反例覆盖自评、计数漂移、未知 Feature 与范围漂移。同时新增重叠订阅请求 QoS 0/1/2、发布 QoS2 的最高匹配 QoS 用例，以及发布/授予组合取最小 QoS 的矩阵用例，并映射至 MQTT-3.3.5-1/3.6.1-1/3.8.4-6；Rust 测试现为228项。完整 `scripts/check.sh` 已重跑并通过（228 Rust/61 脚本，含 Clippy/Rustdoc/契约/示例/TOML/Storage/SIGKILL）。F67-01 实现侧收口，独立审查仍为 0。

magicDataHub 内的通用黑盒入口已补两个方向各 AwaitPubRec/AwaitPubComp 四分支、真实 ACK 截断和 11 种错误校准；仅代表性证据，不关闭 F67-01—08。工具两个正式宿主的独立 Profile 与 Profile/绑定 schema 已落库并绑定执行/证据；当前 56 种事件、worker 原始结果和汇总 schema 已接入门禁，并拒绝重复/计划外/旧 run 等不完整证据；代表性用例/义务/目录、RunManifest/Request schema 与计划/输入/路径/ownership 绑定也已接入门禁，worker 在副作用前重验；完整官方义务/证据模型、官方映射、语义/充分性和独立审查继续单列。

最近功能轮次 219 项 Rust 测试（Core 179、Auth 11）及 53 项工具测试通过，无忽略项；MSRV 1.85、最小 Feature 和 Paho TCP/TLS、MQTT5 TCP/WebSocket 专项通过。这些是清理前历史执行结论，本次文档整理没有重跑功能验证。

同 Packet ID 双向 QoS2 提交故障 24 组合、部分 ACK 写中断 16 组合、Unix 双 SIGKILL 4 组合和 MQTT5 正 TTL 跨阶段/到期/重启 6 组合已完成，不再列为待补的优先场景。当前合计 18 个有限矩阵、222 个组合、1,064 个协议评审对象、844 个对象内部表格行检查；真实独立批准仍为 0。

R-01–R-19 及工具 T-01–T-04 已有对应修复和限定回归；V-02、V-04 已收口，V-03 已取得最终限定配置的成功容量结论。具体处理见[最终修复记录](validation/README.md#closed-issues)，不扩展为全部功能组合或任意负载均已验收。

magicDataHub 工具的 Retained v2 已补齐 RETAIN=0 不替换旧值的订阅对照、不创建保留记录及合法 QoS1 DUP 处理；父进程由原始字节重建阶段、握手、逐次 ACK、正向对照与有界观察。稳定验证缺口 `MQTT-GAP-001` 在选定范围实现，七个官方编号已有绑定固定 OASIS 原文及四个整段的待审映射（magicDataHub 的 `tools/mqtt-broker-validation/normative-map.md`）。该Retained阶段工具共十二份 schema、30 种事件；映射仍 draft/pending，目录内部 official_ids 仍为空，不关闭完整 M0/M1、F6/F7 或任何独立批准。上述收口调整工具与文档，库协议行为未变。

magicDataHub连接v2已补完整TX/RX交换、三个阶段、本地关闭及父进程重建，验证缺口MQTT-GAP-002在选定范围修复，错误PINGRESP校准已补。映射草案扩至连接四个/Retained七个官方ID、八个完整段落，结构版本2，仍draft/pending；工具共31种事件、十一种运行校准，105项工具测试通过。这些为工具验证收口，不是新增库运行时修复或F6/F7独立批准。

## 后续更新规则

按 F67 任务编号执行，发现具体缺口登记稳定 ID。每轮更新任务状态、真实验证结论、适用范围和下一步；没有执行保留 NotRun，没有独立批准保持 Partial。

本地过程日志、数据库、探针和评审包使用忽略目录，不加入源码归档；核对期间仍须保留严格门禁需要的真实材料。归档仅保留[最终记录](validation/README.md)及持续待收口文档，不用摘要代替原始执行证据或批准。

magicDataHub 工具已形成三个固定上游 revision 的首批静态审计：12 次已集成 Mosquitto B 类调用、6 项 Paho Testing 和 3 项 EMQX 候选/参考，含源文件摘要、行引用、许可证观察及耦合/适配边界。候选不执行、不计规范覆盖率；可用工具 `upstream_audit.py --verify-sources` 离线核对，首批初评不关闭完整 M1、F6/F7 或独立审查。

magicDataHub的QoS2 v2已补四个分支入口和十六个连接的父进程原始字节重建，真实ACK截断、持久会话、逐次ACK和应用交付均关联到实际连接；MQTT-GAP-003标签伪证据在选定范围被拒绝。合法提前转发、交换完成后ID复用和重复PUBREL保留为正例；完整帧绝对期限防止半包延长观察窗口。112项工具测试通过，库运行时未改。映射草案最新为结构版本3、十四个ID/十个完整段落，新增QoS2三个编号；MQTT-4.3.3-1/-2的完整列表及其他关联义务仍待导入审核。事件最新32种，独立批准仍0；下一代表场景是Topic的原文映射与证据重建，完整M0/M1、F6/F7保持Partial。

上述QoS2 v2本轮实际回归为112项工具/15项宿主测试、57组黑盒（56 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED），产品/tool/live必需门禁退出0。参考$SYS写入前提继续BLOCKED；54组自有资源已回收、三个常驻实例未替换。正式限定结果见magicDataHub的`docs/validation.md`；逐次产物清理，不替代后续独立评审原始证据。

magicDataHub的Topic v2补16个通用/3个系统向量、候选匹配健康消息与目标精确订阅对照、原始字节/逐次ACK/观察及最终DISCONNECT的父进程重建；MQTT-GAP-004旧标签/错向量内容反例在选定范围修复。122项工具测试通过，库运行时未改。最新草案共十八个ID/十三个整段、结构版本4，Topic新增SUBACK响应及ID、根通配排除$、不归一化四个待审编号；最新35种事件，十二份schema。下一步为QoS2完整发送方/接收方列表及四类草案语义/充分性审查，完整M0/M1、F6/F7和独立批准状态不升级。

Topic v2本轮122项工具/15项宿主测试及57组黑盒为56 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED，必需产品/tool/live门禁退出0。54组自有资源回收，常驻实例未替换；参考$SYS前提仍BLOCKED。正式结果见magicDataHub的`docs/validation.md`，逐次产物清理后不替代新评审所需原始材料。

magicDataHub的QoS2 v3补同一连接读取PUBREC前排入重复健康PUBLISH、原交换完成后输入ID复用及Broker初始新PUBLISH的DUP=0，父进程从实际帧约束这些输入和ACK顺序；128项工具测试及参考专项通过，库运行时未改。来源结构版本5另导入MQTT-4.3.3-1/-2完整列表、12个MUST原子及四段上下文，全部inventory-only/partial-selected-evidence/pending，十八个选定映射不增加。完整列表的结构导入已完成，逐项语义/充分性、关联义务、并发ID、重复PUBREC/PUBREL及多订阅者等继续待审；入口为magicDataHub的`tools/mqtt-broker-validation/qos2-obligations.md`。F6/F7及真实独立批准不因工具收口升级。

本轮QoS2 v3实际回归为128项工具/15项宿主测试、57组黑盒（56 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED），必需门禁退出0。54组完整回归自有资源及两个参考诊断资源均已回收，常驻未替换；参考$SYS继续BLOCKED。正式限定结果见magicDataHub的`docs/validation.md`，临时逐次材料清理后不替代新独立评审证据。

QoS2 FLOW v1（MQTT311-QOS2-FLOW-001，1.0.0）新增两个clean连接、两消息同时在途、重复PUBREC/PUBREL逐次响应、完成后输入ID复用及三次单独应用交付，父进程按原始字节重建四阶段和0.4秒窗口。合法ACK置换、Broker ID完成后复用、额外PUBREL有正例；缺字节/重复控制/ACK/交付、错误顺序、半包/EOF及不足观察均不能PASS。只支持串行发送合法，缺选定出站窗口前提为BLOCKED，不能作为协议违规。来源结构v6为12个复合原子中的11项增加FLOW补充绑定，全部仍partial-selected-evidence/pending；十八个选定编号、独立批准0及完整Partial状态不变。140项工具测试、15项宿主测试通过，库运行时未改。入口仍为magicDataHub的`tools/mqtt-broker-validation/qos2-obligations.md`；下一步关注更大在途规模、双向同号、多订阅者扇出及完整语义/充分性。

FLOW v1本轮完整回归：140项工具、15项宿主测试通过；最新临时镜像、锁定参考及两个常驻入口共60组，59 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED，必需产品/tool/live门禁退出0。十二份schema、60份请求/原始结果和8519条事件经独立Draft 2020-12与父进程复核通过；三个隔离目标的新FLOW各有两个连接、三次应用交付，旧恢复v3四分支/十六连接/八次交付保持通过。源码/输入摘要一致，无中断、超时、漂移或清理失败；57组完整回归自有实例/卷/目录及一组参考专项资源均已回收，三个常驻容器ID和运行状态不变。参考$SYS前提仍BLOCKED；本轮无新增已确认的库运行时缺陷，未形成干净远端CI、匹配常驻部署或真实独立批准，完整符合性仍Partial。正式入口为magicDataHub的`docs/validation.md`，临时逐次产物不归档。

FANOUT v1（MQTT311-QOS2-FANOUT-001，1.0.0）补一个发布者、两个独立clean订阅者的QoS2扇出：读取PUBREC前排入原始及同ID/DUP重复PUBLISH，逐次确认；每个订阅者各一次原始/健康交付，健康输入复用已完成ID并反向完成订阅者交换。三连接、四次交付、五阶段、每订阅者明确起止且各0.4秒观察和最终DISCONNECT均从原始字节重建，不能跨连接借用ACK、ID或交付。相同/不同跨连接Broker ID、完成后复用、合法提前转发和重投保留正例；不要求跨订阅者同时在途或特定输入ACK分发边界。来源v7给接收方五个复合原子追加FANOUT补充绑定，18个选定编号及独立批准0不变，完整M0/M1和F6/F7仍Partial。153项工具、15项宿主测试通过；本轮未改库运行时。该轮为21用例、8个内部范围、12份schema、43种事件及11种运行校准。剩余边界为更大扇出、混合QoS/重叠订阅、双向同号、完整语义/充分性和独立审查，入口仍为magicDataHub的`tools/mqtt-broker-validation/remaining-work.md`。

FANOUT v1本轮完整回归：153项工具、15项宿主测试通过；最新临时mqtt-server/datahub-server镜像、锁定参考及两个常驻入口共63组，62 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED，必需产品/tool/live门禁退出0。十二份schema、63份请求/原始结果及8959条事件通过独立Draft 2020-12与父进程复核；三个隔离目标的新FANOUT分别有三连接、四次应用交付、五阶段及每订阅者独立观察起止，原恢复v3和FLOW v1保持通过。源码/输入摘要一致，无中断、超时、漂移或清理失败；60组完整回归自有实例/卷/目录和一组前期参考诊断资源均已回收，三个常驻容器ID与Running保持不变。前期诊断不替代最终用例证据，最终参考结果以本次完整回归为准。参考$SYS前提仍BLOCKED；完整M0/M1、F6/F7、干净远端CI、匹配常驻部署和独立批准状态不升级，当前无新增已确认的库运行时缺陷。

前轮剩余项复核：库已存在`packet_identifier_namespaces_are_independent_in_simultaneous_qos2_directions`及同号双向SQLite/提交故障/部分ACK/SIGKILL矩阵；此前“剩余双向同号”指magicDataHub通用黑盒的独立证据，不能当作库功能缺失；该正常流程证据现已由DUPLEX补齐。两个连接同号的FANOUT与同一连接双向同号分别核对。混合QoS/重叠订阅及更大在途/扇出先按规范与Profile形成有限必需矩阵，不无界增加组合。本次工具153项测试、固定原文/上游源/依赖身份检查通过，没有重跑库Rust专项、e2e或业务源；无新增已确认运行时缺陷，完整F6/F7和独立批准状态不变。具体分项见magicDataHub的`tools/mqtt-broker-validation/remaining-work.md`。

DUPLEX v1（MQTT311-QOS2-DUPLEX-001，1.0.0）新增同一连接双向同号的两个正常完成顺序。owner读取Broker实际出站ID且不确认，再以同ID发起反向QoS2交换并收到PUBREC；分别先完成输入方向或输出方向，另一方向仍完成。随后输入方向复用已完成ID发送不同健康内容，并验证反向健康交付；共四个clean连接、八次应用交付、每分支五阶段及每连接独立起止/0.4秒观察。父进程以独立发送/接收命名空间逐字节重建，ACK不能因ID相同而互相替代；合法提前转发、重投、重复PUBREL及完成后Broker ID复用均保留正例。来源v8只给三个复合原子追加DUPLEX角色/版本/两分支/五阶段绑定，18个选定ID/13个段落与独立批准0不变。167项工具、15项宿主测试通过；该轮为22用例/8内部范围、12份schema、49种事件、11种运行校准。本轮未改库运行时；库原有同号双向故障矩阵未重新执行，选定正常流程通过不升级完整M0/M1或F6/F7。后续按Profile和库已有证据界定恢复/故障交点、混合QoS与重叠订阅等有限必需项，不把正常双向同号再次登记为待开发。

DUPLEX v1本轮完整回归：167项工具、15项宿主测试通过；最新临时mqtt-server/datahub-server镜像、锁定参考及两个常驻入口共66组，65 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED，必需产品/tool/live门禁退出0。十二份schema、66份请求/原始结果及9885条事件通过独立Draft 2020-12和父进程复核；三个隔离目标的新DUPLEX各有两个完成顺序、四连接、八次应用交付、十个阶段事件和四个独立观察窗口，旧恢复v3/FLOW/FANOUT保持通过。源码/输入摘要一致，无中断、超时、漂移或清理失败；63组完整回归自有实例/卷/目录和一组参考专项资源均已回收，三个常驻容器ID与Running不变。参考$SYS写入前提仍BLOCKED，当前无新增已确认库运行时缺陷；本轮未重跑库故障专项、未更新常驻部署，干净远端CI、真实独立批准及完整M0/M1/F6/F7仍待。

MIXED v1（MQTT311-MIXED-QOS-001，1.0.0）新增九行发布QoS×独立订阅请求QoS矩阵，使用九个独立命名空间、十八个clean连接，逐行由父进程从原始TX/RX重建实际SUBACK授予、按最小QoS的逐级ACK与应用交付、0.4秒观察及最终DISCONNECT。QoS1→QoS0允许的重复副本与QoS2去重分别保留正例；授予不得高于请求、QoS0无ACK、QoS2重复PUBLISH/PUBREL不重交付均有明确断言。受锁定Mosquitto 2.0.22注册顺序影响的同客户端重叠订阅行已从首轮必需用例剔除，保留为MQTT-T07诊断，不改oracle、不交换Filter顺序或启用allow_duplicate_messages制造PASS。工具当前23用例/9内部范围、12份schema、56种事件；181项工具测试、15项宿主测试通过。库运行时未改。完整M0/M1、F6/F7、干净CI、匹配部署和独立批准均不升级。

MIXED v1本轮联合回归记录为68 PASS / 1 BLOCKED / 0 FAIL / 0 ERROR / 0 NOT_TESTED，退出码0；剔除参考重叠行后，九行独立订阅矩阵在两个产品目标和两个live入口均通过，参考$SYS仍为MQTT-T05 BLOCKED。该结果来自magicDataHub的记录，本轮仅同步本页当前事实并重新核对工具目录/测试，未重跑e2e；临时报告、实例和镜像标签已清理，常驻部署未替换。

2026-11-12 工具新增非门禁的 `overlap_diagnostic.py` 与 3 项纯函数测试，以 `diagnostic-only`/`INCONCLUSIVE` 输出；工具测试现为184项。`--requests 0,1,2` 对锁定 Mosquitto 2.0.22 复现 MQTT-T07：`exact→+→#` 为 1 个 QoS0 副本，`#→+→exact` 与 `+→#→exact` 为 1 个 QoS2 副本；当前 standalone 临时实例三种顺序均为 1 个 QoS2 副本，仅为产品诊断观测，不是参考结果或符合性结论。该脚本不进入 catalog 或必需门禁，不改变 MIXED 矩阵、官方映射或独立批准；MQTT-T07 的上游解释/版本处置与独立语义复核仍待。