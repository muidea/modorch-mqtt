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

2026-11-12 新增两版实现侧验收 Profile 契约及 4 项反例、F67-04 矩阵台账校验及 4 项反例，脚本测试由 53 增至 61；新增重叠订阅最高匹配 QoS 与最小(发布,授予) QoS 用例后 Rust 测试为 228 项。`cargo test --workspace --all-features --locked` 为 **228 passed / 0 failed / 0 ignored**，`check-conformance.py --results` 41/41 场景 TargetedEvidence 通过、规范仍 Partial，`check-protocol-review.py --results` 缺失证据 0。`scripts/check-acceptance-profile.py` 与 `scripts/check-matrix-inventory.py` 已通过；完整 `scripts/check.sh` 已重跑并 exit 0，包含 **228 Rust / 61 脚本测试**、fmt/check/all-features Clippy/Rustdoc、契约门禁、示例/TOML/Storage/SIGKILL 检查。该变化固定实现侧范围并防止自评/计数/Feature 漂移，F67-01 独立审查仍为 NotRun。

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

- F6/F7：完整正文、跨段、逐义务及表格行语义审查；明确剩余必需状态/故障/客户端/条件功能组合并补齐实际缺口；真实独立实审和两版 acceptance。
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
