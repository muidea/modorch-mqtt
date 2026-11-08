# F6服务端出站线帧与故障回归

本轮补齐7项回归测试和对应契约映射，未改变生产协议行为、公共API或持久格式。完整MQTT3.1.1/F8发布仍为 **Partial**；未提交或推送。沿用项目MQTT开发Skill1.0.1和Modorch模块Skill1.0.7，固定Modorch0.0.3 rev/源码指纹不变。

## 本轮收口

- 独立[出站字节fixture](../../../contracts/mqtt311-outbound.json)包含16个控制响应、14个PUBLISH向量、12个Remaining Length边界和7种CONNACK上下文；5项[测试](../../../crates/modorch-mqtt-core/src/tests/outbound.rs)核对九类Server输出，包括真实TCP、live/retained、Session恢复、DUP/原ID及PUBREL恢复。预期不由生产Codec生成或解码。
- 一字节duplex触发QoS1/2部分PUBLISH写超时，验证内存持久Session保留原ID并以DUP重连、完整确认后释放。该测试不代表内核/TLS/SQLite的全部写中断时序；QoS0不重发另由出站真实TCP测试核对。
- 真实TCP慢消费者在AwaitPubComp阶段占满窗口/积压，验证项目整体接纳政策：新QoS2 Retained在PUBREC前拒绝、健康目标无部分投递、旧Retained保留、无幽灵入站记录；容量释放后同ID重试成功且重复不再路由。它是有界正确性测试，不是生产慢消费者性能批准。

## 执行证据

[完整检查](check.log)通过fmt/check/clippy、129项Rust测试、2项doctest、24项Python测试、Rustdoc、已有/独立宿主、TOML、TLS/Auth/SQLite QoS2 smoke及SQLite SIGKILL恢复/端口归还。[构建状态](build-status.json)记录MSRV1.85、空默认Feature和仅TOML的7项测试均exit0。[Paho状态](interop-status.json)与[TCP](paho.log)/[TLS](paho-tls.log)日志确认外部客户端QoS0/1/2及安全组合回归。

[框架预检](framework-preflight.json)、[环境](environment.json)、[源码摘要](source-hashes.json)绑定当前工作区。上轮145项源码输入全部保留，新增fixture和测试文件，共147项；相对上轮18个输入新增/变更。累计git diff包含历史未提交内容，不用于推断本轮修改范围。

[规范来源核对](audit.log)通过固定OASIS HTML、117段编号正文和9段未编号候选校验；[限定场景结果](scenarios.json)包含40组场景、17项限定审计及128个编号TargetedEvidence。12个范围说明仍不等于实现通过。首次独立执行日志保留：[出站](outbound-initial.log)、[部分写入](partial-write-initial.log)、[慢消费者](slow-consumer-initial.log)、[Python](python.log)。

[发布计划](release-plan.json)绑定本轮完整日志与源码，178项独立review均NotRun；[严格结果](release.json)为Partial且返回1，开发allow-partial返回0；完整规范模式返回1，见[门禁退出状态](gate-exits.json)。门禁检查按预期阻断缺项不表示真实发布批准。[结构化报告](result.json)符合已安装Modorch结果Schema；[最终检查](auxiliary.log)和[材料摘要](evidence-hashes.json)记录证据完整性。

## 仍需继续

完整跨段语义与CONNECT/Session/Will/订阅/QoS故障及互操作总矩阵仍需实审补测；本轮有限向量不批准完整FSM或全部TLS/存储组合。生产代表性负载、长期RSS、高负载退出与恢复SLO，真实硬件断电/文件系统/跨平台、magicCommon消费、远端CI和独立Agent执行仍NotRun。MQTT5、可选WebSocket/Bridge及后续运维扩展仍未实现，见[持续清单](../../remaining-work.md)。

本轮未新增性能测量；[上轮18项组合短测](../2026-11-08-release-gates/README.md)仅保留为历史实验基线，不能批准本轮生产SLO。SQLite format3升级与停止Will可延迟至重启的既有边界继续有效。
