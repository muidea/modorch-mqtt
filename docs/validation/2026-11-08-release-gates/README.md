# 逐义务发布门禁与组合负载测量

本轮实现状态：门禁及组合测量代码完成；完整MQTT3.1.1/F8发布仍 **Partial**。基线Modorch0.0.3固定rev/源码指纹不变，工作区保留累计未提交修改；没有提交或推送。主Skill为项目MQTT开发Skill，并按Modorch App/Module 1.0.7执行预检和验证。

本轮增加项目§15全部21项及TOML合同的机器映射，117段编号正文/列表项覆盖140编号；同时保留跨段续文/表格/FSM审计。发布策略共176项，全部独立review初始NotRun，UN-008由具体规范审查推导。[发布结果](release.json)为Partial，默认严格命令[返回1](release-status.json)。开发模式允许Partial不代表发布批准。工具fixture具有真正Passed路径，仅验证门禁逻辑，不计入本项目认证。

[原始日志](check.log)完成fmt/check/clippy、122 Rust +2 doctest +24 Python、Rustdoc、已有/独立宿主、TOML CLI、组合TLS/Auth/SQLite QoS2 smoke、SQLite真实SIGKILL及端口归还。[构建状态](build-status.json)核对MSRV1.85、默认空Feature及仅TOML的7项测试；[Paho状态](interop-status.json)和[日志](paho-tls.log)验证TCP/TLS/Auth/ACL/QoS2。最终小幅工具目录/诊断调整另由[24项Python测试](python-final.log)、源码清单核对和严格发布命令复核；未改变Rust协议实现。

- [预检](framework-preflight-final.json)、[环境](environment.json)、[源码摘要](source-hashes.json)。全部既有139项输入保留，加6项新文件，共145项；22个变更/新增输入。
- [规范来源核对](audit.log)、[限定场景](scenarios.json)、[未批准计划](release-plan.json)、[发布诊断](release.json)。37组场景和15项限定审计通过，不升级完整语义。
- [最终测量](baseline-final.json)、[原始测量日志](benchmark-final.log)、[样本和摘要核对](benchmark-validation-final.json)。初轮baseline.json/benchmark.log保留，最终数据以带final的文件为准。
- [Skill验证](skill-validate.log)、[本地归档](skill-feedback.log)、[同步状态](skill-status.json)：项目Skill1.0.1补充TOML与发布/测量边界，仅本地feedback，无远端push。

## QoS2短测观察

18个场景共990条消息：三种组合 × 1/10个客户端 × QoS0/1/2，每客户端10条、payload128字节、单条在途。每次新库、双向完整交换，使用release构建；启动/订阅不计稳态吞吐，连接耗时另记。下表PUB→SUB分位数在完整消息体到达时采样，吞吐包含确认阶段；SQLite使用FULL/DELETE全量image路径，Auth仍对运行期操作做ACL检查。TLS/Auth示例还启用Admin/Metrics，与匿名TCP配置并不完全相同。

| 组合 | Clients | msgs/s | PUB→SUB p50/p95/p99 ms | sampled RSS KiB |
|---|---|---|---|---|
| tcp | 1 | 161.15 | 1.52 / 4.40 / 4.40 | 6208 |
| tcp | 10 | 1524.93 | 2.22 / 6.47 / 10.41 | 6736 |
| tls-auth | 1 | 154.38 | 1.83 / 3.60 / 3.60 | 27020 |
| tls-auth | 10 | 468.94 | 4.47 / 37.05 / 49.99 | 124940 |
| tls-auth-storage | 1 | 12.02 | 47.29 / 82.19 / 82.19 | 29228 |
| tls-auth-storage | 10 | 7.47 | 537.50 / 1199.32 / 1246.86 | 127312 |

SQLite全量image与多次确认提交呈现明显开销；样本与并发编译/文件系统环境有关，不能作为生产容量或SLO。RSS为10ms目标间隔的稳态采样，不代表全生命周期峰值，操作系统可延迟采样。退出耗时从全部交换及客户端清理之后开始，不代表高负载退出。临时数据库退出后清理；报告保留退出前大小，不提供本轮恢复峰值测量。

## 修正与证据完整性

首次Python负例在构造矛盾日志时仍保留旧评审文件摘要，触发了更早的摘要拒绝；修正fixture重新绑定后全部通过，原始[初次日志](python-initial.log)保留。

首次文档批处理变量复用导致四份文档被错误内容覆盖，链接检查阻断；在隔离目录重建本会话仅相关的文档修改，四份恢复内容均与上一轮SHA256一致后重新应用本轮增补。[恢复核对](document-restoration.json)和[最初阻断日志](check-initial.log)保留。最终链接检查成功，没有以HEAD版本覆盖用户累计修改。

## 尚未批准

完整规范跨段/FSM/出站布局/故障及互操作审查，生产负载/长期RSS/慢消费者/高负载退出与恢复SLO，真实断电/文件系统/平台矩阵、magicCommon消费、远端CI及独立Agent均无完整新增执行证据。门禁能判定缺项，不能替代这些执行。MQTT5、可选WebSocket/Bridge及后续运维扩展仍未实现，见[剩余清单](../../remaining-work.md)。SQLite format3升级边界及停止Will可延迟至重启保持既有设计。
