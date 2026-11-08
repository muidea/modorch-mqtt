# 遗留项代码收口（2026-11-08）

本轮修复[复核报告](../2026-11-08-remaining-audit/README.md)确认的R-01–R-04，同步契约范围与能力文档。旧验证目录保留原始快照及反例。本目录保存本轮源码绑定的日志与最终结果，不自动提交或推送。

## 修复与回归

| 问题 | 当前行为 | 回归 |
|---|---|---|
| R-01 | 在线零过期MQTT5 Session可接管；断开后零过期清理，Clean Start明确重建 | 原订阅/标识、QoS1原ID/DUP、出站QoS2 Release、入站QoS2 ownership、后续交付与退出 |
| R-02 | 目标无Session时重提Transfer；Session仍在时沿原ID重发并设置DUP | 两种CONNACK状态、错误ID的PUBACK不释放源、未目标ACK前源积压保持、后续消息继续、ACK后EOF与检查式退出 |
| R-03 | 128普通User Properties加各一个唯一Origin/Hops槽位，全部受字节上限约束 | 两Broker实际Bridge的126/127/128普通属性、已有/新增元数据；超额/重复保留键/字节超限拒绝 |
| R-04 | 幂等记录真实属性参与恢复校验和预算 | 多记录SQLite重启预算一致、原请求幂等/异属性冲突、非法属性/自带订阅标识/预算或包长不足拒绝 |

Bridge额外修复双向select丢弃另一侧未完成poll的问题；poll Future保持所有权直至完成或整个Link取消。每Link仍只有一个受管任务，没有新增Runtime、Hub或后台任务。已解码事件优先于重连，避免PUBACK与EOF同批到达时延后释放源端积压。

## 材料与限制

框架预检见`framework-preflight.json`：固定Modorch0.0.3 rev/源码指纹通过。专项日志分别为`mqtt5-targeted.log`与`bridge-final.log`；`targeted-initial.log`及`bridge-reconnect-debug.log`保留首轮Bridge失败，不当作通过证据。

最终执行结果：

- [check-final.log](check-final.log)：整仓fmt/check/clippy/test/doc/doctest、Python工具与证据门禁、示例、TLS/Auth/SQLite短测、TOML及SIGKILL恢复全部通过，退出0。151个命名Rust测试、2个doctest、26项Python工具测试；相对前轮增加6个回归。
- [optional.json](optional.json)：7组可选场景、22个精确测试映射全部通过；[scenarios.json](scenarios.json)：40组3.1.1限定场景通过，完整规范仍Partial。
- [msrv.log](msrv.log)、[default-features.log](default-features.log)、[toml-only.log](toml-only.log)：Rust1.85全工作区/目标/Feature、Facade空Feature及仅TOML配置的8项测试通过。
- [paho.log](paho.log)、[paho-tls.log](paho-tls.log)、[paho5.log](paho5.log)：独立Paho3.1.1 TCP/TLS/Auth/ACL及MQTT5 TCP/WebSocket QoS0/1/2、属性、别名和订阅标识通过；[mqtt5-build.log](mqtt5-build.log)为对应示例Feature构建记录。
- [release-plan.json](release-plan.json)绑定当前输入；开发清单检查通过，正式评审仍0/178批准。[严格门禁](release-strict.log)和[完整规范门禁](complete-conformance.log)均按预期退出1，继续阻断Partial冒充完整发布；这不是本轮功能测试失败。
- `source-hashes.json`、`evidence-hashes.json`与`result.json`保存本轮输入增量、材料摘要及符合Module Skill结果Schema的报告。旧功能89份材料、旧复核16份材料摘要均保持不变。

`check-initial.log`在最后一处Bridge ACK校验防护之前通过，最终源码证据以`check-final.log`为准；早期专项日志不替代最终整仓执行。既有测试/实验凭据及短loopback测量边界保持原范围。

MQTT5 Enhanced Authentication、Bridge直接TCP及宿主TLS代理、每Core单监听、format2多Release拒绝歧义升级、禁止原地降级及停机Will延迟至重启保持既定边界。完整F6/F7规范、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/文件系统、硬件断电及独立评审未获新批准，正式发布保持Partial。
