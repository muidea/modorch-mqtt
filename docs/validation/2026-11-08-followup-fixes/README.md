# 组合边界代码收口（2026-11-08）

本轮修复R-05–R-07，更新设计§33、持久格式及能力/回归映射。旧反例和验证目录保持原样。当前写format5，兼容读1/2/3/4；No Local对旧格式未知来源Retained采取保守跳过策略，普通订阅继续回放，重新发布恢复来源。跨协议恢复需显式Clean Session/Clean Start新建。完整规范与发布保持Partial。

## 修复行为与回归

| 编号 | 修复行为 | 验证 |
|---|---|---|
| R-05 | 拒绝跨协议恢复，原连接/订阅/QoS/ID保持；显式清理可换协议 | 两种协议、有/无非默认订阅选项、双向QoS2所有权、原连接PING、真实SQLite多轮重启；`mixed-probe.log`独立公开API复测 |
| R-06 | 按实际源属性选择可表示的最小编码，目标新增订阅标识按下行限制处理 | 两种协议8191/8192字节边界、Retained/Pending/入站QoS2/Will恢复、真实属性超限与幂等预算；`packet-probe.log`独立QoS1原反例复测 |
| R-07 | 接纳事务保存Retained来源并计费；回放按发布ClientID执行No Local | QoS0/1/2、其他ClientID、No Local关闭、同ID清理重连/SQLite重启、3.1.1发布及恢复Will、内部发布、旧格式和非法账本/不足预算；`no-local.log`独立TCP及PINGRESP屏障 |

format5增加逐Retained Topic来源账本，包含ClientID、Internal或LegacyUnknown。来源额外逻辑预算32字节加ClientID UTF-8字节数；恢复严格校验引用、身份和预算。来源独立于Session存续；旧Will缺失ClientID时同样采用未知来源策略。旧版本已经写出的协议/订阅选项不一致快照仍拒绝启动并保留原库，本轮不自动推断历史状态修复。

## 实际执行

- `framework-preflight.json`：固定Modorch0.0.3 rev及指纹通过；主Skill为modorch-mqtt-development1.0.2，报告遵循支持Module Skill1.0.7 Schema。
- `check-final.log`：完整`scripts/check.sh`退出0，包括fmt/check/clippy、**157个命名Rust测试、2个doctest、26项Python测试**、Rustdoc、宿主示例、TLS/Auth/SQLite/QoS2短测、TOML和SIGKILL恢复。相对上轮增加6项命名回归。
- `msrv.log`、`default-features.log`、`toml-only.log`：Rust1.85全workspace/all-targets/all-features、Facade空Feature及仅TOML的8项配置测试通过。
- `mqtt5-build.log`、`no-local.log`：从当前源码构建mqtt5/websocket示例，真实TCP确认原No Local反例已消除；记录二进制摘要和检查式退出0。
- `probe/`、`mixed-probe.log`、`packet-probe.log`：复制上一轮公开API探针并改为修复后预期，使用独立workspace和锁文件。跨协议恢复被拒绝后原MQTT5 Session可重启恢复；3.1.1 QoS1的8191/8192字节Retained均可重启。原反例目录不变，新探针返回0表示修复后预期通过。
- `paho.log`、`paho-tls.log`、`paho5.log`：Paho3.1.1 TCP/TLS/Auth/ACL与MQTT5 TCP/WebSocket QoS0/1/2、属性、别名及订阅标识通过。
- `optional.json`：7组可选场景、28个精确测试映射通过；`scenarios.json`：41组3.1.1限定场景通过，规范状态仍Partial。
- `release-plan.json`、`release-development.json`：绑定本轮源码及最终Cargo日志，开发清单通过。`release.json`/`release-strict.log`和`complete-conformance.json`/`complete-conformance.log`按预期退出1，仍阻止Partial冒充完整发布；独立评审仍0/178批准。
- `skill-validation.log`、`report-schema.log`：项目主Skill静态检查和结果Schema校验通过。`source-hashes.json`、`evidence-hashes.json`、`prior-verification.json`保留输入、原始材料及历史完整性核对；没有提交或推送。

## 调试记录与限制

`mixed-initial.log`记录首轮测试将恢复Will与Retained反例复用同一Topic、导致预期值被Will正确替换的测试失败；随后改为独立Topic。`core-initial.log`在扩展回归前124项通过。`core-second.log`记录旧幂等预算测试读到临时4MiB维护工作区的时序失败；改为在2秒有界期限内等待工作区释放后核对持久账本，永久泄漏仍会失败。最终结论以`check-final.log`为准；初始编译警告不作为clippy通过材料。

旧功能、旧复核、上轮修复及本轮前复核分别89/16/30/22份材料保持原摘要。当前源码和状态文档的变化另存新快照，不改写历史输入或旧发布计划。测试与示例仍使用实验凭据/证书；短loopback测量不构成生产SLO。

完整F6/F7规范及互操作/故障总矩阵、生产负载/长期RSS/满载退出恢复SLO、magicCommon消费、远端CI、跨平台/文件系统、硬件断电及独立Agent/安全/性能评审仍待真实材料。单Core单监听、Bridge TCP及显式宿主TLS代理、format2多Release歧义拒绝、禁止降级和停机Will延迟重启的边界保持。
