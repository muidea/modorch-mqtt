# Topic Alias恢复与Skill说明收口（2026-11-08）

本轮对应R-08与D-01。原反例目录保持原样；继续写format5、兼容读1/2/3/4。修复网络消息的恢复大小判断，保留真实属性验证、完整Topic预算、内部消息和Will限制，以及下行包长策略。完整规范及发布仍为Partial。

## 修复与回归

- 网络消息允许合法完整Topic源表示，或消息帧与绑定帧都可在上限内表示的Topic Alias源表示；计算包含真实属性及两类变长长度字段。别名不持久化，完整Topic/属性仍计费。
- 单测覆盖8191/8192与差一字节、Property Length跨127、绑定帧边界、format4迁移、完整预算恢复、非法属性以及Internal Retained/Will/内部幂等严格限制。
- 真实TCP/SQLite覆盖Retained和双向QoS2所有权：原8192监听配置可重启；显式提高至10000后交付展开完整Topic，保留Content Type。MQTT5超下行限额丢弃并继续PING；3.1.1原连接层超限拒绝保持。
- 真实TCP拒绝别名0、未绑定别名、重连后旧别名、8193字节入站线帧和预算不足的合法8191字节帧；旧Retained保持，无幽灵入站QoS2记录。
- Skill1.0.3同步format5、跨协议恢复、LegacyUnknown及别名边界，经validate/feedback预览/本地归档后status为synced；未推送远端。

## 执行记录

- `framework-preflight.json`：固定Modorch0.0.3 rev及源码指纹通过；主Skill MQTT1.0.3，报告遵循支持Module Skill1.0.7 Schema。
- `targeted-final.log`：4项别名相关测试通过，包含3项新增回归；`alias-probe.log`：复制旧公开API探针并改为修复后预期，真实TCP/SQLite确认完整Topic对照和原别名反例均可重启、Retained保持。
- `check-final.log`：完整scripts/check.sh退出0，含fmt/check/clippy、160项命名Rust测试、2项doctest、26项Python测试、Rustdoc、宿主示例、TLS/Auth/SQLite/QoS2短测、TOML及SIGKILL恢复。
- `msrv.log`、`default-features.log`、`toml-only.log`：Rust1.85全workspace/all-targets/all-features、Facade空Feature与仅TOML的8项配置测试通过，汇总见`compatibility.json`。
- `paho.log`、`paho-tls.log`、`mqtt5-build.log`、`paho5.log`：Paho3.1.1 TCP/TLS/Auth/ACL及MQTT5 TCP/WebSocket QoS0/1/2、属性、别名和订阅标识通过；使用隔离venv和本轮构建二进制。
- `optional.json`：7组可选场景、31个精确映射通过；`scenarios.json`：41组3.1.1限定场景通过，完整规范仍Partial。
- `release-plan.json`绑定最终源码与Cargo日志，`release-development.json`开发清单通过；`release.json`严格发布与`complete-conformance.json`完整符合性按预期退出1，0/178独立评审批准。`gate-checks.json`记录真实退出码及预期，不将门禁拒绝写成完整验收通过。
- `skill-validation.log`、`skill-feedback-preview.log`、`skill-feedback.log`、`skill-status.json`与`skill-sync.json`记录Skill校验/归档和文件摘要相同；无全局托管副本，无远端推送。
- `source-hashes.json`绑定本轮输入，`evidence-hashes.json`绑定验证材料；`prior-verification.json`核对六组历史材料89/16/30/22/37/16份全部匹配。`result.json`及`report-schema.log`保存结构化结果和Schema核验；没有提交或推送。

`targeted-initial.log`与`targeted-backtrace.log`保留首轮错误测试假设：在8192监听下尝试交付展开后超过8192的3.1.1报文，连接层按限额关闭。测试改为分别验证原上限恢复和显式提高上限后的交付。`check-initial-links.log`记录证据README尚未创建时的链接校验失败；补齐后再执行完整检查。

完整F6/F7符合性、生产负载/长期RSS/满载退出恢复SLO、magicCommon消费、远端CI、跨平台/文件系统、硬件断电及独立Agent/安全/性能评审仍待真实材料。
