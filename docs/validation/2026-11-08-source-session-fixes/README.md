# Bridge源会话边界修复（2026-11-08）

本轮收口R-09：Transfer绑定源Session代际，源会话丢失后旧目标工作继续，完成后跳过旧源ACK，再订阅新Session。源重连未确定Session时不排ACK，断线错误分支再次清理客户端请求队列，避免清理后排队的旧ACK跨重连发送。公共API、Feature默认关闭和format5不变，完整发布仍Partial。

## 行为与回归

保持每方向一个Transfer。新Session订阅等待旧目标工作完成；原Session恢复可沿用原订阅、ID和目标交换。异常额外可靠投递不作为QoS0丢弃，而是断线保留源端所有权。源Session丢失后，新订阅建立前的发布不能依赖已丢失Session的订阅积压，需由源业务保留/重发；不重建源Broker已经丢失的消息，不承诺跨Broker exactly-once。

新增2个真实Bridge/TCP命名测试覆盖同/不同ID、连续源会话丢失、源和目标同时丢失、源恢复/DUP、源CONNECT等待CONNACK期间目标完成确认、新消息自己的目标ACK及后续屏障。所有等待有界，Keep Alive正向屏障验证没有旧ACK或过早订阅，结束检查式退出。

公开API探针从原审计目录复制后改成修复后预期。源Session丢失轨迹的目标顺序包含`old-owned -> new-session-value -> next-barrier`；新ID1直到自己的目标PUBACK后才获源PUBACK。Session恢复对照仍只提交一次旧目标消息。原审计材料不改写。

项目MQTT Skill同步至1.0.4，按托管Skill流程校验、feedback预览及本地归档，未远端推送。

## 执行记录

- `framework-preflight.json`：固定Modorch0.0.3 rev/指纹通过，主Skill MQTT1.0.4，结果遵循Module Skill1.0.7 Schema。
- `bridge-targeted.log`：6项Bridge网络测试通过，含2项新增命名回归；`source-session-probe.log`和`source-resumed-probe.log`是公开API修复后预期及正常恢复对照，检查式退出均通过。
- `check-final.log`：完整scripts/check.sh退出0，含fmt/check/clippy、162项命名Rust测试、2项doctest、26项Python测试、Rustdoc、示例、TLS/Auth/SQLite/QoS2短测、TOML和SQLite SIGKILL恢复。
- `msrv.log`、`default-features.log`、`toml-only.log`及`compatibility.json`：Rust1.85全workspace/all-targets/all-features、Facade空Feature及仅TOML的8项配置测试通过。
- `paho.log`、`paho-tls.log`、`mqtt5-build.log`、`paho5.log`：Paho3.1.1 TCP/TLS/Auth/ACL、MQTT5 TCP/WebSocket QoS0/1/2、属性/别名/订阅标识通过，使用隔离venv和本轮二进制。
- `optional.json`：7组可选场景、33个精确映射通过；`scenarios.json`：41组3.1.1限定场景通过，完整规范仍Partial。
- `release-plan.json`绑定本轮最终源码及Cargo日志，`release-development.json`开发清单通过；`release.json`严格发布和`complete-conformance.json`完整符合性命令按预期退出1，独立评审仍0/178批准。`gate-checks.json`区分真实退出码和门禁负例预期。
- `skill-before.json`、`skill-before-hash.json`记录项目/归档原1.0.3一致；`skill-validation.log`、`skill-feedback-preview.log`、`skill-feedback.log`、`skill-status.json`和`skill-sync.json`核验1.0.4本地归档同步；没有远端推送。
- `source-hashes.json`和`evidence-hashes.json`保存本轮输入及材料摘要；`prior-verification.json`核对八组历史证据89/16/30/22/37/16/45/18份全部匹配。`result.json`及`report-schema.log`保存结构化结果与Schema校验。没有提交或推送。

`bridge-targeted-initial.log`保留测试端在连续源重连期间未响应目标Keep Alive而停等的首次执行，后主动终止。补齐目标PINGRESP并为整个场景和应用报文循环设置总期限后通过；该记录不作为产品修复通过材料。`bridge-existing.log`为补新回归前的4项既有Bridge测试通过记录。

完整F6/F7、生产负载/长期RSS/满载退出恢复SLO、magicCommon消费、远端CI、跨平台/文件系统、硬件断电及独立Agent/安全/性能评审仍待真实材料，正式发布继续Partial。
