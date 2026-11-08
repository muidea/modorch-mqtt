# Topic共享向量与F8开发交付验证（2026-11-07）

本轮完成共享Topic JSON向量、四份开发文档、MQTT专用Skill、本地文档检查及Linux CI定义。完整设计和发布状态仍为 **Partial**；范围见[设计§24](../../modorch_mqtt_broker_full_design_0.0.3.md)、[剩余需求](../../remaining-work.md)。历史验证目录未修改，本轮未提交或推送。

## 具体交付

- [39组JSON向量](../../../contracts/topic-vectors.json)从固定Modorch-RS rev的独立Rust预期规范化导出，保留ID、来源及SHA256；上游没有JSON fixture，未修改上游或执行magicCommon端。
- 三个新增Rust测试共用JSON，覆盖Core校验/匹配、Facade helper、真实Hub订阅与Handler投递、真实MQTT的Topic/Filter拒绝和路由。网络负例使用同发布者的可靠队列marker作为屏障，避免用短sleep推断未投递。测试专用Hub不影响产品复用宿主Hub。
- [开始使用](../../getting-started.md)、[架构](../../architecture.md)、[API](../../api.md)、[运维](../../operations.md)及[MQTT开发Skill](../../../.agents/skills/modorch-mqtt-development/SKILL.md)已补齐，固定0.0.3基线、Skill1.0.7及源码指纹。MQTT Skill1.0.0经skill-hub校验、演习和本地feedback归档，未推送远端。
- `check-project.py`阻止缺失必需交付文件或失效本地文件链接，接入check.sh并补充负例测试。`check-conformance.py --require-complete`在Partial时返回1；开发检查通过仍不批准完整规范发布。
- [Linux CI定义](../../../.github/workflows/check.yml)覆盖工作区、源码身份、MSRV、默认Feature边界、独立Paho及证据工件；仅本地解析YAML，远端执行NotRun。

## 实际执行

| 检查 | 结果与原始材料 |
|---|---|
| Modorch身份/Feature/指纹 | [framework-preflight.json](framework-preflight.json)：Passed，0.0.3 / Skill1.0.7 |
| 共享向量首次运行 | [topic-first.log](topic-first.log)：3项Passed |
| fmt/check/clippy -D warnings、Rustdoc -D warnings | [check.log](check.log)：Passed |
| Rust / doctest / Python | [check.log](check.log)：95项Rust（Core81、Auth6、Extensions4、Storage3、rumqttc1）、1项doctest、8项Python门禁测试，全部Passed |
| 宿主/安全宿主及SIGKILL恢复 | [check.log](check.log)：真实进程恢复、QoS2阶段/去重、离线订阅/Retained/Will及检查式退出Passed |
| 规范限定场景 | [scenarios.json](scenarios.json)：32组Passed，128编号TargetedEvidence、12编号NotRun；完整规范Partial |
| 完整规范阻断 | [release-gate.json](release-gate.json)、[release-scenarios.json](release-scenarios.json)：实际退出1，符合Partial不可发布的预期 |
| MSRV1.85与空默认Feature/三个示例 | [msrv.log](msrv.log)、[feature-build.log](feature-build.log)：Passed |
| 独立Paho TCP / TLS认证ACL | [paho.log](paho.log)、[paho-tls.log](paho-tls.log)：QoS0/1/2、TLS/Argon2/ACL及资源释放Passed |
| 三个项目Skill | [skills.log](skills.log)、[skills-status.json](skills-status.json)：内容/链接校验Passed，版本同步 |
| 文档/脚本/YAML/diff与摘要 | [auxiliary.log](auxiliary.log)、[source-hashes.json](source-hashes.json)、[environment.json](environment.json) |
| 报告Schema | [result.json](result.json)、[report-validation.log](report-validation.log) |

全部Cargo命令使用任务缓存 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo` 和locked/offline。源摘要覆盖当前累计未提交工程及项目Skill，排除validation目录和Python缓存，不能用旧日志替代后续变更的重验。上游向量源SHA与固定rev的git对象另行核对，见auxiliary.log。CI仅做YAML解析，不声称actionlint或远端行为通过。

## 未完成范围

完整3.1.1未编号MUST/规范表格/全部FSM审计仍未完成；共享向量不代表全部规范。MQTT5、WebSocket/Bridge、TOML接入及部分后续运维能力未实现。TLS/Auth/持久化/QoS2生产性能、长期RSS、恢复SLO、硬件断电、其他平台和独立Agent/完整MUST发布总门禁仍无完整证据。

SQLite format3旧版迁移限制与停止时Will允许延迟到重启的边界沿用[storage.md](../../storage.md)。SIGKILL不等于真实硬件断电。

## 复现

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
python3 scripts/check-conformance.py --results docs/validation/2026-11-07-f6-topic-release/check.log --output /tmp/topic-scenarios.json
# 下行命令在当前Partial状态应返回1
python3 scripts/check-conformance.py --results docs/validation/2026-11-07-f6-topic-release/check.log --output /tmp/topic-release.json --require-complete
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

其他机器使用自己的Cargo缓存和独立Paho虚拟环境。详细依赖与装配见getting-started；命令不会自动安装全局依赖。
