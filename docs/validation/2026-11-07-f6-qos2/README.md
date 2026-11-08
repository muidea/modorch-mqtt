# F6 双向 QoS2 验证（2026-11-07）

本轮在 F5 持久化实验实现上补齐双向 QoS2、资源限额、恢复和协议重传。版本仍为 `0.1.0-alpha.1`，完整设计/发布状态 **Partial**；不从核心 QoS2 FSM 推断完整 MQTT3.1.1 所有 MUST 已通过。当前说明见 [qos2.md](../../qos2.md)，设计进度见[完整设计 §21](../../modorch_mqtt_broker_full_design_0.0.3.md)。历史 F4/F5 证据未修改。

## 最终执行结果

| 检查 | 结果/原始证据 |
|---|---|
| fmt、workspace all-target/all-feature check、clippy `-D warnings` | [check.log](check.log)，全部成功 |
| 工作区 Rust tests / doctest | [check.log](check.log)：68项 Rust tests（Core54、Auth6、Extensions4、Storage3、rumqttc1）+1项doctest |
| Rustdoc `-D warnings`、两个宿主 smoke | [check.log](check.log)，成功 |
| SIGKILL / 检查式退出 | [check.log](check.log)末尾3条PASS：2次SIGKILL/3次启动；QoS1离线索引/Retained/Packet ID/DUP/Will，QoS2接收去重和发送Publish/Release两阶段恢复、原ID、不重复路由；最后SIGINT及端口归还 |
| MSRV 1.85 | [msrv.log](msrv.log)，workspace all-target/all-feature成功 |
| Facade无默认和单独storage/auth/tls/admin/metrics | [feature-checks.log](feature-checks.log)，6组独立检查成功 |
| 框架身份 | [framework-preflight.log](framework-preflight.log)，固定Git rev和逐文件指纹成功 |
| 最终示例重建 | [debug-build.log](debug-build.log)，standalone/secure/durable成功 |
| 独立Paho TCP和TLS | [paho.log](paho.log)、[paho-tls.log](paho-tls.log)：QoS0/1/2、完整发布握手、QoS2Retained、TLS/Argon2id/ACL与退出成功 |
| Python/Shell/diff/链接/输入摘要 | [auxiliary-checks.log](auxiliary-checks.log)、[source-hashes.json](source-hashes.json) |
| 报告契约 | [result.json](result.json)、[report-validation.log](report-validation.log) |

全部 Cargo 验证使用 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo` 的任务缓存、locked/offline，沿用上轮缓存以避开无关项目的全局 Cargo 锁。本轮无依赖或框架rev变化。环境与基准提交见 [environment.json](environment.json)，源码摘要包含累计未提交工作区输入，不含验证证据目录本身。Cargo 的“skipping duplicate package”警告只表示跳过本地同名副本；实际解析源和指纹已核验。

## 用例覆盖

- `src/tests/qos2.rs`：接收端同ID重传（包括不同内容）只PUBREC、不再次路由，重复/未知PUBREL回复PUBCOMP，完成后ID复用；QoS1复用未完成QoS2 ID拒绝；Clean Session清理。
- 同文件验证下行PUBACK/过早PUBCOMP不释放QoS2 ID，PUBREC及重复PUBREC推进/重发PUBREL，重连原Publish阶段DUP和Release阶段只PUBREL；窗口/顺序/容量释放，Will和Retained完整握手，以及Core监督重建和一次激活补偿。
- `src/tests/storage.rs`：3个QoS2用例覆盖跨新Application双向状态恢复；分别延迟首次接收、接收PUBREL、发送PUBREC转Release及发送PUBCOMP收尾的持久提交，确认wire不能提前推进；分别拒绝四阶段提交，确认失败后恢复最后committed状态，不丢消息。
- `state/snapshot.rs`：旧format1缺失字段升级至format2；非法QoS2发送阶段和配额账本拒绝，不替换原状态。Codec验证ACK固定flags/非零ID/精确长度。
- `src/tests/eventhub.rs`：内部QoS2发布与稳定业务ID重试不重复交付，网络QoS2重传只生成一次入站观察。观察继续为best effort。
- Auth：原发布QoS参与ACL，允许方完整QoS2、低QoS策略拒绝方无PUBREC/新接收记录。Metrics固定名字36条，新增接收端QoS2条数/字节。
- 独立rumqttc用例等待上行PUBCOMP与下行客户端实际发出PUBCOMP后结束；Paho另验证TCP和TLS/密码组合。崩溃脚本仅用Python标准库及临时数据库。

## 诊断和边界

[phase-first-failure.log](phase-first-failure.log)保留一次早期并发测试中恢复启动失败的输出，当时该新用例复用150ms请求期限；[单独复跑](phase-diagnostic-rerun.log)成功。后将新的阶段正确性用例改用2s请求期限，原有150ms丢回执/不确定提交用例不变。最终完整check通过；不把早期单独复跑当作最终回归证据。[check-before-packet-id-rule.log](check-before-packet-id-rule.log)为最终规范对齐前的成功中间运行，当前源以check.log为准。

当前仅Linux/WSL2 x86_64，SIGKILL不等于真实硬件断电。SQLite仍为FULL/DELETE/单库独占有界全量image，未测本轮存储性能/恢复SLO。QoS2只保证会话内协议交换，不提供跨系统业务exactly-once。停止时不新建关键持久承诺，已保存armed Will可延迟至重启，沿用F5边界。

format2可由新版读取旧format1后升级；旧二进制拒绝format2，不能原地降级。完整3.1.1规范矩阵、MQTT5、WebSocket/Bridge、生产/跨平台/独立Agent与发布门禁仍未完成。

复现主要命令：

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo check -p modorch-mqtt --no-default-features --features storage --locked --offline
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo build -p standalone-broker -p secure-modorch-server -p durable-broker --locked --offline
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

Paho依赖使用已有虚拟环境，脚本不自动安装。其他机器可使用自己的Cargo缓存，不要求该临时路径存在。
