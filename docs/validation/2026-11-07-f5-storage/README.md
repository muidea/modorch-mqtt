# F5 Storage 实验实现验证（2026-11-07）

本记录对应 `0.1.0-alpha.1` 的可选 SQLite QoS0/1 持久化实现，基准提交 `05b4621`，工作区包含尚未提交的安全运维和 Storage 修改。完整设计状态仍为 **Partial**。范围和限制见 [持久化接入](../../storage.md)及[设计 §20](../../modorch_mqtt_broker_full_design_0.0.3.md#20-f5-sqlite-storage-实验实现2026-11-07)。原 [F4](../2026-11-07-f4/README.md)、[安全运维](../2026-11-07-f5-security-ops/README.md)记录保留历史范围和性能结果，不追溯修改。

## 已执行

| 检查 | 结果与证据 |
|---|---|
| fmt、workspace all-target/all-feature check、clippy `-D warnings` | [check.log](check.log)，全部成功 |
| Cargo tests / doctest | [check.log](check.log)：54项 Rust tests + 1项 doctest；Core41、Storage3、Auth5、Extensions4、rumqttc1 |
| Rustdoc `-D warnings`、已有宿主/安全宿主 smoke | [check.log](check.log)，成功 |
| 真实子进程崩溃恢复 | [check.log](check.log)末尾两行 PASS；临时数据库，2次 SIGKILL、3次启动；离线订阅先于重连、Retained、原 Packet ID/DUP、clean-session armed Will，以及 SIGINT 检查式退出/端口归还 |
| MSRV 1.85 all-target/all-feature | [msrv.log](msrv.log)，成功 |
| Facade无默认/单独storage/auth/tls/admin/metrics | [feature-checks.log](feature-checks.log)，成功 |
| 框架身份 | [framework-preflight.log](framework-preflight.log)：固定 rev、0.0.3/Contract1.2.0/Skill1.0.7，指纹通过 |
| 重建独立客户端示例及 Paho TCP/TLS | [debug-build.log](debug-build.log)、[paho.log](paho.log)、[paho-tls.log](paho-tls.log)，成功 |
| Python/Shell语法、diff、相对链接、源码摘要 | [auxiliary-checks.log](auxiliary-checks.log)、[source-hashes.json](source-hashes.json) |
| 报告契约 | [result.json](result.json)、[report-validation.log](report-validation.log) |

全部 Cargo 验证使用 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo` 的任务缓存，最终使用 locked/offline。新依赖来源与摘要见 [dependency-checksums.json](dependency-checksums.json)，环境见 [environment.json](environment.json)。缓存用于避开其他项目的 Cargo 全局锁，没有修改宿主全局配置或依赖上游。Cargo 会报告跳过本地同名框架副本的重复包警告，实际解析的固定 Git 源与指纹已单独核验。

复现命令：

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo check -p modorch-mqtt --no-default-features --features storage --locked --offline
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo build -p standalone-broker -p secure-modorch-server --locked --offline
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

其他机器可使用自己的正常 Cargo 缓存；Paho 是可选互操作依赖，不由脚本自动安装。崩溃验证仅依赖 Python 标准库，在 check.sh 内先构建 durable-broker 再运行。

## 故障与语义证据

Core `src/tests/storage.rs` 的6个真实 TCP/Hub用例覆盖恢复前禁止监听、离线索引、Retained/Inflight、提交回执延迟屏障、丢回执Query、失败无ACK/下行、错误页面/缩小配额失败、业务幂等/能力校验、owner/Will授权跨新Application。提交成功/失败计数及耗时的断言也在这些用例内。

Provider 的3个用例覆盖 epoch/CAS/摘要/幂等Query、Complete/Abort旧epoch fencing、页进度、独占库、snapshot超限和真实 SQLite `max_page_count` 引起的 DiskFull；失败不推进 committed version。另验证不相关库/非4096页库拒绝且原文件字节不变。上述运行结果见 check.log，不把仅有测试源码当作通过证据。

## 限制与未完成项

- 仅 Linux/WSL2 x86_64；SIGKILL 证明进程崩溃恢复，没有模拟真实硬件断电、设备丢写、任意文件系统或网络盘语义。
- SQLite FULL/DELETE、独占单库、有界全量 JSON image及字节分页；并未实现增量Store、WAL、多writer或生产高吞吐。逻辑内存和主库/journal额度不等于进程RSS/文件系统分配的绝对上限。未测量存储吞吐、恢复SLO或容量门禁，不复用历史内存性能结论。
- 停止阶段不新建关键存储承诺，已提交 armed Will 可延迟到下次启动处理；检查式退出不意味着退出时新做了持久dump。正常客户端 DISCONNECT 运行期持久清除 Will。
- 私有Store capability不替代进程可信边界；数据库无静态加密。Admin审计和运行指标仍为内存，策略/证书无热更新。
- QoS2/完整MQTT3.1.1、MQTT5、WebSocket、Bridge、跨平台与独立Agent/发布门禁未完成。

本轮持久化子集验证通过，完整需求与发布状态为 Partial。
