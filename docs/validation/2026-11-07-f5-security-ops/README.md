# F5 安全与运维子集执行记录（2026-11-07）

本轮新增 Auth/ACL、稳定会话归属、Will 预授权、TLS、按 IP 握手准入、可选 Admin/Metrics，以及安全组合示例和性能基线。**完整设计与 F5 尚未完成**：Storage、磁盘提交幂等和预监听分页恢复仍待开发；全部业务回执仍为 MemoryOnly。报告 overall_status=Partial，当前已实现范围的最终检查通过。

依赖仍为 Modorch 0.0.3 / `336ba20196044e1b81fb2ca297a18b5e258a5800`，Skill 1.0.7 / Contract 1.2.0；[预检](framework-preflight.log)再次核验框架指纹。基于 `05b4621` 的未提交工作区，含此前 F4 修改，本轮不自动提交。[环境](environment.json)与[源码指纹](source-hashes.json)记录测试对象；F4 历史证据保留于原目录，不据此替换其29测试时点。

## 最终验证

| 命令 | 结果/原始证据 |
|---|---|
| `CARGO_NET_OFFLINE=true ./scripts/check.sh` | [check.log](check.log)：fmt、check、clippy -D warnings、**45个Rust测试 + 1个doctest**、rustdoc -D warnings、existing/secure宿主smoke均通过 |
| `cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline` | [msrv.log](msrv.log)：通过，含Argon2/rustls及所有示例 |
| `cargo check -p modorch-mqtt --no-default-features --locked --offline` | [feature-checks.log](feature-checks.log)：基础通过；分别增加 `--features auth`、`tls`、`admin`、`metrics` 的四次独立编译也通过 |
| `python3 /home/rangh/aispace/modorch-rs/.agents/skills/modorch-module-development/scripts/preflight.py --manifest-path Cargo.toml` | [framework-preflight.log](framework-preflight.log)：Passed，指纹94c55b…947a9 |
| `cargo build -p standalone-broker -p secure-modorch-server --locked --offline` | [debug-build.log](debug-build.log)：通过 |
| `/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py` | [paho.log](paho.log)：独立Paho2.1.0验证QoS0/1、Retained、检查式退出和端口归还 |
| `/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py` | [paho-tls.log](paho-tls.log)：证书验证、正确/错误密码、QoS1、通配订阅ACL拒绝、连接隔离、退出和端口归还 |
| `cargo build --release -p standalone-broker --locked --offline` | [release-build.log](release-build.log)：通过 |
| `python3 scripts/benchmark.py --output docs/validation/2026-11-07-f5-security-ops/baseline.json` | [benchmark.log](benchmark.log)与[baseline.json](baseline.json)：8组全部完成，逐组检查式退出 |
| `python3 -m py_compile scripts/*.py`；`git diff --check` | [auxiliary-checks.log](auxiliary-checks.log)：通过 |
| Skill `validate-report.py` 检查 `result.json` | [report-validation.log](report-validation.log)：Schema和状态聚合通过；任务状态仍为Partial |

测试分布为 Core35（含Auth4/TLS3）、密码Provider5、Extensions4、rumqttc1，另有Facade doctest1。具体条款对应关系见[契约映射](../../../contracts/README.md)。这些数字不代表完整设计MUST全覆盖。

Cargo 对相邻上游工作区发出已有 duplicate-package manifest 警告；preflight仍确认所有实际解析的框架依赖为同一锁定Git来源。本项目源码的Clippy/Rustdoc警告门禁通过。

## 性能基线

release、TCP回环、匿名、payload128字节、每客户端20条消息，各客户端订阅自己的Topic，每次最多1条在途；QoS1等待PUBACK并确认下行。建连/订阅不计入吞吐计时。百分位为客户端PUB→SUB，包含Python事件循环、网络和队列开销。Broker配置及CPU/OS/RAM/Rust/框架版本见原始JSON。

`--benchmark` 显式使用4096字节最大包、512个总连接和单IP连接、1024次准入、宿主总任务1024及每组件任务1024；普通启动配置保持原值。逻辑内存预算256MiB；RSS每10ms采样并含建连后的一个采样点，不保证捕获真实瞬时峰值，也不含压测客户端RSS。

| Clients | QoS | Messages | msg/s | p50 / p95 / p99 (ms) | 采样峰值RSS (KiB) |
|---|---|---|---|---|---|
| 1 | 0 | 20 | 1103 | 0.57 / 0.91 / 6.45 | 5224 |
| 1 | 1 | 20 | 2216 | 0.43 / 0.54 / 0.63 | 5028 |
| 10 | 0 | 200 | 11826 | 0.75 / 1.28 / 1.41 | 5860 |
| 10 | 1 | 200 | 7444 | 1.24 / 1.89 / 2.03 | 5708 |
| 100 | 0 | 2000 | 14524 | 6.18 / 10.62 / 11.74 | 7988 |
| 100 | 1 | 2000 | 5145 | 19.00 / 26.76 / 29.67 | 8164 |
| 500 | 0 | 10000 | 5837 | 68.60 / 180.60 / 238.09 | 17532 |
| 500 | 1 | 10000 | 4573 | 93.90 / 209.81 / 280.68 | 17864 |

每组仅一次采样，运行于非独占的Linux/WSL2环境；没有生产SLO、TLS/Auth负载、磁盘恢复性能、内存长期稳定性或跨平台结论。配置允许500连接只表示该受控场景完成，不是默认配置容量承诺。

## 诊断与修复记录

首次全量密码测试中，两个身份/Will用例在debug Argon2并发计算时超过默认5s握手期限，返回EOF（[原始输出](tests-first-attempt.log)）；[独立诊断复跑](auth-diagnostic-rerun.log)通过。非计时密码用例的测试握手/Provider期限调整为30s，收包35s；生产默认期限未变，Core专用短期限拒绝测试保留。最终完整脚本已全部通过。

首次压测在500连接组达到宿主默认每组件256任务额度，失败清理等待未结束，已中断并保留[首次输出](benchmark-first-attempt.log)。显式benchmark模式提高宿主每组件任务额度，脚本现在取消残留建连任务并限制socket关闭等待；最终8组成功结果单独记录。文档已补充每组件任务配额约束。

## 未完成项

持久Storage的writer fencing、提交/查询幂等、离线订阅分页快照预监听恢复和崩溃/磁盘满验证；QoS2/完整MQTT3.1.1符合性；MQTT5、WebSocket、Bridge；持久审计、宿主HTTP入口；生产容量/恢复性能、完整兼容矩阵、跨平台与独立Agent/发布门禁仍待实现或验证。证书与密码策略为不可变配置，未提供热更新、在线撤销及mTLS身份认证。已安装Will按保存的Grant执行。

测试证书私钥及device/demo-password均是显式测试/演示资料，不作为生产默认。Admin无默认允许来源，Admin/Metrics无默认HTTP监听。完整状态见[设计§19](../../modorch_mqtt_broker_full_design_0.0.3.md)和[接入指南](../../security-and-operations.md)。
