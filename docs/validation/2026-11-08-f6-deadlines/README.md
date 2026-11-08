# F6 后端等待期间的连接期限验证（2026-11-08）

本轮修复连接等待Auth/Storage时无法检查期限，以及CONNECT超时取消持久提交的问题。完整设计/发布仍Partial。保留既有未提交修改和历史证据；没有提交或推送。

实现和范围见[设计§26](../../modorch_mqtt_broker_full_design_0.0.3.md)、[可靠性](../../reliability.md)、[剩余需求](../../remaining-work.md)。机器报告见[result.json](result.json)，实际输入及相对上一轮变更见[source-hashes.json](source-hashes.json)。

## 已执行验证

| 命令/范围 | 结果与原始证据 |
|---|---|
| Modorch module Skill1.0.7 preflight.py --manifest-path Cargo.toml | Passed；[最终预检](framework-preflight-final.json)，框架0.0.3实际源码指纹匹配 |
| CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh | Passed；[check.log](check.log)：114 Rust（Core100/Auth6/Extensions4/Storage3/rumqttc1）+1 doctest+12 Python，fmt/check/clippy/doc、宿主smoke与SIGKILL |
| cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline | Passed；[msrv.log](msrv.log) |
| cargo check -p modorch-mqtt --no-default-features --locked --offline | Passed；[default-feature.log](default-feature.log) |
| cargo build -p standalone-broker -p secure-modorch-server -p durable-broker --locked --offline | Passed；[examples-build.log](examples-build.log)、[构建退出码](build-status.json) |
| /tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py | Passed；[paho.log](paho.log)，QoS0/1/2和检查式停止 |
| 同一虚拟环境 scripts/verify-paho-tls.py | Passed；[paho-tls.log](paho-tls.log)，TLS/Auth/ACL/QoS2和检查式停止 |
| python3 scripts/check-conformance.py --results 本目录/check.log --output 本目录/scenarios.json | Passed；[scenarios.json](scenarios.json)：37场景、15个限定审计项通过，128编号TargetedEvidence、12编号NotRun；总体Partial |
| 同一规范命令追加 --require-complete | 预期exit1；[release-gate.json](release-gate.json)证实Partial不能通过完整规范门禁 |
| python3 scripts/audit-mqtt311.py --source-html /tmp/modorch-mqtt311-oasis.html | Passed；[audit-source.log](audit-source.log)，固定HTML摘要与9段候选核对；不代表完整语义审计 |
| check-project.py / Python编译 / sh -n / git diff --check | Passed；[auxiliary.log](auxiliary.log)；[工具及环境](environment.json) |

Cargo命令均使用上述隔离CARGO_HOME，没有改动依赖来源、版本或全局配置。Cargo重复包路径警告未改变预检确认的实际git源码；clippy最终以-D warnings通过。

## 修复与回归范围

新增9个Rust测试覆盖：TCP/TLS在提交回执被阻塞时KeepAlive仍关网；保留任务/许可到已开始提交确定结果；持久QoS2原ID重传不重复路由并发布Will；CONNECT提交/gate等待中超时；已连接发布等待gate超时后不能接纳；Publish/Subscribe迟到授权不写入；身份到期及长有效期/无限有效期；一字节duplex制造整帧写入背压、输入PING不重置期限；检查式停止在已提交回执等待期间先关网并排空，重启恢复已提交数据。精确测试名和实际Passed行由规范清单及check.log关联。

这只验证限定故障时点，不是完整调度/平台/FSM交叉乘积。期限依赖正常执行器调度，后端等待期间不并发解析网络包；内核中未解析的PING不延长期限。Will持久收尾需等待gate，不能保证与关网同时到达。停止时Will允许延迟到重启，format3升级限制保持既有边界。

## 初始失败记录

[reproduction-before.log](reproduction-before.log)记录旧实现在2秒等待窗口内未因KeepAlive关闭网络，命令为cargo test -p modorch-mqtt-core --all-features --locked --offline keepalive_closes_socket_during_commit -- --nocapture，exit101；修复后同一命令exit0见[reproduction-after.log](reproduction-after.log)。该after日志为中间实现，最终输入以source-hashes.json及check.log为准。

第一次补测的[targeted.log](targeted.log)有一个测试装置失败：无订阅/非Retained的QoS1未改变快照，不会触发Store Commit锁存器。改为Retained后最终通过。[check-first.log](check-first.log)因移除重复计时后遗留的未读初始赋值被clippy拒绝，已删除并通过最终门禁。早期日志保留，不作为当前源码失败或通过依据；[core-first.log](core-first.log)和[shutdown.log](shutdown.log)记录中间增量检查。

## 尚未验收

完整MQTT3.1.1跨段语义、全部出站布局与完整状态/故障/互操作矩阵仍未审计。MQTT5、WebSocket/Bridge、TOML、生产负载/长运行内存与恢复SLO、真实断电/跨平台、magicCommon消费共享JSON、远端CI及独立Agent完整发布门禁均未计为通过。当前仅本地Linux/WSL2 x86_64和固定框架版本证据。
