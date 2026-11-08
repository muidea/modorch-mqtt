# Core TOML配置验证（2026-11-08）

本轮实现设计§9的Core TOML入口及宿主Provider引用，并修复上一轮超时监督遗漏的指标分类。完整设计/发布仍Partial；历史证据和累计未提交修改保留，本轮没有提交/推送。

实现与边界见[设计§27](../../modorch_mqtt_broker_full_design_0.0.3.md)、[TOML文档](../../toml.md)、[剩余需求](../../remaining-work.md)。结果见[result.json](result.json)，最终输入摘要及相对上一轮变更见[source-hashes.json](source-hashes.json)。

## 交付范围

Facade默认关闭的 `toml` Feature提供 `MqttTomlConfig`，默认禁用Core；显式安全策略、当前全部Core限额和毫秒期限使用同一Rust validate。独立64KiB文档严格拒绝未知/重复/不支持的配置；诊断不回显原文。TLS相对路径由宿主给定目录解析，持久capability从宿主注入，扩展只声明组件引用。解析/注册不启用组件、不开放端口；账号/ACL、数据库与磁盘额度、Admin/Metrics内部参数仍由宿主配置。

`standalone-broker --config FILE [--once]`及[开发配置](../../../examples/standalone-broker/mqtt.toml)可运行；文件模式与位置地址/benchmark互斥。独立示例只注册Core，完整扩展通过已有宿主显式注册。检查式退出等待已有Application排空；没有新Runtime/Hub或独立后台任务。

超时监督现在保留KeepAlive/写入/身份期限类别；持久提交排空后的Closed不会覆盖超时原因，原timeouts/slow_writers计数继续有效。真实持久回执阻塞测试追加timeouts=1断言，整帧背压测试核对slow writer timeout原因。

## 已执行验证

以下Cargo命令使用 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo`，锁文件完成更新后均加 `--locked --offline` 或设置 `CARGO_NET_OFFLINE=true`；没有修改全局配置。

| 命令/范围 | 结果与证据 |
|---|---|
| app Skill1.0.7 preflight.py --manifest-path Cargo.toml | Passed；[framework-preflight-final.json](framework-preflight-final.json)，Modorch0.0.3及源码指纹匹配 |
| CARGO_NET_OFFLINE=true ./scripts/check.sh | Passed；[check.log](check.log)：122 Rust（Core100/Auth6/Extensions4/Storage3/rumqttc1/TOML8）+2 doctest+12 Python；fmt/check/clippy/doc/宿主smoke/CLI/SQLite SIGKILL |
| cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline | Passed；[msrv.log](msrv.log) |
| cargo check -p modorch-mqtt --no-default-features --locked --offline | Passed；[default-feature.log](default-feature.log)；[依赖树](default-feature-tree.log)确认空默认Feature没有TOML解析依赖 |
| cargo test -p modorch-mqtt --no-default-features --features toml --test config --locked --offline | Passed；[toml-only.log](toml-only.log)：7测试，无TLS/内置Provider Feature，启用TLS的配置被拒绝 |
| cargo build -p standalone-broker -p secure-modorch-server -p durable-broker --locked --offline | Passed；[examples-build.log](examples-build.log)、[各命令退出码](build-status.json) |
| python3 scripts/verify-toml.py | Passed；[toml-cli.log](toml-cli.log)：配置启用/禁用、拒绝/脱敏/UTF8/大小/参数冲突，真实QoS2 Retained双向握手、SIGINT检查式停止和端口归还 |
| /tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py | Passed；[paho.log](paho.log)：QoS0/1/2及检查式退出 |
| 同一虚拟环境 scripts/verify-paho-tls.py | Passed；[paho-tls.log](paho-tls.log)：TLS/Auth/ACL/QoS2及检查式退出 |
| check-conformance.py --results 本目录/check.log --output 本目录/scenarios.json | Passed；[scenarios.json](scenarios.json)：37限定场景和15限定审计项；128编号TargetedEvidence、12编号NotRun，总体Partial |
| 相同规范命令 --require-complete | 预期exit1；[release-gate.json](release-gate.json)阻断Partial当作完整符合性通过 |
| MQTT-CONFIG-001精确测试Passed行核对 | Passed；[configuration-tests.json](configuration-tests.json)，8个测试名及日志/源码摘要；不是完整发布认证 |
| check-project.py / Python编译 / sh -n / git diff --check | Passed；[auxiliary.log](auxiliary.log)与最终文档/摘要检查；[环境](environment.json) |

本轮8个新Rust测试包括全部Core映射及默认值、严格/安全拒绝、边界与脱敏、Provider与密钥、TLS Feature/路径、注册不启用、真实TCP QoS1及完整Auth/TLS/Storage/Admin/Metrics宿主装配。后者使用真实SQLite预监听恢复及Metrics采样；登录与ACL网络行为由Paho TLS回归覆盖，不把无账号的配置装配测试冒充登录通过。

## 依赖与初始记录

[initial-check.log](initial-check.log)记录离线解析新TOML依赖。新增toml0.8.23及7个解析传递依赖；解析器默认不启用display或unbounded。Cargo初次解析同时更新的wasip2/wit-bindgen已恢复原版本，见[lock-preservation.log](lock-preservation.log)。Modorch来源、rev及实际源码指纹未改变。

[config-tests-initial.log](config-tests-initial.log)记录测试误用不存在的BrokerStatusRequest导致的编译失败，已修正为既有状态接口的 `()` 请求；[config-tests.log](config-tests.log)随后8测试通过。最终证据使用check.log和当前source-hashes.json，初始日志不替代最终结果。

## 验收限制

本轮关闭的是Core TOML映射与宿主扩展引用配置；不提供Provider内部策略文件、在线重载、环境插值或include/覆盖合并。完整MQTT3.1.1跨段/全部出站布局/完整FSM、F7 MQTT5、WebSocket/Bridge、生产TLS/Auth/Storage/QoS2负载与恢复SLO、真实断电/跨平台、magicCommon共享JSON消费和独立Agent/全部MUST发布门禁仍未完成。新增CI测试定义未计为远端运行证据；验证平台仅本地Linux/WSL2 x86_64。format3升级与停止Will延迟恢复的边界不变。
