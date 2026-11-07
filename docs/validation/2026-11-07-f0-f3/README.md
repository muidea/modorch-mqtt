# F0–F3 QoS0/1 实验子集验证

执行日期：2026-11-07。目标：按用户确认的首轮范围实现可运行的 QoS0/1 内存版本，不将整个完整设计标为完成。版本 `0.1.0-alpha.1`，框架 `modorch 0.0.3`，Git rev `336ba20196044e1b81fb2ca297a18b5e258a5800`，Contract 1.2.0，使用匹配的 `modorch-module-development` Skill 1.0.7。

实际环境为 Linux x86_64 / WSL2，Rust/Cargo 1.94.0，另用 Rust 1.85.0 检查 MSRV；详见 [environment.json](environment.json)。结果仅覆盖该环境。

| 命令 | 状态 | 原始证据 |
|---|---|---|
| `./scripts/check.sh`：fmt/check/clippy/test/doc/现有宿主示例 | Passed | [check-final.log](check-final.log) |
| `cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline` | Passed | [msrv-final.log](msrv-final.log) |
| 匹配框架 Skill 的 `scripts/preflight.py --manifest-path Cargo.toml` | Passed | [framework-preflight.log](framework-preflight.log) |
| `cargo build -p standalone-broker --locked --offline` | Passed | [standalone-build.log](standalone-build.log) |
| `/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py` | Passed | [paho.log](paho.log) |
| `sh -n scripts/check.sh`、Python `compile()` 语法检查 | Passed | 本轮工具执行结果；未运行静态 Python 类型检查 |

Cargo 测试共 **18 个**：Core 单元/真实 TCP 测试 17 个，独立 rumqttc 0.25.1 集成测试 1 个；另有 **1 个 Rustdoc 编译示例**通过。Paho 2.1.0 测试单独执行，不计入 Cargo 测试数量。

测试覆盖 Ready 前不 listen、后续激活失败/任务配额补偿、取消 start 后资源回收、匿名安全配置、畸形包局部关闭、QoS0 自回送、QoS1 ACK/离线/重连 DUP 和 Packet ID、重叠订阅、Retained 回放/删除、Will 异常断开/接管/KeepAlive、握手期限、保留命名空间（含 Will）、长 Topic、整次可靠发布饱和拒绝、跨应用隔离。监督重建测试额外注入一次替代实例激活失败，验证补偿后 Session/Retained/Inflight 仍保留且旧代际失效。测试明细见[契约映射](../../../contracts/README.md)。

Paho 测试启动真实独立示例进程，验证 QoS0/QoS1、PUBACK、Retained 回放，最后发送 SIGINT 并确认检查式停机正常退出、端口可重新绑定。Paho 安装在 `/tmp` 虚拟环境；仓库保留固定版本 requirements 和测试脚本，不依赖该临时环境路径复现。可用任意独立虚拟环境安装 `scripts/requirements-interop.txt` 后执行。

依赖获取期间 Cargo 的 Git 网络获取较慢，使用本机已核对相同 commit 的框架仓库填充 Cargo Git 缓存。Cargo.toml/Cargo.lock 仍使用远端 URL + 精确 rev，没有引入本地 path 依赖；指纹预检检查解析后的源码摘要。Cargo 在该环境发现重复框架 manifest 并提示选择缓存 checkout；metadata/指纹与 MSRV 均通过，不把这些来源提示隐藏为零警告。`clippy -D warnings` 和 `RUSTDOCFLAGS="-D warnings" cargo doc` 对本项目均成功。

`check.log` / `msrv.log` 是本轮较早快照，最终源码证据以 `*-final.log` 为准。没有提交、推送或发布代码。

尚未覆盖：其他操作系统/架构、生产压测和 RSS 上限、F4 业务 EventHub 消息桥接、F5 Auth/TLS/持久 Storage、断电/fsync/存储不确定提交、F6 QoS2、F7 MQTT5。设计 F0 中面向可选 Storage Module 的 prelisten 恢复用例留待 F5，本轮 MemoryStore 不声称验证过该路径。当前只声明已测试的实验子集，不声明完整 MQTT 符合性或 Release Ready。
