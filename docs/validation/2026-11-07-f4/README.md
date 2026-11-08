# F4 EventHub 集成验证

执行日期：2026-11-07。版本 `0.1.0-alpha.1`，基于 `05b4621` 继续实现 F4；框架仍为 `modorch 0.0.3 / 336ba20196044e1b81fb2ca297a18b5e258a5800`，Contract 1.2.0，匹配 `modorch-module-development` Skill 1.0.7。F0–F3 的[历史记录](../2026-11-07-f0-f3/README.md)保持原样。

| 命令 | 结果 | 原始证据 |
|---|---|---|
| `CARGO_NET_OFFLINE=true ./scripts/check.sh` | Passed | [check.log](check.log) |
| `cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline` | Passed | [msrv.log](msrv.log) |
| 匹配 Skill 的 `scripts/preflight.py --manifest-path Cargo.toml` | Passed | [framework-preflight.log](framework-preflight.log) |
| `cargo build -p standalone-broker --locked --offline` | Passed | [standalone-build.log](standalone-build.log) |
| `/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py` | Passed | [paho.log](paho.log) |
| 匹配 Skill 的 `scripts/validate-report.py docs/validation/2026-11-07-f4/result.json` | Passed | [report-validation.log](report-validation.log) |

工作区共 **29 个 Rust 测试**通过：Core 28 个（原 17 个 + 新增 F4 11 个），独立 rumqttc 集成测试 1 个；另有 1 个 Rustdoc 编译示例。脚本同时运行 fmt/check/clippy（`-D warnings`）、Rustdoc（`-D warnings`）和 `existing-modorch-server --once`。宿主示例通过同一 EventHub 完成状态查询和带可信组件来源的显式发布，打印 `startup-ready-v1 (MemoryOnly)`，断言 Retained/幂等记录各 1 条，随后检查式停机。

新增 F4 测试位于 [eventhub.rs](../../../crates/modorch-mqtt-core/src/tests/eventhub.rs)，覆盖：

- 两个真实业务 Module 接收同 Topic 的类型化入站观察，原始连接字段、独立根来源和 QoS1 正常自回送；无自动出站回环。
- 业务 Handler 的显式发布及内存回执、同源幂等、不同来源隔离、内容冲突、同源结果查询；后加入的订阅者不因重试收到旧发布。
- 伪造 source Header、无来源请求、错误 DTO、普通 Event/Command 不执行发布且不重启 Core；保留 Topic、QoS2、非法 Topic、报文大小、request_id 均受限。
- 全部可靠目标及 Retained 原子接纳/拒绝；记录中的路由拒绝在容量释放后仍返回原结果；账本满不淘汰旧 ID；记录共用全局内存预算。
- 幂等结果跨 Core 监督重建保留，旧代际拒绝；调用方在提交后超时得到 Indeterminate，经同源查询和相同内容重试不重复交付。
- 观察者邮箱满、全部拒绝、部分 fanout、无匹配、合法长 Topic 和 Hub 字节限制均不改变 MQTT PUBACK/客户端投递。
- 多 Broker 由宿主分配唯一全局订阅 ID；内部 Retained 替换/删除及旧请求重试不会覆盖后续网络更新。

Paho 2.1.0 独立回归再次通过 QoS0/1、PUBACK、Retained 回放及 SIGINT 退出后的端口释放。它验证 MQTT 基础回归，F4 业务双向行为由上述真实 Hub/TCP 集成测试验证。Paho 虚拟环境已存在；可在其他独立环境按 `scripts/requirements-interop.txt` 安装后复现。

实际环境 Linux x86_64 / WSL2、Rust/Cargo 1.94.0，另验证 MSRV Rust 1.85.0；详见 [environment.json](environment.json)。Cargo 仍提示发现上游本地仓库与缓存 checkout 的重复 manifest，解析选择精确 rev 的缓存来源；框架指纹不变。未修改上游、全局配置或历史验证记录。

当前结果仅声明上述 F4 内存实验范围。账本不淘汰且无 TTL，默认 10000 条；满额拒绝新 ID。结果跨 Core 重建保留，不跨新 Factory/Application/进程。入站观察 best effort，不等于业务完成；内部发布只有 MemoryOnly。Auth/TLS/持久 Storage/Admin/Metrics/可选 Bridge、QoS2、MQTT5、生产容量及其他平台仍待实现或验证。本轮未提交、推送或发布。
