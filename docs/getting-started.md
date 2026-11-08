# 开始使用

当前为 `0.1.0-alpha.1`，提供 MQTT 3.1.1 QoS0/1/2 实验实现。完整规范符合性、MQTT5、WebSocket、Bridge 和生产容量尚未验收。Rust 最低版本为 1.85，Edition 2024。

## 已有 Modorch 宿主

将 [README 的最小装配](../README.md) 合并到宿主已有的 `ApplicationBuilder`：注册 `MqttBrokerBundle`，将 `module_ids()` 加入宿主的 `ComponentSelection`，再由宿主统一 build/start。宿主继续拥有 Tokio Runtime、Application、EventHub、信号和 `shutdown_checked`。Broker 连接与维护任务交给该宿主的 Scheduler。

本库和宿主必须使用同一来源的 Modorch 0.0.3：

```toml
[dependencies]
modorch = { git = "https://github.com/muidea/modorch-rs", rev = "336ba20196044e1b81fb2ca297a18b5e258a5800" }
modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt" }
```

对应 Skill Bundle 为 1.0.7，解析源码指纹为 `94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。指纹算法和允许的兼容快照由 [framework-identity.json](../.agents/skills/modorch-module-development/references/framework-identity.json) 与预检脚本维护；不要只比较 Cargo 版本字符串。

可编译的集成见 [existing-modorch-server](../examples/existing-modorch-server/src/main.rs)，它同时演示可信业务 Handler 的显式出站 Request：

```bash
cargo run -p existing-modorch-server --locked -- --once
```

## 普通 Tokio 程序

由入口创建一个 Tokio Runtime 和一个 Modorch Application，再按相同注册模式启用 Broker。可以直接运行 [standalone-broker](../examples/standalone-broker/src/main.rs)：

```bash
cargo run -p standalone-broker --locked -- 127.0.0.1:1883
```

使用 MQTT 3.1.1 客户端连接该端口、订阅 `demo/+`，再发布 `demo/temperature`。Ctrl-C 会进入检查式退出。独立客户端自动验证：

```bash
cargo build -p standalone-broker --locked
python3 -m venv /tmp/modorch-mqtt-interop
/tmp/modorch-mqtt-interop/bin/pip install -r scripts/requirements-interop.txt
/tmp/modorch-mqtt-interop/bin/python scripts/verify-paho.py
```

这些命令从仓库根执行。首次构建需要下载依赖；缓存完整后可加 `--offline`。`loopback_development()` 显式允许回环匿名访问。公网监听须配置 Auth 与 TLS；示例证书及演示账号只用于测试。

## 启用扩展

Cargo Feature 仅提供可编译的实现。随后仍须创建、注册并在 Selection 中启用相应 Factory，并向 Core 配置 Provider ID。Bundle 当前只注册 Core。

| 目标 | Feature | 装配示例与说明 |
|---|---|---|
| 密码、ACL、TLS、内部管理及指标 | `auth,tls,admin,metrics` | [安全宿主示例](../examples/secure-modorch-server/src/main.rs)、[安全与运维](security-and-operations.md) |
| SQLite 持久化 | `storage` | [durable-broker](../examples/durable-broker/src/main.rs)、[存储边界](storage.md) |
| 业务消息交互 | Core 默认支持 | [EventHub 接入](eventhub.md) |

参数限制见 [configuration.md](configuration.md)，故障与退出步骤见 [operations.md](operations.md)。


已有独立TOML文件时，可运行 `cargo run -p standalone-broker --locked -- --config examples/standalone-broker/mqtt.toml`。示例明确使用匿名回环；`--once`启动后检查式退出，不能与地址位置参数或benchmark同时覆盖。宿主集成与Provider引用见[TOML配置](toml.md)。
