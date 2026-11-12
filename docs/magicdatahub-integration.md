# magicDataHub 宿主集成

modorch-mqtt 交付可嵌入库和开发样例。正式 `mqtt-server`、`fedengine-server`、`datahub-server` 及其镜像由 [magicDataHub](https://github.com/muidea/magicDataHub) 统一编排；本库不另行交付这些服务。首轮 MQTT 为匿名回环、TCP、3.1.1、内存模式，组合服务的 SQL 与 MQTT 共享一个 Application/Hub。

## 装配与资源所有权

宿主创建唯一 Tokio Runtime、ApplicationBuilder 和 EventHub。`MqttBrokerBundle::register` 只注册 Factory，不监听、不启动线程；宿主将返回的 `module_ids()` 显式加入 Selection。首轮使用 `.with_failure_policy(FailurePolicy::FailFast)`，不依赖 Core 默认 Restart 策略。示例见 [existing-modorch-server](../examples/existing-modorch-server/src/main.rs)。

配置、SecretRef 解析、监听地址、TaskScheduler/Hub 配额、日志和退出策略属于宿主。Core 管理协议状态及连接任务，通过宿主 Scheduler 运行；配置中的连接/会话/报文/队列上限不能替代宿主整体预算，逻辑字节成本不能当作进程 RSS。扩展必须同时满足 Feature、注册、Provider 配置和 Selection；编译通过不会自动开启 Auth/TLS/Storage/MQTT5 等能力。

MQTT 网络 Topic 与内部 Hub Topic 是两个边界。业务订阅 `MqttInbound` 或显式发布 Request 才产生交互；共享 Hub 不会自动将 MQTT 消息写入 SQL。发布接纳/持久回执也不等于终端客户端已收到。业务桥接和正式宿主可选扩展后续单列。

## 启停与错误

开发样例在启动前订阅 Unix SIGINT/SIGTERM，启动后同时等待运行失败与退出信号。启动失败若补偿未完成，使用错误附带的原 ApplicationHandle 继续清理并保留启动主因。退出时检查 `shutdown_checked`：仅 `GracefullyStopped` 为正常完成；`StopIncomplete` 每次等待 30 秒、间隔 50 毫秒，保留同一 Runtime/Handle/实际任务继续等待。这是单次预算，不保证总退出期限。`Forced` 不当成功；运行/信号错误在清理之后仍以失败返回。非 Unix 回退 Ctrl-C，尚未进行跨平台实测。

```sh
cargo run -p existing-modorch-server --locked -- --once
cargo run -p existing-modorch-server --locked -- --listen 127.0.0.1:0
cargo test -p existing-modorch-server --locked
cargo build -p existing-modorch-server --locked
python3 -B scripts/verify-existing-host.py
```

`--once` 默认随机回环端口，常驻默认 `127.0.0.1:1883`；`--listen` 可显式覆盖。信号检查只操作自己启动的 PID，验证真实 MQTT CONNECT/PING、两种信号、退出回执和端口释放。Rust 测试用真实受管任务验证运行错误、信号等待错误、启动补偿和 StopIncomplete 保留。两类检查均进入 [项目门禁](../scripts/check.sh) 和现有 [CI](../.github/workflows/check.yml)。

## 依赖与验收边界

宿主与本库必须解析同一 Modorch `0.0.3` Git 来源及 revision `336ba20196044e1b81fb2ca297a18b5e258a5800`，不能仅比较版本字符串。当前 magicDataHub 通过同级 path 消费业务库；本地验证不证明干净远端 CI 或可复现发布已闭合。正式消费需包含修复的真实业务提交、锁文件和源码身份一致。

宿主正式契约与验证入口以 magicDataHub 的 `docs/architecture/multi-application-composition.md`、`docs/validation.md`、`scripts/verify-mqtt-service.py` 和 `tools/mqtt-broker-validation/README.md` 为准。该黑盒工具分别验证独立与组合宿主，代表性场景通过不批准完整规范。库内 F6/F7、独立审查和发布仍按 [待收口计划](f6-f7-closure-plan.md) 与 [发布门禁](release.md) 执行；不因工具通过自动关闭。
