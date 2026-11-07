# modorch-mqtt：基于 Modorch-RS 的可嵌入式 MQTT Broker 库完整架构与实现设计

> 文档状态：**待实施的设计方案，不是当前已实现的功能说明**  
> 日期：2026-11-07（评审修订）｜项目建议名：`modorch-mqtt`｜实现语言：Rust 2024\
> 基础框架：[`muidea/modorch-rs`](https://github.com/muidea/modorch-rs)，**0.0.3 Candidate**；本次核对 `master` commit [`336ba20196044e1b81fb2ca297a18b5e258a5800`](https://github.com/muidea/modorch-rs/commit/336ba20196044e1b81fb2ca297a18b5e258a5800)  
> 框架契约：Contract **1.2.0**；Skill **1.0.7**；Rust **>=1.85 / Edition 2024**  
> 前置框架增强：**已由 Modorch-RS 0.0.3 实现并在其仓库的本地验收范围内验证**，不再阻断本库开发  
> 文档定位：针对 0.0.3 的 **实现设计/工作分解**，不是 `modorch-mqtt` 已经实现或通过测试的声明  
> 当前实施进度：已启动 F0–F3 的 `0.1.0-alpha.1` QoS0/1 内存实验实现；实际能力以 [README](../README.md)、[当前契约](../contracts/README.md)及[本轮验证记录](validation/2026-11-07-f0-f3/README.md)为准。本设计其余 DTO、配置及后续阶段仍为目标。
> 规范性协议来源：[MQTT 3.1.1 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)、[MQTT 5.0 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)

## 0. 执行摘要与决策记录

**产品目标**：发布一个可以被任意 Rust 服务按需集成的 MQTT Broker **库**，而不是要求部署独立 Broker 进程。Host 已使用 Modorch-RS 时，Broker 以普通 ModuleFactory 注册，所有业务模块、MQTT Core 和可选扩展模块运行在同一个 Modorch Application、使用同一个底层 EventHub 和 Scheduler 生命周期。Host 不使用 Modorch-RS 时，可以由库的可选 Bootstrap Adapter 在其既有 Tokio Runtime 中创建**一个** Modorch Application；独立部署只是一层薄 CLI。

**已定关键决策**：

| ID | 架构决策 | 约束 |
|---|---|---|
| ADR-MQ-001 | 独立 Cargo 库 | 不在 Modorch-RS 核心加入 MQTT 协议代码 |
| ADR-MQ-002 | MQTT Core 为 Module | 标准生命周期、显式启用、可选内部消息 Endpoint |
| ADR-MQ-003 | Application/HUB 单例语义 | 同一 Host Application 内只有一个 EventHub 底层实例，MQTT 不再构造 Hub |
| ADR-MQ-004 | Core 与扩展解耦 | Auth、Storage、Admin、Metrics、Bridge 是独立可选 Module；Core 不依赖其具体 crate |
| ADR-MQ-005 | Topic 统一 MQTT 标准 | EventHub 在 Modorch **0.0.3** 持续提供 MQTT 3.1.1/5.0 Topic Name/Filter 校验；Core 处理协议特有的客户端订阅和共享组语义 |
| ADR-MQ-006 | 双路职责，单一 Hub | MQTT Client→Client 由 Core 保证协议交付；共享 EventHub 是内部 Module 交互通道，而非第二份 Broker |
| ADR-MQ-007 | 交付确认分层 | EventHub Admission、Core 接收所有权、存储提交、客户端 PUBACK、下游 ACK 分别定义 |
| ADR-MQ-008 | 安全默认 | 监听本机、匿名认证显式配置、严格资源上限、拒绝外部访问内部控制 Topic |
| ADR-MQ-009 | 验收先于宣称 | MQTT 3.1.1 完整符合性需覆盖 QoS 2；仅 QoS 0/1 阶段标记为受限能力 |
| ADR-MQ-010 | 直接依赖 0.0.3 模块入口 | `Module::activate_ingress` 已具备，无 MQTT 特有的框架补丁；Broker 不得在 `setup/start/ready` 开始 `listen` |
| ADR-MQ-011 | 代际边界分离 | 旧 TaskScheduler/服务租约在重建后失效；应用级 `EventHub/ExternalEventHub` 句柄仍有效，不能单凭 Hub 有效推断 Broker 仍然存活 |
| ADR-MQ-012 | 恢复先于 TCP 监听 | 通过 EventHub Request 的 Storage 恢复在 `activate_ingress` 入口开放后完成，状态一致并通过恢复检查，才执行 `listen`；失败进入框架补偿 |
| ADR-MQ-013 | 不夸大 Ready 语义 | 所有组件 `ready()` 完成后才开始逐个激活；Application Ready 在全部激活成功后发布。**已激活的 Listener 可能早于 Application 状态 Ready 开始接入**，若应用要求更严格对外就绪需独立门控策略 |
| ADR-MQ-014 | 会话归属先于接管 | 启用 Auth 时，Session 绑定稳定 PrincipalKey；认证成功不等于有权使用任意 ClientID，拒绝跨身份恢复、清除和接管 |
| ADR-MQ-015 | Will 在 CONNECT 接受前授权 | Will 使用发布 ACL、Retain 权限和 Core 保留命名空间校验；记录授权决定及连接 epoch，触发时复用统一发布路径 |
| ADR-MQ-016 | 分页恢复一致性快照 | 恢复必须枚举全部持久 Session（含离线）、订阅、Retained、Inflight 和未完成接纳记录；恢复写入屏障与快照校验完成后才 listen |
| ADR-MQ-017 | 内存状态跨 Core 代际存活 | 每次 Bundle 注册创建独立的状态所有者，由该 Application 的 Core Factory 持有；Module teardown 仅释放代际资源，不清空共享 MemoryStore |
| ADR-MQ-018 | 可靠消息先预留、后接纳 | 首期对一个发布的全部可靠交付目标及 Retained 变更做有界容量预留和原子提交；容量不足则在所有权转移前拒绝整个发布，不逐目标部分成功后 ACK |

**非目标（首期）**：跨节点集群、跨进程 EventHub、动态原生插件 ABI、Broker 完整 MQTT 5.0、自动外发所有业务 Event、外部消息 exactly-once/分布式事务、无需选型的任意第三方存储后端。

## 0.1 与上一版设计的变更清单（0.0.2 → 0.0.3）

| 设计维度 | 旧文档前提 | 当前定案 | 是否要求修改 Modorch-RS |
|---|---|---|---|
| `Module::activate_ingress` | 待实现的框架前置增强 | **0.0.3 已实现**，直接使用 | 否 |
| TCP 激活顺序 | 拟议在全体 Ready 后激活 | 0.0.3 已提供统一激活；需明确局部 Listener 可能先于应用 Ready 对外 listen | 否 |
| 启动激活失败 | 计划扩展清理/补偿 | 0.0.3 已纳入 `StartupError`/清理重试，Broker 实现幂等清理 | 否 |
| 监督激活失败 | 计划补齐 | 0.0.3 重建失败纳入当次恢复与累计预算 | 否 |
| Socket 预绑定 | 过去文字“bind 不 accept”欠严谨 | `TcpSocket::bind` 可在 setup；**不得提前 listen**（`TcpListener::bind` 已 listen） | 否 |
| App Hub 与 Generation | 曾表述为句柄旧代际失效 | 0.0.3 明确 App Hub 长存、只有 TaskScheduler/服务能力受代际约束；Broker 自己 fencing | 否 |
| Storage 恢复时机 | `ready()`/激活前描述存在混淆 | EventHub 根准入在全体 ready 后开放；恢复放 `activate_ingress` 的 `listen` 之前 | 否 |
| 网络 Admin 扩展 | 可能另需 Initiator | 普通 Module 可独立激活 HTTP/TCP Ingress | 否 |
| Topic | 0.0.2 已对齐 | 0.0.3 保留 MQTT Filter / Name 语义；`$share` 仍不等于共享组 | 否 |
| 服务提供者角色 | 原有约束 | Module 不能发布 Service，Auth/Storage 默认用 EventHub Request | 否 |

**冻结结论**：`modorch-mqtt` 不依赖对 Modorch-RS 的额外改造；若未来需要零握手的全应用原子激活、动态 Module ServicePublisher 或跨进程 DurableHub，先另立架构提案，**不把它们当作当前框架已支持功能**。

## 0.2 2026-11-07 评审修订

本轮将五项评审意见转为实施契约；下表表示设计已修订，**不表示功能已实现或测试通过**。

| 评审项 | 定案位置 | 验收契约 |
|---|---|---|
| P1：跨身份 ClientID 接管与会话恢复 | §7.3.1、§8.1、§10.1 | MQTT-SEC-002 |
| P1：Will 缺少发布授权入口 | §7.5、§8.1、§10.1 | MQTT-SEC-003 |
| P1：存储接口无法枚举离线订阅 | §6.2、§8.2.1 | MQTT-STATE-003 |
| P1：MemoryStore 在 Core 重建时的归属不清 | §4.3、§7.3.2、§10.4 | MQTT-STATE-004 |
| P2：可靠队列满时的接纳与重试策略未定 | §7.3.3、§9、§10.3 | MQTT-BOUND-002 |

## 1. 用户场景与验收场景

1. 一个已有 `modorch-rs` 服务器引入 `modorch-mqtt`，注册 MQTT Core Module，启用后在指定端口提供 Broker 服务；不创建第二 Application 或 EventHub。
2. 业务 Module 订阅 `devices/+/telemetry`，外部 MQTT Client 发布 `devices/001/telemetry`，业务模块收到一个类型化 `MqttInbound` Event。
3. 业务 Module 使用明确的 Broker Publish Request，将 MQTT 消息投递到已订阅的外部客户端；没有单独的 MQTT 客户端连接。
4. 一个 MQTT Client 订阅多个 Topic Filter；Broker 保留该客户端自己的 Session、订阅 QoS 和发送队列，不能被 EventHub 的“每组件一次”去重替代。
5. Core 不启用 Auth/Storage/Admin 也能按配置启动基础 Broker（限开发场景/回环地址）；启用扩展后既保持相同 Core 接口，也遵守整体 Application 生命周期。
6. Host 关闭、Module 崩溃或 Broker 监听失败时，所有 Client Task 被监督和排空，端口无泄漏、旧代际不再接入。
7. MQTT Topic 与 EventHub Topic 同名时遵守一致匹配语义；“同名”不意味着默认允许任意 Event 自动穿透网络。
8. 使用 Modorch-RS 0.0.3 的普通 Module 能直接在全体 `ready()` 后启动 TCP 入口，无需额外 MQTT Initiator 或改动宿主框架。
9. 激活过程中通过 Storage Module Request 恢复状态，失败时不开放 TCP，记录 `ActivateIngress` 根因并按照 0.0.3 的补偿/重试约束处理。
10. 有其他 Module 的 `activate_ingress` 后续失败时，已经激活的 MQTT Listener 必须被停用、排空与拆卸；不得将仅完成 MQTT 入口激活误报为全应用 Ready。
11. 启用 Auth 后，不同身份即使知道同一个 ClientID，也不能恢复其离线消息、抢占连接或通过 Clean Session 清除其状态；未授权 Will 在 CONNECT 接受前被拒绝。
12. Broker 从持久 Store 启动时，即使订阅者尚未重连，也能为其原有订阅继续保存匹配的 QoS1/2 消息；分页恢复失败时不开放监听。
13. 内存模式 Core 监督重建保留 Session、Retained 和已接纳交付；可靠队列满时按统一接纳策略拒绝新发布，已接纳消息不被淘汰。

## 2. 总体架构与部署模型

```text
┌──────────────────────────── Host Rust Server / Tokio Runtime ──────────────────────────┐
│ ApplicationBuilder → Application（唯一）                                                │
│  ├── EventHub（唯一；所有组件 clone 同一实例句柄）                                      │
│  ├── Scheduler / OperationTracker / Registry / Lifecycle                                │
│  │                                                                                      │
│  ├── Business Module A ───┐                                                            │
│  ├── Business Module B ───┤ Event / Request / Command ↔ EventHub                        │
│  ├── MQTT Auth Module ────┤                                                            │
│  ├── MQTT Storage Module ─┤                                                            │
│  ├── MQTT Admin Module ───┤                                                            │
│  ├── MQTT Metrics Module ─┤                                                            │
│  │                       │                                                            │
│  └── MQTT Core Module ────┘                                                            │
│       ├── Listener / Connection / Codec / KeepAlive                                    │
│       ├── SessionManager / SubscriptionIndex（按客户端）                               │
│       ├── Router / DeliveryManager / QoS / Retained / LastWill                          │
│       ├── MemoryStore 或 Storage Module 请求代理                                       │
│       └── EventHubAdapter（单向 Inbound Event + 显式 Outbound Request）                 │
└───────────────────────────────┬──────────────────────────────────────────────────────┘
                                │ MQTT TCP（可选 TLS 后续）
                       External MQTT Clients
```

**三种分发物（只有一套协议核心）**：

| 集成形态 | 行为 | EventHub 所有权 |
|---|---|---|
| Existing Modorch App（P0） | `MqttBrokerBundle::register` 注册 factories，宿主显式 enable | **既有 Application**，完全共享 |
| Existing Tokio App（P1） | 可选 `MqttBrokerHost` 只在不存在 Modorch App 时创建一个 Application | Adapter 创建的 Application 独占一个 Hub，不与宿主其他框架自动共享 |
| Standalone CLI（P1） | main 创建 Tokio + Application，加载配置/信号；Broker 核心复用 | CLI Application 唯一 Hub |

**特别约束**：不存在“宿主一个 EventHub + MQTT 库内部第二 EventHub + Bridge 同步”的默认部署。EventHub 与 Broker 客户端订阅索引是不同种类的状态；后者不等于第二个 EventHub。

**0.0.3 运行时基础**：`ComponentContext { component, services, event_hub, tasks }` 由 Application 创建；其 `event_hub` 克隆同一个底层 Hub；`tasks` 由当前组件 Owner/Generation 绑定。宿主拥有 Tokio Runtime、Application/Hub、信号与退出机制。`modorch-mqtt` 不创建进程级全局单例，也不直接控制别的 Module 的生命周期。

**读取更多**：[`docs/releases/0.0.3.md`](https://github.com/muidea/modorch-rs/blob/master/docs/releases/0.0.3.md)、[`contracts/lifecycle-contract.md`](https://github.com/muidea/modorch-rs/blob/master/contracts/lifecycle-contract.md) 的 `LIFE-INGRESS-001—005`、[`crates/modorch/tests/ingress.rs`](https://github.com/muidea/modorch-rs/blob/master/crates/modorch/tests/ingress.rs)。

## 3. Cargo Workspace 与 crate 职责

```text
modorch-mqtt/
├── Cargo.toml
├── Cargo.lock                 # 应用/测试锁文件（库依赖按正常 semver）
├── crates/
│   ├── modorch-mqtt/          # Facade，Bundle + Prelude + Features
│   ├── modorch-mqtt-api/      # DTO、错误、扩展请求/响应、常量、契约
│   ├── modorch-mqtt-core/     # ModuleFactory、Broker Core、协议状态机
│   ├── modorch-mqtt-auth/     # Auth/ACL 可选 Module
│   ├── modorch-mqtt-storage/  # Persistent Store 可选 Module
│   ├── modorch-mqtt-admin/    # 管理 EventHub API 可选 Module
│   ├── modorch-mqtt-metrics/  # 订阅/采集 Broker 事件的可选 Module
│   └── modorch-mqtt-bridge/   # 外部系统或显式业务 Event 映射扩展
├── examples/
│   ├── existing-modorch-server/
│   ├── standalone-broker/
│   ├── anonymous-loopback/
│   ├── auth-and-acl/
│   ├── durable-storage/
│   └── eventhub-business-interop/
├── tests/
│   ├── protocol-311/
│   ├── topic-vectors/
│   ├── qos-state/
│   ├── sessions/
│   ├── integration/
│   ├── fault-injection/
│   └── performance/
├── contracts/
│   ├── architecture.md
│   ├── protocol.md
│   ├── eventhub.md
│   ├── extension.md
│   └── validation.md
├── docs/
│   ├── architecture.md
│   ├── getting-started.md
│   ├── api.md
│   ├── configuration.md
│   ├── reliability.md
│   ├── migration.md
│   └── operations.md
└── .agents/skills/modorch-mqtt-development/SKILL.md
```

依赖方向：

```text
modorch-mqtt-api         ← 所有 crate（可只依赖 serde/bytes 等基础类型）
modorch-mqtt-core        → modorch-mqtt-api + modorch（Facade）
modorch-mqtt-auth        → modorch-mqtt-api + modorch
modorch-mqtt-storage     → modorch-mqtt-api + modorch
modorch-mqtt-admin       → modorch-mqtt-api + modorch
modorch-mqtt-metrics     → modorch-mqtt-api + modorch
modorch-mqtt-bridge      → modorch-mqtt-api + modorch
modorch-mqtt             → 各按 Cargo feature 启用的实现 crate
Host                      → modorch + modorch-mqtt
```

- Core **不直接**依赖 Auth/Storage/Admin/Metrics/Bridge 的实现 crate。
- 编译时 `feature` 决定是否包含扩展 crate；运行时 `ComponentSelection` 决定是否启用其 Factory。**注册不等于启用**。
- 建议 Facade 默认只启用 `core`、`memory`；`auth`、`storage`、`admin`、`metrics`、`bridge`、`tls`、`mqtt5` 按需选择（`mqtt5` 不得在未实现时宣称可用）。
- **基线固定在 0.0.3 + 精确 Git commit**，而非从历史的 0.0.2 继续迁移；0.0.3 仍为 Candidate、未声明 crates.io 发布。`modorch-mqtt` 首次集成建议使用统一 `git+rev` 或同一工作区内 `path`，禁止混用不同来源的 `modorch-*` crate。
- Cargo 片段（面向使用当前框架源码的 Host，示意）：

  ```toml
  [dependencies]
  modorch = { git = "https://github.com/muidea/modorch-rs", rev = "336ba20196044e1b81fb2ca297a18b5e258a5800" }
  modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt" }
  tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "signal", "time", "sync"] }
  ```

  `modorch-mqtt` 内部对 `modorch` 的依赖必须解析到同一来源；正式发布时改为经验证的可解析版本和最小兼容矩阵。F0–F3 实验实现已采用该 Git rev；当前能力和可执行示例以 README 及源码为准。

## 4. 组件注册、依赖 DAG 与共享 EventHub

### 4.1 Bundle 是 Factory 装配器，不是 Runtime

`MqttBrokerBundle` 只负责验证本项目配置、构建 Factory、注册到现有 `ApplicationBuilder` 并返回应启用组件清单，不创建 Tokio Runtime、Application、EventHub 或后台线程。

拟议公共 API：

```rust
pub struct MqttBrokerBundle { config: MqttBrokerConfig }

impl MqttBrokerBundle {
    pub fn new(config: MqttBrokerConfig) -> Self { /* ... */ }

    // register_module / register_initiator 使用 Modorch 的真实 Builder API；
    // 返回本 Bundle 对应的组件名，供调用方显式放进 ComponentSelection。
    pub fn register(
        self,
        builder: &mut modorch::ApplicationBuilder,
    ) -> Result<MqttRegisteredComponents, MqttError> {
        /* only register; do not start */
        todo!()
    }
}
```

> 此处保留接口设计草案；F0–F3 已提供可编译的 Bundle，实际签名及支持能力以 `crates/modorch-mqtt/src/lib.rs` 和 Rustdoc 为准。

**依赖 DAG 推荐**：Auth 和 Storage 扩展 Module 为独立 Provider；Core 若配置为依赖这些 Provider，`descriptor.requires` 指向它们，确保相关 Provider 先初始化；Admin/Bridge/Metrics 可依赖 Core（如确有启动顺序需要），不得创建 Core ↔ Auth 的循环依赖。Provider 与 Core 通信走 EventHub Request，不意味着 ServiceRegistry 将 Module 发布为 Service。

### 4.2 已有 Modorch Host 的集成示例（目标体验）

```rust
use modorch::prelude::*;
use modorch_mqtt::{MqttBrokerBundle, MqttBrokerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = ApplicationBuilder::new("business-server");
    builder.register_module(MyBusinessFactory::new())?;

    let config = MqttBrokerConfig::loopback_development();
    let mqtt = MqttBrokerBundle::new(config).register(&mut builder)?;

    let mut enabled = ComponentSelection::new();
    enabled.enable_module("business");
    for module_id in mqtt.module_ids() {
        enabled.enable_module(module_id);
    }

    let app = builder.selection(enabled).build()?.start().await?;
    // 此处同一 app.event_hub() 与 MQTT Core 的 ComponentContext.event_hub
    // 持有指向同一底层 Hub 的句柄，而不是两个总线。
    tokio::signal::ctrl_c().await?;
    app.shutdown_checked(ShutdownOptions::graceful()).await?;
    Ok(())
}
```

注意：`MqttBrokerBundle`、`MqttBrokerConfig`、`module_ids()` 和 `loopback_development()` 已在 F0–F3 实验实现中提供；`MyBusinessFactory` 是宿主自己的组件；`tokio::signal` 需由宿主启用对应 Tokio Feature。可执行宿主示例位于 `examples/existing-modorch-server`；后续扩展配置仍未实现。

### 4.3 生命周期与网络入口

MQTT Core 作为普通 Module 实现 `Lifecycle`，**直接覆盖 Modorch-RS 0.0.3 已经提供的 `Module::activate_ingress`**，不再为此修改框架。其签名为 `async fn activate_ingress(&mut self, ctx: &RuntimeContext) -> FrameworkResult<()>`，默认空实现。运行时会同时分发 Initiator/Module Hook，全部相关组件通过 `ready()` 后才调用它；初次启动与监督重建共用入口激活和补偿逻辑。

| 阶段 | MQTT Core 行为 |
|---|---|
| `Factory::create` | 仅创建未运行对象，克隆 Factory 所持状态所有者的句柄，无 I/O 副作用；不得每代重新创建 MemoryStore |
| `setup` | 校验配置，挂接已有 MemoryStore 或创建存储代理，建立本代 Router 与受控状态；需要预留端口时只使用**未 listen 的 `TcpSocket::bind`**（不是 `TcpListener::bind`）；不接受 TCP 握手 |
| `handlers()` | 注册本 Module 的控制面 Topic/Command/Request Handler |
| `start` | 准备任务依赖和状态；不对外收连接 |
| `ready` | 检查内部静态配置和依赖组件的就绪约束；不可启动网络监听；避免从尚未开放根准入的 EventHub Request 获取恢复状态 |
| `activate_ingress` | 首先从共享 MemoryStore 或通过**带期限、分页** Storage Request 完成全部 Session/订阅及交付恢复，验证写入屏障和资源预算（此时根准入已开放），再 `TcpSocket::listen` 并用 `ctx.tasks.spawn` 启动受管 accept loop，确认启动结果后快速返回 |
| `begin_shutdown` | 停止 accept；撤销新的业务入口；按协议驱动主动关闭 |
| `quiesce` | 等待在途连接、发送队列及交付状态按配置完成/移交；有超时和可恢复错误 |
| `teardown` | 释放本代 Listener、Router、会话句柄和存储代理；不清空 Factory 所持 MemoryStore，不删除持久 Session；不可释放仍被使用的资源 |

长期接入不能把 `activate_ingress` 变为永不返回的循环。受管任务失败应传播到框架监督；恢复后新的 Generation 必须重新建立端口，旧任务不能继续接入。**意外返回 `Ok(())` 的 Listener 也应由 Core 显式升级为异常**，不以“受管任务退出成功”视为 Broker 仍健康。TaskScheduler 的全局/Owner 限额与 Broker `max_connections` 须联动：如果每个连接占一个受管 Task，则配置时确保容量；若未来要支持十万连接，先通过测量再设计固定 Worker/受管任务分组，不能偷用失管 `tokio::spawn` 绕过额度。

**0.0.3 精确边界**：所有选中组件的 `ready()` 通过后才开始逐个执行 `activate_ingress()`，但最早激活的 Listener 可能在后续组件激活之前完成 `listen`，**不要将 Application 的 `Ready` 状态当作 MQTT Listener 能否已接受连接的绝对分界点**。若宿主对全局 Ready 前绝不接受 MQTT 连接有严格要求，需单独实现应用级双阶段对外门控，不能仅依赖 `listen` 延迟到 `activate_ingress()`。该增强不作为第一阶段必需的框架变更。

**0.0.3 恢复边界**：监督重建先停旧 Owner/依赖闭包、排空与代际推进，再执行新 `setup/start/ready/activate_ingress`；激活失败与同次恢复尝试一起补偿、按累计预算重试。旧 TaskScheduler/服务租约代际失效，Application 级 `EventHub/ExternalEventHub` 仍有效；Broker 自己必须维护 `connection_epoch`/`broker_generation` 阻断旧连接和旧存储请求污染新状态。

**0.0.3 实现依据**：[`component.rs`](https://github.com/muidea/modorch-rs/blob/master/crates/modorch-runtime/src/component.rs)、[`lib.rs`](https://github.com/muidea/modorch-rs/blob/master/crates/modorch-runtime/src/lib.rs)、[`supervision.rs`](https://github.com/muidea/modorch-rs/blob/master/crates/modorch-runtime/src/supervision.rs)、[`graceful-http`](https://github.com/muidea/modorch-rs/tree/master/examples/graceful-http)。

## 4.4 Modorch-RS 0.0.3 实际集成顺序与已验证能力

下表用于防止实施 Agent 把已具备的框架能力重复开发，或把设计中的 MQTT 能力误当成框架已提供：

| 源码 / Contract | 0.0.3 已有行为 | `modorch-mqtt` 必须补充 |
|---|---|---|
| `ComponentContext::event_hub` / `LIFE-INGRESS-005` | 同 Application 的模块取得同一个底层 Hub | 协议消息转换、出网授权、业务控制命名空间和防环 |
| `Module::activate_ingress` / `LIFE-INGRESS-001` | 全体相关 `ready()` 之后激活，支持普通 Module | 恢复完成前不得 listen、监听启动确认、连接准入和握手超时 |
| `LIFE-INGRESS-002` | 激活失败/ panic 做启动补偿，保留主因及清理结果 | 部分 socket、Session、受管任务的幂等清理 |
| `LIFE-INGRESS-003` | 监督重建包括激活失败、累计预算及闭包补偿 | Session/Delivery 状态恢复、旧连接 fencing、重新 bind |
| `LIFE-INGRESS-004` | 受管入口、任务配额、StopIncomplete | 每客户端任务/内存额度、慢连接处理、Will 处理策略 |
| `MSG-TOPIC-001—006` | MQTT Name/Filter 与路由一致性 | 客户端 Session 订阅记录、QoS、Shared Subscription（v5） |
| `MSG-EVT-001` / `MSG-REQ-004` | Event 逐目标准入；Request 运行中超时为 Indeterminate | 与 MQTT PUBACK 独立的 BrokerAccepted/StoreCommitted 回执 |
| `RES-001—003` | Initiator 发布受控服务，Module 不可直接导出服务 | Auth/Storage 默认通过定向 EventHub Request；可选 Initiator 才能提供直接服务 |

### 初始化与恢复时序（必须按实际代码遵循）

```text
ApplicationBuilder.build(): validate only; no Broker socket side effects
  ↓
Factory.create() + Module.setup(): construct state; optional TcpSocket::bind (not listen)
  ↓
Module.handlers(): install Core / Auth / Storage endpoints
  ↓
all selected components start()
  ↓
all selected components ready()   -- still no network listener
  ↓
Modorch tracker opens root admission
  ↓
Module.activate_ingress():
  1. begin fenced recovery from shared MemoryStore or Storage Module requests
  2. enumerate snapshot pages; rebuild ALL subscriptions (including offline sessions)
  3. reconcile accepted work + quotas; complete recovery barrier
  4. TcpSocket.listen(...) + ctx.tasks.spawn(listener)
  5. acknowledge listener startup; return promptly
  ↓
all selected components activate_ingress() succeeded
  ↓
Application state = Ready
```

**时序注意**：第一条 Listener `listen` 可能发生在后一个组件激活失败之前。这是 0.0.3 的 **all-ready-before-activation**，而不是全体入口同时开放的原子提交。Broker 必须支持撤销已激活入口。要求“应用状态 Ready 之前没有任何 TCP 握手”是更强的新需求，不能从 0.0.3 当前契约推导。

### 存储恢复的死锁/准入限制

不能在 `ready()` 内对 `EventHub` 发起必须成功的根 Request 作为 Storage 恢复前提：0.0.3 初次 `tracker.open()` 发生在全体 `ready()` 后。使用 Storage 扩展时，恢复放到 `activate_ingress()` **listen 之前**，设置单独恢复时间预算；通过 `external()` 建立合法根操作，等待 Request 的实际结果。业务恢复过程中必须避免让 Core 自己的 Serial Handler 同步等待回到同一个 Handler；对 Storage 使用独立目标组件，遵守现有等待环检测。若需在任何入口激活之前跨 Provider 恢复所有状态，可评估引入不依赖 EventHub 根准入的初始化资源接口，但属于另行设计，不擅自放开 Modorch-RS 权限。

### 代际与任务安全

`TaskScheduler` 与 ServiceHandle 的代际是强约束；`EventHub` / `ExternalEventHub` 是 Application 级别能力，**不具备自动禁止旧 MQTT Client 连接继续调用的代际语义**。在 Core 建立 `(broker_generation, session_epoch, connection_id)` 三元 fencing，所有入站操作进入 EventHub 之前、所有入/出 QoS 状态更新之前以及写入 Storage 之前校验。Listener 和 ClientTask 须以 `ctx.tasks.spawn()` 受管，并用 `CancellationToken` 在停机/重建时优先停止接入。旧 Task 失效和新 Service 代际保护是框架边界；网络会话授权始终由 Broker 自己负责。

## 5. EventHub 与 MQTT Client 路由边界（核心设计）

### 5.1 一个 EventHub，不等于一个订阅索引

| 概念 | EventHub | MQTT Core |
|---|---|---|
| 订阅主体 | ComponentRef（业务/扩展 Module） | Client Session（ClientID） |
| Topic Filter | MQTT 标准分层匹配 | MQTT 标准分层匹配；v5 Shared Subscription 专属处理 |
| 去重单位 | 同一 Component 去重 | 至少按 MQTT Client 会话处理重叠订阅，QoS 按规范计算 |
| 投递状态 | 各组件邮箱的 Admission/处理完成 | 每连接写队列、Packet ID、Inflight、QoS ACK |
| 离线队列 | 无框架支持 | Core 内存或可选持久 Store |
| Retained / Last Will | 无框架支持 | Core 管理 |
| 原子性 | fanout 非原子 | 接纳时全部可靠目标与相关状态原子提交；后续交付/ACK 各 Session 独立 |

**禁止**：为每个 MQTT Client 建立一个 Modorch Module 或 EventHub Endpoint；禁止把 EventHub 的 PublishReport 转换成 MQTT PUBACK；禁止因为业务 Module 无订阅而使合法 MQTT 发布失败。

### 5.2 入站 MQTT → EventHub（观察/业务事件）

```text
Client A -- PUBLISH(devices/a/telemetry, qos=1) --> MQTT Core
    → Packet/ACL 校验 → 检查 Session 接收状态，尚不转移所有权
    → Router 找到 MQTT Session B/C 的订阅，预留全部可靠交付容量
    → 原子提交所有权 / 可靠交付 / Retained 状态 → 确认 PUBACK 时机
    └── EventHub.external().publish(
           topic="devices/a/telemetry", destination=Broadcast,
           payload=MqttInbound{...})
               → Business Module A / Metrics Module
```

- `MqttInbound` 带 `client_id`、topic、payload、qos、retain、packet_id、时间和受控来源标签；`client_id` 为可信连接上下文而非客户端自报 Header。
- EventHub 无匹配业务模块时可能返回 `Unroutable`，这仅表示**内部观察者不存在**，不能作为 MQTT PUBLISH 失败依据。
- EventHub 满队列或部分 Fanout 拒绝时，需明确观察事件策略：首期 `best_effort` + 计数/日志；可选 `required` 业务接管策略需独立设计，不得暗中改变 QoS/ACK。
- 通过 `ctx.event_hub.external()` 为每条来自网络的消息新建根准入，不得继承 Listener 长生命周期受管任务的旧根权限。**其句柄在组件监督重建后仍可能有效**，所以要额外检查 Broker 当前活跃代际及连接授权状态，不能以句柄仍可调用证明旧连接合法。
- EventHub EventPayload 是 `Arc<dyn Any + Send + Sync>`，不能直接当作 MQTT 字节流；必须用类型化 DTO 和显式序列化。

### 5.3 内部业务 → MQTT 客户端（显式控制通道，默认）

为避免自动把任意业务 Event 发往公网，也避免反向回环，默认在**同一个 EventHub 内**设置内部专用控制 Topic，例如：

```text
$modorch/mqtt/control/publish
$modorch/mqtt/control/disconnect
$modorch/mqtt/control/session/get
$modorch/mqtt/event/client/connected
$modorch/mqtt/event/client/disconnected
```

这是库内定义的 **EventHub 控制命名空间**，不是可以被外部客户端任意调用的 MQTT 管理 API。Core 严格拒绝外部 MQTT Publish 冒充内部控制命名空间；`$SYS` 保留给标准系统主题用途。Source/Destination 仍由框架决定，类型化 payload/ACL 不能仅信任任意字符串 Header。

```text
Business Module
  → EventHub Command/Request($modorch/mqtt/control/publish,
        Destination::Component(mqtt-core), MqttPublishRequest{ actual_topic, bytes, qos, retain })
  → MQTT Core Handler（快速校验/入有界工作队列）
  → Core Router / Delivery → Client B/C
  → Request 返回 BrokerAccepted / DurableAccepted（精确边界）
```

- EventHub `command` 的成功只有邮箱**准入**，不是 Broker 接受。需要可靠的业务返回时必须使用 `request` 并等待 Core 处理回执。
- `request.wait()` 运行中超时可能 `Indeterminate`，不能推断 Client 没有收到消息，也不可无条件重试；业务必须持幂等标识查询状态或去重。
- 可选 Bridge Module 可以订阅指定业务 Topic（如 `devices/+/command`），只将明确 `MqttOutbound` 类型事件转换成控制请求；默认**不**订阅所有 `#` 并盲目转发。若需要“同名 Topic 自动外发”，须单独配置可出网的 Topic 范围、序列化策略与来源过滤。

### 5.4 防回环与安全边界

1. 入站网络事件使用 `MqttInbound` 载荷；Core **不**订阅广播业务 Topic 作为默认出口，仅订阅专用控制 Topic。
2. Outbound 通过 `MqttPublishRequest` 明确发起，采用内部来源/操作 ID，重复请求可幂等判定。
3. Bridge 只处理显式允许的来源/类型，禁止同一个入站事件立即反向发布到原 Broker。
4. 外部 Client 不得通过 MQTT 访问保留的内部控制 Topic；所有外部 PUBLISH/SUBSCRIBE 按认证 ACL 和保留命名空间规则审查。
5. MQTT 客户端订阅了自己发布的 Topic 时，按 MQTT 3.1.1 正常交付给该客户端，这是合法回送，不能误认为内部 EventHub 桥接环路。

## 6. Public API / DTO 契约

### 6.1 面向 Host 的核心数据模型（拟议）

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Qos { AtMostOnce, AtLeastOnce, ExactlyOnce }

#[derive(Clone, Debug)]
pub struct MqttPublishRequest {
    pub request_id: String,         // 业务幂等标识；必须能跨超时重试保持稳定
    pub topic: String,              // MQTT Topic Name，严格校验
    pub payload: bytes::Bytes,
    pub qos: Qos,
    pub retain: bool,
}

#[derive(Clone, Debug)]
pub struct MqttInbound {
    pub client_id: String,
    pub topic: String,
    pub payload: bytes::Bytes,
    pub qos: Qos,
    pub retain: bool,
    pub packet_id: Option<u16>,
    pub received_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MqttAcceptReceipt {
    pub request_id: String,
    pub accepted: bool,
    pub durability: AcceptanceDurability, // MemoryOnly | DurableCommitted
}
```

- `ExactlyOnce` 只在 QoS2 已实际实现的版本可选；首期若未支持必须显式拒绝，而非静默降级或谎称完整 MQTT 3.1.1。
- `MqttPublishRequest` 的 Topic 是实际外部消息 Topic，**不是** EventHub 控制 Topic。
- 网络 inbound 消息可按场景提供 `Arc<Bytes>`/共享缓冲，避免对每个业务订阅者复制大载荷。
- `MqttInbound` 的 Packet ID 仅用于连接内调试，不能当作跨连接的全局幂等 ID。

### 6.2 Core 内部 Trait（不是 Modorch 公共 Trait）

```rust
#[async_trait::async_trait]
pub trait BrokerStateStore: Send + Sync {
    async fn begin_recovery(&self, request: RecoveryRequest) -> Result<RecoverySnapshot, StoreError>;
    async fn recover_page(
        &self,
        snapshot: &RecoverySnapshot,
        cursor: Option<RecoveryCursor>,
        limits: RecoveryPageLimits,
    ) -> Result<RecoveryPage, StoreError>;
    async fn complete_recovery(&self, request: RecoveryComplete) -> Result<RecoveryReceipt, StoreError>;
    async fn abort_recovery(&self, snapshot: &RecoverySnapshot) -> Result<(), StoreError>;
    async fn load_session(&self, client: &str) -> Result<Option<SessionSnapshot>, StoreError>;
    async fn commit(&self, mutation: StateMutation) -> Result<CommitReceipt, StoreError>;
    async fn query_status(&self, key: &OperationKey) -> Result<CommitStatus, StoreError>;
    async fn retained_for(
        &self,
        filter: &str,
        page: RetainedPageRequest,
    ) -> Result<RetainedPage, StoreError>;
}

pub trait SubscriptionIndex: Send + Sync {
    fn upsert(&self, client: &str, filter: &str, qos: Qos) -> Result<(), MqttError>;
    fn remove(&self, client: &str, filter: &str) -> Result<(), MqttError>;
    fn match_clients(&self, topic: &str) -> Vec<MatchedSession>;
}

#[async_trait::async_trait]
pub trait ClientAuthorizer: Send + Sync {
    async fn authorize_connect(&self, context: &ConnectContext) -> Result<Principal, AuthError>;
    async fn authorize_session_attach(
        &self,
        principal: &Principal,
        context: &SessionAttachContext,
    ) -> Result<(), AuthError>;
    async fn authorize_publish(
        &self,
        principal: &Principal,
        context: &PublishAuthContext,
    ) -> Result<PublishGrant, AuthError>;
    async fn authorize_subscribe(&self, principal: &Principal, filter: &str) -> Result<(), AuthError>;
}
```

实现建议：Core 的 `MemoryStore` 直接实现内部 `BrokerStateStore`；启用 Storage Module 后，Core 创建 `EventHubStoreAdapter`，通过目标 Module 的 Request 提交有幂等键的 Store 操作，不直接依赖其实现类型。`Auth` 同理使用 `EventHubAuthAdapter`。这些 Trait 是 Core 内部抽象，不得被误读为框架允许任意 Module 发布共享 Service。

以上 DTO 为拟议类型，实施时必须保持以下字段和语义：

- `PrincipalKey` 是认证域与稳定主体 ID 的组合；`SessionAttachContext` 包含 ClientID、已有 owner、Session 版本、Clean Session 和接管意图。`authorize_session_attach` 检查 ClientID 使用策略；已有 owner 与新 PrincipalKey 的相等校验由 Core 强制执行，Auth 返回允许不能绕过。
- `PublishAuthContext` 包含 Topic、QoS、Retain 和 `kind = ClientPublish | Will | InternalPublish`；`PublishGrant` 绑定主体、上述参数及策略版本。Core 的保留命名空间检查独立于 Auth，匿名模式也执行。Will 在 CONNECT 接受前获取并记录 Grant，见 §7.5。
- `RecoverySnapshot` 包含 Broker ID、新写入 epoch、快照版本及快照标识；`RecoveryPage` 带有界记录、续页 cursor 和结束标记。记录类型及跨页一致性见 §8.2.1；`load_session` 仅供运行期已知 ClientID 查询，不能替代全量恢复。
- `StateMutation` 支持一次发布所需的全部可靠目标、Retained、配额计数和操作结果的原子变更，携带 Broker 写入 epoch、各 Session epoch/expected_version 和稳定 OperationKey。首个存储后端必须支持该事务范围；不支持者不能注册为持久模式 Provider。
- `query_status` 返回 `Committed(receipt)`、`Rejected(reason)`、`Pending` 或 `Unknown`。Unknown 不表示操作未执行；同键重试必须在 Store 侧与原提交串行判定，已有终态返回原结果，同键不同内容拒绝。已提交记录的清理不能使仍可重试的操作再次执行。

### 6.3 交付回执层级

| 名称 | 所属层 | 能保证什么 | 不能保证什么 |
|---|---|---|---|
| EventHub `AdmissionReceipt` | Modorch | 目标组件邮箱已准入 | Core 已处理/持久化/Client 已收到 |
| `BrokerAccepted` | MQTT Core | Core 已获取所有权，全部可靠目标状态已原子接纳；内存状态跨同一注册实例的 Core 监督重建保留 | 新 Application 或进程重启后仍可恢复（内存模式） |
| `DurableCommitted` | 持久 Store | 所声明的状态变更已达到配置的持久化边界 | 每个订阅客户端已消费 |
| `PUBACK` | MQTT 3.1.1 QoS1 | Broker 按协议接纳入站 PUBLISH 所有权 | 业务 Module 已执行、下游客户端已 ACK |
| `PUBCOMP` | MQTT QoS2 | QoS2 四阶段协议完成 | 业务跨系统事务 exactly-once |

不能为统一 API 方便而将上述边界合并为一个 `success=true`。

## 7. MQTT Core 内部架构与并发模型

```text
Network Listener (1 managed task)
  └── Connection Admission (connection/task limits)
      └── ConnectionTask (one per accepted Client; managed; handshake/Auth included)
          ├── Reader/Decoder → Packet FSM
          ├── KeepAlive / Timeout
          ├── SessionManager (ClientID-scoped ownership/generation)
          └── Writer (single ordered outbound stream per connection)
                      ↑
             bounded per-client output queue
                      ↑
             DeliveryManager / Packet ID / Inflight
                      ↑
           SubscriptionIndex / Router / RetainedStore
                      ↑
         Publisher: MQTT Inbound / EventHub Outbound Request
```

### 7.1 连接管理

- `TcpListener`/`TcpStream`（Tokio `net`, `io-util`），先按 listener / IP / 连接数限额准入，防止握手前无限分配。
- MQTT CONNECT 包须满足版本、固定头、剩余长度、协议名、ClientID、用户名密码、Will Flag、KeepAlive 等校验。非法报文按对应协议版本执行关闭或拒绝。
- ClientID 同名新连接必须处理旧连接接管，旧任务通过 `connection_epoch` 失效；禁止旧任务再次写入新 Session。
- 1.5×KeepAlive 无控制报文时关闭连接并触发应有的 Will 处理；正常 DISCONNECT 按协议关闭并清理 Will。
- 单连接写方向由一个 writer 串行化；read/write 队列有界，避免并发写造成报文交错。
- TCP Backpressure、慢客户端、半开连接、握手超时、空闲无订阅连接都受统一容量限制。

### 7.2 SubscriptionIndex

- 索引单位：`(session_id, filter)`，不是 `ComponentRef`；一 Session 可多次变更订阅。
- 实现可从分段 Trie + 精确 Topic 哈希表起步；新订阅变更构造过滤器、提交索引版本和 Session 记录；关键临界区不做外部 I/O。
- `+` 匹配单层（含空层），`#` 只能末尾且零层匹配；根通配不匹配 `$` 起始 Topic；`:id` 只是普通文本。协议字符和 UTF-8 校验遵循 OASIS 标准。
- 重叠订阅同一 Client 的消息交付按 MQTT 规则计算最高匹配订阅 QoS 并限制在 PUBLISH QoS 以内；可选重复交付能力不得默认导致非预期重复。
- 订阅/取消订阅应和可见的 Router 版本线性化；已开始路由的消息可沿当时快照处理，但不可产生永久悬挂订阅。
- MQTT 5 `Shared Subscription` 需要 `$share/{ShareName}/{Filter}` 专用解析和组选择算法，不是 EventHub 的普通 Topic Filter。

### 7.3 SessionManager 与离线队列

`ClientSession` 建议包括：`client_id`、`owner: PrincipalKey`、`subscriptions`、`clean_session`（3.1.1）/ `clean_start`+`session_expiry`（5.0）、`session_epoch`、`connection_epoch`、连接关联的 `will` 记录、`outbound_pending`、`outbound_inflight`、`inbound_qos2_state`、`last_seen`、`expiry`、存储版本。owner 是会话归属；当前连接的 Principal 和认证有效期另行管理。

- clean session=true：断开时清理 Session 状态；false：按适用协议及声明的保留策略保存订阅和 QoS 在途/待发记录；内存模式的保证范围是**同一 Application/Broker 注册实例存活期间，包含 Core 监督重建**，不扩展到新 Application 或进程重启。
- reconnect 时先通过 §7.3.1 的归属检查，再按 Session Present 等规则恢复旧会话；同 ClientID 接管需保证旧连接无法继续改变状态。
- Clean Session=false 的离线 Session 必须为匹配的 QoS1/2 交付保留消息（MQTT 3.1.1 §3.1.2.4）；首期不保存离线 QoS0。可靠积压的容量接纳与重试执行 §7.3.3，不静默淘汰已接纳消息。
- Session 状态更新由 per-session actor 或短临界区维护；禁止将慢 Store 请求持有全局 Router 锁。
- RetainedMessage 按 Topic 保存最新值，带空 payload 且 RETAIN=1 的发布执行删除语义；订阅时根据 TopicFilter 投递匹配保留消息。

### 7.3.1 Session 归属与接管授权

启用 Auth 后，首次创建 Session 时原子写入稳定 `owner: PrincipalKey`。已存在 Session 的恢复、活动连接接管及 Clean Session=true 导致的删除/替换，都必须先校验新身份与 owner 相同，并通过 `AuthorizeSessionAttach` 的 ClientID 使用策略。不同身份一律拒绝；首期不提供通过 CONNECT 转移 owner 的能力。匿名开发模式使用该 Broker 独立的匿名身份域，不宣称匿名客户端之间具备身份隔离；启用 Auth 或切换认证域需要显式迁移状态，不能自动认领旧匿名 Session。

检查期间仅只读加载旧 Session；未经授权不得停掉旧连接、修改 epoch、删除状态、安装 Will 或发送成功 CONNACK。同一 ClientID 的竞争通过 Session 版本 CAS 线性化，提交时再次校验 owner/version，避免“检查后被另一连接接管”的竞态。拒绝时按协议返回未授权结果，旧会话和连接保持原状。

同一身份恢复时，已有订阅须按当前 ACL 重新授权；任一订阅已失权或检查超时，首期拒绝恢复且不发出缓存消息。策略变更需要另行执行受审计的会话清理流程，不通过悄悄恢复旧权限处理。授权通过后才进行旧连接接管和协议要求的 Will 处理。

### 7.3.2 MemoryStore 的所有权与重建

`MqttBrokerBundle::register` 为该次注册创建 `Arc<BrokerStateOwner>`，由注册到 Application 的 Core Factory 持有；其中包含 MemoryStore、接纳操作记录、配额账本及单调递增的 Broker 写入 epoch，不包含长期运行的网络任务。内存对象的构造无 I/O 副作用。同一 Factory 每次 `create` 只克隆句柄，不新建空 Store；不同注册和不同 Application 不共享此对象。

Core `setup` 挂接 Store，`activate_ingress` 取得新 epoch 并按 §8.2.1 重建本代订阅索引。`begin_shutdown/quiesce` 排空或保存已接纳工作，`teardown` 仅释放本代资源；Session、Retained、QoS 状态及配额由状态所有者继续持有。旧代际清理重试和激活失败补偿均不得清空这些数据。Clean Session 的正常清理仍是显式状态变更，不受此保留规则阻止。

Application 最终终止后停止所有能力，待 Factory/状态所有者的句柄释放时回收内存；新 Application 即使在同一进程也创建新状态。若状态损坏、恢复无法证实一致，禁止以空 Store 自动重启并继续声称保留了 Session，应进入可诊断的恢复失败。

### 7.3.3 可靠积压、容量预留与发布原子性

首期采用 **整次发布接纳或拒绝**：先取一致的订阅快照，按每 Session 最终交付 QoS 计算目标，再为所有 QoS1/2 目标及 Retained 变更预留容量。`max_offline_messages_per_client` 统一限制一个 Session 的可靠待发与 Inflight 总数，在线和离线使用同一账本，断线不能重置限额；字节限额及全局内存/持久存储预算同时生效。QoS0 目标不占可靠积压，按其独立 Writer 队列 best effort。

预留只占短临界区，不持有路由锁等待 I/O。持久模式通过单次 Store 事务验证目标 Session 的 epoch/version、写入消息和全部可靠交付记录、Retained 变更、配额计数及操作结果；内存模式在共享 MemoryStore 内实现等价原子变更。提交冲突只允许有界重试；结果不确定时保留预留并使用同一 OperationKey 查验，不先释放容量再提交另一份工作。状态版本提交后才使新交付对 Writer 可见。

| 条件 | 首期行为 | 已接纳消息 |
|---|---|---|
| 全部可靠目标和状态变更有容量 | 原子提交，之后允许 QoS1 PUBACK / 内部接受回执；QoS2 按其所有权接纳阶段执行 | 保留至协议完成或显式会话清理 |
| 任一可靠目标或全局预算不足 | 不提交任何可靠目标和 Retained 变更，不先向 QoS0 目标发送；MQTT 3.1.1 QoS1 在接纳前断开发布连接且不发 PUBACK，内部 Request 返回 `CapacityExceeded` | 不淘汰、不挪用其配额 |
| QoS0 队列满 | 丢弃该目标的 QoS0 交付并计数，不等待其他客户端 | 可靠交付记录不受影响 |
| 提交成功后 Writer 队列满或订阅者断线 | 从已计费的可靠 Pending 中稍后发送，不重复创建离线副本 | 不丢弃，不因再次进入队列重复计费 |
| 提交超时或 ACK 丢失 | 按持久操作结果恢复；同一内部请求/Store 操作不产生部分目标的重复提交 | MQTT QoS1 的发布者重传仍可能造成协议允许的重复，不宣称跨连接全局去重 |

一个满额订阅者会使仍匹配它的后续可靠发布被有限地拒绝；本设计承认此影响，不承诺同一扇出内其他客户端必定收到这些被拒绝的消息。其他 Topic/独立发布继续处理，不持锁无限等待。断开慢订阅者不能清空 Clean Session=false 的积压；释放空间依赖其消费 ACK、协议规定的 Session 清理或显式管理删除，不自动删除会话来制造成功。

已提交操作的目标集合、每目标交付 ID 和载荷指纹必须保存在状态中；恢复及幂等重试沿用该集合，不能重新匹配后只补发一部分。内部发布以 `(可信来源, request_id)` 为幂等域；同键不同内容拒绝。网络 QoS 状态和 Packet ID 按协议管理，不将 Packet ID 当作跨连接的发布幂等键。QoS2 在 PUBREC 之前完成其接纳所需的持久化/容量保留，后续 PUBREL/PUBCOMP 不再因普通队列满而拒绝已取得所有权的消息。

### 7.4 QoS 与交付状态机

**阶段性实现安排**：

| 版本阶段 | 入站/出站协议 | 交付声明 |
|---|---|---|
| 开发 Milestone A | MQTT 3.1.1 CONNECT、QoS0/1、PUBACK、SUB/UNSUB、PING、Will、Retained；QoS2 非支持 | **仅 MQTT 3.1.1 子集**，不得宣称完整协议符合性 |
| 正式 3.1.1 Conformance | 新增 QoS2：PUBLISH→PUBREC→PUBREL→PUBCOMP，持久状态与重连完整路径 | 完成测试后方可声明 MQTT 3.1.1 Broker 支持 |
| MQTT 5.0 | Properties、Reason Codes、Session/Message Expiry、Receive Maximum、Shared Subscription 等 | 依协议能力逐项验收，不在半成品阶段宣称完整支持 |

**QoS 0**：接收即尝试路由，不回 ACK；满队列/断开可能丢失，统计并对外提供指标。

**QoS 1（上行）**：按 §7.3.3 为全部可靠目标预留容量并原子提交接纳状态后返回 PUBACK；Broker 必须能独立继续对所有适用 Session 执行后续交付。容量不足时在所有权转移前拒绝，不能部分交付后返回成功。业务 EventHub 接收失败不等同 MQTT 失败。内存 Store 模式保留跨 Core 监督重建的状态，不能承诺进程崩溃恢复。

**QoS 1（下行）**：分配当前连接未使用的 Packet ID，设置 `inflight`；收到 PUBACK 后完成；重连续会话时重新发送未确认的 PUBLISH，并按协议设置 DUP 与 Packet ID。避免对 ACK 丢失和断开重试产生“至多一次”的错误假设。

**QoS 2**：设计完整双向 FSM 与重连恢复，必须验证重复 PUBLISH/PUBREL、Packet ID 生命周期、持久化崩溃点；未实现前拒绝相关不支持操作，不能静默把 QoS2 转成 QoS1 并同时宣称 MQTT 3.1.1 完整兼容。由于 MQTT 3.1.1 对 QoS2 PUBLISH 响应有规范要求，非支持版本仅能定义为受限试验实现，不用其完成正式符合性验收。

**顺序**：按协议要求保证每连接/有序 Topic 所需的接收与交付顺序，不将多工作线程的完成顺序冒充协议顺序。内部 Router 可并发，但单客户端 Writer 必须保证顺序约束，特别是 QoS1/2 ACK 流。

### 7.5 Will、Retained、KeepAlive

- CONNECT 携带 Will 时，在成功 CONNACK、接管旧连接及安装 Will **之前**，由 Core 校验 Will Topic Name、支持的 QoS、payload/资源限额和保留命名空间，再以认证后的 Principal 调用 `AuthorizePublish(kind=Will, topic, qos, retain)`。Retain 权限必须明确判定；失败或超时拒绝 CONNECT，不安装 Will、不影响已有连接。匿名模式也不能绕过 Core 的保留命名空间检查。
- 接受连接时保存不可变 Will 内容、`PublishGrant` 和 `(broker_generation, session_epoch, connection_id)`，其所有权属于该次连接。异常断开、KeepAlive 超时及同 ClientID 接管等场景按协议触发；正常 DISCONNECT 清除。触发由当前 Core 接管执行，不能让已失效旧任务自行修改新 Session，也不能因旧连接 epoch 已失效而漏发其应触发的 Will。
- Will 的发布授权使用 CONNECT 接受时记录的决定；Auth 暂时不可用或原连接认证到期不使已承诺的 Will 无声消失。管理员撤销待发 Will 必须是显式、受审计的操作。触发通过统一的 Topic/保留命名空间校验、路由、Retained 和存储路径，禁止以 Broker 自有身份绕过原始发布权限。
- Will 无法在触发时向已断开的发布者拒绝，因此成功 CONNECT 前需预留一个有界 Will 记录及其载荷预算；触发时将其原子转为待发布工作并固定目标快照。可靠目标暂无容量时保留该工作，空间释放后按 §7.3.3 原子接纳，不能丢弃或逐目标部分提交。目标元数据也计入预算，按 `max_sessions` 为其预留最坏情况空间；未触发、待发和执行中的 Will 共用 `max_will_records` 限额，无空位时拒绝新的带 Will 连接。正常 DISCONNECT 或发布完成后释放记录；持久模式将其纳入恢复快照。
- Broker 整体关闭和网络突断场景须按适用协议版本处理 Will，不能无条件抑制；停机期间可交付与可保存范围需明确。
- KeepAlive 定时器须基于实际收到的 MQTT Control Packet 更新，避免仅收到 TCP keepalive 就误判活跃。
- Retained 首期内存可用；Storage Module 接入后先持久提交，再按协议发布/确认；删除同步索引。

### 7.6 数据面线程模型与 Scheduler 任务配额（0.0.3 特别约束）

0.0.3 的 `TaskScheduler::spawn` 是绑定组件 Owner/Generation 的受管异步任务，提交前可能因全局或 Owner 任务配额拒绝；不能把它当作无限制的 Tokio spawn。初版采用 **每 Listener 一个受管任务 + 每连接一个受管任务**，读写和定时控制合并进每连接任务的 `select!` 循环，减少额外任务。状态存储和核心队列由共享受控对象持有，禁止把整份 Module 作为服务导出。

- 监听循环先观察取消并取消后停止 `accept`；每次 Accept 后先取得连接许可与任务资源，失败即关闭新 socket，并上报限流/拒绝指标。
- Listener 非预期退出即显式报告错误，使 Supervisor 依据组件 FailurePolicy 处理；正常 shutdown 的取消退出不计为故障。
- `max_connections` 与 Application `task_limits` 之间并非简单全局等式（其他 Module 也有任务），需校验最低预算、运行时抢占与超额拒绝策略；在不够额度时不能退化为失管 `tokio::spawn`。
- 连接 Writer 使用有界队列且只允许单消费者写；优先保证 ACK/控制报文所需容量，业务 PUBLISH 扇出对每客户端独立施压。指标、ACL 与存储操作不持有全局路由锁等待 I/O。
- 自定义 Supervisor 重试可造成连接中断和 Will 语义变化。仅依赖 Modorch 的故障监督不等于 MQTT Session 恢复，Core 必须定义“计划性关闭 / 异常断开 / 模块故障 / 宿主中止”的分类。

## 8. 可选扩展 Module 的详细接口与职责

### 8.1 Auth Module：认证与授权

- 实现单独 ModuleFactory + Handler；Core 在 CONNECT/PUBLISH/SUBSCRIBE 过程中向 `Destination::Component(auth-id)` 发送类型化 Request。
- 必需操作：`Authenticate`、`AuthorizeSessionAttach`、`AuthorizePublish`、`AuthorizeSubscribe`；响应包含稳定 `PrincipalKey`、角色/权限判定、有效期、策略版本和审计理由码（不包含明文凭据）。
- `Authenticate` 只证明当前连接身份；随后 Core 按 §7.3.1 强制校验会话 owner，Auth 检查 ClientID 使用策略与恢复订阅权限。`AuthorizePublish` 覆盖正常 PUBLISH、CONNECT Will 和内部发布的相应主体，不只检查带 PUBLISH 固定头的报文；Will 授权及资源预留必须在 CONNECT 接受前完成。
- **安全默认**：未配置 Auth 且未显式开启 `anonymous_access` 时拒绝连接；开发模式只监听 `127.0.0.1` / `[::1]`，非回环绑定强制 TLS/Auth 或强制显式风险确认策略（配置时拒绝不安全组合更好）。
- 认证和 ACL 超时执行 `fail_closed`；MQTT 3.1.1 对未授权 PUBLISH 缺少负面 ACK，按标准许可策略断开连接或正向 ACK（本库推荐断开），不能回自造 ACK 码。
- 不在日志记录密码/token；加连接限速、防爆破、原始连接 IP 限额。

### 8.2 Storage Module：可替换持久状态

- Provider Module 通过 EventHub Request 实现 §6.2 的全部 Store 操作（分页恢复、加载、原子提交、状态查询、删除、清理），先提供**一种可验证**实现（推荐 SQLite/WAL 或本地 KV，最终按 benchmark 决定），再开放其他实现。
- 对外契约按 §7.3.3 的整次发布事务范围更新，携带稳定 `operation_id`、Broker 写入 epoch、各目标 `session_epoch` 和 `expected_version`，在 Store 端原子校验，保证重试幂等和旧 Generation 无权覆盖新状态；不要用进程局部 EventHub OperationId 作为跨重启幂等主键。
- `commit` 返回的是“已按选定 durability policy 提交”的回执；Sync/Async fsync 模式必须明确。首期内存模式不装 Storage Module，也不声称重启后持久 Session。
- 容错：请求正在执行却超时 → 结果不确定；Core 应用同一幂等键做 `query_status`/恢复校验，未确认成功时不提前发送会形成不可恢复承诺的 ACK。
- 恢复：Broker 在 `activate_ingress` **内部**先经 EventHub `external().request` 执行 §8.2.1 的分页快照恢复，再调用 `listen`。不能在 `ready` 内依赖尚未开放的 EventHub 根准入；也不能先 `listen` 后恢复。异常断电后做 crash-recovery 测试。恢复超时属于不确定状态，不得开放入口。
- 如纯 EventHub Request 往返成本在高吞吐测试中成为瓶颈，可引入**独立 Initiator + 窄服务 API**，保持 Module 不发布共享服务的框架契约。

### 8.2.1 全量恢复快照与离线索引

恢复入口对 MemoryStore 与 Persistent Store 使用同一语义；后者所有操作经定向 EventHub Request，Store 内部的数据库访问不泄漏到 Core。

1. `begin_recovery` 原子取得该 Broker 的新写入 epoch、关闭普通写入并固定一致性快照。旧 epoch 写入在 Store 端拒绝，不能只靠 Core 提交前检查。重复开始/完成请求以稳定恢复 operation_id 幂等处理；上次未完成的恢复由新尝试接管和清理，不无限保留旧快照。
2. `recover_page` 按快照和 cursor 枚举**全部** Session（含无活动连接的 Session、owner、订阅及 epoch/version）、可靠 Pending/Inflight、入站 QoS2 状态、Retained、Will 工作、接纳操作结果和配额账本。Retained 枚举包含 `$` 开头的合法 Topic，不使用 `#` 匹配冒充全库扫描。每页有记录数和字节上限，超大单条记录返回可诊断错误；总恢复受时间和总内存预算约束，禁止无界 Vec。
3. Core 在尚未可见的本代索引中重建所有 Session 的订阅，不等待客户端重连。恢复连接记录只用于会话/Will 收尾，不将失去 TCP 的连接标记为在线。按版本恢复 Packet ID 占用与 QoS FSM，处理未完成接纳结果，重算资源占用并核对账本；不能通过丢弃已接纳数据来满足新配置的较小预算。
4. 页面必须属于同一快照版本，引用完整、无重复身份，cursor 前进且最终有明确结束标记。完成后由 `complete_recovery` 校验快照和新 epoch、解除恢复屏障，Core 原子发布重建索引，再允许控制面发布及网络 `listen`。恢复期间包括 EventHub 控制面在内的新发布返回 `BrokerNotReady`，不能提前修改快照。
5. 任一步失败、超时、不确定、页缺失或预算不足均不得开放入口。释放本代临时索引，并以 `abort_recovery` 幂等回收快照资源；终止恢复不得删除原有 Session/交付或重新允许旧 epoch 写入。补偿中的 Store 请求在生命周期因果链中执行，使用保存的普通 EventHub 句柄，不能通过 `external()` 在关闭的根准入上新建操作。

验收必须包含：客户端 A 创建持久订阅后离线，Broker 重启，A 尚未重连时 B 发布匹配 QoS1 消息；A 随后以 Clean Session=false 重连，应收到该消息。仅验证“A 重连后再发消息”不能证明离线订阅索引已恢复。

### 8.3 Admin Module

- 接受 `ListClients`、`GetSession`、`DisconnectClient`、`ListSubscriptions`、`BrokerStats` 等类型化 Request/Command。
- HTTP 管理界面是另一个可选 **Module**，在 Modorch-RS 0.0.3 中可直接覆盖 `activate_ingress()`，无需独立 Initiator；不放进 Core，也不默认暴露公网。审计、鉴权、分页和大结果上限必须具备。
- 管理操作不直接写 Core 的私有 Map；全部使用经过鉴权的 Core Control Plane API，保持一致性与线程安全。

### 8.4 Metrics Module

- 订阅 `$modorch/mqtt/event/#` 中的连接、断开、发布计数、存储故障通知；高频消息采用 Core 内本地聚合或有界快照拉取，不对每条 MQTT 报文必然产生 EventHub Fanout。
- 至少统计：当前连接、会话、订阅数、入/出消息吞吐、ACK 待确认、写队列深度、拒绝/超时、积压、慢客户端、认证拒绝、存储提交延迟、重连和 Will 数。
- 不以 ClientID/Topic 原文作为无界时间序列标签；限制基数，保护敏感信息。

### 8.5 Bridge Module

- 显式配置 Topic 映射与单向/双向方向；在出网时要求类型化 `MqttPublishRequest`，并执行 ACL、Hop/Origin 过滤和可控重试。
- 需要跨其他 Broker 的桥接时再实现独立 MQTT Client 功能；此能力不是 Core Broker 的基本依赖。
- 若与业务 EventHub 订阅同一 Topic，必须以类型、受控 Origin 和映射策略识别消息，避免“Client 发布→EventHub→Bridge→Core→Client”无限回环。

## 9. 配置体系、资源预算和安全默认

拟议 TOML（宿主可自行装载，Bundle 负责强校验）：

```toml
[mqtt.core]
enabled = true
name = "mqtt-broker"
protocols = ["3.1.1"]        # 仅声明已实现且验收的版本
listener = "127.0.0.1:1883"
max_connections = 500
max_sessions = 2000            # 含离线 Session，不能仅用连接数限制会话总量
max_packet_bytes = 1048576
max_client_id_bytes = 128
handshake_timeout_ms = 5000
connection_idle_ms = 0         # 0=按 MQTT KeepAlive 规则，不是禁用必要定时器
per_client_outbound_queue = 128
max_inflight_per_client = 16
max_subscriptions_per_client = 256
max_offline_messages_per_client = 2048
max_pending_bytes_per_client = 16777216  # 在线/离线可靠 Pending + Inflight 的字节上限
max_will_records = 500         # 未触发及待发布 Will 共用，另受全局字节预算约束
max_retained_topics = 10000
memory_budget_bytes = 268435456
reliable_overflow_policy = "reject_publish"  # 首期唯一可靠溢出策略，见 §7.3.3

[mqtt.security]
anonymous_access = true         # 开发用：仅回环地址
allow_public_listener_without_tls = false
reserved_internal_prefix = "$modorch/mqtt/"
session_owner_policy = "same_principal"      # 启用 Auth 时强制；匿名模式的限制见 §7.3.1

[mqtt.eventhub]
inbound_mode = "best_effort"
inbound_topic_mode = "original"
control_prefix = "$modorch/mqtt/control/"
allow_implicit_outbound = false

[mqtt.auth]
enabled = false

[mqtt.storage]
enabled = false
mode = "memory"
recovery_timeout_ms = 30000
recovery_page_max_records = 128
recovery_page_max_bytes = 4194304  # 含响应封装开销，必须适配 Host 的 HubLimits
# 持久模式启用时显式配置该值，并验证后端物理存储/WAL 上限与预留空间
# disk_budget_bytes = 1073741824

[mqtt.admin]
enabled = false

[mqtt.metrics]
enabled = false
```

注意：当前 Modorch-RS 自己的 EventHub 默认 Topic / Header 容量有独立限制（如 Topic 本地默认 1024 字节）；**MQTT 协议允许的 Topic 长度上限与 EventHub 本地限制不相同**。当合法 MQTT Topic 超出 EventHub 本地预算时，Core 必须继续按协议处理 Client→Client 路由，可选择跳过/拒绝映射到内部 EventHub 并记录原因，不能因为内部观察失败就错误返回 MQTT 协议异常。也可由宿主明确增大 HubLimits，但必须按内存预算评估。

其他必须明确的配置边界：

- TCP 连接数上限、Scheduler Task 上限、每连接内存、总内存、队列大小、报文尺寸要联合校验；接入高峰先拒绝/限流，禁止无限堆积。
- `max_sessions` 包含离线 Session；`max_offline_messages_per_client` 与 `max_pending_bytes_per_client` 对在线/离线可靠积压统一计费，Writer 队列仅持交付引用，不能成为绕过账本的另一份积压。限制订阅/Retained/Will 元数据及载荷、未决提交预留、恢复临时索引和操作记录的总内存；恢复页/响应还须适配 EventHub 的消息字节限额。
- 持久模式必须声明 `disk_budget_bytes` 及后端物理容量门禁，覆盖消息、索引、操作结果、WAL/事务临时空间和回收预留；单纯逻辑消息计数不能证明磁盘有界。已提交消息不会因后续缩减预算而被自动淘汰，预算不足时启动/重配置失败或拒绝新接纳。
- 初版强校验 `reliable_overflow_policy = "reject_publish"`；拒绝配置成 drop-oldest/drop-newest 来淘汰已接纳可靠消息。QoS0 队列丢弃单独计数，不冒充可靠发布成功。受限开发阶段未实现的 QoS2 仍显式拒绝，不能套用 QoS1 的 ACK 行为。
- `TLS` 建议通过独立 Listener/TLS 配置扩展（`8883` 可配置，不强制端口）；证书热更新仅在明确受支持时开启。
- Listener 改配置需要受控重启或版本化 reconfigure，而不是修改运行中静态 Map。
- 自定义 Broker 名称不能产生控制 Topic 冲突；同一 Application 首期最多启用一个 Core 实例，未来多实例需以 Broker ID 分区控制命名空间。
- 未启用 Storage 的 Core 不能声称 crash-safe durable Session。

## 10. 关键流程与故障处理

### 10.1 CONNECT → Session

```text
TCP Accepted
  → capacity/rate/task limit → spawn ONE managed ConnectionTask
  → decode CONNECT + validate version/flags/ClientID within handshake deadline
  → optional Authenticate Request (fail-closed)
  → read existing Session without mutation; check owner + AuthorizeSessionAttach
  → reauthorize restored subscriptions; validate Will namespace/ACL/QoS/Retain
  → reserve Session/Will capacity; CAS owner/version before any takeover or deletion
  → take over old connection; hand old Will to Core according to protocol
  → load/reset/create Session according to Clean Session; persist accepted state
  → install new connection / authorized Will / negotiated parameters
  → send CONNACK / Session Present
  → continue read/write/timeout FSM in the SAME managed ConnectionTask
```

握手失败不能安装半成品 Session；在 owner、恢复订阅或 Will 检查失败时，旧连接/Session 必须不变。若检查通过后存储完成但连接失败，恢复路径以已提交状态为准，不回滚覆盖较新的连接；Will 的安装和发布使用独立记录及连接 epoch，避免旧连接竞争。握手和 Auth 请求在已计入配额的 ConnectionTask 内执行，不能串行阻塞 Listener 的 accept。

### 10.2 SUBSCRIBE → Topic Filter

```text
Client SUBSCRIBE
  → decode filter(s), requested QoS, packet ID
  → authz each filter (if configured)
  → validate/compile filters, allocate/replace per-session subscription
  → persist Session mutation when configured
  → SUBACK with granted QoS / per-filter failure
  → send matching Retained Messages according to MQTT rules
```

MQTT 3.1.1 SUBACK 可对单个订阅条目返回 failure；不能因同一个包某一 Filter 失败就错误声称其他成功项失败。请求 QoS2 在当前仅支持 QoS0/1 的 Milestone A 必须被明确判定为受限能力，正式 3.1.1 符合性阶段补齐 QoS2 协议处理。

### 10.3 PUBLISH → Clients / EventHub

```text
Client PUBLISH
  → validate Packet/Topic/ACL
  → match a consistent per-session subscription snapshot; compute delivery QoS
  → reserve capacity for ALL reliable targets + retained/state mutation
      → if full: reject BEFORE ownership transfer, no partial delivery
  → atomically commit ownership + target delivery IDs + retained/state + quotas/result
  → make reliable Pending visible; independently schedule per-client Writer queues
  → attempt best-effort QoS0 fanout
  → send PUBACK if QoS1 after successful ownership acceptance
  → separately emit MqttInbound Event to SAME Application EventHub (best effort by default)
```

- Broker 对不同 Client 的交付状态相互独立。
- 不订阅该 Topic 的业务模块不会让入站 PUBLISH 失败。
- 对一个慢 Client 施加其专属队列限额；容量满按 §7.3.3 有界拒绝仍匹配它的新发布，其他独立发布继续执行。不得无限等待，也不得以先 ACK 再丢弃 QoS1/2 消息来掩盖容量不足。
- 若 storage Required 而 commit 不成功，不能发送会误报所有权已转移的 PUBACK；按版本、认证/协议策略关闭连接或保持待处理，禁止无限内存堆积。
- 内部 `MqttPublishRequest` 使用相同的容量预留和原子提交边界；`CapacityExceeded` 表示整次发布未接纳，`Indeterminate` 必须同键查询/重试。提交成功但回执丢失时返回原结果，不重新扇出一部分目标。

### 10.4 Shutdown / Restart

```text
Host shutdown_checked
  → root ingress close & tasks cooperative cancel
  → MQTT Core begin_shutdown: stop accepting new TCP, mark sessions draining
  → Core quiesce: wait for listener/connection tasks to acknowledge cancellation;
      bounded flush / save accepted state / hand off Will work / finish pending commits
  → Modorch tracker.drain()
  → Core teardown: release generation Listener / Router / Store adapter handles
      preserve Factory-owned MemoryStore and persistent Session records
  → Application GracefullyStopped
```

要将 TaskScheduler 先触发取消的当前行为与 MQTT 客户端连接收尾协调，不以等待任务自然退出为由无限停机。Broker 原因导致非正常断连时的 Will 按协议版本处理，重启时应先恢复持久状态再开放端口；不能因框架重建默默丢弃已确认的持久交付。

Core 监督重建走相同代际资源清理，之后由同一 Factory 克隆状态所有者、推进 Broker 写入 epoch、恢复全部 Session 和未完成工作，再开放入口。最终 Application 终止与 Module teardown 区分：后者不负责销毁共享 MemoryStore，前者待 Factory/状态句柄释放后回收；进程退出或新建 Application 不享有内存恢复保证。停机的存储/Will 收尾使用已保存的普通 EventHub 句柄和框架提供的生命周期/已准入任务因果链，不用 `external()` 绕过已关闭的根准入，也不在 teardown 后补做关键持久提交。

### 10.5 故障矩阵

| 故障 | 预期行为 | 核验重点 |
|---|---|---|
| Listener bind 失败 | Application StartupError + 清理 | 端口无泄漏、业务未暴露半 Ready |
| MQTT Client 发送畸形包 | 关闭连接/对应规范错误 | 无 panic、无内存泄漏 |
| ClientID 重复接管 | 先校验 owner/ClientID 权限，再使旧 connection epoch 失效 | 跨身份拒绝且旧状态不变；同身份接管与 Will 正确 |
| CONNECT Will 未授权 | 在成功 CONNACK/接管/安装 Will 前拒绝 | Topic、QoS、Retain、保留命名空间与 Auth 超时均覆盖 |
| Auth 超时 | fail-closed，不继续握手/订阅 | 不被不确定 Request 误当允许 |
| Storage 超时 | idempotency/status check，不提前承诺 ACK | 幂等恢复和不确定状态 |
| EventHub 满 | 内部观察被拒绝或降级 | 客户端 QoS 路由不因观察丢失而失败 |
| 慢 MQTT Subscriber / 离线可靠积压满 | 接纳前拒绝整次新发布，已接纳消息保留 | 无部分目标提交后 ACK；独立发布不被锁住 |
| 恢复分页失败或快照不一致 | 不 listen，丢弃临时索引，回收快照资源 | 离线订阅无漏恢复、含 `$` Retained、预算不足不删数据 |
| 连接突然断开 | Session 状态转移、Will 规则 | 重连与重复 ACK 处理 |
| Broker Module 监督重建 | 0.0.3 先停旧 Generation；保留 Factory MemoryStore/持久状态，再恢复并激活 Listener | 无双 Listener/失管 Client Task；内存 Session/Retained/已接纳交付不丢；旧代际写入被拒绝 |
| Host shutdown 超时 | StopIncomplete，可继续检查式完成 | 不假报 GracefullyStopped |
| 突发断电 | 内存模式丢状态；持久模式按 durability policy 恢复 | 不扩大保证 |

## 11. 协议版本与能力矩阵

| 能力 | 研发阶段 A | 正式 MQTT 3.1.1 | MQTT 5.0 阶段 |
|---|---|---|---|
| TCP / CONNECT / DISCONNECT / PING | 要求 | 要求 | 要求 |
| Topic Name / Filter | 要求 | 要求 | 要求 |
| PUBLISH / SUBSCRIBE / UNSUBSCRIBE | 要求 | 要求 | 要求 |
| QoS 0 / 1 与正确 ACK | 要求 | 要求 | 要求 |
| QoS 2 四阶段 | 不声明支持 | **必须通过** | 必须通过 |
| Session（Clean Session / 恢复） | 内存模式 + 明确限制 | 必须符合 MQTT 3.1.1 | Clean Start / Expiry |
| Retained / Will / KeepAlive | 要求 | 要求 | 要求 |
| Auth/ACL、Storage | 可选扩展 | 按库声明策略 | 扩展 |
| Shared Subscription / Reason Code | 不提供 | 不适用 | 专项实现 |
| Receive Maximum / Message Expiry / Topic Alias | 不提供 | 不适用 | 专项实现 |
| WebSocket / TLS | 后续可选 | Transport 扩展 | 可选 |
| 单 Application / 共享 EventHub | **必须** | **必须** | **必须** |

协议符合性不是只看可连接或可运行的 QoS0 测试。正式声明之前需通过合法/非法控制包、状态流、重连、重试、顺序、Will/Retained、边界长度等完整规范矩阵。

## 12. 测试体系、性能目标与可观测验收

### 12.1 单元测试

- Codec：Remaining Length 编解码、截断帧、非法 flags、UTF-8、超长包、Packet ID、MQTT 3.1.1 所有目标控制包。
- Topic：使用与 magicCommon / Modorch-RS 共享的 JSON Topic Vector 断言；额外覆盖 `$share` 专有解析（仅 MQTT5）。
- Session：Clean Session、Session Present、同/跨 PrincipalKey 的 ClientID 接管和删除、并发 CAS、恢复订阅 ACL、过期、Will、离线队列边界。
- Router：单/多客户端、多重叠订阅、QoS 计算、Retained 回放、动态取消订阅。
- QoS FSM：入/出 QoS0/1、Packet ID 分配及复用、重复包、丢 ACK、重连 DUP；QoS2 阶段追加全部状态转移。
- 限额：buffer、连接、在线/离线 Session、Inflight、积压条数/字节、内存/磁盘、Will 记录、恢复页、Topic/客户端数、超时/取消。
- Store：一致性快照分页（含 `$` Retained）、cursor 完整性、恢复屏障、新旧 epoch 写入、整次发布事务回滚、配额核对、同键重试/不同载荷冲突。
- Will：CONNECT 前的发布/Retain 授权、匿名模式保留命名空间拒绝、失败不安装、接管时旧连接 Will 移交、有界待发与恢复。

### 12.2 端到端与框架集成

1. **单 Application/Hub**：用真实 Host Application 启用 2 个业务 Module + MQTT Core + 1 个扩展；业务事件可达，确认没有第二 `EventHub::new` / 第二 App。
2. **PUBLISH/Subscribe**：两个真实 MQTT Client 连接并发送/接收匹配与非匹配 Topic；发布者自身订阅时按协议回送。
3. **Inbound Event**：业务 Module 接收 `MqttInbound`，Topic 与客户端的 MQTT Topic 一致。
4. **Outbound Control**：业务 Module 向内部控制 Topic 发 `MqttPublishRequest`；客户端收到正确 MQTT 报文；禁止任意业务 Event 隐式出网。
5. **QoS1**：中断网络、掉 PUBACK、重复 PUBLISH、Session 重连，验证正确重发与状态回收。
6. **Retained/Will**：订阅时 Retained 回放，retain 空 payload 删除；正常 DISCONNECT 与异常断线行为。
7. **Auth/Storage**：独立启用和禁用验证；认证超时拒绝、存储不确定提交与 crash 恢复。
8. **Lifecycle 0.0.3**：所有 `ready()` 完成前不对外 `listen`；端口被占用或恢复失败返回含 `ActivateIngress` 根因的 `StartupError`；Shutdown 无遗留 TCP/Task；Restart 不形成双监听；后续入口激活失败时已激活 MQTT 入口能被补偿。注意 App 全局 Ready 与首个 Listener `listen` 之间的先后区别。
9. **跨应用隔离**：同一 Tokio 进程中两个不同 Application 不共享 EventHub 与 Broker Session。
10. **安全**：保留控制 Topic 不可经 MQTT 被外部 Client 操纵；topic ACL、认证暴力重试、包体 DoS。
11. **真实客户端兼容性**：至少用两个独立 MQTT 客户端实现交叉验证（如 Eclipse Paho / Mosquitto 客户端），再加协议测试框架。
12. **会话归属（`session-owner-isolation`）**：A 身份订阅私有 Topic 后离线，B 使用自己的有效凭据和 A 的 ClientID，分别尝试 Clean Session=false 恢复和 true 清除，均拒绝且不泄漏缓存消息；A 在线时验证 B 无法抢断。追加并发接管、认证域变化和恢复订阅已失权的拒绝用例。
13. **Will 授权（`connect-will-authorization`）**：通过合法身份设置越权 Topic、禁止 Retain 或保留命名空间 Will，以及 Auth 超时，均在 CONNECT 接受前拒绝，不触发 Will、不修改旧 Session；正常授权的 Will 在异常断开/同身份接管时正确触发，空间不足时保留待发记录，重建后继续。
14. **离线恢复（`offline-subscription-recovery`）**：A 以 Clean Session=false 订阅后离线，重启 Broker，在 A 尚未重连时由 B 发匹配 QoS1 消息；A 重连后收到。用小页跨多页恢复并包含 `$` Topic、Retained、Inflight 和未完成提交；中间页错误、重复/失效 cursor、预算不足时端口不可连接且原数据不丢。
15. **内存代际保留（`memory-generation-recovery`）**：不开 Storage，准备 Session/Retained/已接纳 Pending 和 Inflight，再触发 Core Restart 及一次激活失败补偿；恢复后均可继续，旧 epoch 无权写入。销毁旧 Application 后新建应用，应为独立空状态，不扩大内存保证。
16. **饱和接纳（`reliable-admission-saturation`）**：一个发布匹配 B/C 两个可靠目标，使 C 满额；发布应整体拒绝，B/C 和 Retained 均无部分新增，MQTT 3.1.1 QoS1 不发成功 PUBACK。再验证条数/字节/全局预算、在线转离线不重置计费、已提交后 Writer 满不丢消息、同键提交超时恢复，以及其他 Topic 继续处理。

### 12.3 性能与资源测试（先设测量方法，再设阈值）

| 维度 | 采样内容 | 初版门禁 |
|---|---|---|
| 吞吐 | 1/10/100/500 Clients 下 QoS0/1 msgs/s | 输出可复现 baseline，不编造生产 SLO |
| 端到端延迟 | Client PUB→Client SUB p50/p95/p99 | 含网络及队列延迟，而非 EventHub Handler 时间 |
| 内存 | 每连接/每条 inflight/每个订阅/Retained 的估算 | 不突破 `memory_budget_bytes` 承诺 |
| 背压 | 慢消费者、离线满额、条数/字节/磁盘预算、高峰突发 | 整次发布接纳前拒绝；不淘汰已接纳可靠消息；独立发布继续执行 |
| 容量 | Max tasks 与 Max connections 组合 | 容量配置错误尽早拒绝或诊断 |
| 恢复 | Core 重建、持久重启、分页快照与临时索引峰值 | 离线订阅可在重连前路由；内存跨代际保留；恢复不突破预算 |
| 退出 | 高负载 Shutdown/StopIncomplete | 所有可完成任务受跟踪，不假报正常释放 |

每份性能报告必须包含 CPU、内存、操作系统、Rust 版本、Modorch Commit、MQTT 版本、Qos、payload 大小、连接数、Broker 配置和原始测量日志。压测结果不能反推协议正确性。

## 13. 分阶段交付计划（工作包）

| 阶段 | 交付内容 | 依赖 | 退出门禁 |
|---|---|---|---|
| F0 基线核验（已具备框架能力） | 固定 Modorch-RS **0.0.3** commit + Contract 1.2.0 / Skill 1.0.7；核对其 LIFE-INGRESS-001—005 和 Topic 合同 | 无 | 依赖来源唯一；本项目新增一组 0.0.3 集成冒烟验证。**不再实施框架改造** |
| F1 Workspace & API | Facade/DTO/Bundle/配置/Factory 状态所有者、分页 Store/授权契约 | F0 | `cargo check`、注册/启用/禁用/共享 Hub E2E；同 Factory 重建不更换 MemoryStore |
| F2 Core Protocol | TCP Listener、Codec、Connect、KeepAlive、QoS0、Pub/Sub | F1 | 真实客户端互通、异常帧与资源限额 |
| F3 QoS1 & Session | QoS1、Retained、Will、Clean Session、统一可靠积压账本/预留/原子接纳 | F2 | 网络故障注入、Session FSM E2E、memory-generation-recovery、reliable-admission-saturation |
| F4 EventHub Integration | Inbound DTO、Outbound Control Request、来源防环 | F2/F3 | 多业务 Module/外部 Client 双向集成 |
| F5 Extensions | Auth、Persistent Storage、Admin、Metrics；复用 F3 的 MemoryStore 语义 | F3/F4 | 独立选装；session-owner-isolation、connect-will-authorization、offline-subscription-recovery 及提交故障恢复 |
| F6 Full 3.1.1 | QoS2 FSM、重连/异常/顺序/一致性 | F3/F5 | MQTT 3.1.1 完整符合性矩阵通过 |
| F7 MQTT 5.0 | v5 properties/Reason Codes/Shared Subscription 等 | F6 | MQTT5 独立矩阵通过 |
| F8 发布准备 | README、Rustdoc、Skill、兼容矩阵、安全与性能报告 | 每阶段持续 | 全部 MUST Contract 有执行证据 |

可以发布带有明确 `experimental`/`subset` 标识的研发制品，但在 F6 之前不得称作“完整 MQTT 3.1.1 Broker”，在 F7 之前不得称作“完整 MQTT 5.0 Broker”。

## 13.1 0.0.3 依赖门禁与最小验收顺序

**F0 已完成的是上游框架能力，不是本库测试已通过。** `modorch-mqtt` 的第一条 PR 应先建立下列真实可执行用例：

1. 在同一 Application 中注册 Business Module 与 `MqttBrokerModule`，二者的 Context 取得同一 Hub 并在该 Hub 上成功通信；不调用 `EventHub::new()`。
2. 使用当前普通 `Module::activate_ingress` 监听真实 TCP Socket，证明所有相关 `ready()` 已执行；启动失败可清理部分绑定端口。
3. 在下一个 Module 激活时故意失败，证明已启动的 MQTT Listener 被取消、端口释放、`StartupError` 保留 `ActivateIngress` 根因；随后重试时不存在两个 Listener。
4. 强制触发 `TaskScheduler` 配额拒绝和连接任务取消，证明无未跟踪 Client Task、无新连接继续接入。
5. 通过独立 Storage Module 的定向 Request 模拟恢复，并在 Listener 启用前完成；失败/超时保持端口不可连接。
6. 启用 `FailurePolicy::Restart` 故障注入，证明旧 `connection_epoch` 无法使用仍有效的 Application Hub 重放入站消息。
7. 以最小状态对象验证 Core Factory 多次 `create` 克隆同一 MemoryStore，旧代际 teardown/激活失败补偿不清空它；另一个 Application 的状态独立。此项只验证状态所有权，§12.2 的真实 MQTT Session/交付用例仍在 F3/F5 完成。

这些通过后才进入 F2 协议实现。上游 0.0.3 的 10 个真实 TCP 框架回归可以作为依赖依据，但不能替代本库自己的 MQTT 网络/存储/端到端用例。任何验收报告写明基线 `336ba201...`、Modorch 0.0.3 Contract 1.2.0、本库版本和所用实际可复现命令/日志。

## 14. Agent / 开发者文档与 Skill

为了让其他服务器和 Agent 安全集成，至少交付：

- `README.md`：定位、安装依赖、单 Application/HUB 约束、最简示例、支持矩阵。
- `docs/getting-started.md`：已使用 Modorch-RS 与普通 Tokio 两种路径，明确前者不新建 Application。
- `docs/architecture.md`：控制面/数据面、订阅索引、EventHub Adapter、防回环。
- `docs/api.md`：Bundle、DTO、EventHub 控制 Topic、超时/回执、错误码。
- `docs/configuration.md`：全部字段、缺省、安全默认、Feature 与 Module 启用区别。
- `docs/reliability.md`：QoS/Session/Retained/Will、存储边界、ACK 与幂等模型。
- `docs/operations.md`：日志、指标、资源预算、故障恢复、优雅停机。
- `contracts/`：条款 ID、级别、源码路径、测试 ID、证据、已实现/未实现状态。
- `.agents/skills/modorch-mqtt-development/SKILL.md`：使用场景、安装/兼容预检、模块注册模式、常见错误和验证命令；**使用 Modorch-RS 0.0.3 对应 Skill Bundle 1.0.7**，不得沿用 0.0.2 指纹。

文档与 Skill 必须标明 Modorch-RS 依赖版本及其指纹；公共 API 或契约变更时同步更新兼容记录和示例。**不允许只更新设计文档就声明功能通过。**

## 15. 规范性验收契约（建议编号）

| ID | MUST 条款 | 证据 |
|---|---|---|
| MQTT-ARCH-001 | 已有 Modorch Host 只使用现有 Application、EventHub 和 Scheduler | shared-hub-integration |
| MQTT-ARCH-002 | Core 与扩展是可选择性启用的标准 Module，Core 不依赖具体实现 crate | bundle-dependency-matrix |
| MQTT-LIFE-001 | 全体相关 `ready()` 通过后才执行 `listen`；全部入口激活成功后 App 才为 Ready；Shutdown/Restart 无孤儿 Task | ingress-lifecycle-e2e |
| MQTT-TOPIC-001 | Topic Name/Filter 与 OASIS 相符，合法 Topic 超 Hub 本地预算不影响 Client→Client | topic-and-budget-vectors |
| MQTT-ROUTE-001 | Client Session 订阅按客户端维护，不复用组件级去重 | multi-session-overlap |
| MQTT-EVENT-001 | inbound 可观察，outbound 必须显式请求；无出网回环 | eventhub-interop-loop |
| MQTT-QOS-001 | QoS0/1 拥有权、ACK、重传、Packet ID 与顺序符合所声明范围 | qos1-fsm + fault-injection |
| MQTT-QOS-002 | 只有 QoS2 完整通过才能声明完整 MQTT 3.1.1 | qos2-compliance |
| MQTT-STATE-001 | 内存与持久模式恢复保证明确且可测 | session-restart-crash |
| MQTT-SEC-001 | 非授权连接/Topic 被拒绝，内部控制命名空间不可经网络伪造 | auth-acl-security |
| MQTT-BOUND-001 | 所有网络/消息/存储路径有界，慢消费者无无限内存增长 | saturation-fault-injection |
| MQTT-DOC-001 | 源码、Rustdoc、Contracts、Skill、Feature/能力矩阵一致 | release-validation-report |
| MQTT-LIFE-002 | 活动入口随后发生激活失败，Broker 必须取消任务、释放端口、返回 `ActivateIngress` 根因并按 0.0.3 规则可重试 | ingress-partial-failure + fault-injection |
| MQTT-LIFE-003 | 重建后旧受管 Task/Service 失效；长期 EventHub 句柄不代表 Broker 连接授权，旧连接 epoch 绝不可发送 | ingress-generation-fence |
| MQTT-STATE-002 | 启用 Storage Module 后，在 `activate_ingress` 完成恢复后才 listen；Request 未就绪/超时/不确定状态不可开放 | prelisten-recovery-e2e |
| MQTT-COMPAT-001 | 基线为 Modorch-RS 0.0.3 精确来源，MSRV >=1.85；新库所有目标 crate 解析同一个 Modorch 依赖 | cargo-source-lock + msrv-build |
| MQTT-SEC-002 | Auth 身份须先通过 ClientID 使用授权及 Core owner 校验才能恢复、接管或清除 Session；跨身份拒绝不修改旧状态，恢复订阅须重新授权 | session-owner-isolation |
| MQTT-SEC-003 | Will 在 CONNECT 接受前通过 Topic/保留命名空间、QoS/Retain 和发布 ACL 检查并预留资源；拒绝不安装，已授权触发经统一发布路径且可恢复 | connect-will-authorization |
| MQTT-STATE-003 | 同一有界快照枚举全部 Session/离线订阅、Retained、QoS、Will 和接纳记录，校验写入 epoch/配额后才开放入口；订阅者重连前即可为其路由 | offline-subscription-recovery |
| MQTT-STATE-004 | 同一 Core Factory 的 MemoryStore 跨监督重建及补偿保留，teardown 不清空已接纳状态；不同 Application 独立，新进程/新 Application 不享有内存恢复保证 | memory-generation-recovery |
| MQTT-BOUND-002 | 可靠发布先对全部目标预留条数/字节/全局预算，再原子提交；容量不足整次拒绝且不发成功 ACK，无部分扇出和已接纳消息淘汰；不确定重试保留幂等目标集合 | reliable-admission-saturation |

## 16. 开发与持续验证命令（拟议）

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
cargo run -p existing-modorch-server --locked
cargo run -p standalone-broker --locked
# Host 与 Broker 工程使用同一 git rev/path 来源的 modorch 0.0.3；
# 必须额外运行真正的 TCP/MQTT 客户端、协议符合性、故障注入与恢复测试。
```

另需专门执行协议兼容测试、断线重连、持久化崩溃注入、真实 Client 双向收发，以及基于固定环境的性能基线。文档中代码片段为**拟议接口/伪实现**，不能代替可编译的项目与真实测试记录。

## 17. 总体完成定义（DoD）与暂不承诺事项

完成定义：Host 通过 Cargo 引入、单个 `ApplicationBuilder` 注册并显式选择 Core，Broker 使用**宿主同一 EventHub**，真实 MQTT Client 可完成已声明的协议功能；网络入口经统一 Ready 屏障启动；协议状态机、背压与停机经过故障注入；各扩展可独立按需启用；所有 Contract 与测试证据一一对应。

在做到之前，不声明支持：跨进程可靠 EventHub、默认所有业务 Event 自动出网、所有 MQTT 协议级 QoS 完整支持、未经验证的第三方后端、生产级连接容量、无条件安全强制杀停或跨业务系统 exactly-once。

---

**实施前最终复核重点（Modorch-RS 0.0.3）**：

1. **无需对 Modorch-RS 再实施 `Module::activate_ingress` 改造**。将依赖锁在 0.0.3 commit `336ba20196044e1b81fb2ca297a18b5e258a5800`，首先验证本库 Module 在宿主共享 EventHub 上的实际运行。
2. 确认 Core 只注册到现有 Application，不另建 EventHub；用真实集成测试验证。
3. 明确 EventHub 的观测失败不会决定 MQTT PUBACK，且 EventHub 事件不会未经授权自动发出网络。
4. 先实现可测的 QoS0/1 子集，再完成 QoS2 以满足完整 MQTT 3.1.1；v5.0 单独演进。
5. 对 Auth/Storage Module 的超时、背压、错误与数据持久边界制定独立契约，不滥用 EventHub Request 的返回作为磁盘持久化证明。
6. 用 §12.2 的五组新增用例落实会话归属、CONNECT Will 授权、离线分页恢复、MemoryStore 跨代际保留与可靠发布饱和接纳；设计修订不替代这些实现和执行证据。

**说明**：本设计于 2026-11-06 依据 Modorch-RS **0.0.3 Candidate** / `master` commit `336ba20196044e1b81fb2ca297a18b5e258a5800` 核对，并于 2026-11-07 根据五项评审意见修订，框架基线不变。后续同日按用户确认的 F0–F3 范围开展 QoS0/1 内存实验实现；本库执行证据单独维护在本轮验证记录中，不继承上游 Verified 状态，也不代表完整 MQTT 3.1.1 或后续 Auth/Storage/MQTT5 已验收。
