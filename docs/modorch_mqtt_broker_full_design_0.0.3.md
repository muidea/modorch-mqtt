# modorch-mqtt：基于 Modorch-RS 的可嵌入式 MQTT Broker 库完整架构与实现设计

> 归档说明（2026-11-09）：后续章节保留历轮实现与修复的设计说明；过程验证文件已按用户要求清理，历史链接统一指向[最终结论摘要](validation/README.md)。文中各轮计数和“当时保留材料”等叙述对应历史执行时点，不代表原始材料仍在仓库或当前输入已获批准。当前待办以[待收口计划](f6-f7-closure-plan.md)为准。

> 当前功能实施状态以[§30](#30-功能路径收口2026-11-08)为准；历史章节保留当轮验证边界。

> 文档状态：**完整目标设计，已部分实施；当前实现范围见 §18–§29**
> 日期：2026-11-07（评审修订）｜项目建议名：`modorch-mqtt`｜实现语言：Rust 2024\
> 基础框架：[`muidea/modorch-rs`](https://github.com/muidea/modorch-rs)，**0.0.3 Candidate**；本次核对 `master` commit [`336ba20196044e1b81fb2ca297a18b5e258a5800`](https://github.com/muidea/modorch-rs/commit/336ba20196044e1b81fb2ca297a18b5e258a5800)  
> 框架契约：Contract **1.2.0**；Skill **1.0.7**；Rust **>=1.85 / Edition 2024**  
> 前置框架增强：**已由 Modorch-RS 0.0.3 实现并在其仓库的本地验收范围内验证**，不再阻断本库开发  
> 文档定位：针对 0.0.3 的 **实现设计/工作分解**，不是 `modorch-mqtt` 已经实现或通过测试的声明  
> 当前实施进度：`0.1.0-alpha.1` 已实现 F0–F3 的 QoS0/1 内存实验子集、F4 EventHub 集成及 F5 安全/运维和 SQLite 持久化实验子集，并补齐 F6 双向 QoS2及部分规范场景（具体化见 §18–§23）；实际能力以 [README](../README.md)、[当前契约](../contracts/README.md)及[最终阶段记录](validation/README.md#implementation)为准。Core TOML配置、逐义务发布门禁及出站故障回归见§27–§29；完整符合性和后续阶段仍为目标。
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
                                │ MQTT TCP（可选 TLS）
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

注意：`MqttBrokerBundle`、`MqttBrokerConfig`、`module_ids()` 和 `loopback_development()` 已在 F0–F3 实验实现中提供；`MyBusinessFactory` 是宿主自己的组件；`tokio::signal` 需由宿主启用对应 Tokio Feature。可执行宿主示例位于 `examples/existing-modorch-server`；Auth/TLS/Admin/Metrics 的实际装配见 `examples/secure-modorch-server`；Storage 等仍待实现。

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

Core TOML接入已实现，API与严格字段表见[toml.md](toml.md)，当前实施边界见§27。宿主有界读取独立配置，调用Facade的 `MqttTomlConfig::parse`，注册引用的Provider，再显式选择返回的组件ID；Bundle仍负责Core强校验。下面为匿名回环配置；Provider自己的账号/ACL、数据库/磁盘预算、Admin/Metrics策略仍属于宿主配置：

```toml
[mqtt.core]
enabled = true
name = "mqtt-broker"
protocols = ["3.1.1"]        # 已实现的实验协议；完整符合性仍Partial
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
reserved_internal_prefix = "$modorch/"
session_owner_policy = "same_principal"      # 启用 Auth 时强制；匿名模式的限制见 §7.3.1

[mqtt.eventhub]
inbound_mode = "best_effort"
inbound_topic_mode = "original"
control_prefix = "$modorch/mqtt/control/"
allow_implicit_outbound = false

[mqtt.auth]
enabled = false

[mqtt.tls]
enabled = false

[mqtt.storage]
enabled = false
mode = "memory"
# 启用时 mode="external"、component 指向宿主注册的Provider，另行注入capability。
# max_snapshot_bytes / recovery_page_bytes / timeout_ms / recovery_timeout_ms 为Core参数。
# 数据库路径、disk_budget_bytes及Provider订阅ID由宿主配置并强校验。

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

**说明**：本设计于 2026-11-06 依据 Modorch-RS **0.0.3 Candidate** / `master` commit `336ba20196044e1b81fb2ca297a18b5e258a5800` 核对，并于 2026-11-07 根据五项评审意见修订，框架基线不变。后续同日按用户确认的 F0–F3 范围开展 QoS0/1 内存实验实现；本库执行证据单独维护在本轮验证记录中，不继承上游 Verified 状态，也不代表完整 MQTT 3.1.1 或完整 F5 Storage/MQTT5 已验收。


## 18. F4 实验实现进度（2026-11-07）

在 F0–F3 QoS0/1 内存子集上，已实现同宿主 EventHub 的入站观察、显式出站 Request、MemoryOnly 业务回执及来源隔离的幂等与查询。本节记录 F4 时点实现对 §5–§6 的具体化；后续持久化行为以 §20 为准，不代表完整设计或生产发布通过。

- `MqttInbound` 仅由网络 PUBLISH 成功接纳后产生，携带 broker、generation、connection_id、可信连接 ClientID 和原始报文元数据；原 MQTT Topic / Broadcast / `external()`。独立 best effort，失败/部分投递计数，业务执行不参与 PUBACK。Will 与内部发布不生成此类网络事件。已进入 Hub 的旧代际观察不会因重建撤回。
- `$modorch/mqtt/control/publish` 只执行直接寻址 Core 的类型化 Request。来源取框架 `MessageEnvelope.source`；当前框架只有 Handler 子调用带此来源。无来源、任意 Header、普通 Event/Command 不能发布。生命周期钩子及受管后台任务可先向宿主业务 Handler 提交，再由 Handler 明确发起 Core Request。该规则是内部来源隔离，尚非 Auth ACL。
- `MqttPublishResult = Result<MqttAcceptReceipt, MqttPublishRejection>`，成功仅 `MemoryOnly`；不提供未实现的 DurableCommitted 成功值。Core 校验 request_id、Topic、保留命名空间、完整 MQTT 包长度、QoS0/1、活跃代际，复用网络发布相同的原子路由/Retained/背压路径。
- 幂等键为 `(source ComponentRef, request_id)`；完整内容相同返回原结果，不重匹配、不重投递；不同内容返回 IdConflict。账本完成预留后的路由成功/拒绝终态均保留；前置校验或账本预留失败不创建记录。已提交目标集合由原交付状态承载，重复请求不重新解析订阅。
- 通过 `$modorch/mqtt/control/publish/query` 的 `MqttPublishQuery` 查询同源终态，`None` 不否定并发排队的原请求。Indeterminate 不能解释为回滚；同源同 ID 同内容重试受账本保护。
- `max_publish_records` 默认 10000，记录计入全局内存、跨 Factory 内的 Core 重建保留且不淘汰。满额拒绝新 ID，旧记录仍可读；无 TTL/清空接口，新 Factory/进程丢失记录。此保守实验策略后续可由明确的持久 epoch/保留合同替代。
- Modorch 订阅 ID 是 Application 全局唯一；宿主通过 `control_subscription_ids`（默认 `[1,2,3]`）统一分配，多个 Broker 使用不同 ID。控制 Topic 相同仍用 Destination 选择具体 Broker。

可运行示例为 `existing-modorch-server`，接入说明见 [eventhub.md](eventhub.md)，历史验证结论见 [验证结论摘要](validation/README.md#implementation)。本节证据记录 F4 时点；后续安全/运维具体化见 §19，Storage 见 §20；可选 Bridge、QoS2、MQTT5 及生产容量门禁仍待开发/验收。


## 19. F5 安全与运维子集（2026-11-07）

本节记录安全/运维子集的历史实施时点：Auth、Admin、Metrics 为独立可选 Module，并提供 TLS 与按 IP 的握手准入。当时状态与幂等限同 Factory 的内存生命周期；后续已增加 §20 的 SQLite Storage 与 DurableCommitted。**F5 仍未整体完成生产验收**，历史验证记录不追溯扩大范围。

- Auth 的类型化 Request 契约位于 API crate，Core 仅使用定向 EventHub Adapter；密码 Provider 使用宿主 CPU Scheduler 验证有成本上限的 Argon2id PHC。失败、满额、超时不回退匿名；账号表与策略不可变，权限默认拒绝。
- Principal 使用稳定域/主体键并带期限及策略版本；Core 强制 owner 相等，ClientID 使用策略、恢复订阅重新鉴权和 Session 版本 CAS 在接管前完成。未授权 Clean Session 不清除他人状态。
- Will 在成功 CONNACK/接管前取得参数及策略绑定的 Grant 并保存；Core 独立拒绝保留命名空间。触发按已保存授权执行，策略热更新/撤销仍未提供。
- TLS 在受管连接任务中执行，与 MQTT/Auth 共用绝对握手期限，使用显式 ring Provider、禁用 early data；非回环地址要求 Auth + TLS。连接/任务/内存/IP 桶先于握手准入，失败只关闭当前连接。
- Admin 为显式启用的来源受限 Request 代理，支持有界分页的 Session/订阅查询、统计和按 expected_connection_id 断连；默认业务来源白名单为空，不创建 HTTP 入口。审计为有界内存元数据环，不是持久审计。
- Metrics 用一个受管周期任务拉取状态，提供采样时间/失败计数和固定名字 Prometheus 文本，无 ClientID/Topic 标签，无默认 HTTP exporter。计数保留于 Factory，未采用逐消息 Hub 广播。
- `scripts/benchmark.py` 提供1/10/100/500客户端 × QoS0/1 的 release loopback 基线，记录环境/配置/吞吐/PUB→SUB百分位及采样RSS；TLS/Auth/持久化性能和生产容量门禁仍待验证。

真实接口与装配见 [安全与运维接入](security-and-operations.md)，历史执行结论及范围见 [验证结论摘要](validation/README.md#implementation)。完整需求状态如下，只有已列出的子集具有当前执行证据。

| 阶段/需求 | 当前状态 | 剩余工作 |
|---|---|---|
| F0–F3 基础/协议/内存QoS0/1 | 实验子集已实现 | 不含完整3.1.1符合性 |
| F4 EventHub | 已实现实验契约 | 跨进程可靠桥接不在当前承诺内 |
| F5 Auth/TLS | 已实现本节范围 | 动态策略/证书更新、更多Provider为后续能力 |
| F5 Admin/Metrics | 已实现内部接口 | 持久审计、宿主HTTP管理/指标出口未提供 |
| F5 Storage | 已实现 §20 的实验子集 | FULL SQLite 全量 image、writer fencing/CAS、提交查询、预监听字节分页恢复及 SIGKILL/磁盘满测试；真实断电、生产性能和跨平台待验收 |
| F6 完整 MQTT3.1.1 | 双向QoS2已实现 §21 范围；完整矩阵待验收 | 重复报文、重连阶段、持久化/故障、独立客户端已有证据；完整3.1.1符合性仍未完成 |
| F7 MQTT5 | 待开发 | Properties/Reason Codes/Shared Subscription等及独立矩阵 |
| 可选网络与桥接 | 待开发 | WebSocket、Bridge及防环/重连/交付边界 |
| F8 发布准备 | 部分完成 | 生产负载与恢复性能、跨平台、独立Agent/完整MUST发布门禁及兼容矩阵 |


## 20. F5 SQLite Storage 实验实现（2026-11-07）

本轮具体化 §8.2/§8.2.1 和 MQTT-STATE-002/003 的 QoS0/1 子集。Facade 新增真实 `storage` Feature，Provider 为独立 `modorch-mqtt-storage` Module，Core 只依赖 API DTO，不创建第二 Hub、不直接访问 SQLite。完整装配和运行限制见 [storage.md](storage.md)，可运行示例为 [durable-broker](../examples/durable-broker/src/main.rs)。

### 20.1 选定的事务与恢复形式

- 采用 SQLite `synchronous=FULL`、DELETE rollback journal 和独占连接；本次未选 WAL。一个数据库只保存一个 Broker 的受限完整 JSON image，包含所有会话/订阅、owner、epoch/revision、Pending/Inflight/Packet ID/ever_sent、Retained、Will 授权/冻结目标、同源业务接纳终态和配额账本。无 QoS2 FSM，不把它视为完整协议快照。
- Store 使用 `$modorch/mqtt/storage/v1` 的定向 Request，API 为 Begin/Page/Complete/Abort/Commit/Query。Core 负责加载、路由、删除和清理，再将结果作为全量 image 原子提交；不逐个实现 §6.2 拟议 Trait 的增量读写方法。
- Commit 带稳定 operation_id、writer epoch、整个 image 的 expected_version CAS 及 SHA256。整个 image 事务比逐 Session CAS 更粗，所有 Session epoch/revision 一起受保护。回执为选定 FULL policy 下已提交；Hub Admission 或 Request 执行成功本身不等于磁盘保证。
- Provider 仅保留最新 Store 操作回执；Core 串行提交并先消除不确定再发下一操作，旧操作不能越过 CAS 重复提交。业务发布幂等账本另随 image 完整持久保存且有界不淘汰，跨新 Application 仍返回原结果、不再次路由。
- Begin 推进 epoch、冻结 image 并关闭写入；重复未完成的同一 recovery_id 幂等。Page 采用**字节 offset 和字节上限**，允许业务记录跨页，而不是 §8.2.1 拟议的业务记录 cursor；总 image 有界，所以不另设每页条目数。页校验 epoch/version/offset/进度/done/总长度/SHA256，随后核对配额及引用。恢复所有离线订阅后处理失去 TCP 的 Clean Session/Will，normalization 事务确认后才 listen。
- Complete 同 epoch 幂等，Abort 不删除原 image、不重开旧 writer；迟到 Complete 不能复活已 Abort 的 epoch。恢复期间控制发布返回 Inactive，失败/预算不足保持无监听，不丢弃数据迁就新配置。

### 20.2 来源、提交屏障与资源

Modorch 生命周期/网络根没有 Handler source，宿主生成随机32字节 capability，只共享给 Core 与选定 Provider；所有 Store 请求均校验，Debug 脱敏且不保存到 image。它不是任意进程内代码的安全隔离，业务模块不得获得此能力。

Core 的异步全局 gate 串行协调状态变更、下行选择、Will 和内部发布；同步 State mutex 不跨 await。编码/摘要走宿主 CPU Scheduler，SQL 走 blocking-I/O Scheduler，等待实际任务退出。先确认相关存储提交，再发送 CONNACK/SUBACK/PUBACK、下行或网络观察；Packet ID/ever_sent 先保存再写，重启以原 ID/DUP 重发。QoS0 队列仍为 best effort，不能从 DurableCommitted 推断其可恢复。

Commit 超时/错误先 Query 同 ID/digest；Unknown 才重发同操作，仍不确定则关闭本代入口/连接并由监督恢复。成功内部回执为 DurableCommitted；未配置 Storage 时仍为 MemoryOnly。全量快照上限1KiB..64MiB，恢复页1..1MiB，预留四倍快照逻辑内存；连同现存状态/连接必须满足全局预算和 Hub 字节准入，不自动扩大额度。

磁盘额度至少 `3 * (max_snapshot_bytes + 1MiB)`，Provider 校验4096字节页，按三倍主库页限加1MiB余量设置 `max_page_count`，涵盖主库与 rollback journal；不引入无界 WAL。拒绝内存/URI数据库、不相关数据库及非标准页。额度不预分配磁盘、不涵盖外部备份和文件系统快照，仍须处理文件系统先满。生产容量与全量序列化/提交吞吐尚无验收结论。

### 20.3 停止边界与验收状态

框架在停止阶段会取消新调度的 CPU/I/O 任务，不能在 quiesce/teardown 新建关键 durable promise。Core 关闭入口、等待已有连接退出，仅做本地收尾；磁盘中已提交的 armed Will 在下次启动时触发并在监听前落盘。因此 broker 停止导致的 Will **允许延迟到重启**，不是关闭时保证立即持久发布。运行期正常 DISCONNECT 在事务中持久清除 Will。Provider 等待现有 SQL job 结束再释放空闲 DELETE-journal 连接，不在 teardown 后 checkpoint/提交。

新增固定指标为存储提交成功/失败数、耗时微秒总和和最大值，计时从获得 gate 到确定结果，不包括排队，也不是纯 fsync 延迟。Admin 审计仍限内存。

[验证结论摘要](validation/README.md#implementation)覆盖离线订阅在重连前恢复、Retained、Packet ID/DUP、持久 owner/Will Grant/幂等、回执延迟/丢失查询、失败无 ACK/下行、恢复错误/预算不足不开端口、真实 SQLite 页限 DiskFull、独占库与真实子进程 SIGKILL。SIGKILL 不是硬件断电或任意文件系统验证；历史内存性能基线不适用于存储模式。

当前仍缺 F6 QoS2/完整3.1.1符合性、F7 MQTT5、WebSocket/Bridge、生产性能与故障恢复SLO、真实断电/跨平台及独立 Agent/发布门禁。完整需求状态为 Partial，不声明全部 MUST 或完整 F5 已验收。


## 21. F6 双向 QoS2 实验实现（2026-11-07）

本轮在 F5 Storage 基础上补齐 QoS2 的 PUBLISH/PUBREC/PUBREL/PUBCOMP 状态机。§18–§20 保留各阶段实施时点；其中“QoS2待开发”“快照无QoS2”由本节的当前范围替代。**完整3.1.1符合性和生产发布仍为 Partial**，不能仅以增加 QoS2 或测试数量推断全部 MQTT MUST 已通过。

### 21.1 接纳与状态机

接收端采用 MQTT3.1.1 §4.3.3 Method B：第一次 PUBLISH 在同一事务中预留接收状态及可靠目标/Retained，路由并记录 AwaitPubRel 后才 PUBREC。等待期间同 ID 的 QoS2 PUBLISH 忽略内容，只重发 PUBREC，不再次路由/修改Retained/观察。PUBREL 清除记录后回 PUBCOMP，重复/未知 PUBREL 仍回复 PUBCOMP；完成后可以复用该 ID。业务幂等 request_id 的异内容冲突语义不用于此处。

发送端可靠队列增加 Release 阶段；PUBREC 推进并发出固定 flags=0010 的 PUBREL，不释放交付或 ID。重复 PUBREC 重发 PUBREL，正确阶段的 PUBCOMP 才释放。持久会话重连在原 Publish 阶段重发原 ID/DUP PUBLISH，在 Release 阶段只重发 PUBREL。错误 PUBACK、过早 PUBCOMP 和未知 ACK 不删除其他消息；上下行 ID 空间独立。PUBREL 控制帧优先收尾，避免可靠窗口已满时等待循环。

网络订阅、内部发布、Retained回放和Will允许QoS2；ACL按原发布QoS校验，恢复/接管继续执行owner限制。SUBACK失败由显式无授权条目标记，不再把QoS2当作失败哨兵值。MqttInbound对第一次网络接纳生成一次best-effort观察，不承诺业务处理exactly-once。

### 21.2 资源、持久化与兼容

新增 `max_inbound_qos2_per_client`（默认128，1..65535）和 `max_inbound_qos2_bytes_per_client`（默认16MiB）；每条接收记录按 `256 + Topic字节 + payload字节` 独立计费，另受全局预算。满额在路由/PUBREC前拒绝，不淘汰已接纳记录；重传已有ID不新增记录。下行QoS1/2仍共用可靠队列额度，QoS2到PUBCOMP才释放。状态/固定名字指标增加接收端交换条数和字节，Metrics共36条。

持久模式所有ACK/阶段推进遵守既有确认屏障，四类新状态写入分别有延迟/失败注入测试。快照format2新增接收端账本/配额及发送Release阶段；读取旧format1并在listen前升级，旧版拒绝format2，不支持直接降级。非法phase、ID、配额和缩小预算失败时不重置数据库。

### 21.3 当前验收与剩余项

历史验证结论见 [验证结论摘要](validation/README.md#protocol)，接入细节见 [qos2.md](qos2.md)。覆盖双向重复报文、ID复用/占用、错误ACK、条数/字节限额、Will/Retained、顺序与窗口、同Factory监督重建、跨Application恢复、四阶段存储屏障/失败、旧快照升级、真实SIGKILL，以及独立rumqttc和Paho TCP/TLS/密码ACL。

F6尚余完整MQTT3.1.1规范性符合性矩阵及更广协议互操作/故障验证；F7 MQTT5、WebSocket、Bridge、生产吞吐/恢复SLO、真实硬件断电、跨平台和独立Agent/发布门禁仍未完成。SQLite停止时的Will延迟恢复策略沿用§20，不扩大为停机立即持久发布。

## 22. F6 规范符合性推进与顺序修复（2026-11-07）

§18–§21 为各实施阶段记录；当前快照和验证状态由本节补充。完整设计/发布仍为 Partial。

- 修复 MQTT-4.6.0-4：每个有效 PUBREC（包括重复）生成一个 PUBREL，持久提交后按到达顺序进入连接的有界控制 FIFO；写端背压不再按原 PUBLISH 队列重排或合并回复。控制队列满时暂停输入。未确认 PUBLISH 的相对顺序与原 Packet ID/DUP 重发语义保持。
- format3 沿用 format2 image 字段，以可靠队列中 Release 的相对位置保存每个未完成 ID 最近一次 PUBREL 顺序。恢复在监听前确认 normalization；旧 format1 可升级，旧 format2 每持久 Session 至多一个 Release 可升级。多个 Release 缺失历史顺序时返回 `legacy QoS2 release order`，不监听、不重置数据库；需旧版先排空相关交换再升级。旧二进制不能读取 format3。
- `max_client_id_bytes` 验证改为23..65535，保证配置不排除 MQTT-3.1.3-5 必须支持的1..23字节字母数字ID。低于23的历史配置需修正。
- 新增连接拒绝后的流水数据隔离、malformed CONNECT 无 CONNACK、首包/二次CONNECT、非法固定标志/ID/UTF8/订阅负载的连接隔离、订阅替换重放Retained/精确取消订阅、入站PUBACK/PUBREC和含重复PUBREC的PUBREL顺序、BOM/Unicode不归一化，以及SQLite逆序确认重启恢复用例。
- [受控规范清单](../contracts/mqtt311.json)记录官方URL/SHA256及140个编号，13组场景关联62个编号，其余78个仍NotRun。编号清单不含未编号规范性要求的完整审计；关联测试只产生限定场景的 `TargetedEvidence`，不能据此声明完整条款或完整MQTT3.1.1符合性通过。
- `scripts/check.sh` 接入实际测试日志校验；缺失/ignored/失败不算通过，单独测试总数不算证据。说明见[符合性范围](mqtt311-conformance.md)，历史结论及限制见[验证结论摘要](validation/README.md#protocol)。

剩余：完整3.1.1规范矩阵（含未编号要求及Server/Client/可选传输适用性审计）、更广互操作/异常链路；F7 MQTT5；可选WebSocket/Bridge；生产吞吐/恢复SLO、真实断电、跨平台和独立Agent/发布门禁。SQLite停机Will延迟恢复边界不变。

## 23. F6 发布/订阅和会话协议收口（2026-11-07）

本节补充§22，完整设计/发布仍为Partial。最新执行见[验证结论摘要](validation/README.md#protocol)，用例与限定范围见[规范清单](../contracts/mqtt311.json)。

- CONNECT错误分类不再把所有未知错误兜底为Identifier rejected(2)。仅明确ClientID拒绝使用2；容量/存储不可用使用3；授权拒绝使用5；不匹配标准原因码的错误关闭且不发CONNACK。网络覆盖协议名、ID、Will授权及Session满；线帧分类测试覆盖存储和未知错误。
- 新增九种发布/订阅QoS组合、二进制载荷与独立DUP标志、QoS0无ACK/ID；混合SUBACK输入顺序、原ID及重复过滤器按序替换；Retained的QoS2→0→1替换、live/replay标志、非Retain不改旧值和空Retain删除。
- 补齐UNSUB后已发送QoS1/2继续完成、新消息停止路由；CleanSession接管清理订阅/双向QoS2但保留Retained；离线持久会话为新QoS1/2入队并重连按序交付。
- 实际TCP覆盖16组Topic匹配、65535字节多字节UTF8 Topic和Filter、非法UTF8的各CONNECT/过滤器字段、密码/Will二进制载荷、Will六种QoS/retain组合，以及非法多过滤器报文不部分变更订阅。强制PacketID65535回绕跳过未完成QoS1/2，正确完成才复用；双向同时使用ID1互不干扰。
- 受控清单现为31组场景、128个关联编号。未映射12个编号仍NotRun：7项客户端职责、4项可选WebSocket、1项条件性Bridge，各有适用性依据；门禁验证集合完整且类别有效。适用性不代表执行通过，128个编号也只标TargetedEvidence。

尚余未编号规范性要求、表格/状态机的完整审计和更广故障/互操作验证，不能仅凭上述轨迹将完整3.1.1或F6标为Passed。F7 MQTT5、可选WebSocket/Bridge、生产吞吐/恢复SLO、真实断电、跨平台和独立Agent发布门禁尚未完成。format3升级和停机Will延迟恢复边界沿用§22/§20。


## 24. 共享Topic向量与F8开发交付收口（2026-11-07）

本轮不扩大协议版本或生产支持声明。最新历史验证结论见[验证结论摘要](validation/README.md#verification)，持续剩余项见[需求清单](remaining-work.md)。

- `contracts/topic-vectors.json`保存固定Modorch-RS rev的39组独立预期，注明上游Rust来源及SHA256；上游没有JSON fixture。Core校验/匹配、Facade helper、真实Hub订阅/Handler及真实MQTT TCP共用此文件。magicCommon端尚无消费执行证据，MQTT5的共享订阅解析仍待开发。
- F6矩阵增至32组限定场景，128个编号仍仅为TargetedEvidence；未编号义务、表格/FSM完整审计与更广异常链路尚未完成。新命令 `check-conformance.py --require-complete` 在Partial时返回非零，不因限定场景通过而批准完整F6。
- 补齐§14的getting-started/architecture/api/operations文档与项目专用开发Skill；项目正式启用Modorch应用/模块Skill1.0.7，并完成当前0.0.3依赖源码指纹预检。MQTT Skill经skill-hub验证并归档本地技能库，未推送远端。
- `check-project.py`和check.sh验证必需文档、Skill/CI入口及本地链接；Linux CI定义覆盖工作区检查、MSRV、空默认Feature及Paho TCP/TLS，保存执行工件。远端CI未执行，不计为平台验收。

完整F8仍需所有MUST Contract的机器映射及独立Agent/兼容/安全/性能总门禁；F7 MQTT5、可选WebSocket/Bridge、生产负载/恢复SLO、真实断电和跨平台仍未完成。format3升级限制和停机Will延迟恢复策略保持§22/§20范围。


## 25. F6 报文表、未编号候选与发送ACK转换审计（2026-11-08）

本轮实际实现及执行见[验证结论摘要](validation/README.md#verification)，范围见[审计说明](mqtt311-audit.md)。完整设计/发布仍Partial。

- 修复非法固定头和不可能Remaining Length必须等完整包体才拒绝的问题。现在首字节即可拒绝保留类型、错误方向或非法固定flags；长度解码后按报文布局检查固定/最小值，不等待声称的包体。私有decode必须是一个完整帧；连接层仍按流提帧，支持分片与合并。
- 独立JSON报文表覆盖16种类型×16种flags、256 CONNECT flags、8个Remaining Length编码边界和所有接收端ID报文的0/1/65535。真实TCP覆盖响应、健康连接隔离、头部错误触发Will及持久Session/QoS2接收阶段保留。
- 对固定来源正文执行未编号关键词候选提取并核对9段；3段实际为MQTT-4.3.2-1续文，其余区分定义/客户端/服务端与总体符合性。排除非规范附录B重复文本。此候选提取不代表覆盖全部隐含/跨段语义。
- 审计清单单列8张规范相关表、7个非规范例表/图和完整FSM待办；QoS2发送端4状态×3ACK的12项转换用JSON独立预期验证阶段/ID/所有权和PUBREL回复。
- 编号清单现35组场景，128编号仍仅TargetedEvidence。check-conformance.py同时验证审计测试；缺失、重复、与ignored矛盾或失败日志均不能通过。总体/完整FSM项不因限定测试通过而升级，完整规范模式继续阻断Partial。

完整跨段语义、全部出站布局与CONNECT/Session/Will/订阅/QoS状态/故障/互操作总矩阵仍需继续；F7、可选Transport/Bridge、生产负载/恢复SLO、硬件断电/平台和独立Agent发布门禁保持未完成。SQLite format3迁移及停止时Will延迟恢复边界不变。


## 26. F6 后端等待期间的连接期限与安全排空（2026-11-08）

实际代码和故障记录见[验证结论摘要](validation/README.md#verification)。本节补充§25，完整设计及发布仍为Partial。

- 修复主协议循环等待Auth、存储gate/提交期间无法检查网络期限的缺口。同一个受管任务监督协议Future和期限更新，超时先关闭底层TCP/TLS并取消连接令牌，再等待已开始事务确定结果；没有增加独立后台任务、Runtime或Hub。
- CONNECT期限从任务准入前计算，包含调度排队/TLS；KeepAlive在解析完整报文时刷新，0禁用。Principal按实际有效期计时，不将长有效期截为24小时；当前整帧写入期限不因输入PING或部分写入进度而重置。
- Auth的可取消认证/授权与不可丢弃的持久事务分离。等待gate可以取消，获得事务后即使attach失败也必须完成提交，避免丢弃Transaction造成全Broker storage-poison。State额外检查连接取消令牌；detach保留仅按连接ID校验，确保关闭后仍可收尾。
- 连接许可、内存和生命周期保留到任务实际退出；慢提交回执期间网络虽已关闭，已接纳QoS2仍可持久恢复，原ID重传不重复路由。已开始的CONNECT提交超时后仍排空并登记清理；尚未获得gate的CONNECT超时则不能迟到接管。
- 故障验证覆盖Publish/Subscribe迟到授权、身份到期、TCP/TLS KeepAlive、CONNECT提交/gate等待、已连接发布等待gate、整帧写入背压及检查式停机排空。受控规范场景增至37组，编号仍为128项TargetedEvidence；新增期限FSM限定项不替代完整状态/故障总矩阵。

期限依赖正常执行器调度，不保证硬实时；等待后端时不并发解析网络输入，尚未解析的PING不能刷新期限。Will的持久收尾需等待gate/提交，不能承诺与关网同时完成；停止时Will允许延迟到重启的边界继续沿用§20。完整跨段/出站/FSM规范审计、F7 MQTT5、WebSocket/Bridge、TOML、生产负载/恢复SLO、真实断电/平台与独立Agent发布门禁仍待完成。


## 27. Core TOML配置接入与宿主显式装配（2026-11-08）

本轮实现§9的Core配置入口，证据见[验证结论摘要](validation/README.md#implementation)，字段/边界见[toml.md](toml.md)。完整设计及发布仍Partial。

- Facade新增默认关闭的 `toml` Feature及 `MqttTomlConfig`。输入限64KiB，仅接受独立 `[mqtt.*]` 文档；Core默认禁用，匿名访问必须显式确认。支持全部当前Core资源字段与毫秒期限，并复用Rust配置校验。
- Auth/Storage/Admin/Metrics区块声明宿主已注册的组件引用，返回ID仍由宿主显式选择；TLS路径基于配置目录，Core受管IO负责加载。Storage capability由宿主单独注入，不读取TOML内密钥，不自动注册/启用任何Provider或扩展。
- 固定拒绝未知字段、重复键/表、未实现协议/策略、不安全公网配置、歧义禁用参数和ID冲突。解析错误仅包含静态原因或行/字节列，不回显原文；Provider自己的账号/ACL、数据库与磁盘额度、审计/采样及出口继续由宿主管理。
- `standalone-broker --config FILE [--once]`可运行；原地址/benchmark参数保留，与文件模式互斥。示例配置、真实TCP QoS1与子进程QoS2/Retained/停止测试接入门禁，完整Auth/TLS/Storage/Admin/Metrics组合另有显式宿主装配验证。
- 同时修复§26超时监督遗漏的指标分类：排空中的连接取消不再覆盖KeepAlive/写入超时原因，继续计入timeouts/slow_writers，身份期限按Auth拒绝统计。补充持久提交超时后的计数断言。

这里交付Core TOML映射和宿主引用配置，不支持Provider内部策略文件、热更新、环境插值或include/覆盖合并。完整MQTT3.1.1审计、F7 MQTT5、WebSocket/Bridge、生产负载/恢复SLO、真实断电/平台与完整发布门禁仍未完成。format3迁移、存储确认后ACK和停止Will延迟到重启的边界不变。


## 28. 逐义务发布门禁与TLS/Auth/Storage/QoS2测量（2026-11-08）

本轮将§15的21项MUST与Core TOML配置合同映射为[机器清单](../contracts/project-must.json)，开发检查逐项核对精确测试名。固定官方来源的117段编号正文/列表项保留原文并覆盖140编号；官方正文`MQTT-3.3.1.-1`显式映射到清单`MQTT-3.3.1-1`，不改写原文。未编号续文/结构/FSM仍与既有审计共同构成审查输入。

[统一发布工具](../scripts/check-release.py)提供176项当前源码绑定的独立审查入口和真正完整通过路径：22项项目合同、128项适用规范编号、16项语义/表格/FSM、10类发布报告。UN-008总体符合性由具体义务推导，不能循环批准；12项客户端/可选WebSocket/Bridge保持范围排除。开发CI使用显式Partial模式，严格发布默认拒绝缺项。报告、日志、契约和新增/删除输入的摘要受检；评审身份与制品可信来源仍须由真实发布流程保证，摘要不是身份签名。

`secure-modorch-server --demo --database PATH`可显式组合TLS/Auth/Admin/Metrics/SQLite；Storage Factory、capability及选择由宿主负责。`benchmark.py`增加TLS/Auth与TLS/Auth/SQLite组合和双向QoS2四阶段测量，记录连接耗时、PUB→SUB分位数、完整交换吞吐、原始采样/RSS、二进制摘要及空载检查式退出时间。默认TCP测量及安全示例原有参数兼容；组合实验保留32连接/IP、4MiB image、64MiB磁盘限额。测试凭据/证书仅用于本地实验。

实测与剩余项见[验证结论摘要](validation/README.md#verification)及[清单](remaining-work.md)。短loopback结果不能批准长期RSS、慢消费者、高负载退出/恢复或生产SLO。协议完整语义/FSM评审、独立Agent、magicCommon消费、远端CI、跨平台/真实断电等缺失证据仍阻断；MQTT5和可选扩展仍未实现。


## 29. 服务端出站线帧与写中断/慢消费者故障收口（2026-11-08）

本轮补齐§12编解码/QoS/限额中缺少的出站独立预期与具体故障轨迹，生产协议/API及持久格式不变。[mqtt311-outbound.json](../contracts/mqtt311-outbound.json)包含16个控制响应、14个PUBLISH向量、12个RL边界和7种CONNACK上下文；覆盖九类已实现Server输出，期望来自规范字节布局，不经过生产Codec生成/解码。5项测试核对编码器和真实TCP线帧，含live/retained/重连DUP、错阶段ACK与PUBREL恢复。

新增一字节duplex部分PUBLISH写中断测试：本地写超时关闭连接后，QoS1/2仍保留原ID并以DUP重连，直至完整确认后才释放；QoS0重连不重发。新增真实TCP慢消费者故障：窗口1/可靠积压2在AwaitPubComp阶段满额，新QoS2 Retained发布在PUBREC前整体拒绝；健康目标无部分投递、旧Retained保留、无幽灵入站去重记录，容量释放后同ID重试成功且后续重复不再路由。

[验证结论摘要](validation/README.md#protocol)对应40组限定场景、128项编号TargetedEvidence及17项限定审计；两个新增审计入口使发布独立评审项增至178。`FSM-COMPLETE`仍NotRun，完整跨段语义/故障/互操作组合仍需实审补测。这些是有限正确性回归，不替代真实TLS/持久写中断、长期RSS、代表性慢消费者、加载退出/恢复或生产SLO；后续清单见[remaining-work](remaining-work.md)。

## 30. 功能路径收口（2026-11-08）

本节替代§29及更早验证时点中“MQTT5/WebSocket/Bridge、动态更新、持久审计/HTTP、Tokio适配未实现”的当前状态判断；历史执行证据不改写。实际接入、资源边界与失败语义见[可选功能](optional-features.md)，当前剩余项见[清单](remaining-work.md)，本轮历史结论见[验证结论摘要](validation/README.md#implementation)。

| 功能项 | 实现入口 | 功能收口 |
|---|---|---|
| 普通Tokio宿主 | Facade `bootstrap` / `MqttBrokerHost` | 明确创建一个Application或使用给定Builder；复用调用方Runtime；检查式退出及StartupError清理信息 |
| MQTT5 | Core `mqtt5` / `wire` / `mqtt5::Codec` | 属性、Reason Codes、Clean Start、Session/Message Expiry、协商窗口/大小、别名、订阅选项/标识、共享组、Will Delay、负PUBREC；沿用QoS0/1/2事务与format4恢复 |
| WebSocket/WSS | Core `websocket` / Transport | 有界升级和Binary字节流；mqtt子协议、分片/合并、控制帧、TLS组合和取消/期限 |
| 网络Bridge | `modorch-mqtt-bridge` 独立Module | 一/双向显式前缀映射、独立账号ACL、Origin/Hop过滤、受管重连、单在途及源端可靠积压 |
| 动态Auth/TLS | `PasswordAuthHandle::replace` / `BrokerHandle::reload_tls` | revision/代际校验、完整校验后原子替换、失败保留原策略/证书、现存可靠交换与TLS会话边界 |
| 持久审计 | Admin `audit` / SQLite | 操作intent先提交、结果outcome、拒绝记录、专库/排他writer、行/页上限和重启保留 |
| HTTP管理/指标 | 独立 `HttpFactory` | 回环Bearer认证、有界请求/响应、可信Admin定向转发、分页、连接fencing、Metrics出口与退出 |

Bridge 的业务侧仍要求可信 Handler 的类型化 `MqttPublishRequest`；网络 Broker 间的桥接使用独立 MQTT5 Client，不依赖 best-effort 的 MqttInbound 观察。Bridge当前采用TCP，两侧加密由宿主显式TLS代理提供。目标PUBACK后才确认源消息；跨进程崩溃可能重复，不承诺跨Broker exactly-once。

资源与生命周期补充：accept与维护拆为两个Owner受管任务，维护等待持久gate不阻塞新连接握手；等待gate时响应取消，已开始提交保持确定结果后再退出。现有任务配额测试相应覆盖两个常驻任务加连接/扩展的实际额度。MQTT5别名与冻结目标的订阅标识纳入记账预算；超客户端报文上限的下行按目标丢弃并计数。

持久格式当前为4，新增MQTT5属性、Session/订阅选项、延迟Will与时间字段；读取1/2/3后先归一化提交再开放入口，format2多Release歧义拒绝规则保留。在线Session崩溃恢复采用最后秒级快照作为失联起点，停机时间不重置过期；不能原地降级。

本节完成已列出的实验功能实现，不升级完整规范/生产发布状态。Enhanced Authentication不属于当前认证机制；完整F6/F7规范审计、生产容量/恢复SLO、magicCommon跨实现消费、独立Agent、远端CI、跨平台和硬件断电仍须补齐真实验收证据。

后续[验证结论摘要](validation/README.md#verification)确认上述实现仍有在线MQTT5会话接管、Bridge目标Session丢失后的重发、桥接属性上限兼容及幂等记录恢复预算四项功能缺陷。本节的“功能路径收口”记录主要路径已实现，不再作为全部功能正确的结论；当前修复清单见[剩余需求](remaining-work.md)。

## 31. 遗留项代码修复（2026-11-08）

按§30后续复核结果修复R-01–R-04，执行材料见[验证结论摘要](validation/README.md#closed-issues)：

- MQTT5 Session Expiry=0在实际断开后结束Session；仍在线时Clean Start=false接管保留订阅、标识和双向QoS所有权，Clean Start仍明确重建。
- Bridge目标Session丢失时重提应用持有消息；恢复时使用原客户端交换/Packet ID并设置DUP，保持每方向一个在途。双向poll不相互取消未完成握手/写入，已解析ACK不被后续EOF/重连遮蔽，成功PUBACK后才释放源所有权。
- 普通User Properties限128项，Origin/Hops各有唯一保留条目；所有字节仍受包长和预算限制。126/127/128普通属性与新增/已有防环元数据由两个真实Broker的Bridge组合回归覆盖。
- format4恢复幂等记录时，真实properties参与校验、包长和预算；原请求保持幂等，异属性冲突；非法属性、自带订阅标识、包长或预算不足均拒绝恢复。

同步可选WebSocket/Bridge的契约范围说明，既定MQTT3.1.1 TCP/TLS/SQLite发布Profile不扩大。当前修复只关闭已确认反例，不替代完整F6/F7、安全/性能/平台和独立发布验收，整体发布继续Partial。

## 32. 修复后的组合边界复核（2026-11-08）

本轮未修改产品代码；原169项源码输入和30份修复验证材料在核对起点全部匹配。新材料见[验证结论摘要](validation/README.md#verification)，当前待办见[剩余需求](remaining-work.md)。R-01–R-04的限定回归保持原记录，新发现三项问题：

- R-05（P1）：MQTT5持久Session保留非默认订阅选项，随后以3.1.1成功接续。attach更新Session协议却保留选项，快照恢复拒绝该组合，导致下一次Core启动失败。必须明确跨协议策略并使接纳与恢复保持一致。
- R-06（P1）：MQTT5混合监听下，恢复校验统一按5.0编码消息。合法8192字节的3.1.1 Retained PUBLISH已确认并持久化，恢复因额外Property Length字节而失败；8191字节对照成功。这是§31恢复校验改动后的组合回归，修复时须保留真实属性验证。
- R-07（P2）：No Local实时路径生效，但Retained未保存发布ClientID，订阅回放未过滤自身来源，实际收到自身Retained PUBLISH。按MQTT5官方MQTT-3.8.3-3核对，需补齐来源、持久恢复及旧格式策略。

本轮为定向反例核对，不是完整F6/F7审计；不把未复现的怀疑计作缺陷。两项恢复失败的启动补偿及原应用检查式退出均完成。完整规范、生产SLO、magicCommon消费、远端CI、平台/真实断电及独立评审仍待验收，整体状态继续Partial。


## 33. 组合边界代码收口（2026-11-08）

本节更新§32三项反例的当前状态，原反例结论汇总至最终记录；执行材料见[验证结论摘要](validation/README.md#closed-issues)，尚待外部验收见[剩余清单](remaining-work.md)。

- R-05：Session按协议隔离恢复，不清理的跨3.1.1/5.0 CONNECT在分配ID、替换Session或撤销原连接之前拒绝，双向QoS所有权/订阅不变。显式Clean Session/Clean Start可以换协议重建，Session Present=0；相同协议恢复继续保留所有权。已有旧版不一致快照不自动推断修复，仍保留原库并拒绝开放入口。
- R-06：快照消息包长按实际属性选择可表示的最小协议编码，避免为3.1.1空属性消息补算MQTT5 Property Length；真实属性仍验证、计费。目标附加的Subscription Identifiers不占源消息包长，实际下行继续按目标/协议包长限制处理。统一覆盖Retained、可靠Pending/入站QoS2、Will及内部幂等记录恢复。
- R-07：Retained来源在接纳事务中与消息一起保存、原子替换，ClientID字节进入预算；No Local回放比较发布ClientID，适用于QoS0/1/2、3.1.1发布、Will与同ID的新连接。内部发布单独标记，不作为网络客户端自发消息。

当前写format5、兼容读1/2/3/4，新增逐Retained Topic来源账本并校验引用及ClientID；恢复和normalization提交仍发生在listen前。format1–4没有来源身份，迁移为LegacyUnknown，普通订阅回放保持，No Local保守跳过未知来源；重新发布后采用新来源。旧Will缺失来源时沿用未知身份策略，不能凭空恢复历史来源。format2多Release歧义拒绝、禁止原地降级及停止Will允许重启收尾的边界不变。

本轮关闭R-05–R-07限定反例，完整F6/F7符合性、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立安全/性能/Agent评审仍待实际材料，整体发布保持Partial。

## 34. format5后续遗留项复核（2026-11-08）

本轮核对起点，上轮169项源码输入和37份验证材料全部匹配；没有产品代码变动。材料见[验证结论摘要](validation/README.md#verification)，待办见[剩余清单](remaining-work.md)。

R-08（P1）确认Topic Alias展开与持久恢复的大小约束仍不一致。MQTT5连接先绑定1000字节Topic到Alias=1，再以空Topic/同别名发送8191字节QoS1 Retained PUBLISH；收到PUBACK且Retained=1。检查式退出后，恢复使用展开后的完整Topic编码并按8192字节上限拒绝，整个Core不能启动。完整Topic、较小Payload的8188字节对照可恢复。§33的R-06关闭空属性/附加订阅标识的已测反例，不覆盖别名展开；后续修复必须统一线帧限额、展开消息预算及恢复校验，不能只删除真实属性校验。

D-01（P3）为项目主Skill1.0.2仍记录format4/读1–3的说明滞后；当前源码写format5/读1–4，框架版本及指纹预检通过。后续按托管Skill流程刷新并校验，本轮不修改Skill。

另核对Payload Format Indicator=0省略行为：官方§3.3.2.3.2明确其与不发送该属性等价，因此不作为MQTT-3.3.2-4违例；取值1和属性缺省对照均符合预期。首次探针的缺陷标签撤回，保留初始日志及最终排除依据。

完整F6/F7规范、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍未获新验收材料。上轮发布0/178批准保持历史状态，本轮不升级发布结论。


## 35. Topic Alias恢复及Skill说明收口（2026-11-08）

本节更新§34的R-08/D-01状态，原审计材料保留；新执行材料见[验证结论摘要](validation/README.md#closed-issues)，剩余验收见[清单](remaining-work.md)。

R-08区分网络入站线帧与展开后的存储消息。恢复允许完整Topic的最小源编码，或合法别名消息帧与独立绑定帧均能在max_packet_bytes内表示的消息；两种可行表示取较小所需上限，纳入真实属性、Property Length及Remaining Length宽度。存储始终保存完整Topic/真实属性并按实际逻辑预算计费，不持久化或跨连接复用别名。目标新增订阅标识仍不反向占用源线帧；真实属性验证、UTF-8、引用和预算检查保持。Will、内部幂等记录及已知Internal Retained不使用别名推断。

当前继续写format5、兼容读1/2/3/4，不新增字段；旧版已确认并落盘的合法网络别名消息可直接恢复。SQLite覆盖原8191字节反例、带Content Type的8192字节帧、Retained和双向QoS2所有权恢复；超入站线帧、非法/未绑定/重连失效别名及预算不足在确认前拒绝，不产生部分Retained或入站QoS2记录。恢复预算边界与内部消息限制另有定向回归。

MQTT5下行在编码前按完整Topic及目标/监听上限判限，超限按原策略丢弃计数并继续后续交付；不以恢复成功承诺超限交付。3.1.1保持原连接层超限拒绝；只有显式提高监听上限才能交付超原上限的完整Topic报文。

D-01将项目主Skill更新为1.0.3，同步format5/读1–4、跨协议恢复、LegacyUnknown迁移和别名恢复边界；经托管Skill校验、feedback预览及本地归档同步，未执行远端推送。

本轮只关闭已确认反例。完整F6/F7、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍待真实材料，整体发布保持Partial。


## 36. 别名修复后的源会话边界复核（2026-11-08）

本轮核对起点，上轮172项源码/探针输入和45份验证材料摘要全部匹配，Modorch固定rev/指纹预检通过。没有修改产品代码。R-08的4项别名相关回归与Bridge既有4项网络回归均通过；修复与验证结论见[验证结论摘要](validation/README.md#verification)，当前待办见[剩余清单](remaining-work.md)。

R-09（P1）确认Bridge在源Session丢失时混用旧Transfer的Packet ID。Bridge已收到源QoS1 ID1消息并交给目标，目标暂未PUBACK；源断开后以Session Present=0重连，新Session合法发送另一条ID1消息。Bridge忽略源CONNACK的Session Present，持有旧Transfer期间仅比较Packet ID，因而将新消息当成旧消息重复。目标随后确认旧Transfer，Bridge向新源Session发送旧ID1的PUBACK，误确认尚未转发的新消息。目标继续收到下一条屏障消息，中间新消息确实缺失，已观察到数据丢失。

修复必须将源Session身份/代际与Transfer及ACK绑定；源会话丢失后隔离旧ACK，保留仍在目标执行的旧交换，并保证新QoS1不被吞掉。回归须覆盖同/不同ID、源Session恢复与丢失、目标延迟确认、断线和后续进度。MQTT5官方§2.2.1明确Packet Identifier属于各Session，§3.2.2的MQTT-3.2.2-5要求收到Session Present=0时丢弃旧协议Session State；应用层继续持有的旧目标工作不能沿用新源Session的确认身份。

源Session Present=1且DUP重发同一旧消息的对照通过。§31的R-02与现有Bridge回归覆盖目标会话丢失/恢复，不覆盖本次源侧情形；它们的历史限定证据保留。本轮探针退出0表示确认反例，不能写为产品验收通过。

R-01–R-08/D-01已有对应修复，新R-09尚未收口。完整F6/F7规范、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍待实际材料，整体发布保持Partial。


## 37. Bridge源会话身份与确认边界收口（2026-11-08）

本节更新§36的R-09状态，原反例结论汇总至最终记录。材料见[验证结论摘要](validation/README.md#closed-issues)，剩余验收见[清单](remaining-work.md)。

每个Transfer保存源Session代际，源CONNACK Session Present=0递增代际，Session Present=1保持代际。旧目标交换不因源会话丢失而撤销或新建；完成时仅向同代际且已连接的源排ACK，其他代际只释放本地旧工作。源连接未收到CONNACK时不排ACK；源错误分支再次清理客户端请求队列，避免ACK在第三方poll已经清理后入队并跨重连发送。

每方向继续只有一个Transfer。若新源Session已建立但旧目标工作未结束，延后订阅新Session，保持Keep Alive；旧工作完成后不发送旧ACK，建立新订阅再接纳新QoS1。同Session的重复PUBLISH沿用原交换；异常额外可靠投递通过断线留给源端重发，不静默计作QoS0丢弃。Session丢失后新订阅建立前的发布，不能依赖已丢失的订阅形成可靠积压；源业务需保留或重发。此边界不恢复源Broker丢失的旧状态，不承诺跨Broker exactly-once。

新增真实Bridge/TCP回归覆盖新Session同/不同ID、源连续丢失、两端同时丢失、源Session恢复/DUP、源CONNECT等待CONNACK期间目标完成确认、新消息独立目标ACK及后续屏障。公开API探针按修复后预期复测原R-09轨迹：目标顺序为旧消息、新Session消息、屏障，新消息只在自己的目标确认后才获源PUBACK；恢复Session对照不重复提交旧目标工作。

不改变公共API、Feature默认关闭、持久format5或既定MQTT3.1.1发布Profile。项目MQTT Skill更新至1.0.4，说明源代际/ACK和延后订阅边界，经托管Skill校验及本地归档同步；未推送远端。

本轮关闭R-09限定反例，完整F6/F7规范、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍待实际材料，整体发布保持Partial。


## 38. 提交后的延迟 Will 与背压组合核对（2026-11-08）

以提交 `61db632` 为起点，功能代码未变；原619份原始验证文件保持原字节，验证索引按本轮结果更新。Modorch 固定 rev/指纹预检、MQTT5 与 Bridge 限定回归、公开 TCP 反例的本轮材料见[验证结论摘要](validation/README.md#verification)。

R-10（P1）：目标可靠积压上限1且已占满时，MQTT5 发布者的 QoS1 Will Delay=1 已经过期并等待投递。发布者在1.6秒后以 Clean Start=false 恢复原 Session，State::attach 中只按 client 与 delay>0 清理 Will，没有区分未到期、已到期或已冻结目标的状态，导致待投递 Will 被删除。释放目标容量后，目标能收到后续屏障消息，却收不到 Will。Delay=0 的背压对照保留并完成 Will；Delay=2 且到期前0.2秒重连的对照正确取消。

MQTT5 §3.1.2.5（MQTT-3.1.2-8）要求连接关闭且延迟到期或 Session 结束后发布 Will，仅允许正常 DISCONNECT 或延迟到期前重连的取消例外；§3.1.3.2（MQTT-3.1.3-9）规定到期前恢复 Session 不得发送 Will。修复须保持这两个边界，不能用删除待投递 Will 规避背压。后续须覆盖到期前后重连、容量恢复、Clean Start/接管、持久重启及检查式退出，不扩大停机 Will 可延迟到重启的既定范围。

R-01–R-09/D-01的限定修复记录仍保留，新增R-10尚未收口。本轮为定向核对，未修改产品代码；完整F6/F7、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍缺真实验收材料，正式发布继续Partial。


## 39. 延迟 Will 到期后的恢复与可靠背压收口（2026-11-08）

本节关闭§38确认的R-10，原反例材料保留。实现与本轮验证见[验证结论摘要](validation/README.md#closed-issues)，当前验收边界见[剩余清单](remaining-work.md)。

恢复 Session 时，只取消同 Client、正延迟、尚未到期且未冻结目标的 Will。已到期的记录即使尚未来得及冻结目标也保留；已冻结目标、因可靠积压满额等待的记录继续独立持有。多次恢复、Clean Start重建及新连接的正常DISCONNECT不删除旧待投递记录；在线接管仍可取消新连接自身尚未触发的正延迟Will。原零延迟接管、预授权、目标Session epoch/订阅标识、Message Expiry及容量/预算规则保持。

新增两项命名回归：真实TCP覆盖零延迟、到期前重连、在线接管、到期后恢复/Clean Start及旧记录与新Will的重复接管；SQLite覆盖尚在延迟与已触发两个保存点，恢复发布者后再次停机/重启，确认旧Will仍待投递，目标原ID/DUP恢复并确认积压后收到Will，随后可继续处理屏障消息。回归先观察pending_wills状态屏障，避免仅凭sleep假定维护任务已触发。原公开TCP探针另复制为修复后预期，保留零延迟与到期前重连对照。

不新增持久字段，format5及读1–4边界不变；恢复和重连仍先提交存储，再确认/下行，所有任务沿用宿主管理。停机Will允许延迟到重启的既定语义保持。R-01–R-10/D-01已关闭限定反例，完整F6/F7规范、生产负载/恢复SLO、magicCommon消费、远端CI、跨平台/真实断电及独立评审仍待真实材料，正式发布继续Partial。

## 40. 协议完整性本轮收口

[协议完整性](protocol-integrity.md)补齐3.1.1四组QoS条款的15条跨段义务与角色，发布目录拒绝孤立编号/截短义务；增加16组合接收端内存/快照状态矩阵。MQTT5规范正文单独固定216段/250编号，但不将编号抽取当成完整语义批准。

R-11–R-14修复短DISCONNECT被第三方解码器误拒绝、非零断开原因错误取消Will、客户端接纳Server专用原因及重复UNSUBSCRIBE未顺序计算原因码。仅对严格验证后的短报文临时解码副本补Property Length，不改变真实线帧上限。只有Reason Code=0删除Will；其他合法客户端原因继续沿用延迟、过期和可靠预算语义。取消订阅逐项删除及生成原因，已开始QoS交换保留。

新增原因码/属性类型、重复/截断、真实TCP与SQLite回归，Paho脚本覆盖TCP/WebSocket的非零DISCONNECT Will和重复取消。执行及失败复核材料见[验证结论摘要](validation/README.md#protocol)。原API、Feature、format5与读1–4边界不变；完整F6/F7逐义务、状态/故障/互操作总矩阵和178项独立发布批准仍待实际验收，发布保持Partial。


## 41. 存储时序诊断与F6/F7核对输入收口

增加脱敏Store调用分类和确定性超时/丢回执验证，保留原operation_id/digest查询及失败关闭语义。功能测试的正常I/O期限与150ms超时探针分开，生产请求/总恢复期限不变。并行失败的原始复现、诊断与最终限定结果见[验证结论摘要](validation/README.md#verification)。

R-15修复MQTT5重连/SQLite恢复时旧PUBREL错误占用新连接PUBLISH配额；连接级quota_used不持久化，不改变format5。R-16接受RL=0正常DISCONNECT，取消当前Will并保持持久Session；与RL=1共用严格验证后的临时解码归一化，原始线帧大小仍用于接纳。

双协议QoS2四提交点故障矩阵与新增MQTT5固定头/CONNECT Flags、出站字节、属性/Will顺序、Retain Handling、共享QoS2所有权、KeepAlive及配额场景均按实际日志验收。533条完整文字核对记录绑定编号、未编号候选及表格单元格，有限FSM维度及缺失执行由check.sh门禁校验。完整逐表行语义与独立F6/F7符合性仍须真实实审，不能用文字索引或有限组合替代；详见[协议完整性](protocol-integrity.md)。


V-01进一步定位到两类可复现时序：150ms请求Running超时，以及73字节分页的大量Hub往返耗尽10s总恢复期限。新增RecoveryDeadline脱敏分类；功能分页4096字节、专用小页/超时/丢回执探针分开，生产期限保持不变。三进程三轮共252项存储测试通过，8进程扩大负载仍有客户端3s等待超时，暂不承诺该负载稳定。533条核对记录进一步映射全部布局表的限定断言，3.1.1/5.0分别188/270条TargetedEvidence；全部逐行语义和独立批准仍未完成。


## 42. 共享订阅离线投递与完整验收通道核对（2026-11-08）

[验证结论摘要](validation/README.md#verification)在上轮179项受控输入和162份绑定材料匹配后复跑Core153项、MQTT5存储专项7项，均通过；公开TCP反例新增R-17/P1。

R-17：MQTT5共享组仅一个持久成员，Session Expiry=60、订阅QoS2后正常断开；发布者完成QoS1和QoS2交换。该成员恢复时Session Present=1，后续匹配屏障可收到，但两条离线消息丢失。普通持久订阅对照得到完整的两条消息和屏障。State::targets_from只从在线共享成员构造目标；MQTT5 §4.8.2关于唯一共享成员与普通订阅等价的上下文尚未落实，不能以已开始QoS2交换的恢复回归覆盖此处。后续须收口全离线组的可靠接纳、预算不足拒绝、Session过期/清理和SQLite恢复，保持已有QoS2目标所有权规则。本轮仅确认，不修改功能代码。

T-01：MQTT5完整验收的批准输入及判定通道仍待开发。当前check-protocol-review.py只接受NotRun且require-complete固定失败，是防误报门禁；3.1.1批准由check-release.py处理且Profile不包含MQTT5。需要接入真实独立批准、绑定原文/实现/执行/身份并验证全覆盖，不能仅新增结构映射即宣布完整F7通过。

R-01–R-16限定修复保持；V-01的8进程客户端等待超时、F6/F7完整语义、生产/跨实现/平台/断电及独立发布验收仍未关闭，整体发布保持Partial。


## 43. 遗留项1–5的代码与测量收口

R-17共享订阅路由先选在线成员；无在线成员时，从未过期、非零Session Expiry且有效QoS为1/2的持久成员中选一个固定目标。复用普通可靠投递的整体准入和format5存储，不在ACK之后丢弃可靠工作，不离线积压QoS0，不重新分配已接纳的QoS2所有权。单/多离线、在线优先、合并目标、选项、Session/Message过期、清理、全局/单客户端预算及SQLite恢复均有定向断言。

V-01保留客户端三秒边界，新增调用点及耗时诊断。两项握手故障探针改为真实I/O完成后按受控时钟检查500ms期限前后，避免正常建连被短故障期限干扰。8进程、默认进程内并行、三轮共696项通过；原alias超时未重新定位，不宣称其历史根因已关闭。

F6/F7审查输入扩展到完整章节正文，含无大写MUST的行为说明，合计1,044项；编号、候选段落和表格继续保留。T-01实现当前源码、原始日志、逐义务、矩阵用例及协议Profile的独立材料导入，完整判定存在真实通过路径，缺项/自评/陈旧/失败仍阻断。详见[协议评审格式](protocol-review.md)。工具的合成验证不代替真实独立批准，完整F6/F7继续Partial。

按用户选择，第5项交付[本机持续基线工具](load-baseline.md)：TLS/Auth/SQLite/QoS2流量、无ACK慢消费者、活跃交换中SIGINT检查式退出、原库恢复和可靠FIFO屏障；保存原始RSS/延迟、确认边界、配置/环境及源码/二进制摘要。可选阈值明确输入后才判定本机SLO；生产SLO和长期生产趋势仍待验收。实际结果见[验证结论摘要](validation/README.md#verification)。


## 44. 收口后的工具边界与遗留核对（2026-11-09）

起点183项受控输入与上轮一致，102份摘要绑定材料完整；本轮Core159项、Python工具39项及8进程默认并行存储232项通过。R-17共享离线可靠投递修复保留限定证据；V-01历史alias接收超时未复现，根因不因此关闭。

新增T-02（P2）：check-protocol-review.py和check-release.py使用原路径字符串排除评审文件，等价`./`路径可绕过“评审不得作为执行证据”。隔离fixture中合法日志通过，直接评审路径拒绝，等价路径错误通过。T-01导入通道已实现，但证据身份排除仍待统一修复及反向回归；合成批准不代表当前项目通过。

新增V-02（P2）：load-soak.py仅记录恢复报文的DUP/Packet ID，没有对已观察未确认消息建立恢复DUP/原ID拒绝断言。短真实Broker探针在观察器中明确把恢复DUP从1改成0，工具仍报告functional_status=Passed；这是验证缺口，实际Broker的线帧DUP正确。原五分钟测量值保留，生产SLO按用户选择仍待验收。

本轮仅核对并更新状态文档，未修改产品代码或产品测试。完整F6/F7逐义务语义、状态/故障/客户端组合、独立批准和跨实现/平台/硬件故障/远端CI发布证据仍待实际验收。原审计/修复结论保留，过程目录已清理，具体反例与关闭条件见[验证结论摘要](validation/README.md#verification)和[遗留清单](remaining-work.md)。


## 45. 遗留项1–4继续收口（2026-11-09）

T-02：两个评审门禁按实际文件身份排除评审文件，覆盖`./`路径、内部符号链接和硬链接；主结果日志及嵌套报告执行证据遵循同一规则，报告自身也不可作为执行证据。保持合法日志及其别名通过，合成fixture只检验工具逻辑。

V-02：本机负载记录首条已观察未确认报文的原Packet ID及载荷；恢复时该帧必须DUP=1、原ID/载荷一致。未观察报文不推定已经发送。真实Broker观察端DUP清零和错误ID两种注入均使实际测量CLI非零；正常恢复、可靠FIFO屏障及显式本机阈值通过。旧五分钟测量不追溯补充断言，生产SLO仍待验收。

F6/F7：在现有1,044条审查输入、5个矩阵及2个协议验收对象内，新增114表/844行的明确检查，绑定逐单元格文本及rowspan/colspan。Passed表行须绑定真实通过测试；表头等不适用行须有实审理由和材料。完整义务语义、所有状态/故障/互操作组合及真实独立批准仍未完成；新增检查不会自动批准任何条款。Paho TCP/TLS与MQTT5 TCP/WebSocket专项只提供本轮限定互操作证据。

V-01：接收诊断进一步区分固定头、Remaining Length与正文进度，alias两种属性变体记录每个握手、恢复及保留回放阶段的Broker状态，三秒客户端边界不变。8进程、默认进程内并行、三轮696项通过，两个alias变体共48次完成；历史超时根因没有复现或确认，继续保留未关闭状态。

具体正反回归、历史执行结论见[验证结论摘要](validation/README.md#closed-issues)。历史验证目录不改写，完整规范和发布继续Partial。


## 46. 遗留项1–4收口后的核对（2026-11-09）

[验证结论摘要](validation/README.md#verification)核验183项输入和83份上轮材料，复跑Core159项、Python工具48项及8进程存储232项通过；未确认新增Broker运行时功能缺陷，V-01原alias三秒超时未复现且根因未定位。

新增T-03/P2：T-02按文件身份排除同一文件的直接/点路径、符号链接和硬链接，但逐字节复制的已知评审副本文件号不同、SHA256相同，仍可作为执行材料。协议CLI及发布evaluate的合成对照均为合法日志通过、直接评审拒绝、副本错误通过。应在各级执行材料核对中排除已知评审等内容副本，保留合法证据路径；这不证明当前项目取得批准或绕过全部校验。

新增V-03/P2待归因异常：与8进程存储扩压重叠的首次负载DUP探针在初次CONNECT等待CONNACK时EOF，尚未进入注入；扩压完成后相同配置、期限及二进制的受控DUP复跑通过。错误ID拒绝及正常恢复也有定向复核。保留原失败并定位具体握手阶段/期限与负载条件；不能据此认定Broker功能缺陷、原alias根因或通过放宽期限关闭。

F6/F7仍须完整语义、全部状态/故障/客户端组合与真实独立验收；844表行检查和有限矩阵不等于实审批准。生产SLO按用户选择保留，跨实现/平台/文件系统/硬件断电/远端CI及独立发布材料仍待实际验收。当前清单和顺序见[遗留项](remaining-work.md)。本轮不改产品代码、产品测试或契约，历史验证目录保持原样。

## 47. V-01、T-03、V-03 与 F6/F7 继续收口

本轮实现与验证结论见[验证结论摘要](validation/README.md#closed-issues)。

1. T-03：两门禁以实际文件身份及已验证SHA256排除已知评审、嵌套报告自身与等内容副本；合法日志及其副本允许。保留修复前失败和实际CLI对照，不升级项目独立批准。
2. V-01：同名alias测试再次在恢复后PINGRESP等待三秒失败。受控无关Store持有反例确认PING/空闲出站检查不应产生持久化工作；现仅无状态变更路径跳过事务，可靠消息/ACK仍先提交后响应，同连接PING不得越过先前提交。故障分类的150ms等待只用于受控时钟的Running/Queued注入，普通Provider回复另用正常请求期限。客户端三秒、生产存储及握手期限不变；不能将本轮定位倒推为所有旧失败唯一根因。
3. V-03：带诊断压力探针的初次slow/seed CONNECT均出现Authenticate阶段约五秒握手超时（观察5000–5011ms），未观察到Metrics宿主失败。TCP/TLS受控4999/5001ms回归验证期限前等待、期限后关闭、迟到授权不建Session及随后同ID正常连接；实际超时仍记负载失败。
4. F6/F7：新增双协议×进程内重连/SQLite重启×重复DUP标志8组合，真实线帧检查原Payload/Content Type/Retained、PUBREL幂等及完成后Packet ID重用。有限矩阵增至6个58组合，1044条规范输入及2验收对象共1052对象，844个表行检查保持。完整语义与真实独立实审批准仍未完成，整体Partial。

`MODORCH_MQTT_DIAGNOSTICS=1`显式开启有界连接阶段诊断，默认关闭、不创建任务/Runtime。失败及至少250ms阶段切换输出进程内数字追踪号、固定阶段、耗时与脱敏类别；测试构建额外带测试线程名，不输出客户端身份、地址、Topic、载荷、凭据或原始IO/配置错误。诊断会增加stderr开销，仅用于定位；不能据此承诺生产延迟SLO。

## 48. 四项收口后剩余遗留核对

[验证结论摘要](validation/README.md#closed-issues)验证184项当前输入与174份绑定材料，复跑Core162项、工具52项、8进程存储248项及两个实际门禁CLI副本对照通过。TLS/Auth/SQLite/QoS2短正常对照完成慢消费者恢复及可靠FIFO屏障，未配置SLO；不能抵销上轮压力2/4认证五秒期限拒绝。

未确认新增可复现功能缺陷；T-02/T-03/V-02、PING无关持久化等待及认证期限的限定修复/回归保持。V-01缺阶段日志的历史失败仍不能全部唯一归因；V-03仍需验证所要求压力下的认证成功容量，正常期限拒绝不算压力通过。

完整F6（437输入/339表行）和F7（607输入/505表行）的跨段/逐义务/逐表行语义及其余状态/故障/客户端组合仍须真实实审。现有6矩阵58组合、844表行检查和1052评审对象的真实独立批准仍0，3.1.1发布178项批准仍0。生产SLO按用户选择保留，magicCommon、远端CI、跨平台/文件系统、硬件断电及独立安全/性能/接入/发布材料仍待验收，整体Partial。本轮仅核对并维护状态文档，不改产品代码、产品测试或契约。


## 49. 入站流控及认证容量继续收口

R-18：MQTT5的Receive Maximum使用`max_inbound_qos2_per_client`宣告，QoS1/2共用连接级网络额度；QoS1在PUBACK、QoS2在PUBCOMP完整写出时归还，成功PUBREC不归还。QoS0不占额度，重复接收的QoS2不重复计数；PUBREL删除持久接收记录后，未完成写出的PUBCOMP仍占网络额度。超额PUBLISH在授权/路由/持久化前拒绝，按控制FIFO发送DISCONNECT 0x93并关闭。旧排队PUBCOMP绑定原交换标识，不能释放重用Packet ID的新交换；未知/重复PUBREL不增加额度。网络记录有独立全局预算、随连接释放，不新增快照字段。持久QoS2条数/字节限额继续独立生效，重连不直接继承旧网络额度。

F6/F7新增双协议×部分PUBACK/PUBREC/PUBCOMP×后续QoS1/2的12组合，以及连接重建、重复完成、真实TCP无Retained覆盖/无部分路由和其他连接健康断言。MQTT5的3.3.4-7/8整段虽以Client义务编号，正文也包含Server处理规则；角色改为Both并补Server限定证据。7矩阵70组合、1053对象及844表行不等于完整实审，真实独立批准仍0。

V-01保留历史无阶段日志的归因限制；本轮32个存储扩压进程共992项通过，受控慢写、PING与持久屏障回归保持。V-03负载工具增加有界并发建连及逐连接TCP/TLS、CONNACK耗时。在八进程存储测试重叠下，release两次四客户端并发测量完成TLS/Auth/SQLite/QoS2、活跃退出、原库恢复及FIFO屏障；debug两次失败，包含Authenticate以及认证完成后等待SessionAuthorize时的五秒总握手期限。新增后者TCP/TLS受控4999/5001ms和迟到授权不建Session回归。优化构建的通过不抵销debug失败，不放宽期限或密码参数；未配置SLO，长期生产容量仍待验收。

具体修复与验证结论见[验证结论摘要](validation/README.md#closed-issues)。完整规范和外部发布继续Partial，生产SLO按用户选择保留。


## 50. 流控收口后剩余遗留核对

[验证结论摘要](validation/README.md#verification)确认186项当前输入和96份上轮绑定材料完整，复跑Core167项、Python工具52项及release四客户端/四并发短正常对照通过。R-18流控及迟到授权等限定回归保持，本轮未确认新增可复现运行时功能缺陷。

新增T-04/P2：MQTT5的3.2.2-19/20正文明确包含Server MUST，协议清单却仍为Client/ClientOnly且无映射断言；另3.2.2-11/14/15包含条件性Server错误处理，需要复核整段角色和适用条件。当前结构/执行证据CLI仍返回validation Passed、overall Partial，表明它不代替语义实审。CONNACK当前不发送Reason String/User Property，不能仅由分类错误推断包长违约或完整批准绕过。应先修正角色/适用性和准确证据，补针对错误分类的回归，再继续完整F7实审。

V-03的上轮八进程重叠负载仍为debug0/2成功、release2/2成功；本轮正常对照不抵销失败，也未重跑相同压力。V-01无阶段历史失败仍不能全部唯一归因。F6/F7的完整语义、其余状态/故障/客户端组合和真实独立批准继续待验收；437/607输入、339/505表行、188/273限定映射及7矩阵70组合只是限定证据。1053对象、844表行的真实协议批准仍0，发布178项批准仍0。

生产SLO按用户选择保留，magicCommon、远端CI、平台/文件系统/硬件断电及独立安全/性能/接入/发布材料继续待执行。整体Partial；本轮不修改产品代码、产品测试或契约，不重写历史验证材料。

## 51. 角色覆盖、认证队列与中断恢复收口

T-04：MQTT5的3.2.2-19/20改为Server，3.2.2-11/14/15按整段的条件性Server行为改为Both；MQTT3.1.1的3.1.3-7整段亦包含Server分配/拒绝空ClientID的义务，改为Both。新增防回退规则拒绝明确Server主语或已识别Server条件上下文被排除为ClientOnly；该规则是保守结构检查，不替代完整语义实审，也不把“Client to the Server MUST”误当Server主语。精确CONNACK字节覆盖省略Reason String/User Property/Maximum QoS/Retain Available、默认允许QoS2/Retained及对端包长15/16/17边界；超监听8192字节的入站发布关闭且不覆盖原Retained。不声称未发送的可选原因码或未宣告的禁用能力分支已经执行。

认证Provider原串行Handler存在密码任务阻塞SessionAttach的确定性反例。改为Concurrent Handler（max_in_flight=8），密码请求最多4个已准入、物理Argon2任务仍为1；超额返回Unavailable。等待密码的任务不会占满所有轻量授权执行容量，密码参数和Core五秒总握手期限不变。准入与执行permit移入宿主受管spawn_cpu闭包，持有到实际退出；Running请求超时不推断取消或提前归还。每操作使用不可变策略快照，开始中的旧认证保留旧revision、新ACL使用新revision；并发不保序，Core逐连接的处理顺序继续保持。未新增公共配置字段或独立Runtime。 本仓库dev/test仅对Argon2和Blake2启用编译优化，应用代码继续使用开发Profile；Release不变，外部宿主需自行决定workspace Profile。

新增双协议×PUBACK/PUBREC/PUBCOMP部分写×仅Owner取消/仅对端EOF共12组合。仅取消场景保留对端连接直至服务端关闭，避免用EOF代替取消证据；重连验证Session Present、接收端QoS2所有权、旧PUBREL完成、Packet ID重用和PING，显式清理重建后Session/连接/接收记录及记账内存全部归还。该矩阵在受控内存传输中执行，不冒充进程崩溃或物理断电测试。

本轮有限矩阵共8个82组合、1054评审对象、844表行；MQTT3.1.1为437输入/189限定映射，MQTT5为607输入/278限定映射。完整逐义务/逐表行及剩余状态/故障/客户端组合仍须真实实审，真实协议与发布批准均为0，整体Partial。容量、原始失败、全检查与当前输入绑定见[验证结论摘要](validation/README.md#closed-issues)。V-01缺阶段日志的历史失败不能唯一重建，生产SLO和外部验收保持原边界。

最终全检查通过workspace207项Rust（Core169项）、53项工具测试，MSRV1.85/最小Feature与Paho TCP/TLS、MQTT5 TCP/WebSocket通过。隔离队列但尚未优化密码库的debug压力仍两次失败，release两次通过；启用dev/test密码库优化后，相同八进程存储重叠条件下debug与release各两次四客户端并发测量均成功，debug健康连接约158–559ms。两批64套存储扩压共1984项通过。成功仅关闭该有限本机配置的失败容量缺口，不批准任意接入负载；旧失败、历史归因限制和生产/外部验收均保留。

## 52. 认证容量收口后的剩余核对

[验证结论摘要](validation/README.md#verification)确认187项当前输入与154份上轮材料一致，Auth9项、Core169项和工具53项复跑通过，未确认新增可复现运行时缺陷。T-04角色和限定证据修正保持，V-03最终debug/release各两次成功及1984项扩压为摘要核验的上轮材料，不声称本轮重新执行。

新增V-04/P2验证项：密码准入测试的Running超时发生在执行permit被夹具占用、真实密码工作等待期间；正常退出在全部额度归还后才进行。应补物理密码工作执行中Owner取消、StopIncomplete/Forced、同Factory重建和实际退出后归还额度的受控验证。实现已将双permit移动进受管CPU闭包，当前证据不表明额度提前释放或重复物理执行；不能用等待阶段超时/排空后的退出替代这些生命周期场景。

完整F6/F7仍须全段/逐义务/844表行和其余状态/故障/客户端组合实审；437/607输入、189/278限定映射、8矩阵82组合及1054对象均不代表独立批准，真实批准0，发布178项批准亦0。Paho/rumqttc已有限定互操作，完整矩阵未批准。V-01无阶段历史失败是不可唯一重建的归因限制；V-03所测有限配置已有成功证据，其余容量及外部宿主Profile仍需另验。生产SLO按用户选择保留，magicCommon、远端CI、跨平台/文件系统、硬件断电及独立审查仍待执行。整体Partial，本轮未改产品代码、测试或契约。


## 53. 物理认证生命周期与发送端持久恢复收口

[验证结论摘要](validation/README.md#closed-issues)关闭V-04验证缺口：仅在Auth单元测试构建内，真实受管CPU闭包持有双permit后进入同步屏障。Running超时、Owner取消、StopIncomplete或Forced不能提前归还物理任务额度；实际退出后pending与额度恢复，Forced仍不能报告正常排空。同Factory受控监督重建必须等旧物理工作退出/已准入请求排空，再建立新代际；外部旧Scheduler持续拒绝，新认证可成功，物理并发峰值1。原operation lease允许已准入旧请求继续派生工作排空；首次夹具误要求所有旧请求必须拒绝的失败原样保留，修正后的测试及全回归通过。不改变生产默认FailFast、密码成本或握手期限，不新增公开生命周期钩子。

F6/F7增加26个真实TCP/SQLite组合：双协议×发送端AwaitPubRec/AwaitPubComp×三类错误ACK共12组合，在错误输入前后各检查式重启，核对原Packet ID、PUBLISH DUP、PUBREL阶段、载荷及MQTT5订阅标识17；错误ACK不提前完成，正确ACK清除后下一消息继续。MQTT5另有两阶段×七种合法负PUBREC原因共14组合：Publish阶段释放所有权，Release阶段迟到负PUBREC保留已移交交换，经第二次重启确认。迟到矛盾ACK是健壮性输入，不声称对端序列符合规范；检查式重启不冒充新增SIGKILL或物理断电。

有限矩阵现为10个108组合、1056评审对象、844表行，437/607规范输入和189/278限定映射保持。完整逐义务/表行、跨段语义、其余状态/故障/客户端组合及真实独立评审仍未完成，协议与178项发布批准均0，整体Partial；Enhanced Authentication仍明确未支持。全检查通过211项Rust（Core171项、Auth11项）、53项工具测试，MSRV1.85/最小Feature及Paho TCP/TLS、MQTT5 TCP/WebSocket通过。本轮不重新运行上轮容量/1984项扩压；V-01历史归因、用户保留的生产SLO及外部验收边界保持，未提交。


## 54. 物理生命周期收口后剩余遗留核对

[验证结论摘要](validation/README.md#verification)确认187项受控输入与37份上轮材料匹配，Auth11项、Core171项及工具53项复跑通过。V-04限定验证保持，不重新列为待开发。上轮211项workspace测试、MSRV/最小Feature及Paho、以往容量和1984项扩压是摘要核验的历史材料，不声称本轮重新执行。

新增R-19/P2：MQTT5的TABLE-026/029明确说明PUBACK/PUBREC的0x10（No matching subscribers）仅由Server发送；当前Client入口校验仅读取ACK原因码，Codec接受0x10，穷举测试也误将其列为合法。真实TCP覆盖两种ACK×省略/显式Property Length：四个违规输入均保持PING可用，QoS1原交换完成、QoS2转PUBREL且会话重连继续该阶段；四个合法0x00对照通过。应补发送角色校验、修正测试预期和准确映射，再验证拒绝路径与其他健康连接隔离。该反例由非法对端输入触发，不声称正常客户端存在消息丢失；本轮未扩展为SQLite或崩溃反例。

完整F6/F7仍需跨段/逐义务/844表行语义、其余状态/故障/客户端组合及真实独立实审；先处理R-19。437/607规范输入、189/278限定映射、10矩阵108组合及1056对象并非完整批准，真实协议批准0，178项发布批准0，整体Partial。Enhanced Authentication明确未支持；V-01历史归因限制、用户保留的生产SLO及外部执行边界保持。本轮只核对并维护状态文档与审计材料，不改产品代码、测试或契约，不提交。


## 55. ACK发送角色与持久恢复继续收口

[验证结论摘要](validation/README.md#closed-issues)关闭R-19：MQTT5 Client入口在ACK解码完成/推进交换之前，按类型验证原因码。PUBACK/PUBREC只允许Success和七种合法Client失败原因，拒绝TABLE-026/029限定Server发送的0x10；PUBREL/PUBCOMP保留0/0x92。省略原因码的隐式Success合法短格式不变。错误沿用异常连接关闭路径，公共API、持久格式5、密码成本和五秒握手期限不变。纠正原穷举测试把0x10视作Client合法码的预期，保留修复前准确执行失败及最初空匹配命令，不将0测试当通过证据。

F6/F7新增44个真实TCP/SQLite组合：双协议×QoS1/AwaitPubRec/AwaitPubComp×两种额外字段形式共12组合，在违规ACK关闭后保持原所有权，健康连接PING/发布正常；重启核对原ID、阶段、PUBLISH DUP、载荷和MQTT5订阅标识，正确完成后下一消息继续交付。3.1.1按非法长度拒绝，MQTT5按Client原因码角色拒绝。另PUBACK/PUBREC×八种合法Client原因×两种Property Length形式共32组合，首次恢复后处理ACK、第二次恢复验证无旧重投，成功QoS2完成PUBREL、负PUBREC终结原PUBLISH且无PUBREL。检查式SQLite重启不冒充新增SIGKILL或物理断电。

有限矩阵现12个152组合、1058对象、844表行，437/607输入、189/278限定映射保持。完整F6/F7的跨段/逐义务/逐表行语义及其余状态/故障/客户端组合与真实独立实审仍未完成；协议/178发布项真实批准0，整体Partial，Enhanced Authentication明确未支持。V-04保持关闭；V-01历史归因、用户保留的生产SLO和外部验收边界保持，本轮未提交。

最终全检查通过workspace213项Rust（Core173项、Auth11项）和53项工具测试，Rust1.85/最小Feature及Paho TCP/TLS、MQTT5 TCP/WebSocket专项通过。输入摘要和严格计划绑定本轮原始日志，完整协议/发布保持Partial，生产SLO及外部执行边界不变。


## 56. ACK角色收口后的剩余遗留核对

[验证结论摘要](validation/README.md#closed-issues)确认187项当前受控输入和38份上轮绑定材料匹配，Core173项、Auth11项及工具53项复跑通过，未确认新增可复现功能缺陷。R-19的Client0x10拒绝与正确原因码处理、异常连接隔离、原交换ID/阶段/DUP/载荷恢复和随后交付保持；V-04物理CPU任务取消、StopIncomplete/Forced、监督重建及实际资源归还也保持，不重新列为待开发。

F6/F7的完整跨段/逐义务/逐表行语义、其余状态/故障/客户端组合和真实独立实审仍未完成；437/607输入、339/505表行、189/278限定映射、12矩阵152组合及1058对象不代表独立批准。Enhanced Authentication明确未支持，相关11项UnsupportedConditional是声明范围内的适用性边界；未知Authentication Method关闭连接是MQTT-4.12.0-1允许的拒绝方式，不自动列作缺陷或新功能要求。

协议和178项发布真实批准均0，整体Partial。生产SLO按用户选择保留；长期/代表性负载、外部宿主Profile、magicCommon、远端CI、平台/文件系统/硬件断电及独立安全/性能/接入/发布材料仍待执行。V-01历史无阶段失败保持不可唯一重建的归因限制。上轮workspace213/MSRV/最小Feature/Paho和以往容量/1984项扩压仅作摘要核验历史材料，不冒称本轮重跑。本轮只核对并维护状态文档与审计材料，不改产品代码、测试、契约或提交。


## 57. 双向同Packet ID与立即过期的持久恢复收口

[验证结论摘要](validation/README.md#protocol)继续F6/F7的协议验证代码收口，新增20个真实TCP/SQLite组合。双协议×出站AwaitPubRec/AwaitPubComp×入站/出站先完成共8组合：两方向使用相同Packet ID，首次恢复核对出站原ID/DUP或PUBREL/载荷/订阅标识；入站改内容重复不重投，完成任一方向不推进或清除另一方向；第二次恢复后剩余交换仍可完成，随后入站ID重用与出站新消息正常。

MQTT5另补QoS0/1/2×Retain×空/非空载荷共12组合，使用确定性的Message Expiry Interval=0。过期消息不向现有订阅转发，Retain=0保留旧Retained、Retain=1删除旧Retained，两次重启后新订阅回放一致。QoS2在PUBREC后重启仍保留接收所有权，改成未过期的新内容重复也不能复活消息或覆盖Retained；PUBREL/PUBCOMP继续完成，二次恢复重复PUBREL仍成功，完成后的同号新发布可正常交付。字面量线帧与FIFO屏障直接核对，不冒称正TTL全边界、进程崩溃或硬件断电。

新增测试在现有实现上通过，本轮未确认新运行时缺陷、不修改状态机。契约新增两个有限矩阵，合计14个172组合、1060对象、844表行；MQTT311为437输入/190限定映射，MQTT5为607输入/281限定映射，新增章节证据限定于独立Packet ID命名空间与Retained/Expiry上下文。完整跨段/逐义务/表行语义、其余状态/故障/客户端组合及真实独立实审仍未完成，F6/F7继续Partial，真实协议与178发布项批准0；Enhanced Authentication仍明确未支持。

R-19、V-04保持关闭，V-01历史归因限制、用户保留的生产SLO与外部验收边界保持；旧容量和1984项扩压不在本轮重跑。本轮完整检查及输入/材料绑定见本轮记录，未提交。


最终全检查通过215项workspace Rust（Core175项、Auth11项）、53项工具测试；Rust1.85/最小Feature与Paho TCP/TLS、MQTT5 TCP/WebSocket专项通过。完整协议与发布独立批准仍缺失，严格门禁保持Partial并阻断完整发布，最终输入及原始日志绑定本轮独立目录。


## 58. 双向同号与立即过期收口后的剩余核对

[验证结论摘要](validation/README.md#protocol)确认187项输入与39份上轮材料摘要匹配，Core175项、Auth11项和工具53项复跑通过。上轮8个同号双向QoS2隔离及12个MQTT5零过期组合保持；R-19及V-04的关闭依据保持，未确认新的可复现功能缺陷。

完整F6/F7仍需跨段/逐义务/逐表行语义、其余状态/故障/客户端组合和真实独立实审。下一批可优先核对同号双向QoS2与提交失败/结果未知、部分ACK写中断和SIGKILL的交叉场景，以及MQTT5正TTL在未开始交付、已发PUBLISH/AwaitPubRec、AwaitPubComp阶段的跨到期/重启、剩余Expiry属性与Retained回放。现有同号矩阵是检查式重启，Expiry=0只证明立即过期；这些方向是验证缺口，不是已确认的产品缺陷，也不代表完整剩余组合的穷举。

当前14有限矩阵172组合、1060对象、844表行；MQTT311为437输入/190限定映射，MQTT5为607输入/281限定映射，真实协议及178发布项批准均0，整体Partial。Enhanced Authentication仍明确未支持，11项UnsupportedConditional保留声明范围内边界。V-01历史无阶段失败保留归因限制；用户保留的生产SLO、长期/代表性容量、外部宿主Profile、magicCommon、远端CI、平台/文件系统/硬件断电及独立安全/性能/接入/发布材料仍待真实验收。

上轮workspace215/MSRV/最小Feature/Paho与以往容量/1984项扩压仅作摘要核验的历史材料，本轮不冒称重新执行。本轮仅核对并维护状态文档和审计材料，未改产品代码、测试、契约或提交。


## 59. 同号双向QoS2故障交叉与正TTL收口

[验证结论摘要](validation/README.md#protocol)新增50个组合。双协议×两出站阶段×两修改方向×提交前失败/提交后丢回执可查询/提交与查询均不可确认共24组合，真实TCP/SQLite验证失败关闭与按实际落盘结果恢复，原ID/DUP/阶段/载荷及订阅标识保持、两方向独立、完成后ID重用和二次恢复无旧交换。单字节传输另补双协议×两出站阶段×部分PUBREC/PUBCOMP×Owner取消/EOF共16组合，重连恢复并在显式清理后归还全部会话/连接记账；首次夹具清理仍带60秒Expiry的错误已改为零Expiry，原失败保留，未误判运行时泄漏。

Unix子进程补双协议×两出站阶段共4组合，各两次真实SIGKILL（校验signal=9），首次恢复双向原交换及去重，完成后的二次崩溃验证ACK持久化不依赖正常shutdown，最终新消息继续。fixture在父测试内启动同测试二进制中的独立Host Application/SQLite，ready后提供地址，RAII回收，无生产运行时或公开钩子变化；不声称硬件断电或跨平台运行。

MQTT5另有未发送/AwaitPubRec/AwaitPubComp×首次重启前/后到期共6组合。真实正4秒TTL在到期前回放剩余1–2秒Expiry，未到期恢复分支校验绝对到期时间不重置；到期后未发送消息清除，已发QoS2继续按原ID/DUP重发或PUBREL完成，过期Retained不再回放，后续正常交付及二次恢复无旧积压。真实时钟测试不代替所有边界值、流控和平台组合。

新增验证代码在既有生产状态机上通过，未确认新运行时缺陷。当前18有限矩阵222组合、1064协议对象、844表行，MQTT311/MQTT5限定映射190/281项。完整跨段/逐义务/逐表行语义、其他状态/故障/客户端组合及真实独立实审仍待完成，F6/F7保持Partial，协议和178发布项真实批准0；Enhanced Authentication仍明确未支持。R-19/V-04保持关闭，V-01历史归因、用户保留的生产SLO和外部验收边界保持，不重跑旧容量/1984项扩压，未提交。


最终全检查通过workspace219项Rust（Core179项、Auth11项）及工具53项，无忽略项；Rust1.85/最小Feature与Paho TCP/TLS、MQTT5 TCP/WebSocket专项通过。首次全量Rust219项通过后，严格证据门禁拒绝ignored子进程fixture；已将入口合并为父测试的显式子进程分支，无放宽规则，完整检查重新通过，首次失败保留。最终输入与严格计划绑定本轮check-corrected.log，完整协议及发布仍Partial、0真实批准。
