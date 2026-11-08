---
name: modorch-mqtt-development
description: "在宿主 Modorch 应用中接入或修改本项目 MQTT Broker、配置可选扩展、验证 Topic/持久化/生命周期及更新能力证据时使用。"
compatibility: "Rust >= 1.85; Modorch 0.0.3, Skill Bundle 1.0.7"
metadata:
  version: "1.0.4"
  author: "modorch-mqtt maintainers"
---

# MQTT Broker 开发与接入

## 适用范围

用于 modorch-mqtt 的集成、实验功能开发、扩展配置、故障验证和能力文档维护。先定位目标工程与本库源码；消费方无需复制整份 Broker 仓库。本文描述当前实现，完整目标由源码仓库的设计文档维护。

## 兼容预检

1. 读取目标工程 Cargo.toml、Cargo.lock 和实际启用的 Feature。当前 Broker 为 `0.1.0-alpha.1`，Rust 最低 1.85 / Edition 2024。
2. Host 与 Broker 的 Modorch 必须解析为同一个来源：`https://github.com/muidea/modorch-rs`，git rev `336ba20196044e1b81fb2ca297a18b5e258a5800`，版本 0.0.3，Contract 1.2.0 / Skill Bundle 1.0.7。当前解析源码指纹 `94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。
3. 使用同一项目安装的 `modorch-module-development` / `modorch-app-development` 1.0.7 执行其 `scripts/preflight.py --manifest-path Cargo.toml`，从 Cargo metadata 校验实际 API/指纹；不要依赖相邻仓库或把版本字符串相同当作身份一致。
4. 预检不匹配、工具缺失或离线依赖不全时，记录实际限制，避免依赖未知框架 API 的实现。普通可逆改动按用户授权继续，不增加例行审批。

## 装配与实现规则

- 已有宿主只复用其 ApplicationBuilder、EventHub 和 Scheduler。普通 Tokio 入口自行创建一个 Application，再采用相同模式。Bundle 只注册 Core Factory；宿主将返回的 module_ids 加入 ComponentSelection，再统一启动并检查式退出。
- Cargo `auth/tls/admin/metrics/storage/toml/mqtt5/websocket/bridge/bootstrap/audit/http` 均默认关闭。Feature 编译、Factory 注册、Core Provider 配置、Selection 显式启用分别处理；不要默认激活扩展。订阅 ID 是 Application 全局唯一。
- `toml`只解析独立64KiB Core配置，默认禁用、未知/不支持字段拒绝且错误脱敏。Provider引用不注册Factory；宿主注入Store capability、注册Provider并显式选择。相对TLS路径由宿主提供基目录，不在解析器内读文件或创建Runtime。
- 网络入口只在相关 ready 和持久恢复完成后 listen；监听、连接、维护和 CPU/I/O 工作使用宿主管理的任务。保留旧代际 fencing，禁止 detached 后台任务或锁内等待 I/O。
- MQTT Topic 与 Hub 的语义通过 contracts/topic-vectors.json 核对。`+` 含空层、末尾 `#` 含零层、根通配排除 `$`，`:id` 是字面量。MQTT3.1.1 的 `$share` 不是共享订阅；MQTT5 显式启用后专有解析共享组及订阅选项，不能仅依据监听配置把3.1.1过滤器解释成共享组。
- 入站 MqttInbound 是 best effort 观察，其失败不决定 PUBACK。出站只用可信业务 Handler 的显式定向 Request；任意 Event 不自动出网。
- 发布幂等键为 `(source, request_id)`。Running 超时可能 Indeterminate，先查原 ID，再同源同 ID 同内容重试。MemoryOnly / DurableCommitted 均不代表客户端已完成接收或业务 exactly-once。
- Storage capability 仅共享 Core 和 Provider，不输出密码、私钥或 capability。相关提交确定后才 ACK/下行；未知提交用原 operation_id/digest 查询，失败不开入口、不降级空状态。
- 当前写 format5，兼容读1/2/3/4；format2 同 Session 多个 Release 拒绝歧义升级，须旧版先排空；不得直接降级。存储模式停机 Will 允许延迟到重启，不能宣称停机立即落盘外发。

- `bootstrap` 在调用方Runtime上明确创建一个Application或复用Builder，不创建Runtime/隐藏信号；必须检查式退出。
- `mqtt5=true` 需编译Feature；TOML protocols允许[3.1.1]或[3.1.1,5.0]。属性、Session/Message Expiry、延迟Will及订阅选项写format5；完整5.0符合性仍未批准，Enhanced Authentication不是当前认证机制。
- Session按协议隔离恢复；跨3.1.1/5.0且不清理的CONNECT拒绝并保留原所有权。切换协议须显式Clean Session/Clean Start重建，不能把旧订阅选项带入另一协议。旧版已写出的协议/订阅选项不一致快照不自动猜测修复，保留原库并拒绝开放入口。
- format5记录Retained发布来源并计入预算，No Local同时过滤实时与保留回放；旧format1–4缺失来源时迁为LegacyUnknown，普通订阅仍回放，No Local保守跳过，重新发布后采用新来源。不能恢复旧格式已经丢失的发布身份。
- max_packet_bytes约束入站线帧。持久恢复网络消息允许合法Topic Alias的压缩表示，但别名绑定帧与消息帧都须能在该上限内表示；别名仍只在连接内，存储保留完整Topic/属性并完整计费。目标附加订阅标识不计入源线帧，真实属性仍受校验；内部发布与Will不能套用别名放宽。MQTT5下行继续按完整Topic编码及目标/监听限额判断，超限丢弃不阻塞后续交付。
- WebSocket为每Core单监听的显式入口适配，严格路径/mqtt子协议/Binary/缓存期限；可与TLS组合WSS。accept和维护各占一个受管任务，维护等Store不能阻塞accept。
- 网络Bridge独立Module、MQTT5 TCP客户端、显式一/双向前缀与账号ACL、Origin/Hop防环；每方向一个在途，目标PUBACK后确认源，可靠积压留源Broker，跨崩溃允许重复。加密由宿主显式TLS代理提供，不把best-effort观察当可靠源。
- Bridge Transfer绑定源Session代际；源Session Present=0时旧目标工作继续，完成后不向新Session发送旧ACK。每方向一个在途，新Session的订阅等待旧工作完成；恢复Session可沿用原ID/ACK。断线时再次清理请求队列，不能把清理后排入的旧ACK带入重连。新订阅建立前的发布不能依赖已丢失Session的订阅积压，需由源业务保留/重发。
- Auth策略replace严格递增revision；TLS reload校验后原子替换且不影响现有会话，预期加载错误不能报告组件故障。已接纳可靠交换/Will沿用原接纳语义。
- 持久Admin审计独立SQLite、intent先于转发、outcome记录结果未知边界及滚动行/页上限。HTTP独立Module只回环Bearer认证、经可信Admin转发，Header/分页/JSON/并发和绝对期限有界，不能在TOML存Bearer或Bridge凭据。

## 验证与交付

在 Broker 仓库运行 `./scripts/check.sh`；增加或修改3.1.1协议行为时更新 contracts/mqtt311.json 的精确测试名，可选功能更新 contracts/optional-functional.json，并检查真实 `test ... ok` 日志。公共 API 示例应可编译，Rustdoc 的构建和 doctest 分别执行。新增外部客户端使用隔离虚拟环境运行 scripts/verify-paho.py 和 scripts/verify-paho-tls.py，构建对应示例后再执行；MQTT5/WebSocket另运行scripts/verify-paho5.py（standalone需编译mqtt5与websocket）。

执行 `cargo +1.85.0 check --workspace --all-targets --all-features --locked` 和 `cargo check -p modorch-mqtt --no-default-features --locked` 核对兼容边界。首次下载依赖按宿主策略执行，缓存完整后使用 `--offline`；不要改全局 Cargo 配置。

每轮保留独立原始日志、预检结果与输入摘要；不要改写旧验证目录。场景通过只标 TargetedEvidence；当前完整规范和发布证据仍为 Partial。`scripts/check-conformance.py --require-complete` 必须阻止把 Partial 当成完整发布；使用 `scripts/check-release.py` 对照项目MUST、规范原文及发布策略核对本轮输入摘要、原始日志与独立评审，默认严格阻断缺项；`--allow-partial`只用于开发清单检查，不是发布批准。补全真实评审/执行前不得升级；不声明未执行的远端 CI、跨平台、硬件断电、性能 SLO 或独立 Agent 验收。

测量TLS/Auth/SQLite及QoS2时使用显式组合；secure示例的`--demo --database PATH`启用Storage。保留测试凭据/证书的实验边界和32连接/IP限额，短loopback测量不能替代长期RSS、慢消费者、高负载退出或生产SLO。

报告说明具体变更、已执行命令与结果、材料路径、剩余需求。源码/API/能力边界改变时同步 README、对应文档、契约和能力矩阵。框架相关报告遵循已安装 Modorch Skill 的结果 Schema。

## Formatter

Markdown/YAML 使用稳定标题、列表和标量 frontmatter。归档前运行 `skill-hub validate --pattern modorch-mqtt-development --links`。Rust 使用 `cargo fmt --all`；Python 按项目四空格和无尾随空格风格维护，Shell 运行 `sh -n scripts/check.sh`。本 Skill 不携带脚本或依赖仓库外相对资源。
