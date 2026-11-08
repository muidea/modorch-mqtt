# SQLite 持久化接入（QoS0/1/2 实验版本）

Storage 是独立可选 Module，Core 仅依赖 API 契约。未配置时保持内存行为；配置后，可靠状态、Retained、Will、会话归属和内部发布幂等账本经 SQLite 事务提交，确认后才产生协议成功回包或 `DurableCommitted`。该回执不保证客户端收到/确认，QoS0 队列仍为 best effort。

## 显式装配

```toml
modorch-mqtt = { path = "../modorch-mqtt/crates/modorch-mqtt", features = ["storage"] }
```

可编译的宿主装配见 [durable-broker](../examples/durable-broker/src/main.rs)。宿主生成随机32字节 `StoreAccessKey`，分别配置 `SqliteStorageConfig.access` 和 Core 的 `StorageProviderConfig.access`，注册 `SqliteStorageFactory` 与 Core Bundle，再显式选择两个组件。Core 的 descriptor 声明 required Provider；注册、编译 Feature 都不自动启用。Auth、TLS 与 Storage 可独立组合；示例只显式开放匿名回环开发模式。

```bash
# 目录由宿主准备；一个数据库只能交给一个 Broker 实例使用
cargo run -p durable-broker --locked -- /tmp/mqtt-broker.sqlite 127.0.0.1:1883
# 真实子进程崩溃与恢复验证，数据库由脚本在临时目录创建
cargo build -p durable-broker --locked
python3 scripts/verify-storage.py
```

示例配置32连接、64KiB报文、4MiB快照、64MiB磁盘额度、4096字节恢复页，单请求2s、总恢复30s。Facade 的 `storage` Feature 只提供 SQLite 实现；Core 的存储配置/API 不强制依赖此实现，其他 Provider 须遵循相同协议。

## 接口与信任边界

`STORAGE_TOPIC = "$modorch/mqtt/storage/v1"`，精确 Destination 为 Provider 的 ComponentRef，只接受 `Request<StoreEnvelope, StoreResult>`。Modorch 的网络/生命周期根没有可信 Handler source，所以所有请求都校验宿主生成的 capability，包括具有组件 source 的请求。`StoreAccessKey` 的 Debug 脱敏，禁止全零。它不随数据库保存，重启由新宿主重新生成并共享即可。

capability 仅交给 Core 与选定 Provider；不放入 MQTT/HTTP DTO、日志、配置展示或业务 Handler。它不是进程内恶意代码隔离机制，宿主仍需保证组件可信。普通业务只使用 Core 的发布接口，不直接操作 Store。数据库保存消息 payload、PrincipalKey、Grant 和业务结果，**没有静态加密**；目录权限、备份与文件系统由宿主负责。

| Request | 语义 |
|---|---|
| Begin { recovery_id } | 原子推进 writer epoch、关闭写屏障并固定最新 image；同一未完成尝试可重试 |
| Page { epoch, offset, max_bytes } | 按字节读取固定 image，返回 epoch/version/offset/data/done |
| Complete { epoch } | 解除当前恢复屏障，可重复；已 Abort 的 epoch 不能重新开放 |
| Abort { epoch } | 失败恢复保持关闭，不删除 image、不重新开放旧 writer；已 Complete 后迟到的 Abort 不关闭新 writer |
| Commit { epoch, expected_version, operation_id, digest, image } | 校验 epoch/CAS/SHA256，原子替换完整 image 与当前操作回执 |
| Query { epoch, operation_id, digest } | 恢复当前操作的确定结果；同 ID 异摘要冲突，未知返回 Unknown |

这里的 `expected_version` 是整个 Broker image 的 revision，事务粒度比逐 Session CAS 更粗；Session epoch/revision 也保存在 image 并在恢复时校验。Core 每次确认当前操作后才开始下一次，因此 Provider 只保留一个最新 Store 操作回执，避免无界增长；更旧请求即使查不到也不能绕过 CAS。业务发布的 `(source, request_id)` 终态账本则完整保存在 image 中，受 `max_publish_records` 限制且不自动淘汰，二者不是同一账本。

## 提交、恢复和失败

等待 gate/提交回执时，连接任务仍检查 CONNECT、KeepAlive、身份和整帧写入期限。到期先关闭 TCP/TLS；已获得的事务继续确定结果，不取消 Commit，也不发送迟到 ACK。任务、连接许可和预留内存保留到实际退出；事务结果未知时仍按查询/失败关闭规则处理。等待 gate 尚未获得事务时可直接取消，迟到请求不能接纳新数据。超时会话的延迟 detach/Will 由后续受控事务处理，慢存储会延迟 Will 发布。实测边界见[连接期限验证](validation/2026-11-08-f6-deadlines/README.md)。


Factory 持有异步事务 gate，协调协议状态变更、下行选择、内部发布、Will 与延迟断连；同步 State mutex 不跨 await。编码/摘要通过宿主 CPU Scheduler，SQL 通过 blocking-I/O Scheduler，等待 `wait_finished`。每次可能改变可靠状态的操作生成有界 JSON image，摘要不变时跳过磁盘写。配置启用后所有变更使用同一 writer epoch 和递增 image version。

成功 CONNACK/SUBACK/PUBACK、下行及入站观察必须等相关提交确认。下行先保存 Packet ID/ever_sent，再写 socket；崩溃可能使实际未写出的消息恢复为 DUP，这属于 QoS1 允许的重复。可靠目标与 Retained 的全量变更作为单事务提交，失败不能部分外发或先 ACK。

Commit 的 I/O 错误或 Hub 超时均可能发生于实际提交之后，Core 查询同 operation_id/digest；Unknown 才重发同一请求。仍无法确认时将本代状态标记不可用，停止入口和连接，由监督恢复最后 committed image。不能用新操作 ID 猜测重试，不能用空数据库掩盖错误。

恢复在 `activate_ingress` 中进行，**完成恢复和 normalization 提交后才 listen**。Core 验证全部页面的 epoch/version、精确偏移、进度、结束标记、长度、SHA256，再解析 format1/format2（旧版）或当前 format5 image 并核对 Broker identity、配额账本和引用。image 含离线 Session 的 owner、epoch/revision、全部订阅、可靠 Pending/Inflight、Packet ID/ever_sent/发送握手阶段和接收端 QoS2 账本、Retained、armed/triggered Will 及保存的 Grant/冻结目标、业务发布终态。

所有持久订阅在客户端重连前已重建，可继续接纳离线 QoS1。失去 TCP 的持久会话恢复为离线，Clean Session 数据清理；其 Will 单独恢复。armed Will 在监听前按已保存授权收尾，可靠目标满额则保留待发。恢复校验、预算或期限失败均不开放端口，通过生命周期因果 Hub 发 Abort 补偿，不删除原 image。缩小额度若不足容纳恢复状态，启动失败，不淘汰已承诺消息。

## 存储与资源策略

本版本采用 **SQLite synchronous=FULL + DELETE rollback journal + exclusive locking**，不是 WAL。每个数据库只保存一个 Broker 的有界完整 image 和元数据，只允许单连接独占；其他实例不能同时写。同一稳定 Broker name 才能重用数据库。拒绝 `:memory:`、`file:` URI、不相关数据库和非4096字节页数据库。

`max_snapshot_bytes` 范围1KiB..64MiB，Core 与 Provider 应配置相同上限。恢复页1..1MiB，支持业务记录跨字节页，不按每页业务条目数计数。Core 每次恢复/编码预留 `4 * max_snapshot_bytes` 作为 DTO、编码、传输与 scratch 的保守逻辑预算；取消时 CPU 任务继续占用的预留随任务实际退出归还，连接延迟断连记录继续持有原许可/内存，不能无界增长。这些预留不是进程 RSS 硬上限；库/分配器/SQLite 页缓存等额外开销仍需测量。

`disk_budget_bytes` 至少 `3 * (max_snapshot_bytes + 1MiB)`、最大1TiB。Provider 设置4096字节页及 `max_page_count = floor((disk_budget_bytes - 1MiB) / 3 / 4096)`，为主库、rollback journal 和开销留余量；SQLite 临时数据放内存，不引入无限 WAL。主库页限触发 Capacity，提交不确认。此额度是 Provider 控制的文件包络，不预分配磁盘空间，不涵盖宿主备份/文件系统快照/外部写入；文件系统本身仍可能先满。适用于可靠支持 SQLite 锁和 fsync 的本地文件系统，未验证网络文件系统或任意存储设备的断电语义。

Host Hub 的请求字节上限须容纳 `max_snapshot_bytes + 512`，恢复响应和总在途预算也须适配。宿主任务配额须在 Listener/连接之外为 Core 编码与 Storage I/O 留额度；准入失败关闭本代，不降级成内存 ACK。全量编码/提交串行且开销与总状态相关，维护 tick 也可能编码未变状态以核对摘要；尚无持久化生产吞吐、恢复耗时或容量 SLO。

## 停止与可观测性

Modorch 0.0.3 在停止阶段取消新调度的 CPU/I/O 任务，teardown 后关闭准入。Core 停止入口并等待连接实际退出后，只做本地断连收尾，不新建关键 durable promise。停止前已经提交的 armed Will 由下一次启动处理，再持久化后 listen；**停机导致的 Will 可能延迟到重启**。运行期正常客户端 DISCONNECT 会在事务中持久清除 Will，不能把 broker 停机等同于客户端正常 DISCONNECT。

Provider 清除任务准入句柄，等待已有 SQL job 释放连接锁，再释放空闲 autocommit DELETE-journal 连接；不在 teardown 后补建 checkpoint/提交任务。检查式退出证明资源排空，不承诺停机新状态已经落盘。若需要关机时立即持久发布所有 Will，需另行设计框架停止前的显式排空阶段。

BrokerStatus 和 Metrics 增加实际存储提交成功/失败数、耗时微秒总和与最大值。计时从取得事务 gate 到确认/失败，包含编码和 Hub 往返，不包含 gate 排队；不是 fsync 专项测量，也不持久化指标。Admin 审计仍为内存环，与 Store 的可靠状态分开。

[Storage 验证记录](validation/2026-11-07-f5-storage/README.md)覆盖故障注入、真实 SQLite 页限 DiskFull 和真实子进程 SIGKILL。SIGKILL 不等于真实硬件断电验收。QoS2 持久恢复的补充记录见 [F6 验证](validation/2026-11-07-f6-qos2/README.md)。完整 MQTT3.1.1 符合性、MQTT5、WebSocket、Bridge、跨平台、生产性能和独立 Agent/发布门禁仍未完成。

当前写入快照版本为 format5，包含 QoS2 接收账本、配额、下行 Release 阶段及其发送顺序（可靠队列相对位置）。读取旧 format1 的 QoS0/1 数据，以及每个持久 Session 至多一个未完成 Release 的 format2 数据，恢复后在监听前提交 format5。format2 同一持久 Session 有多个 Release 时缺失历史顺序，返回 `legacy QoS2 release order` 并保留原数据、不监听；需用旧版先排空相关交换，再停机升级。旧二进制不能读取 format5，不能原地降级。兼容测试覆盖旧字段升级、歧义拒绝、失败不替换原状态和非法阶段/配额拒绝；升级前备份由宿主负责。补充证据见[符合性验证](validation/2026-11-07-f6-conformance/README.md)。

format4 增加 MQTT5 Session/订阅选项、消息属性、过期期限、延迟 Will 与快照时间；继续读取 format1/2/3，format2 多 Release 的歧义限制不变。升级前备份，不能原地降级。MQTT5 崩溃过期时间与维护检查点见[可选功能](optional-features.md)。Rust 公开发布 DTO 的结构体字面量需补 properties 默认值。

内部发布幂等记录的properties同样参与恢复校验、报文大小及内存预算；恢复后原请求仍幂等，异属性请求冲突。非法属性、内部自带订阅标识或不足以保留完整账本的配置使恢复失败，端口不开放，保留原存储。相关回归见[遗留项修复](validation/2026-11-08-remaining-fixes/README.md)。

format5新增逐Retained Topic的来源账本，值为发布ClientID、Internal或LegacyUnknown。ClientID长度/UTF-8和Topic引用一一对应受检，来源额外逻辑预算为32字节加ClientID UTF-8字节数，恢复与接纳保持一致；账本缺失/孤立/非法值拒绝恢复并保留原状态。旧format1–4按LegacyUnknown迁移并在监听前提交format5；No Local订阅跳过未知来源，普通订阅不变，重新发布可确定来源。来源独立于Session存续，也适用于QoS2和恢复Will；旧二进制不可读取新格式，升级前备份且不能原地降级。

跨协议持久会话恢复请求被拒绝，显式Clean Session/Clean Start可换协议新建会话。恢复消息校验兼容混合监听中已接纳的3.1.1上限包长，保留真实MQTT5属性校验；订阅标识属于目标下行元数据，不反向计入源报文上限。新修复避免继续产生R-05异常快照，不自动修复旧版本已写出的协议/订阅选项不一致数据；遇到此类历史损坏仍保留原库并拒绝开放入口。专项材料见[组合边界修复](validation/2026-11-08-followup-fixes/README.md)。


Topic Alias展开后的完整Topic可使存储消息大于入站max_packet_bytes；该值约束入站线帧。恢复网络消息时允许合法别名源表示，但别名绑定帧和消息帧都必须能在上限内表示，真实属性及变长长度字段均计入；完整Topic/属性仍保存并计入预算，不持久化别名。Will、内部幂等记录及已知Internal Retained不能套用此界限。format5不变，旧format4/5已接纳的合法别名消息也可恢复。下行仍受完整Topic编码及目标/监听上限约束，详见[修复验证](validation/2026-11-08-alias-fixes/README.md)。
