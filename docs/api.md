# API 与回执

当前公开 API 的准确信息以 Rustdoc 为准，可运行 `cargo doc --workspace --all-features --no-deps --locked`。使用 Modorch 0.0.3 / git `336ba20196044e1b81fb2ca297a18b5e258a5800`、Skill Bundle 1.0.7，源码指纹 `94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。

## 注册与状态

`MqttBrokerBundle::new(config).register(&mut builder)` 返回 `MqttRegisteredComponents`。宿主显式选择 `module_ids()`；`handle()` 返回`BrokerHandle`，`local_addr()` 表示绑定地址，绑定成功本身不表示已监听。`status()` 返回本地瞬时 `BrokerStatus`，也可从同宿主 Hub 请求状态。

| Topic 常量 | DTO / 返回 | 访问语义 |
|---|---|---|
| `STATUS_TOPIC` | `()` → `BrokerStatus` | 定向 Core 的状态 Request |
| `PUBLISH_TOPIC` | `MqttPublishRequest` → `MqttPublishResult` | 定向 Core；只接受可信 Handler source |
| `PUBLISH_QUERY_TOPIC` | `MqttPublishQuery` → `MqttPublishQueryResult` | 同来源查询 request_id 终态 |

控制 Topic 位于 `$modorch/mqtt/control/`，网络客户端不能发布、订阅或将其用作 Will。订阅 ID 为 Application 全局唯一，多个 Broker 和扩展必须统一分配。

## 发布与幂等

`MqttPublishRequest` 包含 `request_id`、原 MQTT Topic、Bytes payload、`Qos`、retain 和默认空的 `MqttProperties`。request_id 限 1..128 UTF-8 字节且无 NUL；完整报文仍受 Core 大小和预算限制。可编译用法见 [业务 Handler 示例](../examples/existing-modorch-server/src/main.rs) 与 [EventHub 说明](eventhub.md)。

`MqttPublishResult` 为 `Result<MqttAcceptReceipt, MqttPublishRejection>`。成功回执的 `MemoryOnly` 表示在当前 Factory 状态中接纳；启用 Storage 后 `DurableCommitted` 表示在所选事务策略下提交。二者都不保证订阅客户端已收到或确认，也不保证跨业务系统 exactly-once。

账本以 `(source ComponentRef, request_id)` 隔离。同源同 ID 同内容返回原终态，不重新匹配订阅；异内容返回 `IdConflict`。前置校验/账本预留失败不建立记录，已建立终态不自动淘汰。满额拒绝新 ID；持久化时终态跨进程恢复。查询结果为 None 不能否定并发队列中的原请求。

## 超时与错误

Hub Request 的准入成功不等于业务成功或磁盘提交。Queued 超时表示 `CanceledBeforeStart`；Running 超时可能为 `Indeterminate`，不能推断回滚或以新 ID 安全重发。优先查询原 request_id，必要时用同源同 ID 同内容重试。

Core 的 `MqttError::Storage { operation, failure }`保留固定操作名和脱敏的`StoreCallFailure`：Admission、CanceledBeforeStart、Indeterminate、Transport、Provider(StoreError)或RecoveryDeadline。不附带SQL、路径、capability、image或业务标识；RecoveryDeadline标识整个预监听恢复超时并保持入口关闭；提交结果未知时仍先查询原operation_id/digest，无法确定则关闭本代，不能将诊断类别当作回滚证明。

Core 用 `MqttError` 区分配置、协议、授权、容量、存储与连接错误；内部发布使用独立的 `MqttPublishRejection`。MQTT3.1.1 线上错误遵循具体控制报文：CONNECT 明确 ID 拒绝使用 CONNACK 2，不可用使用 3，授权拒绝使用 5；无法归类时关闭连接。非法报文通常关闭当前连接，不能把任意内部错误转换成成功 ACK。

## 扩展接口

Auth DTO 在 API crate 中，通过指定 Provider 完成连接认证、订阅/发布授权和 Will Grant。Storage 使用 `StoreEnvelope/StoreResult` 以及宿主私有 capability；业务组件不能取得该 capability。Admin 提供有界分页/统计/条件断连与内存审计；Metrics 提供周期采样和固定标签文本，均没有默认 HTTP 入口；可显式装配独立 HTTP Module 和持久审计。装配见[安全与运维](security-and-operations.md)、[存储协议](storage.md)。


## TOML装配

启用Facade `toml` 后，`MqttTomlConfig::parse(input, config_dir, storage_access)`只解析/校验；`core_config()`检查当前设置，`module_ids()`返回Core与声明启用的外部组件引用。`register(&mut builder)`仅注册Core：Core禁用返回None，启用返回MqttRegisteredComponents，宿主再显式选择其module_ids。Provider须由宿主另行注册。详情见[TOML配置](toml.md)及[可运行装配测试](../crates/modorch-mqtt/tests/config.rs)。

MQTT5 metadata、新宿主适配、Bridge、HTTP 与动态更新 API 见[可选功能](optional-features.md)。
