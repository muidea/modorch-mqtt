# 运维与验证

当前为实验版本，完整设计与生产发布状态仍为 Partial。依赖 Modorch 0.0.3 / git `336ba20196044e1b81fb2ca297a18b5e258a5800`、Skill Bundle 1.0.7；已解析源码指纹为 `94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。

## 启动和资源

启动前校验 [Core 配置](configuration.md)、宿主 Scheduler 任务配额和 Hub 字节准入。逻辑内存账本不等于 RSS；Listener、每个连接、维护、编码和 Storage I/O 共同消耗宿主任务资源。扩展的 Feature、注册、Core Provider 配置和 Selection 缺一不可。

Storage 目录须预先存在，同一数据库只允许一个 Broker 独占。容量、解析、身份、摘要或恢复失败时保留原数据库并停止监听；不要删除数据库或切换内存模式来消除失败。使用与生产分离的临时目录运行故障测试。

## 观察与诊断

`BrokerHandle::status()` 用于瞬时状态；Metrics 周期样本具有采样时间和失败数，不能视为当前实时状态。固定名字指标不包含 ClientID/Topic 标签。Storage 提交耗时包含 gate 取得后的编码、Hub 往返与提交确认，不包含排队，也不是单独 fsync 延迟。

宿主决定日志 Subscriber、持久审计和 HTTP 出口。默认 Admin 来源白名单为空，内存审计有界且重启丢失。日志不要包含密码、私钥、StoreAccessKey、完整认证请求或消息载荷；排错可记录操作 ID、错误类别、代际和非敏感统计。

| 现象 | 处理 |
|---|---|
| 无法建立连接 | 检查启动错误、恢复是否完成、监听地址、IP/连接/任务额度及 TLS 配置 |
| Pub/Sub 被拒绝或断开 | 检查 ACL、保留命名空间、报文大小、会话/可靠队列/全局额度 |
| MQTT 成功但业务观察缺失 | 检查 Hub Topic 本地预算、邮箱和字节准入；观察为 best effort |
| Storage 操作超时 | 保留原 operation_id/digest，由 Core 查询消除不确定；不能自行更换 ID 盲重试 |
| `legacy QoS2 release order` | 使用旧版先排空同 Session 的多个 Release，再停止并备份升级 |

## 升级、备份和退出

检查式停止并确认数据库资源释放后，再由宿主制作离线备份。当前写 format5；format1/3/4 可升级，format2 每持久 Session 至多一个 Release 可升级，多个 Release 拒绝歧义迁移。旧二进制无法读取 format5，回退应使用升级前备份，期间新接纳数据不能凭空恢复。

宿主收到退出信号后必须等待 `shutdown_checked` 并检查结果。超时、Forced 和 StopIncomplete 均不表示正常排空。存储模式下停止阶段不新建关键 durable promise，已提交 armed Will 可能在下次启动时发布；需要持续运行的接收方应将此延迟计入设计。

[开发样例退出策略](magicdatahub-integration.md)在启动前安装信号处理，并等待运行失败；StopIncomplete 使用同一应用继续排空。`scripts/verify-existing-host.py` 实测 SIGINT/SIGTERM、成功回执和端口释放，已接入 check.sh。正式服务由 magicDataHub 交付。

## 检查与发布状态

```bash
./scripts/check.sh
cargo +1.85.0 check --workspace --all-targets --all-features --locked
cargo check -p modorch-mqtt --no-default-features --locked
python3 .agents/skills/modorch-module-development/scripts/preflight.py --manifest-path Cargo.toml
```

`check.sh` 检查格式、编译、Clippy、测试、Rustdoc、真实宿主和 SIGKILL 恢复，并从实际测试日志核对规范场景。共享 Topic 向量、忽略/缺失/失败日志的反向断言都进入该流程。[CI](../.github/workflows/check.yml)在push、PR和手动触发时执行，固定Ubuntu24.04、Rust1.94.0和Python3.12，另安装Rust1.85.0检查MSRV，并运行Paho TCP/TLS及MQTT5/WebSocket互操作。Action使用固定发布commit，日志进入CI artifact；开发Partial清单不作为完整发布批准，工作流存在不代表远端已运行通过。

开发检查允许报告 Partial；完整规范门禁使用相同日志并加 `--require-complete`：

```bash
python3 scripts/check-conformance.py --results CHECK_LOG --output SCENARIO_REPORT --require-complete
```

将大写参数替换为实际文件路径。当前该命令应返回非零，因为未完成完整规范审计；不能将开发检查成功视为发布批准。详细缺口见[剩余需求](remaining-work.md)。SIGKILL 不等于硬件断电；历史 loopback QoS0/1 性能不能用于推断 TLS、持久化、QoS2、长期 RSS 或生产恢复 SLO。

显式HTTP管理/指标、持久审计、Auth/TLS更新与Bridge运维见[可选功能](optional-features.md)。接入新功能前按format5迁移说明备份；Bridge当前为TCP并在源端保留可靠积压。

持续流量、慢消费者、负载中退出及原库恢复测量见[本机基线](load-baseline.md)；完整协议评审材料导入见[评审流程](protocol-review.md)。这些本机材料不自动批准生产SLO。

## 连接阶段诊断

定位握手EOF或存储压力等待时，可在启动宿主前显式设置`MODORCH_MQTT_DIAGNOSTICS=1`。默认关闭；每条连接使用固定大小追踪状态，包含在既有连接预算内，不新增任务。stderr记录进程内数字追踪号、固定阶段、实际耗时和脱敏原因；阶段切换超过250ms也记录前一等待阶段。测试构建额外标出测试线程名。无客户端身份/地址、Topic、载荷、密码、capability及原始IO/配置字符串。

诊断属于临时定位输出，不提供稳定日志API，不代表延迟SLO；stderr接收端必须能及时消费。未增加重试、未延长认证/握手/存储期限。五秒Authenticate超时仍拒绝当前连接，迟到Provider结果不得建立Session；可靠提交一经开始继续按原operation_id确认，不能为及时返回PING取消已接纳承诺。无状态变更的PING与空闲出站检查不进入持久事务；同连接的PING继续排在先前可靠报文提交之后。证据及历史归因限制见[验证结论摘要](validation/README.md#closed-issues)。
