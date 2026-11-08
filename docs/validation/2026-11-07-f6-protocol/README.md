# F6 发布、订阅与会话协议收口验证（2026-11-07）

本轮修复CONNECT失败的返回码分类，并补充设计F6的发布/订阅/Retained/Will/Session规范场景。完整设计和发布状态仍为 **Partial**。范围见[符合性说明](../../mqtt311-conformance.md)、[规范映射](../../../contracts/mqtt311.json)及[设计§23](../../modorch_mqtt_broker_full_design_0.0.3.md)。历史验证目录未修改。

## 实现与覆盖

CONNECT失败原先除容量/授权等少数情况外统一使用Identifier rejected(2)。现在只有明确ClientID拒绝使用2，存储不可用属于Server unavailable(3)，未知错误按MQTT-3.2.2-6直接关闭且不发CONNACK。已分类错误在发送前记录一次，未分类错误交给连接任务统一记录，避免重复计数。协议级别不支持的CONNACK1处理保持。

新增15项Rust测试：13项真实TCP协议测试、Packet ID回绕状态测试、CONNECT错误线帧分类测试。覆盖：

- 九种发布/订阅QoS组合、二进制载荷、入站DUP不传播、QoS0无ACK/ID；混合SUBACK的原ID和逐项返回码顺序、重复过滤器按序替换。
- QoS2→0→1 Retained替换、live/replay RETAIN标志、普通及空的非Retain消息不改旧值、空Retain删除且向既有订阅者正常转发。
- UNSUB不丢已开始的QoS1/2交付、新发布不再匹配；CleanSession接管清理订阅/双向QoS2，Retained独立保留；离线持久会话继续为新的QoS1/2入队。
- 16组Topic真实匹配、65535字节多字节UTF8 Topic/Filter、各CONNECT/过滤器字段的非法UTF8、密码/Will二进制、Will六种QoS/retain组合、非法多过滤器报文不部分安装/删除。
- 强制Packet ID从65535回绕、跳过未完成QoS1/2和Release、正确ACK后才复用；同时上下行ID1互不影响。
- 错误协议名关闭、超长ClientID返回2、禁止Will返回5、离线Session满返回3；存储不可用/未知错误另核对完整线帧分类。没有把存储故障注入结果误写成真实网络收到CONNACK3。

## 最终执行证据

| 检查 | 结果与原始日志 |
|---|---|
| fmt、全workspace/all-target/all-feature check、clippy -D warnings | [check.log](check.log)，成功 |
| Rust tests / doctest | [check.log](check.log)：92项Rust（Core78、Auth6、Extensions4、Storage3、rumqttc1）+1项doctest |
| 规范日志校验器 | [check.log](check.log)的6项Python测试，成功；新增适用性集合/类别校验，角色说明不把NotRun变为Passed |
| 规范场景精确执行匹配 | [scenarios.json](scenarios.json)、[matrix.log](matrix.log)：31组场景Passed；128个编号TargetedEvidence，12个编号NotRun并附角色/可选能力依据；完整符合性Partial |
| Rustdoc -D warnings、既有/安全宿主smoke | [check.log](check.log)，成功 |
| 真实进程SIGKILL、检查式退出 | [check.log](check.log)末尾3条PASS：2次SIGKILL/3次启动，离线路由、QoS2去重/两阶段/ID及端口回收 |
| Rust 1.85 MSRV | [msrv.log](msrv.log)，最终源码全workspace/all-target/all-feature成功 |
| 无默认Feature与三个示例重建 | [feature-build.log](feature-build.log)，成功；all-feature组合已由check.log覆盖，Feature接线未变 |
| 固定Modorch版本/Feature/逐文件指纹 | [framework-preflight.log](framework-preflight.log)，成功 |
| 独立Paho TCP / TLS密码ACL | [paho.log](paho.log)、[paho-tls.log](paho-tls.log)，QoS0/1/2及检查式退出成功 |
| Python/Shell语法、diff、链接、输入摘要 | [auxiliary.log](auxiliary.log)、[source-hashes.json](source-hashes.json)、[environment.json](environment.json) |
| 报告Schema | [result.json](result.json)、[report-validation.log](report-validation.log) |

所有Cargo命令使用任务缓存 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo` 与locked/offline，框架仍固定Modorch0.0.3/rev336ba20196044e1b81fb2ca297a18b5e258a5800。无依赖或上游修改；Cargo.lock和根Cargo.toml对上一轮不变。摘要覆盖累计未提交源码，排除所有验证目录；环境与基准提交见environment.json。本轮未提交或推送。

## 诊断记录

[protocol-first.log](protocol-first.log)保留新测试首次编译时误用retained_messages字段的诊断，已改为真实BrokerStatus.retained；[core-first.log](core-first.log)随后73项Core成功；[protocol.log](protocol.log)为扩展至11项TCP场景时的成功中间运行。[check-first.log](check-first.log)保留Clippy对测试模块位置的失败，已移至文件末尾；[check-before-error-accounting.log](check-before-error-accounting.log)为错误分类前后计数整理前的92项成功中间运行。最后统一错误计数出口后重新完整check，最终以check.log为准，不用过滤测试或中间成功代替最终回归。

## 尚未验收

140个官方编号清单不包含所有未编号MUST。128个编号仅关联所述轨迹，不能推断完整规范条款或完整3.1.1全部Passed；未编号要求、规范表格/FSM全面审计和更广故障链路仍待完成。12个未映射项逐条注明7项客户端职责、4项可选WebSocket、1项条件性Bridge，并继续NotRun。

MQTT5、可选WebSocket/Bridge、生产吞吐/恢复SLO、真实断电、跨平台和独立Agent发布门禁未完成。本次只有Linux/WSL2 x86_64，SIGKILL不等于硬件断电。format3升级限制和SQLite停机Will延迟恢复边界保持，详见[Storage](../../storage.md)。

## 复现

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
python3 scripts/check-conformance.py --results docs/validation/2026-11-07-f6-protocol/check.log --output /tmp/modorch-mqtt-protocol.json
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

其他机器使用自己的Cargo缓存和Paho虚拟环境；脚本不自动安装依赖或修改全局配置。
