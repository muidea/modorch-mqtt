# 遗留项复核（2026-11-08）

本轮核对设计§7、§8、§11–§15、§30与当前源码、契约及上一轮证据，重点检查新增MQTT5/Bridge/format4的组合边界。确认至少4项功能遗留及1项契约描述同步工作，修正此前“只剩正式验收”的结论。本轮没有修改产品Rust代码，没有提交或推送；没有执行完整规范实审。

## 功能问题

### R-01 / P1：在线零过期会话被错误地视为不可恢复

[state.rs:398](../../../crates/modorch-mqtt-core/src/state.rs#L398)以`!s.clean`判断旧Session是否可恢复；MQTT5用`Session Expiry==0`设置该字段。零过期表示断开后立即结束Session，在线时仍有可接管的Session，不能据此提前丢弃。

复现：A以MQTT5、Session Expiry=0连接并订阅；保持A在线，B使用同ClientID、Clean Start=false、Session Expiry=30连接。当前B的CONNACK返回Session Present=0，随后发布匹配消息，B只收到PINGRESP，订阅已丢失。相同重建分支还会替换旧Session的可靠交付/FSM。

证据：[probe.py](probe.py)、[probe.log](probe.log)。修复后应保留在线旧Session的订阅和QoS状态，区分MQTT3.1.1 Clean Session与MQTT5 Clean Start/Session Expiry；补在线接管、离线过期及QoS1/2回归。

### R-02 / P1：Bridge在目标Session丢失后停止转发

[Bridge:316](../../../crates/modorch-mqtt-bridge/src/lib.rs#L316)只提交`submitted=false`的Transfer；[目标重连分支:375](../../../crates/modorch-mqtt-bridge/src/lib.rs#L375)忽略CONNACK的Session Present。当前rumqttc 0.25.1在目标返回Session Present=false时清空旧pending请求，而Bridge仍保留`submitted=true`，因此既收不到原PUBACK，也不再重新提交。

受控目标端接收首条PUBLISH后不ACK即断开，接受重连并返回Session Present=false；连接保持可用，1秒观察窗口内无重发，Bridge pending=1、forwarded=0、源Broker pending=1。源码状态转换确认该状态不会自行恢复，后续可靠消息堵在源端。

证据：[实际Bridge探针](bridge-probe/src/main.rs)、[执行日志](bridge-probe.log)。修复须协调客户端在途所有权和应用Transfer，处理Session存在/丢失两种重连；补ACK丢失、目标重启、Session超时及后续消息推进测试，避免盲目重提造成多条独立在途。

### R-03 / P2：Bridge元数据扩容与Core接纳上限冲突

[Bridge:273](../../../crates/modorch-mqtt-bridge/src/lib.rs#L273)为无防环元数据的消息追加Origin/Hops两个User Properties；[MQTT5校验:51](../../../crates/modorch-mqtt-core/src/mqtt5.rs#L51)固定限制128项。源端合法的127/128项经映射成为129/130项，目标Core关闭连接而不PUBACK；当前Bridge没有对应的可交付性处理，可能使该Link持续重试或进入R-02停滞状态。

本轮证据为映射源码核对及真实TCP边界实验：128项收到PUBACK，130项连接关闭。**未执行完整的128属性消息穿过Bridge的组合实验**，不能把这项边界复现写成该组合已经执行。

证据：[probe.log](probe.log)。修复应统一普通属性与保留桥接属性的数量/字节预算，并检查映射后的报文大小；不能通过静默删除业务属性或提前确认源消息规避。需补126/127/128项、已有Origin/Hops和接近包长上限的组合回归。

### R-04 / P2：恢复幂等记录时漏计MQTT5属性预算

[snapshot.rs:566](../../../crates/modorch-mqtt-core/src/state/snapshot.rs#L566)构造校验/计费用消息时将properties置空，[594行](../../../crates/modorch-mqtt-core/src/state/snapshot.rs#L594)据此预留预算，然后保存带完整原始属性的`p.request`。这也跳过了该记录真实属性的恢复校验。

真实SQLite重启实验：内部发布携4096字节Content Type，接纳后memory_used_bytes=4619，恢复后=491，少计4128字节，恰好等于属性逻辑成本。恢复后使用原请求重试成功，证实属性仍保留在幂等记录中。问题是预算/校验遗漏，不是属性数据丢失；重复积累记录可侵蚀全局内存上限。

证据：[存储探针](bridge-probe/src/bin/storage.rs)、[执行日志](storage-probe.log)。修复时须使用真实属性重建和计费，并补多记录、接近全局上限、调小配置后拒绝恢复以及非法属性快照的回归。

## 契约与验收遗留

1. `contracts/mqtt311.json`的4个OptionalWebSocket条款仍写“WebSocket未实现”，ConditionalBridge条款及`mqtt311-audit.json`仍写“Bridge未实现”。`release-policy.json`明确只面向MQTT3.1.1 TCP/TLS/SQLite，维持这个独立Profile合理，但排除理由应改为当前范围说明，可选功能需另列验收矩阵。当前严格门禁仍会阻断缺项；没有证据表明它已错误批准。
2. F6完整3.1.1逐义务/跨段/FSM/故障互操作实审仍未批准；F7独立MQTT5完整矩阵尚缺。上一轮40组3.1.1限定场景、16个可选精确测试映射仅为TargetedEvidence。
3. WSS、MQTT5与Auth/Storage组合、Bridge持久积压/拒绝/重启等交叉验证仍需系统补齐。本轮探针只覆盖报告列出的轨迹。
4. 性能/恢复仍缺代表性生产负载、长期RSS、高负载退出/恢复SLO及阈值批准；短loopback测量不替代。
5. magicCommon实际消费共享JSON、远端不可变CI执行记录、macOS/Windows及文件系统矩阵、真实硬件断电材料尚缺。
6. 独立Agent、安全及性能评审未执行；正式发布仍为0/178项批准，保持Partial/NotRun。

Enhanced Authentication、每Core单监听、Bridge直接TCP及宿主TLS代理、format2多个Release拒绝歧义升级、禁止原地降级、停机Will允许延迟到重启，均是现有文档明确的能力边界。本轮未把它们自动列为新功能缺陷；变更这些边界需另定范围。

## 执行与证据边界

- 核对开始时，上一轮169份源码输入、89份材料SHA256全部匹配；旧验证目录未改写。
- [框架预检](framework-preflight.json)通过：Modorch0.0.3固定rev，Skill1.0.7，源码指纹`94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。
- Python探针复用既有`standalone-broker`构建产物并记录其SHA256；Rust Bridge/Storage探针从当前源码离线构建、单独工作区和Lock，不修改根Cargo.lock。所有探针完成检查式退出。
- 探针退出0表示**预期缺陷已复现**，不表示需求Passed。存储探针首次缺少常量import的编译失败保留于`storage-build-initial.log`，最终结果见`storage-probe.log`。
- 本轮仅新增核对材料并更新剩余清单/设计进度说明，未运行全仓检查，也未把上一轮测试冒充本轮全量执行。文档变化后原发布计划只对应更新前输入；本轮追加独立输入摘要。

复现命令（项目根执行）：

```sh
python3 docs/validation/2026-11-08-remaining-audit/probe.py --skip-historical-source-check
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$PWD/target" cargo run --offline --locked --manifest-path docs/validation/2026-11-08-remaining-audit/bridge-probe/Cargo.toml --bin mqtt-bridge-audit-probe
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$PWD/target" cargo run --offline --locked --manifest-path docs/validation/2026-11-08-remaining-audit/bridge-probe/Cargo.toml --bin storage
```

首次Python执行中的历史源码摘要断言绑定核对开始时输入；上面的重跑命令显式跳过该历史源码核对，仍检查89份旧材料完整性，网络探针行为保持不变。建议下一轮按R-02、R-01、R-03、R-04逐项修复，每项把本轮反例改成正式通过断言，再补组合验收。
