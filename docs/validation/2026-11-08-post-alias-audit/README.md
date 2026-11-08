# 别名修复后的遗留项核对（2026-11-08）

按设计§36复核当前输入，主Skill为modorch-mqtt-development1.0.3，报告遵循支持Module Skill1.0.7 Schema。本轮没有修改产品代码，没有提交或推送。R-01–R-08与D-01保持已修复的限定结论，新增确认一项P1缺陷R-09，整体Partial。

## 已确认问题

| 编号 | 优先级 | 实际结果 | 根因 |
|---|---|---|---|
| R-09 | P1 | Bridge持有`old-owned`（源ID1），目标延迟ACK；源丢失Session，新Session发送ID1的`new-session-value`。目标确认旧消息后，Bridge向新Session发PUBACK ID1；目标下一条却直接是`next-barrier`，新消息没有转发 | Bridge `src/lib.rs:381`忽略源CONNACK的Session Present；`:388`在pending期间仅按Packet ID处理新消息；`:332`完成旧Transfer后无条件向当前源连接ACK旧ID |

触发的是源Session丢失，可以对应会话过期、清理或源Broker未能保留旧Session；即使旧源状态已经丢失，Bridge也不能确认新Session中另一条尚未转发的消息。`forwarded=2/pending=0`且无丢弃计数，无法通过现有进度计数识别这次漏转发。

修复需将源Session身份/代际绑定到Transfer和确认；隔离新旧ACK，保留已开始的目标工作，并对新Session订阅/接纳安排有界处理。必须覆盖同/不同ID、源Session Present=0/1、目标延迟ACK、重连与后续进度。这里只登记缺陷，不提前选择具体实现。

## 反例和对照

`probe/`是独立workspace，仅通过公开Modorch/Core/Bridge API装配实际Bridge Module；源和目标是受控MQTT5 TCP端点。第一次建立源Session并发送旧消息；目标收到后不确认，源端断开并返回新Session。新Session重用ID1合法，不是同一Session内违反Packet ID所有权的输入。

发新消息后，等待源端收到Bridge的Keep Alive PINGREQ并回PINGRESP，确认源poll已推进，再确认旧目标消息。随后收到源PUBACK ID1，再发送ID2的`next-barrier`并确认目标下一条就是该消息；用正向消息序列证明漏转发，不用短时间“没收到”推断数据丢失。全部网络读取和总运行时间有界，最后检查式退出。

`bridge-source-session-final.log`完整复现数据丢失；`bridge-source-resumed-control.log`把第二次CONNACK改成Session Present=1并DUP重发旧消息，确认正常恢复只转发一次旧消息、源ACK正确、后续消息继续。两个探针返回0分别表示反例确认和对照预期通过，不能将反例日志作为产品正确性证据。

`bridge-source-session-initial.log`在已收到错误PUBACK后，探针未处理目标侧Keep Alive PINGREQ，导致下一帧类型断言失败；后续增加PINGREQ/PINGRESP处理，最终完整消息屏障和检查式退出均执行成功。`bridge-source-session.log`保留添加对照参数前的成功反例；最终结论以`*-final.log`和对照日志为准。

官方MQTT5缓存、原文摘要及Windows-1252解码摘录见`normative-quotes.json`：§2.2.1的Packet Identifier按Session分配，MQTT-3.2.2-5要求Session Present=0时丢弃旧协议Session State。源新Session中的ID1是另一条消息；旧目标工作属于应用状态，不能靠旧ID确认新消息。

## 实际执行

- `framework-preflight.json`：固定Modorch0.0.3 rev及指纹通过。
- `prior-verification.json`：起点172项源码/探针和45份上轮材料摘要全部匹配。
- `alias-regressions.log`：4项别名相关回归通过，包含R-08恢复、完整预算和拒绝边界。
- `bridge-regressions.log`：4项现有Bridge网络回归通过，目标Session恢复测试未覆盖源Session丢失。
- `skill-validation.log`、`project-check.log`：Skill链接及项目交付物/文档链接检查。
- `result.json`：探针执行与产品缺陷分开标记；`report-schema.log`核验结果结构。
- `source-hashes.json`、`evidence-hashes.json`绑定本轮输入和材料；历史目录保持原摘要。

从仓库根重跑，使用现有依赖缓存：

```sh
export CARGO_HOME=/tmp/modorch-mqtt-storage-cargo
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$PWD/target"
cargo run --manifest-path docs/validation/2026-11-08-post-alias-audit/probe/Cargo.toml --locked --offline
cargo run --manifest-path docs/validation/2026-11-08-post-alias-audit/probe/Cargo.toml --locked --offline -- resumed
```

## 其他剩余项

本轮同时静态核对会话/消息过期、延迟Will、共享订阅、恢复校验和预算相关路径；未复现的怀疑不登记为缺陷，也不据此宣称这些组合已全面通过。没有重跑全套检查；上轮160项命名Rust测试、2项doctest和26项Python测试只保持其原始限定证据。

完整F6/F7逐义务审计及故障/互操作总矩阵、生产负载/长期RSS/满载退出恢复SLO、magicCommon消费、远端CI、跨平台/文件系统、硬件断电和独立Agent/安全/性能评审仍待真实验收。既定单Core单监听、Bridge TCP加显式TLS代理、format2多Release歧义拒绝、禁止降级及停机Will延迟重启边界保持。没有生成或批准新发布计划；上轮0/178独立评审仍是历史未批准状态。
