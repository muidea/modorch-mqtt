# 修复后的遗留项复核（2026-11-08）

按当前设计§32复核组合边界，主Skill为modorch-mqtt-development 1.0.2，报告遵循支持Module Skill 1.0.7 Schema。没有修改产品代码，没有提交或推送。R-01–R-04既有专项证据保留，当前新增三项未修复缺陷，整体Partial。

## 已确认反例

| 编号 | 优先级 | 实际执行与影响 | 源码根因 |
|---|---|---|---|
| R-05 | P1 | MQTT5持久Session设置No Local和Subscription Identifier，3.1.1相同ClientID以CleanSession=false接续收到SessionPresent=1；检查式退出后，SQLite重启报invalid persistent snapshot，整个Core无法启动 | `state.rs:496`切换protocol5但保留原订阅选项；`state/snapshot.rs:334`拒绝3.1.1 Session携带这些选项 |
| R-06 | P1 | mqtt5=true、max_packet_bytes=8192时，合法8192字节3.1.1 QoS1 Retained PUBLISH获PUBACK并落盘；重启失败。8191字节对照可恢复 | `state/snapshot.rs:127`的valid_message按Core启用协议编码，空属性的MQTT5 Property Length额外多一字节；接纳和恢复边界不一致 |
| R-07 | P2 | 同ClientID先发布Retained再以NoLocal=1订阅，仍收到自身Retained；其他ClientID接收正常，自身实时发布过滤正常 | `state.rs:54`的Retained无发布ClientID；`state.rs:1014`回放仅判断过期和Topic匹配，没有No Local来源判断 |

R-05限定结论是**已经成功接纳的状态不能恢复**；本轮不推断规范要求跨协议会话必须采用哪种兼容策略。探针第一次重连可能因deferred detach与expected_version检查的竞争收到CONNACK 0x03，随后有限重试取得成功CONNACK后才执行重启结论；最初未重试的失败材料保留为调试记录，不作为恢复缺陷证据。

R-06是上轮R-04加强恢复包长校验后暴露的组合回归，不推翻R-04真实属性预算专项证据。修复需保留真实属性校验，统一各持久消息类型的接纳/恢复边界。

R-07规范依据为官方MQTT5 [MQTT-3.8.3-3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)：“If the value is 1, Application Messages MUST NOT be forwarded to a connection with a ClientID equal to the ClientID of the publishing connection.” 原文保留为`mqtt-v5.0-os.html`，下载摘要、解码与摘录见`normative-quotes.json`。测试通过PINGRESP屏障确认消息顺序，不依赖“等待超时即没有消息”的推断。未知PUBREL的PUBCOMP原因码怀疑未取得充分规范依据，不计为已确认缺陷。

## 执行与重跑

- `framework-preflight.json`：Modorch0.0.3固定rev/指纹通过。
- `standalone-build.log`、`no-local-final.log`：从当前源码构建mqtt5/websocket示例，真实TCP复现R-07，子进程SIGINT检查式退出0，记录二进制SHA256。
- `probe/Cargo.toml`、`probe/Cargo.lock`、`probe/src/main.rs`：独立workspace，只使用Modorch/Core/SQLite公开API，端口0、临时数据库及总期限。
- `mixed-version-final.log`：R-05成功接续及SQLite重启失败，启动补偿成功、无残留监听。
- `packet-boundary-final.log`：R-06边界反例和差一字节对照，原应用检查式退出、失败启动补偿成功。
- `mixed-version-initial.log`、`mixed-version.log`为最初未成功接续的失败尝试；`mixed-version-retry.log`与其余非final日志为中间执行，不替代最终日志。

从仓库根，使用当前依赖缓存重跑：

```sh
export CARGO_HOME=/tmp/modorch-mqtt-storage-cargo
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$PWD/target"
cargo build -p standalone-broker --features modorch-mqtt/mqtt5,modorch-mqtt/websocket --locked --offline
python3 docs/validation/2026-11-08-followup-audit/no-local.py
cargo run --locked --offline --manifest-path docs/validation/2026-11-08-followup-audit/probe/Cargo.toml
cargo run --locked --offline --manifest-path docs/validation/2026-11-08-followup-audit/probe/Cargo.toml -- packet-boundary
```

这些探针返回0表示**确认反例已出现**，不是产品验收通过；修复后应改变预期并进入正式回归。`result.json`将探针执行标Passed、三项产品合同检查标Failed，整体Partial。

## 输入与未覆盖项

核对起点上轮169项源码摘要、30份证据摘要全部匹配。本轮只刷新设计/遗留清单并新增本目录；`prior-verification.json`记录结束时所有旧证据仍匹配，源码仅两份状态文档变化。`source-hashes.json`绑定当前输入与探针，`evidence-hashes.json`绑定本轮材料；旧目录不改写。上轮151个命名Rust测试、2个doctest和26项Python测试是历史限定证据，本轮未重跑整套检查，不能覆盖本轮反例。

完整F6/F7规范/互操作/故障矩阵、代表性负载/长期RSS/满载退出恢复SLO、magicCommon实际消费、远端CI、跨平台/文件系统、硬件断电和独立Agent/安全/性能评审仍待真实验收。上轮严格发布仍0/178批准，当前文档变更使旧发布计划成为历史快照；本轮没有生成或批准新发布计划。
