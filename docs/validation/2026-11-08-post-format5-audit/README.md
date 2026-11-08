# format5后续遗留项核对（2026-11-08）

本轮只核对设计、源码和执行证据，更新设计§34及剩余清单，没有修改产品代码或Skill，没有提交/推送。主Skill为modorch-mqtt-development1.0.2，报告遵循支持Module Skill1.0.7 Schema。当前确认一个未修复功能缺陷和一个说明同步项；完整规范与发布仍Partial。

## 已确认项目

| 编号 | 优先级 | 实际结果 | 根因与建议 |
|---|---|---|---|
| R-08 | P1 | max_packet_bytes=8192，先以1000字节Topic绑定Alias=1；空Topic/同别名的8191字节QoS1 Retained PUBLISH获PUBACK并保存，SQLite重启报invalid persistent snapshot。完整Topic/较小Payload的8188字节对照恢复成功 | `mqtt5.rs:182`展开Topic Alias，`state.rs`直接接纳展开消息；`state/snapshot.rs:129`按展开后的完整消息再套入站包长上限，导致接纳/恢复不一致。需定义展开消息的有界存储规则并覆盖可靠交换和真实属性，保持下行协商限额 |
| D-01 | P3 | 项目主Skill仍声称写format4、读1–3，源码已写format5、读1–4 | `.agents/skills/modorch-mqtt-development/SKILL.md:33`及36行，后续按托管Skill流程同步说明/版本并校验；框架预检本轮通过，不是Skill未启用 |

R-08探针仅通过公开Modorch/Core/SQLite API搭建真实Broker，使用端口0和临时数据库。反例与对照均收到成功PUBACK且Retained=1，原应用检查式退出成功；失败启动的补偿成功且无残留监听。返回0表示反例被确认，不代表产品通过。复现范围为QoS1 Retained，其他持久消息类型须在修复时加入回归，不冒充本轮已执行。

## 排除项

Payload Format Indicator=0在出站PUBLISH中被省略，曾初判为属性未保留。进一步读取官方§3.3.2.3.2，其明确说明0是“unspecified bytes, which is equivalent to not sending a Payload Format Indicator”，因此本轮撤回缺陷判断，**不登记R-09**。取值1以`01 01`保留，缺省保持缺省；最终脚本将0省略标记为RULED OUT。初始日志中的REPRODUCED R-09是已撤回标签，不作为当前遗留项。

官方MQTT5原文复用上轮下载的`../2026-11-08-followup-audit/mqtt-v5.0-os.html`，下载地址、SHA256与完整相关段落见`normative-quotes.json`。不能只截取MQTT-3.3.2-4的MUST而忽略同段的等价说明。

## 执行与材料

- `framework-preflight.json`：实际Cargo依赖解析的Modorch0.0.3固定rev/指纹通过。
- `probe/Cargo.toml`、`probe/Cargo.lock`、`probe/src/main.rs`及`alias-recovery.log`：独立workspace和有界公开API探针，真实TCP/SQLite反例及对照。
- `format-indicator.py`、`format-indicator.log`：独立线帧检查三种PFI输入；复用上轮二进制，SHA256为`8bf16c76a36c94870e0f18e8b3d5e753b8f4402c8f37f99ad418d38be21044b0`，与上轮`no-local.log`记录相同，产品源码摘要匹配。
- `format-indicator-initial.log`：保留初始错误标签；最终结论见排除项及规范原文。
- `prior-verification.json`：起点上轮169项源码/37份材料全部匹配；结束时产品源码仍相同，仅本轮两份状态文档改变。全部旧验证目录保留原材料。
- `project-check.log`及`skill-validation.log`：文档链接与主Skill静态检查通过；静态工具未检查D-01的格式说明语义。
- `source-hashes.json`、`evidence-hashes.json`、`result.json`及`report-schema.log`：当前输入、证据摘要及结构化结果。探针执行Passed、R-08和D-01检查Failed，整体Partial。

从仓库根重跑：

```sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$PWD/target" cargo run --locked --offline --manifest-path docs/validation/2026-11-08-post-format5-audit/probe/Cargo.toml
python3 docs/validation/2026-11-08-post-format5-audit/format-indicator.py
```

第二条需先有启用mqtt5/websocket的standalone-broker；本轮复用了上轮相同源码/摘要的构建。未重跑整仓check/MSRV/最小Feature，上轮157项命名Rust测试、2个doctest和26项Python测试仍是其限定范围的历史证据，不覆盖本轮新反例。R-01–R-07已测修复保持原记录，不能据此推断所有组合正确。

## 未获验收的既定事项

完整F6/F7规范/互操作/故障总矩阵、代表性负载/长期RSS/满载退出恢复SLO、magicCommon实际消费、远端CI、跨平台/文件系统、硬件断电和独立Agent/安全/性能评审继续待真实材料。当前状态文档刷新后，旧发布计划是历史输入快照，本轮没有新建或批准发布计划。
