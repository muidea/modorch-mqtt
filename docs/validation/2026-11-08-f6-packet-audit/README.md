# F6 报文表、正文候选与发送ACK矩阵验证（2026-11-08）

本轮修复非法头部/不可能长度延迟拒绝，补充独立报文表与QoS2发送ACK转换矩阵，并将正文候选/结构表审计接入实际日志门禁。完整设计和发布状态仍为 **Partial**。详见[审计说明](../../mqtt311-audit.md)、[设计§25](../../modorch_mqtt_broker_full_design_0.0.3.md)和[剩余需求](../../remaining-work.md)。历史验证目录未修改，本轮未提交或推送。

## 实现与范围

- 之前frame_len只判断包长，非法flags/方向要等完整包体后decode才拒绝。现在首字节检查合法接收类型和flags，Remaining Length解出后检查各报文最小/固定长度；无须等待声称的包体。decode只接受恰好一个完整帧，TCP提帧继续支持分片/合并。
- 新增9项报文表Rust测试：全部256首字节及256 CONNECT flags的语法/真实TCP；8个长度编码边界和预算；ID0/1/65535；精确CONNACK flags；非法部分头部触发Will及持久Session订阅/QoS2接收状态保留。
- 新增1项状态矩阵测试，逐项执行4状态×3ACK共12转换，核对QoS2消息/ID所有权与PUBREL输出。仅覆盖所述发送ACK范围，不等于全部网络/故障/持久化FSM已认证。
- 固定官方HTML正文提取出9段无本段编号的MUST/SHALL/REQUIRED候选；其中3段是MQTT-4.3.2-1跨段续文。手工分类定义、客户端、服务端及总体符合性，原文/来源/依据随审计清单保存。8张结构表有限定映射，7个非规范例表/图单列Informative；完整FSM聚合项仍NotRun。
- check-conformance同时核对35组编号场景和14项审计限定轨迹；缺失、重复、与ignored矛盾或失败日志均拒绝。26项审计记录的其余12项为定义/上下文/非规范/客户端或总体待办，均无执行通过声明。

## 最终执行

| 验证 | 结果与材料 |
|---|---|
| Modorch0.0.3固定来源/Feature/源码指纹 | [framework-preflight.json](framework-preflight.json)：Passed，Skill1.0.7 |
| 全Core回归 | [core.log](core.log)：91项Passed |
| fmt / workspace all-target all-feature check / Clippy -D warnings | [check.log](check.log)：Passed |
| Rust / doctest / Python | [check.log](check.log)：105项Rust（Core91、Auth6、Extensions4、Storage3、rumqttc1）、1项doctest、12项Python全部Passed |
| Rustdoc -D warnings及既有/安全宿主 | [check.log](check.log)：Passed |
| 真实SIGKILL与检查式退出 | [check.log](check.log)末尾：QoS2阶段/去重、离线路由、Retained/Will和端口释放Passed |
| 编号与附加审计轨迹 | [scenarios.json](scenarios.json)、[matrix.log](matrix.log)：35组场景、14项审计限定轨迹Passed；128编号TargetedEvidence，12编号NotRun；完整符合性Partial |
| 正文候选来源核验 | [audit-source.log](audit-source.log)：实际读取固定SHA官方HTML，9段清单吻合；不等于全规范语义审计通过 |
| 完整规范阻断 | [release-gate.json](release-gate.json)、[release-scenarios.json](release-scenarios.json)：实际exit1，符合Partial不可批准的预期 |
| MSRV1.85 / Facade无默认Feature / 三个示例 | [msrv.log](msrv.log)、[feature-build.log](feature-build.log)：Passed |
| Paho TCP及TLS/Auth/ACL | [paho.log](paho.log)、[paho-tls.log](paho-tls.log)：Passed |
| 文档、脚本、diff及输入摘要 | [auxiliary.log](auxiliary.log)、[source-hashes.json](source-hashes.json)、[environment.json](environment.json) |
| 结果Schema | [result.json](result.json)、[report-validation.log](report-validation.log) |

全部Cargo命令使用任务缓存 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo` 与locked/offline。框架固定rev336ba20196044e1b81fb2ca297a18b5e258a5800，未修改依赖或上游。源摘要覆盖当前累计未提交工程与项目Skill，排除全部validation目录；changed_files相对上一轮已记录输入摘要计算。

## 修复前与诊断记录

[packet-before-fix.log](packet-before-fix.log)保留新测试在旧实现上的3项失败：单字节非法头未拒绝、不可能长度未拒绝、真实TCP等待关闭超时；全256完整帧网络对照原先已通过。[packet-after-fix.log](packet-after-fix.log)为修复后首批4项通过，最终10项新增测试均由完整check.log验证。

[core-first.log](core-first.log)记录新增测试误用未导入Packet类型的首次编译错误，已使用codec::Packet修正；[core.log](core.log)为最终91项成功。仅用Core日志核对全工作区清单的[core-scenarios.json](core-scenarios.json)按预期拒绝缺少Auth和rumqttc证据；最后以全工作区scenarios.json为准。

## 尚未完成

段落规则不覆盖所有隐含/跨段语义，表格及ACK矩阵不等于全部出站布局、CONNECT/Session/Will/订阅/QoS状态/故障/互操作总审计。Server总体符合性和完整FSM聚合项保持NotRun。

MQTT5、可选WebSocket/Bridge、TOML及后续运维目标、生产负载/RSS/恢复SLO、硬件断电/跨平台、独立Agent与完整MUST发布总门禁仍未完成。远端CI和magicCommon端本轮未执行。format3升级限制及SQLite停机Will延迟到重启边界保持，见[storage.md](../../storage.md)。SIGKILL不是硬件断电。

## 复现

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
python3 scripts/check-conformance.py --results docs/validation/2026-11-08-f6-packet-audit/check.log --output /tmp/packet-audit-scenarios.json
python3 scripts/audit-mqtt311.py --source-html /tmp/modorch-mqtt311-oasis.html
# 当前Partial状态下应返回1
python3 scripts/check-conformance.py --results docs/validation/2026-11-08-f6-packet-audit/check.log --output /tmp/packet-release.json --require-complete
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

其他机器使用自己的Cargo缓存、Paho虚拟环境与经摘要核验的官方HTML路径。脚本不默认下载HTML或安装全局依赖。
