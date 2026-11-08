# F6 规范场景与 PUBREL 顺序验证（2026-11-07）

本轮继续按设计收口F6，修复MQTT-4.6.0-4顺序偏差，补充协议边界和可执行场景清单。完整设计/发布状态仍为 **Partial**。说明见[符合性范围](../../mqtt311-conformance.md)、[QoS2](../../qos2.md)、[Storage升级限制](../../storage.md)及[设计§22](../../modorch_mqtt_broker_full_design_0.0.3.md)。历史验证目录未修改。

## 实现与兼容性

- 每个有效PUBREC（含重复）进入有界控制FIFO，持久确认后按收到顺序发PUBREL；队列满暂停输入。未确认PUBLISH相对顺序不变。
- format3可靠队列保存每个未完成ID最近一次PUBREL顺序。旧format1可升级；旧format2同持久Session有多个Release时缺失历史顺序，拒绝监听并保留原库，需旧版先排空相关交换。至多一个Release可升级；旧版不能直接读取format3。
- ClientID配置上限现在至少23字节。原来低于23的配置需修正，避免排除规范要求支持的字母数字ID。
- 新增9项Rust测试：7项规范场景、1项确定性状态/快照测试、1项真实SQLite逆序确认重启测试。另有4项Python日志门禁测试。

## 最终执行

| 检查 | 实际证据 |
|---|---|
| fmt、全workspace/all-target/all-feature check、clippy -D warnings | [check.log](check.log)，成功 |
| Rust tests / doctest | [check.log](check.log)：77项Rust（Core63、Auth6、Extensions4、Storage3、rumqttc1）+1项doctest，成功 |
| 日志门禁测试 | [tooling.log](tooling.log)及check.log，4项成功；缺失/ignored/失败/仅总数不会冒充通过 |
| 规范场景精确执行映射 | [scenarios.json](scenarios.json)、[matrix.log](matrix.log)：13组场景成功；62个编号只标TargetedEvidence，78个未映射编号NotRun，完整符合性Partial |
| Rustdoc -D warnings、既有/安全宿主smoke | [check.log](check.log)，成功 |
| 真实进程SIGKILL/检查式退出 | [check.log](check.log)末尾：2次SIGKILL/3次启动，QoS1基础、QoS2去重/发送阶段/ID及端口回收成功 |
| Rust 1.85 MSRV | [msrv.log](msrv.log)，全workspace/all-target/all-feature成功 |
| 无默认及单独storage/auth/tls/admin/metrics、三个示例重建 | [feature-build.log](feature-build.log)，成功 |
| Modorch固定版本/Feature/逐文件指纹 | [framework-preflight.log](framework-preflight.log)，成功 |
| Paho TCP与TLS/密码/ACL | [paho.log](paho.log)、[paho-tls.log](paho-tls.log)，QoS0/1/2及检查式退出成功 |
| Python/Shell语法、diff、Markdown链接、输入摘要 | [auxiliary.log](auxiliary.log)、[source-hashes.json](source-hashes.json)、[environment.json](environment.json) |
| 报告Schema | [result.json](result.json)、[report-validation.log](report-validation.log) |

全部Cargo命令用任务缓存 `CARGO_HOME=/tmp/modorch-mqtt-storage-cargo`、locked/offline；未修改依赖或上游。实际框架指纹为 `94c55b12e921954eabe9008354e347e31c9b0ec2d577689ca6a18c2234a947a9`。环境、基准提交和Cargo.lock对前轮不变的核对见environment.json。源码摘要覆盖累计未提交工作区，排除全部历史及本轮验证目录；本轮未提交或推送。

## 范围和诊断

[core-first.log](core-first.log)保留首次核心运行：新增状态测试重连时遗漏expected_version，测试fixture报SessionConflict，其余60项通过；随后修正fixture。[core-compile-first.log](core-compile-first.log)保留新增ClientID测试误用消耗self的disconnect导致编译失败，改用原始DISCONNECT帧后修正。[core.log](core.log)为修正后的63项核心测试通过；最终全工作区以check.log为准。两项诊断均非未解决的产品失败，不用单独复跑掩盖最终回归。[auxiliary-first.log](auxiliary-first.log)保留首次辅助检查在摘要文件生成前检查其链接而失败的记录；调整生成/检查顺序后auxiliary.log成功。

规范编号取自官方OASIS文本及记录的SHA256；140个编号不代表所有未编号MUST。场景通过只说明该具体轨迹已验证，不授予整条规范完整符合性。清单当前仍有78个未映射编号，尚需Server/Client/可选传输适用性和完整规范审计。

仅Linux/WSL2 x86_64；SIGKILL不等于硬件断电。未补生产吞吐/恢复SLO、跨平台和独立Agent发布证据；MQTT5、WebSocket、Bridge尚未实现。SQLite沿用FULL/DELETE/独占有界全量image，停机Will可延迟到重启收尾。

## 复现

```bash
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo CARGO_NET_OFFLINE=true ./scripts/check.sh
CARGO_HOME=/tmp/modorch-mqtt-storage-cargo cargo +1.85.0 check --workspace --all-targets --all-features --locked --offline
python3 scripts/check-conformance.py --results docs/validation/2026-11-07-f6-conformance/check.log --output /tmp/modorch-mqtt-conformance.json
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho.py
/tmp/modorch-mqtt-interop-venv/bin/python scripts/verify-paho-tls.py
```

其他机器使用自己的Cargo缓存和已安装Paho的虚拟环境；脚本不安装依赖或修改全局配置。详细Feature与build命令记录在feature-build.log。
