# MQTT3.1.1 发布证据门禁

`check.sh` 验证开发基线；`check-release.py` 默认执行严格发布判定。当前项目仍为 **Partial**，尚无完整协议审查及生产/平台/独立Agent证据。门禁代码完成不表示这些验证已经执行。

## 受控范围与清单

当前保守发布目标为 `mqtt311-tcp-tls-sqlite`，要求Linux/macOS/Windows运行、硬件断电和文件系统、安全、性能、远端CI及独立Agent报告。这个目标不是现有支持声明；MQTT5、WebSocket、Bridge在此配置范围外。改变发布目标应审查并修改受控策略和工具，不能在证据文件中临时豁免。

- [项目MUST](../contracts/project-must.json)：设计§15的21项及Core TOML合同，精确测试/源码映射只标TargetedEvidence。
- [规范原文索引](../contracts/mqtt311-text.json)：117段正文和列表项，覆盖140个编号。完整保留段落，正文 `MQTT-3.3.1.-1` 显式对应清单 `MQTT-3.3.1-1`；原文不改写。跨段续文与结构表仍在[审计清单](../contracts/mqtt311-audit.json)，关键词提取不等于语义审核。
- [发布策略](../contracts/release-policy.json)：10个发布域和每域必须执行的检查。与128个适用编号、18个正文/结构/FSM审查项合计178项。12个客户端/可选功能编号保留带依据的范围排除，不能称为已通过。

`UN-008`总体符合性由全部适用规范审查推导，不允许拿“总体符合”评语循环证明单项。`FSM-COMPLETE`必须独立审查，现有报文表和ACK矩阵不足以批准它。

## 准备与复核

先运行工作区检查并保存原始日志，然后为该源码准备新的证据计划。以下路径是操作者自选的新目录，工具拒绝覆盖已有计划：

```bash
mkdir -p /tmp/mqtt-release
CARGO_NET_OFFLINE=true ./scripts/check.sh > /tmp/mqtt-release/check.log 2>&1
```

证据在核对期间必须放在仓库内部的本地忽略目录（如 `validation/<本轮>/`）。归档只保留最终结论，历史过程材料已清理；严格评审仍需真实同轮材料，摘要不能替代日志或批准。将本轮真实日志放入新目录后执行，以下 `CURRENT` 是后续执行占位符：

```bash
python3 scripts/check-release.py --prepare \
  --producer developer-name \
  --results validation/CURRENT/check.log \
  --bundle validation/CURRENT/example-plan.json
python3 scripts/check-release.py \
  --bundle validation/CURRENT/example-plan.json \
  --output validation/CURRENT/example-release.json
```

初始178个`reviews`均为`null`，严格判定返回1。`--allow-partial`仅用于开发清单检查，可在无Failed的Partial状态返回0；发布流程不得使用它。`check.sh`用临时计划核对全部MUST测试映射；CI归档的计划也不自动批准评审。

计划自动发现Cargo、源码、示例、脚本、契约、文档、Skill和CI输入；包括新增/删除文件，排除验证输出。任何输入变化都须重跑验证并生成新计划。`source_digest`是按键排序、无空白JSON的SHA256；`source_inputs`记录每文件摘要。日志也绑定SHA256。旧日志没有自动获得当前源码身份，采集者必须保证计划来自真实同轮执行。

## 评审与发布域报告格式

每个非空review引用为`{"path":"仓库内相对路径","sha256":"文件SHA256"}`。引用的JSON至少包含：

```json
{
  "id": "MQTT-ARCH-001",
  "source_digest": "本轮计划source_digest",
  "obligation_sha256": "本轮计划obligation_digests中对应值",
  "reviewer": "与producer不同的真实评审者身份",
  "decision": "Approved",
  "rationale": "审查的行为、适用边界、故障矩阵和结论依据",
  "evidence": [{"path": "本轮原始日志路径", "sha256": "日志SHA256"}]
}
```

规范评审须逐项阅读原文、相邻义务和全部相关状态/故障/出站布局，补齐缺失实现/测试后才能批准。测试存在、一个Happy Path通过或手填状态不能替代审查。项目MUST同样需核对合同全部语义；代码中的测试映射仅为最低证据。

`F8-*`评审还必须有`report`文件引用，且该引用包含在`evidence`里。报告格式为：

```json
{
  "gate_id": "F8-REMOTE-CI",
  "source_digest": "本轮计划source_digest",
  "checks": [{
    "id": "immutable-run",
    "status": "Passed",
    "command": "真实执行命令和远端不可变run URL/commit",
    "evidence": [{"path": "下载的原始执行日志路径", "sha256": "日志SHA256"}]
  }]
}
```

每域`checks`的ID必须与策略精确相等。未运行保留NotRun；Approved评审夹带Failed/NotRun检查会被拒绝。性能报告须含环境、Broker配置、测量方法、原始采样、阈值及依据；短loopback实验不能批准长运行/生产SLO。断电、跨平台、magicCommon、远端CI和独立Agent各自需要真实执行材料。

门禁核验来源摘要、精确Passed测试、所有必选ID、评审身份分离、报告检查和原始文件摘要；拒绝路径穿越、外部符号链接、重复JSON键、删项、自评、循环引用及陈旧证据。全部适用规范批准后`normative_status`可为Passed，全部178项满足后`overall_status`可为Passed；工具测试中的合成Passed用例不计入项目能力。

这是离线完整性与人工评审流程。身份字符串和SHA256不能证明执行真实性或评审独立性，也不是数字签名；发布操作者须从可信CI制品/真实评审流程接收材料，核对原始命令和结论。工具不会因为某份通用报告自称Passed就推断规范符合性。
