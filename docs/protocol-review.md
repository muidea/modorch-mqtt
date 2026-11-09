# 完整协议评审材料

后续任务及具体缺口见[F6/F7 待收口计划](f6-f7-closure-plan.md)，当前执行结论见[最终验证与收口记录](validation/README.md#protocol)。计划状态和最终摘要均不替代真实独立批准。

`contracts/protocol-review.json` 控制 MQTT 3.1.1 的 437 项及 MQTT5 的 607 项输入，合计 1,044 项。当前 18 个有限矩阵、222 个组合和两版 acceptance 合计 1,064 个对象，表格行检查 844 项；限定映射为 190/281，真实独立批准为 0，完整 F6/F7 保持 Partial。

按用户归档规则，仓库仅保留最终记录；历史原始材料和未批准包已清理。后续真实评审使用 Git 忽略的 `validation/CURRENT/` 等本地目录，核对期间保留同轮原始材料，归档时更新最终结论。清理旧材料不降低门禁要求；需要当前输入批准时须实际执行并提供有效材料，不能引用摘要冒充执行日志。

## 创建未批准计划

先执行 workspace 测试并保留未拼接、未重复的原始 Cargo 日志，再创建计划：

```bash
python3 scripts/check-protocol-review.py --prepare --producer IMPLEMENTER_ID \
  --results validation/CURRENT/workspace.log \
  --bundle validation/CURRENT/protocol-bundle.json
```

将 CURRENT 和 IMPLEMENTER_ID 替换为真实路径和身份。输出文件必须不存在；工具不会覆盖已有计划。计划绑定动态发现的全部受控输入（包括新增/删除文件）、清单 Profile、原始日志和 SHA256，所有 reviews 初始为 null。编号义务使用 `mqtt311:MQTT-…` / `mqtt5:MQTT-…`，章节使用协议前缀加 SECTION 编号；状态矩阵和两版协议的 acceptance 也分别登记，不以一版协议的批准代替另一版。

## 独立评审记录

真实评审者逐项检查全文、相关上下文、适用性和断言充分性，并提供独立客户端互操作、状态/故障、持久恢复及条件功能的验收材料。每个矩阵维度的每个组合都要有检查项。114个表格还要求844个物理行检查（3.1.1为339行，MQTT5为505行），使用`table-row:0001`等ID，包括表头、保留值和合并单元格；`table_rows`保留完整单元格文本及rowspan/colspan。`scripts/check-protocol-review.py` 的 `obligations(manifest, root)` 返回完整对象与 checks；对该对象使用 `digest()` 计算 obligation_sha256。不能只对原文 source_digest 或 ID 做批准。

每项 reviews 引用格式为 `{"path":"validation/CURRENT/review.json","sha256":"…"}`。被引用文件结构如下，示例仅说明字段，不是批准材料：

```json
{
  "id": "mqtt5:SECTION-252",
  "source_digest": "计划中的 source_digest",
  "obligation_sha256": "完整评审对象摘要",
  "results": {"path": "计划中的原始日志", "sha256": "日志摘要"},
  "reviewer": "真实独立评审者身份",
  "decision": "Approved",
  "rationale": "基于完整义务的评审理由",
  "checks": [
    {
      "id": "whole-text",
      "status": "Passed",
      "rationale": "具体核对结论",
      "command": "真实执行命令或审查步骤",
      "assertions": "该项完整语义由哪些断言或材料证明",
      "tests": ["精确通过的测试名"],
      "evidence": [{"path": "真实原始材料路径", "sha256": "材料摘要"}]
    }
  ]
}
```

示例中的 checks 被省略了其余项，直接使用会被拒绝。必须使用对象要求的全部唯一 checks。映射测试必须在原始日志中精确通过，并由 checks 绑定；矩阵每个 Passed 用例及每个 Passed 表格行必须关联原始日志中的精确测试。表头等不产生线帧行为的行可明确声明NotApplicable，须给出具体理由和证据；适用的表格整体语义检查仍不能豁免。适用的 TargetedEvidence 和矩阵用例不能用 NotApplicable 跳过；其余不适用项必须给出具体理由和材料。检查项 Failed/NotRun 与 Rejected 决定阻止完整通过；失败不会被后续 NotRun 覆盖。

```bash
python3 scripts/check-protocol-review.py \
  --bundle validation/CURRENT/protocol-bundle.json --require-complete
```

工具拒绝自评（忽略身份大小写及两端空格）、遗漏/重复、陈旧源码/正文/日志、路径越界、材料篡改、使用评审文件充当执行材料和缺失断言。执行材料和主结果日志按实际文件身份及已校验SHA256排除所有已知评审文件和等内容副本，包含`./`、内部符号链接和硬链接；合法执行日志的这些别名仍允许。发布门禁对嵌套报告自身采用同样排除；普通合法执行日志的副本仍允许。只有全部义务、矩阵及两个 acceptance 获得当前输入的真实材料，严格命令才有通过路径。独立身份的真实性和材料的事实内容仍需外部核实，这个离线工具不提供身份签名认证。合成单元测试验证通过与拒绝分支，不作为项目批准；当前真实独立批准仍为 0。

完整协议评审与 `check-release.py` 的发布门禁分别执行。后者当前发布 Profile 为 MQTT3.1.1 TCP/TLS/SQLite，还包括平台、安全、性能和独立发布验收；本工具通过不自动批准生产发布。
