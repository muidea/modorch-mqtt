# 验证材料索引

这里保存各轮验证的原始日志、输入摘要、审计反例和未批准的发布计划。材料反映执行时的源码与环境；失败日志和修复前探针是有意保留的证据，不是临时产物。不要清理行尾、重编码或改写已有目录；新执行使用独立目录。Git 属性保留原始字节，避免跨平台换行转换破坏 SHA-256。

最近一次功能修复为 [Bridge 源会话修复](2026-11-08-source-session-fixes/README.md)，覆盖 R-09。当前能力与剩余验收以[需求清单](../remaining-work.md)为准。本地场景提供 `TargetedEvidence`，完整规范与生产发布仍为 **Partial，0/178 独立评审批准**。文档或源码变化后，旧 release-plan 只保留历史意义；当前输入须按[发布流程](../release.md)重新绑定执行证据。

## 功能阶段与专项验证

| 材料 | 范围 |
|---|---|
| [F0–F3](2026-11-07-f0-f3/README.md) | QoS0/1 实验子集 |
| [F4](2026-11-07-f4/README.md) | EventHub 集成 |
| [F5 安全与运维](2026-11-07-f5-security-ops/README.md) | Auth、ACL、TLS、Admin、Metrics |
| [F5 Storage](2026-11-07-f5-storage/README.md) | SQLite 持久化 |
| [F6 QoS2](2026-11-07-f6-qos2/README.md) | 双向 QoS2 |
| [F6 规范场景](2026-11-07-f6-conformance/README.md) | PUBREL 顺序与场景清单 |
| [F6 协议收口](2026-11-07-f6-protocol/README.md) | 发布、订阅、会话生命周期 |
| [Topic 与开发交付](2026-11-07-f6-topic-release/README.md) | 共享向量与开发门禁 |
| [报文审计](2026-11-08-f6-packet-audit/README.md) | 头部、正文候选、发送 ACK 矩阵 |
| [连接期限](2026-11-08-f6-deadlines/README.md) | 后端等待时的连接期限 |
| [出站线帧与故障](2026-11-08-f6-wire-fsm/README.md) | 出站编码、状态机、故障回归 |
| [TOML 配置](2026-11-08-toml-config/README.md) | Core 配置与宿主装配 |
| [逐义务发布门禁](2026-11-08-release-gates/README.md) | 义务映射、严格阻断与负载测量 |
| [功能路径](2026-11-08-functional/README.md) | 可选扩展与组合功能 |

## 遗留项审计与修复

下表按核对与修复的先后排列。原审计中的反例不会被后续修复覆盖。

| 核对材料 | 修复材料 | 主要内容 |
|---|---|---|
| [遗留项核对](2026-11-08-remaining-audit/README.md) | [遗留项修复](2026-11-08-remaining-fixes/README.md) | R-01–R-04：在线会话接管、Bridge 目标恢复、防环属性、幂等恢复 |
| [修复后复核](2026-11-08-followup-audit/README.md) | [组合边界修复](2026-11-08-followup-fixes/README.md) | R-05–R-07：跨协议会话、包长恢复、Retained No Local |
| [format5 后核对](2026-11-08-post-format5-audit/README.md) | [Topic Alias 修复](2026-11-08-alias-fixes/README.md) | R-08、D-01：别名消息恢复与 Skill 边界说明 |
| [别名修复后核对](2026-11-08-post-alias-audit/README.md) | [源会话修复](2026-11-08-source-session-fixes/README.md) | R-09：Bridge 源 Session 代际与旧 ACK 隔离 |

## 提交准备

[提交前清理](2026-11-08-submission-cleanup/README.md)记录入口文档整理、忽略规则、Git 字节保真与本轮静态检查。该轮未修改功能代码，也未重跑协议、互操作或性能验证；最近一次完整功能执行仍见源会话修复目录。
