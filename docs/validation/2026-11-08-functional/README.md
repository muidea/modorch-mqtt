# 功能路径验证（2026-11-08）

本轮在既有工作区上补齐 MQTT5、WebSocket、网络Bridge、Tokio宿主适配、动态Auth/TLS、持久审计与HTTP管理。实现范围见[可选功能](../../optional-features.md)，剩余正式验收见[清单](../../remaining-work.md)。未自动提交或推送。

## 已执行的专项证据

- [整仓功能测试](check-complete.log)：145个命名Rust测试、2个doctest通过，含MQTT5字节测试、SQLite恢复、Bridge离线积压和防环、真实证书轮换、HTTP和持久审计。
- [可选功能精确映射](optional-latest.json)：7组限定场景，通过日志中的精确测试名核对，不升级完整符合性。
- [Paho MQTT5](paho5-latest.log)：独立Paho2.1 TCP和WebSocket QoS0/1/2，校验Properties、Topic Alias和Subscription Identifier。
- [Python工具测试](python-final.log)：26项通过。

构建与门禁收尾日志在本目录分别保存；较早的 `initial/second/...` 与编号尝试保留实际失败过程，不能当作最终通过证据。期间磁盘不足，清理的仅是本项目可再生成的 target/debug/incremental，之后使用 CARGO_INCREMENTAL=0；源码、Cargo依赖缓存和旧验证目录未删除。

## 关键修复

维护不再阻塞accept；维护等Store gate时响应取消，已有提交仍等待确定结果。配额测试按两个常驻任务核对补偿。MQTT5元数据/别名纳入资源预算，超客户端包限的下行按目标丢弃；Will Delay、Session与Message Expiry跨恢复保留期限。TLS校验失败作为业务错误返回，不触发Supervisor重建。

快照当前写format4并兼容读1/2/3；旧format2多Release歧义升级仍拒绝，不能原地降级。Bridge为MQTT5 TCP客户端，TLS由宿主显式代理；最多每方向一个在途，源端积压，跨崩溃至少一次。Enhanced Authentication未启用，完整F6/F7规范与生产发布仍为Partial；外部CI、跨平台、硬件断电和独立评审无新增批准。

## 最终验证入口

- `check-complete.log`：整仓fmt/check/clippy/test/doctest/doc、开发证据门禁、示例启动、TLS/Auth/Storage短测、TOML与SIGKILL恢复；最终退出码0。
- `msrv-latest.log`、`default-latest.log`、`toml-only.log`：Rust1.85、默认空Feature和仅TOML边界。
- `paho-final.log`、`paho-tls-final.log`、`paho5-latest.log`：独立3.1.1/TLS/Auth与5.0/TCP/WebSocket。
- `framework-preflight-final.json`：固定Modorch0.0.3 rev/指纹通过；`skill-status-final.json`：项目MQTT Skill1.0.2已本机归档同步。
- `source-hashes.json`、`evidence-hashes.json`、`result.json`：本轮源码增量、原始材料摘要和Modorch结果Schema。
- `release-plan.json` 是当前源码绑定的未批准计划；正式评审仍NotRun，严格门禁拒绝缺项，开发清单允许Partial。完整规范审计结果不因新功能升级。

最终检查通过：145个Rust测试、2个doctest和26项Python工具测试；7组可选功能/16个精确测试映射全部通过。正式发布178项评审均NotRun，严格release与require-complete退出1，符合当前Partial预期。源码摘要覆盖169个输入，相对上一轮变化68个文件；旧验证目录32份材料摘要复核未变化。
