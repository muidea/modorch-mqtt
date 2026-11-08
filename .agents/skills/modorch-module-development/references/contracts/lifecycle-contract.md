> Generated snapshot; source: contracts/lifecycle-contract.md; source sha256: 06aee9770c2d0ffd00832cbcd67e1aa7d5f496c632829fec09b3d1b95aff96dc; revision 1.0.7. Do not edit.

# Lifecycle Contract

适用于 modorch 0.0.3 当前开发树，Contract revision 1.2.0。维护责任：Framework Maintainer + QA。发布前必须以验证报告确认；规则文本和源码冲突时阻断发布。

## LIFE-001：宿主所有权与应用隔离

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: 同一 Tokio 宿主创建多个 Application。
- Trigger / Guarantee / Boundaries: 宿主持有 Runtime、信号、入口与 Subscriber；应用之间独立注册、消息与健康状态，框架不调用 process::exit。
- Test ID: CT-LIFE-001
- Test: applications_are_isolated_in_one_runtime（源码定位：`../crates/modorch/tests/application.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: SEM-009
- Responsibility: Framework Maintainer / QA

## LIFE-002：启动失败补偿

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: create 已返回对象，setup 中途失败。
- Trigger / Guarantee / Boundaries: 先登记对象再 setup，失败进入部分初始化清理。StartupError.source 保留主因，cleanup 和 handle 可查询及重试；取消调用者等待也不能遗留已创建资源。
- Test ID: CT-LIFE-002
- Test: startup_error_is_bounded_and_preserves_failed_cleanup_for_retry（源码定位：`../crates/modorch/tests/application.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: 补充规则
- Responsibility: Framework Maintainer / QA
- Additional Test ID: CT-LIFE-002-CANCEL；cancellation_of_startup_does_not_abandon_created_resources（源码定位：`../crates/modorch/tests/application.rs`；从匹配框架源码查询）


## LIFE-003：关闭等待超时

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: 关闭时仍有在途操作或钩子未完成。
- Trigger / Guarantee / Boundaries: shutdown_checked 超时返回 StopIncomplete；后台事务继续，资源保留，后续等待或重试只调用未完成 checkpoint。force_shutdown 返回 Forced，不能作为正常释放的证据。
- Test ID: CT-LIFE-003
- Test: shutdown_timeout_preserves_resources_and_accepts_only_causal_children（源码定位：`../crates/modorch/tests/application.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: SEM-010
- Responsibility: Framework Maintainer / QA

## LIFE-004：强制关闭回执

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: 在途工作尚未真正退出时强制关闭。
- Trigger / Guarantee / Boundaries: 强制中止框架 Future 不证明外部副作用结束，不调用忙碌资源的普通 teardown，不返回 GracefullyStopped。
- Test ID: CT-LIFE-004
- Test: force_shutdown_never_reports_a_graceful_stop_or_releases_busy_owners（源码定位：`../crates/modorch/tests/application.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: 补充规则
- Responsibility: Framework Maintainer / QA

## LIFE-005：重启代际屏障

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: 已启用 FailurePolicy::Restart 且组件故障。
- Trigger / Guarantee / Boundaries: 先封锁故障依赖闭包并排空旧消息、任务和租约，拆卸后推进代际并重建。旧 TaskScheduler 与服务能力失效；应用级 EventHub/ExternalEventHub 不绑定生产者代际。不自动重放消息。累计预算耗尽转 FailFast。
- Test ID: CT-LIFE-005
- Test: restart_waits_for_old_handlers_and_leases_and_invalidates_old_handles（源码定位：`../crates/modorch/tests/supervision.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: 补充规则
- Responsibility: Framework Maintainer / QA

## LIFE-006：重建失败与预算

- Level: MUST
- Applies to: 0.0.3 + 本次框架指纹
- Status: Implemented；Verified 范围由能力矩阵（源码定位：`../docs/reference/capability-matrix.md`；从匹配框架源码查询）与执行日志限定
- Precondition: 替代实例 setup、start、ready 或 activate_ingress 失败。
- Trigger / Guarantee / Boundaries: 部分实例先完整补偿，每次重建消耗应用生命周期内累计预算，不因短暂 Ready 重置；预算耗尽转 FailFast。
- Test ID: CT-LIFE-006
- Test: replacement_setup_failure_is_cleaned_before_retry_and_budget_is_bounded（源码定位：`../crates/modorch/tests/supervision.rs`；从匹配框架源码查询）
- Evidence: 本次验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询），不得用测试源码存在代替执行。
- Semantic ID: 补充规则
- Responsibility: Framework Maintainer / QA

## LIFE-INGRESS-001：统一入口就绪屏障

- Level: MUST
- Applies to: 0.0.3 开发树 + 本次框架指纹（已发布 v0.0.2 不含此增强）
- Status: Implemented；Verified 范围由本轮执行证据限定
- Precondition: 同一 Application 启动或依赖闭包重建。
- Trigger / Guarantee / Boundaries: Initiator 与 Module 的默认无操作 activate_ingress 均在全体相关组件 ready 通过后调用；初始启动为全体选中组件，重建为受影响闭包。全部激活成功后才统一标记相关组件及 Application Ready。组件必须到激活阶段才 listen；setup 可 bind 未 listen 的 TcpSocket。框架不拦截任意用户网络调用。
- Test ID: CT-LIFE-INGRESS-001
- Test: both_roles_wait_for_all_ready_before_tcp_listen_and_build_has_no_side_effects（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Evidence: 本轮验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询）
- Responsibility: Framework Maintainer / QA

## LIFE-INGRESS-002：入口激活失败补偿

- Level: MUST
- Applies to: 0.0.3 开发树 + 本次框架指纹（已发布 v0.0.2 不含此增强）
- Status: Implemented；Verified 范围由本轮执行证据限定
- Precondition: 某入口已激活，后续入口返回错误或 panic。
- Trigger / Guarantee / Boundaries: 不可进入 Ready；初次启动保留 ActivateIngress 主错误、原始 FrameworkError source、清理结果及 retry handle；部分已激活入口必须经过停止生产、排空与 teardown。取消 start 等待不取消拥有资源的事务；Hook 须及时返回，不新增强制 timeout-drop。
- Test ID: CT-LIFE-INGRESS-002
- Additional Test ID: CT-LIFE-INGRESS-002-QUOTA；listener_task_quota_failure_compensates_all_partially_activated_sockets（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Additional Test ID: CT-LIFE-INGRESS-002-BIND；real_bind_failure_in_activation_retains_source_and_compensates_previous_ingress（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Additional Test ID: CT-LIFE-INGRESS-002-CANCEL；cancelled_start_wait_still_compensates_activated_module（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Test: partial_activation_error_and_panic_preserve_cause_and_release_all_ports（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Evidence: 本轮验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询）
- Responsibility: Framework Maintainer / QA

## LIFE-INGRESS-003：重建激活与重试

- Level: MUST
- Applies to: 0.0.3 开发树 + 本次框架指纹（已发布 v0.0.2 不含此增强）
- Status: Implemented；Verified 范围由本轮执行证据限定
- Precondition: 启用 Restart 且重建入口激活失败。
- Trigger / Guarantee / Boundaries: 激活纳入同一次恢复尝试；先关闭受影响根准入、停止任务、排空、移除 Handler 并 teardown 后才重试。累计预算耗尽转 FailFast；失败保留 ActivateIngress 阶段及根因。旧受管任务须退出、旧任务/服务能力失效；应用级 Hub 句柄保持可用，不作为入口存活的证据。
- Test ID: CT-LIFE-INGRESS-003
- Additional Test ID: CT-LIFE-INGRESS-003-BUDGET；activation_retry_budget_exhaustion_closes_all_ingress_and_reports_activation_phase（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Test: restart_activation_failure_compensates_before_retry_and_preserves_application_hub（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Evidence: 本轮验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询）
- Responsibility: Framework Maintainer / QA

## LIFE-INGRESS-004：入口关闭资源屏障

- Level: MUST
- Applies to: 0.0.3 开发树 + 本次框架指纹（已发布 v0.0.2 不含此增强）
- Status: Implemented；Verified 范围由本轮执行证据限定
- Precondition: Listener 或 Connection 任务尚未实际退出。
- Trigger / Guarantee / Boundaries: 组件须以取消优先的监听循环停止接单，外部消息经 external() 建立新根准入；连接/监听任务受 Owner 配额管理。StopIncomplete 不释放仍被使用的资源，继续检查式关闭只完成未完成阶段；受管任务错误进入现有监督，意外正常退出由组件升级为故障。
- Test ID: CT-LIFE-INGRESS-004
- Additional Test ID: CT-LIFE-INGRESS-004-LIMIT；connection_task_limit_rejects_without_losing_listener_or_leaking_work（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Test: slow_connection_retains_resources_until_checked_shutdown_can_finish（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Evidence: 本轮验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询）
- Responsibility: Framework Maintainer / QA

## LIFE-INGRESS-005：Context Hub 共享与隔离

- Level: MUST
- Applies to: 0.0.3 开发树 + 本次框架指纹（已发布 v0.0.2 不含此增强）
- Status: Implemented；Verified 范围由本轮执行证据限定
- Precondition: 同一 Application 中存在业务和网络 Module，宿主还可创建另一 Application。
- Trigger / Guarantee / Boundaries: 所有组件 Context 使用同一应用拥有的 Hub；组件禁止另建 Hub 用于替代应用内通信。Context 间可互通，不同 Application 不交叉路由；禁止另建 Hub 是组件契约，集成测试验证指定通信路径，不声称能拦截任意用户创建。
- Test ID: CT-LIFE-INGRESS-005
- Test: context_hubs_share_routing_and_different_applications_remain_isolated（源码定位：`../crates/modorch/tests/ingress.rs`；从匹配框架源码查询）
- Evidence: 本轮验证（源码定位：`../docs/validation/2026-11-06/release-0.0.3/README.md`；从匹配框架源码查询）
- Responsibility: Framework Maintainer / QA
