# 本机持续负载与资源基线

用户选择先完成本机可复现测量，生产 SLO 保留待验收。`scripts/load-soak.py` 使用实际 secure-modorch-server，显式组合 TLS/Auth/SQLite、QoS2、自订阅客户端及不确认消息的持久慢消费者。默认持续 300 秒、8 个健康客户端、每客户端最多一个交换；每客户端每次完成后再等待 100ms，因此 rate-per-client 是间隔配置，不是保证达到的总吞吐。

```bash
cargo build -p secure-modorch-server --locked --offline
python3 scripts/load-soak.py --profile debug --duration 300 --clients 8 \
  --slow-messages 64 --payload-bytes 128 --output validation/load-baseline.json
```

容量比较应先运行`cargo build -p secure-modorch-server --release --locked --offline`，再使用`--profile release`；debug与release分别记录，优化构建通过不能抵销旧debug失败。`--connect-concurrency`默认1，可设为1..30以控制健康客户端的并发建连/订阅；不是稳态交换并发上限。`connection_attempts`记录每次建连的TCP/TLS、等待CONNACK阶段、耗时和真实失败，所有并发尝试完成后汇总，不自动重试掩盖拒绝。所有测量及互操作脚本支持 Cargo 标准的 `CARGO_TARGET_DIR`，便于与编辑器后台检查隔离。使用已有的隔离 Cargo 缓存和 offline 模式前，应确认依赖已经缓存。

输出 JSON 与同名 SQLite 数据库必须不存在。测量保存环境、动态输入摘要、二进制摘要、实际配置、100ms RSS/计数时间序列和原始延迟。实验使用 loopback、演示证书和测试账号，保留宿主 32 连接/IP 限额；健康客户端最多 30 个，另留两个连接给慢消费者和种子发布者。

64 条慢消费者消息在发布方完成 QoS2 交换后仍不发送下行 PUBACK，慢消费者保持连接并停止读取。健康客户端同时完成入站和出站 QoS2 交换才计入完成数；延迟包含两边握手、调度、网络和持久化开销。进程有活跃交换且所有客户端仍连接时发送 SIGINT，等待检查式退出，再从原数据库启动。重连必须 Session Present=1，慢消费者逐条核对载荷和顺序，并记录重复标志与Packet ID；对重启前明确观察且未确认的报文强制检查DUP=1及原Packet ID/载荷一致，再发送确认；最后使用同 Topic 的可靠 FIFO 屏障确认没有多余旧消息。信号时尚未完成的交换单独计数，不声称全部排空。

吞吐只使用信号前的完成数和计时时段；P99 使用信号前已完成的样本。RSS采样持续到进程退出。恢复计时从重新启动进程到慢消费者恢复 CONNACK，包含演示进程初始化、预监听存储恢复、TLS 和认证；不是单独 SQLite 恢复时延。数据库与原始材料在本地核对期间保留供复核；归档时仅保留最终结论，不将失败改称成功。

可显式传入 `--max-rss-kib`、`--max-rss-growth-kib`、`--max-p99-ms`、`--max-shutdown-seconds`、`--max-recovery-seconds` 和 `--min-messages-per-second`。未配置项为 NotRun；任一已配置阈值违反会返回非零，六项均满足才有本机阈值 Passed。未配置阈值的成功测量仍只表示本机功能和测量流程通过。五分钟实验不替代长期生产 RSS 趋势，也不代表生产 SLO 验收。

历史测量结论摘要见[验证结论摘要](validation/README.md#verification)。

[验证结论摘要](validation/README.md#verification)发现的V-02已由[验证结论摘要](validation/README.md#closed-issues)收口：`slow_consumer.observed_before_restart`保存实际已观察报文的原ID与载荷，`recovered_slow_messages`标注哪些帧有原始观察依据。未被客户端读到的后续帧可能已经发送或仍在排队，不对其DUP作无依据假设。真实Broker观察器分别清除DUP、替换原ID时，实际测量CLI均返回非零；正常短测、四条恢复消息及可靠FIFO屏障通过。实验显式阈值仅用于验证工具的通过路径，不作为生产目标。本轮短测不替代历史五分钟基线；旧报告缺少这些断言，结论不追溯升级。


[验证结论摘要](validation/README.md#verification)汇总V-02修复的受控正常恢复、错误DUP及错误原ID拒绝结论。另记录V-03：首次DUP探针与8进程存储扩压重叠时，在初次CONNECT等待CONNACK阶段EOF，尚未进入注入；受控复跑通过。该次前置失败不能计为DUP拒绝能力通过，也不能直接归因于Broker功能缺陷或历史alias超时；该历史失败的结论保留，原临时数据库和日志已按归档要求清理；需新现场时重新执行。

本轮[验证结论摘要](validation/README.md#closed-issues)在初次slow/seed CONNECT定位到Authenticate阶段约5s握手期限（观察5000–5011ms）；不是DUP注入失败，也不计作正常负载通过。TCP/TLS的受控时钟回归验证超时后的无迟到会话与正常对照。原无诊断历史EOF不唯一倒推；各次尝试的历史结论已汇总，原始过程文件不再归档。生产SLO仍按用户选择待验收。
