# PF007H 4000 TPS 1 小时（PF007 的流量）

**运行**：`bin/perf_long.sh cases/PF007_4000tps_1h --no-capture --no-replicas`。流量、bind、客户和通道配置和 `PF007_4000tps_90m` 完全相同，只是发送 60 分钟（预热 5 分钟、排空 30 分钟、空闲 10 分钟不变）。

**用途**：和 11-20 的 PF007（4000 TPS 90 分钟，`perf_long_PF007_1120_0901`、`perf_long_PF007_1120_1201`）的前 60 分钟对比。客户、通道都没配 `StreamShards`，用 `StreamShardsDefault`（8），验证 2026-11-25 分片间隔改为 `TotalShards / S + 1` 后同一 ssid 的流在各 MQ 主之间平均（《5000TPS一小时测试瓶颈分析》6）。

**看什么**：同 PF007（`PF007_4000tps_90m/README.md`）：积压是否增长、提交 → 通道和提交 → DR 的延时、各 MQ 主的 CPU 和命令数是否接近、状态 Redis、有没有 2700 误判和 `[VDR-DROP]`。
