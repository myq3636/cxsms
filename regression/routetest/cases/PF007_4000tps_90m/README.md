# PF007 4000 TPS 90 分钟：过载时只积压，不再连锁恶化

**运行**：`bin/perf_long.sh cases/PF007_4000tps_90m --no-capture --no-replicas`。流量、bind 同 `PF006_4000tps_5h`，持续 90 分钟（PF006 在第 35 分钟开始出现 2700 误判，1 小时 28 分钟时状态 Redis 触发保护）。

预期（《DR早到重试与outbox应答超时改进设计》）：Core 日志没有 `outbox timeout result produced ... sent by the client`（2700 误判）；`[VDR-EARLY-STATS]` 的 dropped 为 0 或很少；状态 Redis 8386 不随时间上涨；`submit 被拒` 0、`预热后没 DR 的` 0。4000 TPS 超过这台机器的稳定容量时，`mt:routed` 可以积压、延时变大。
