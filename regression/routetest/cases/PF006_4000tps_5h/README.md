# PF006 5 小时 4000 TPS：营销 + OTP（通道分开），不积压、不丢 DR；之后空闲 10 分钟看内存

**运行**：`bin/perf_long.sh cases/PF006_4000tps_5h --no-capture --no-replicas`（《性能测试部署与执行手册》4.3）。不抓包：5 小时约 7200 万条 MT，`rt.FlowCheck` 要把每条 MT 留到最后，内存放不下；一致性看 `submit 被拒`、`预热后没 DR 的`。压测期间只开 Redis 主库。

流量和配置同 `PF005_step`（第二轮）：5 个营销客户（各 640 TPS，共 3200，MultiSmppServer1，86138 → 营销通道 RTVM1）、5 个 OTP 客户（各 160 TPS，共 800，MultiSmppServer2，86139 → OTP 通道 RTVO1），每个客户 6 个 bind，每个通道 6 个 bind（两个 Client 各 3 个），都不限速。4000 TPS × 18000 秒（预热 300 秒），排空 1800 秒，空闲 600 秒。

预期：`report.md` 积压"稳定"、`submit 被拒` 0、`预热后没 DR 的` 0；`mem_report.txt` 为 `MEMORY: PASS`。
