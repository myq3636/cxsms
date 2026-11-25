# PF009 1000 TPS 1 小时，20 个客户 × 20 个通道

**运行**：`bin/perf_long.sh cases/PF009_1000tps_20x20_1h --no-capture --no-replicas`（预热 5 分钟、发送 60 分钟、排空 10 分钟、空闲 5 分钟）。

**流量**：客户 rtcu01..rtcu20（ssid 9601..9620）按权重 20..1 发送，合计 1000 TPS（约 95 到 5 TPS）；单数客户连 MultiSmppServer1，双数连 MultiSmppServer2。客户 NN 发号段 86130NN，路由到通道 rtchNN（ssid 9621..9640）。

**通道**：模拟器的响应 20..180 ms、DR 600..2500 ms 不等；rtch05 的窗口 30、响应 300 ms，接近它的上限（约 100 TPS），会出现窗口满。

**配置**：`gmms.append` 打开 metrics 日志（`Metrics.LogLevel=INFO`、瞬时值、按 ssid 计数、DB 连接池），供 gw-metrics-agent 采集、Portal 网关监控图展示；跑完和其他配置一起恢复。

**看什么**：稳定性（积压不增长、延迟稳定、内存 PASS）；Portal 的网关性能监控和客户/通道监控两个页面的数据是否和压测报告一致。
