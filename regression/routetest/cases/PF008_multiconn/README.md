# PF008 多连接：2000 个客户、2000 个通道，各 8 个 bind

由 `bin/gen_multiconn_case.py 2000 8 1000 8` 生成，不要手改。

- `gmms.append`：`StreamShardsDefault=8`，每个客户、通道的 stream 只用 8 个分片（0 = 全部 64 个，即改进前的样子；《多连接性能分析与优化设计》3.1）。

- 客户 `rtc0001`～`rtc2000`（CCBSSID 20001～22000），每个 8 个 bind（`IncomingConnection` 的 `SessionNumber=8`，测试工具 `soak.binds=8`）；序号为奇数的连 MultiSmppServer1（16272），偶数的连 MultiSmppServer2（16273），每个 Server 1000 个客户、8000 个 bind。
- 通道 `rtv0001`～`rtv2000`（CCBSSID 30001～32000），每个 8 个 bind（`SessionNumber=8` 是整个通道的会话数，两个 Client 各 4 个），每个 Client 8000 个 bind，都连测试工具的 27001。测试工具等每个通道 8 个 bind 都到齐（`vendor.minLinks`，最多等 30 分钟）才开始发。
- 路由：号码前缀 `87` + 4 位序号 → 同序号的通道；客户 i 只发 `87{i:04d}` 开头的号码，所以每个通道的流量来自一个客户。
- 流量平均分到 2000 个客户：1000 TPS 时每个客户约 0.5 TPS。每档 10 分钟（预热 60 秒，排空 300 秒）。

运行：`bin/perf_step.sh cases/PF008_multiconn "1000 2000 ..." --no-replicas`（《性能测试部署与执行手册》4.2）。测试工具每个连接一个线程（2 × 16000 个连接），运行前 `vm.max_map_count` 要调大（默认 65530 不够）。
