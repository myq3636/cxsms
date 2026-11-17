# 高 TPS 一致性和内存测试设计

> 日期：2026-11-17。用例 `regression/routetest/cases/PF002_5000tps_1h`，由 `regression/routetest/bin/run_hightps.sh` 运行。测试服务器 8.219.93.186。

## 1. 要回答的问题

1. 在高 TPS（目标 5000 TPS）下持续 1 小时，**客户侧和通道侧实际收发的消息，和系统 CDR 记录的流程是否完全一致**：没有多发、没有丢失；
2. 高 TPS 之后**有没有内存泄漏**；
3. 跑不住时，**瓶颈在哪里**（各段的处理速度、Redis、各进程的 CPU）。

## 2. 流量

测试工具的压测模式（`soak.*`，同《内存泄漏测试设计》），混合流量：

| 模板 | 比例 | 路径 |
|---|---|---|
| M1 | 50% | 客户 rtcs1（MultiSmppServer1）普通 MT → 通道 rtvs1 → DR |
| M2 | 15% | 客户 rtcs3（MultiSmppServer2）普通 MT |
| M3 | 10% | 通道 rtvs2 回 0x45，改道到备份 rtvs1 |
| M4 | 10% | 3 段长短信 |
| M5 | 5% | 没有路由：失败 DR |
| M6 | 10% | 通道 rtvs3 不回 DR，客户 rtcs2 的假 DR 方式 4（5 秒）回成功 |

- 5000 TPS × 60 分钟（前 5 分钟不计入延迟），之后排空 2 分钟、空闲 10 分钟；每个客户 2 个 bind（`soak.binds`）；不重连、不 reload。
- 限速：客户 `SMSOptionIncomingThrottlingNum` 10000、通道 `SMSOptionOutgoingThrottlingNum` 10000、`ThrottlingControl.SystemIncomingThreshold` 20000；客户 `WindowSize` 5000（每个会话最多 5000 条 DR 在等 deliver_sm_resp），通道 1000。
- `com.king` 日志改为 INFO（TRACE 在 5000 TPS 下写不过来）。
- 每次运行的标记带时间（`{case}T{hhmm}-S{n}`，`RT_SOAK_TAG`），和之前运行留下、迟到的 DR 不混；运行前删掉之前这个用例留在 RDQ、WDQ、RMQ、SMQ 里的行。长短信的文本里重复放标记，任何一段都有完整的标记。

测试工具能发 5000 TPS 的改动：延迟改为按 1 ms 分桶的直方图（内存和条数无关）；每个客户多个 bind 轮流发；发送间隔 5 ms。

## 3. 抓包和对账（`rt.FlowCheck`）

1. 两个 tcpdump（各自 1 GB 缓冲）在回环接口上抓：客户侧（Server 端口 16272、16273）和通道侧（工具的通道端口 27001），只抓带数据的包和 SYN/FIN/RST，每 500 MB 一个文件，放在 `/usr/local/a2p/log/pcap_{case}_{时间}/`（保留；`CAP_KEEP=false` 读完删掉）。
2. `rt.FlowCheck` 边抓边读：重组 TCP 流、解出 SMPP PDU，按标记（或客户 DR 的 id → InMsgID → MT）计数到每条 MT：客户 submit_sm 和 resp、给客户的 DR 和 resp、通道 submit_sm 和 resp（成功 / 拒绝）、通道 DR 和 resp。
3. 抓包结束后读这次运行的 CDR（`cdr/`、`cdr/link/`，Core 的 `*.asyn.cdr`），按标记计数 IN_SUBMIT、IN_SUBMIT_PARTIAL、OUT_SUBMIT（成功 / 失败）、IN_DELI_REP（发给客户的 DR）、OUT_DELI_REP（通道的 DR）。
4. 逐条 MT 比较：

| 规则 | 说明 |
|---|---|
| 每个客户 submit_sm 正好一个 resp，没有被拒 | |
| CDR IN_SUBMIT = 客户的段数 | 长短信每段一条 IN_SUBMIT 和一条 IN_SUBMIT_PARTIAL |
| 给客户的 DR = 段数，= CDR IN_DELI_REP，每条都有 deliver_sm_resp | 少了是丢，多了是多发 |
| 通道接受的段 = 客户的段（有外发的 MT） | 少了是丢，多了是多发 |
| 通道接受（抓包）有 → CDR OUT_SUBMIT 成功；通道拒绝 = CDR OUT_SUBMIT 失败 | 无路由的 MT 有一条失败的 OUT_SUBMIT |
| 通道 DR（抓包）有 → CDR OUT_DELI_REP | |
| ID 作为多重集合比较 | 客户 resp 的 message_id = CDR InMsgID；给客户的 DR id = IN_DELI_REP 的 InMsgID（重复的单独计数）；CDR 里的通道 MsgID 必须都在抓包里 |

抓包完整的判据：重组时没有 TCP 缺口、SMPP 没有错位（tcpdump 的 "dropped by kernel" 只做记录）。结果写到 `result/{时间}/flow/report.txt`，最后一行 `RESULT: PASS / FAIL`。

## 4. 内存

和 MEM001 相同：`bin/memwatch.sh` 每 30 秒采样一次，每隔一次先 Full GC；`rt.MemReport` 判断（Full GC 后的堆斜率和增长、空闲后的保留、线程、句柄、Metaspace、Redis 不过期的 key），阈值见 `case.properties` 的 `mem.*`。

## 5. 运行和输出

```bash
cd /usr/local/a2p/routetest
bin/run_hightps.sh cases/PF002_5000tps_1h
```

`result/{时间}/`：`summary.txt`、`soak.txt`（流量和延迟）、`mem/`、`mem_report.txt`（内存）、`flow/`（`report.txt`、`capture_stats.txt`、tcpdump 和 FlowCheck 的输出、`pcap_dir.txt`）。

## 6. 注意

- **流量超过处理能力时对账没有意义**：积压到结束都处理不完，大量 MT 没有 DR；要用系统处理得完的速率（或延长空闲时间）。
- Redis 的数据目录在根分区：积压很大时 AOF 会涨到几十 GB（2026-11-17 第一次一小时测试写满了根分区，见《5000TPS一小时测试瓶颈分析》第 3 节），运行前确认磁盘空间，或把 Redis 数据目录移到 `/mnt`。
- 测试工具发 submit 不受窗口限制，Server 读得慢时数据积在 TCP 里，比真实客户更容易把同一连接上的 deliver_sm_resp 堵住（《Server发DR慢的原因分析》）。

## 7. 结果

见《5000TPS一小时测试瓶颈分析》（2026-11-17 一小时测试）、《Server发DR慢的原因分析》、《Redis CPU瓶颈分析》第 8～11 节。
