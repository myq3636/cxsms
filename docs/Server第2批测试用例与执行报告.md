# Server 第 2 批：测试用例与执行报告（日志、共享保活调度、有界发送队列、DR 收到响应后确认消费）

> 设计见《Server第2批优化设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

与 Server 第 1 批相同：在测试进程里启动真实的 `MultiSmppServer`，用测试代码中的 SMPP 客户端连接，本地 Redis 7.2.5，JDK 17，8 核 WSL2。

为测试新增的客户配置（cm.cfg）：

| 客户 | 配置 | 用途 |
|---|---|---|
| SMPPCUST30 | `ConnectionSilentTime: 3` | 静默超时 |
| SMPPCUST31 | `BufferTimeout: 3` | DR 没有响应时的超时 |
| SMPPCUST32 | `ServerEnquireLink: true`、`EnquireLinkTime: 1`、`FailEnquireLinkTime: 1` | 服务端主动发 enquire_link |

## 2. 功能用例（`ServerBatch2IT`）

| ID | 场景 | 预期 | 级别 |
|---|---|---|---|
| S2-01a | 一条 submit_sm | 日志依次出现 `[RECV]`（PDU）→ `[SUBMIT-IN]` → `[STREAM-PUT]` → `[SUBMIT-TO-CORE]` | P0 |
| S2-01b | 同上 | 不再在 INFO 级别打印整个消息（`Smpp receive message`） | P1 |
| S2-01c/d | 一条 DR，客户正常应答 | 日志依次出现 `[STREAM-GET]` → `[DR-OUT]` → `[DR-RESP]` → `[DR-RESULT-TO-CORE]`；`[STREAM-ACK]` 出现在 `[DR-RESP]` 之后 | P0 |
| S2-01e/f | 同上 | `[STREAM-GET]` 只打印简短形式；重复的 `resolved session` 不再是 INFO | P1 |
| S2-01g | 热加载 `SmppPduLogEnable=false` | 不再有 PDU 日志，业务日志仍然保留 | P1 |
| S2-02 | 再建立 20 个连接 | 没有 `ServerSessionTrd` 线程；共享的 `SmppSessionKeeper` 线程不超过 4 个；新增 20 个读线程和 20 个写线程（每连接 2 个） | P0 |
| S2-03a | 不 bind | 约 2 秒后被关闭（`SmppServerBindTimeoutMs`），共享调度下仍然生效 | P0 |
| S2-03b | bind 后不再发任何 PDU（`ConnectionSilentTime` 3 秒） | 约 3 到 4 秒后被关闭 | P0 |
| S2-03c | 开启 `ServerEnquireLink` | 服务端按间隔发 enquire_link，客户应答时连接一直保持 | P0 |
| S2-04 | 对端不读 socket（双方的 socket 缓冲都设为 4 KB，发送队列上限 200） | 队列达到上限后，`send` 抛出异常并关闭连接；关闭不会卡住 | P0 |
| S2-05a | 给不应答的客户发 5 条 DR | 5 条 stream 记录一直处于待确认状态（此时崩溃的话，会由自动重投重发） | P0 |
| S2-05b/c | 同上，`BufferTimeout` 3 秒 | 超时后写失败结果并确认消费；每条 DR 只有一个结果 | P0 |
| S2-05d/e | 正常应答的客户，300 条 DR | 全部送达、全部确认消费，没有一条发两次（客户的响应比 DR worker 记录序列号还快时也正确） | P0 |
| S2-05f | `ServerDrAckOnResp=false` | 恢复发出后就确认消费 | P1 |
| CFG（补充） | 5 个新配置项热加载 | 新值生效 | P1 |

## 3. 性能测试

本机 Redis 的限制同第 1 批（绝对数值偏低，只用于比较）。

### 3.1 1000 个已 bind 的空闲连接

| 保活方式 | Server 线程数 | 每连接线程数 | 堆 | 空闲 CPU |
|---|---|---|---|---|
| **共享调度（默认）** | 1952 | **2.0** | 58 MB | 6.3% 个核 |
| 每连接一个线程（`SmppServerSessionKeeperShared=false`） | 2902 | 2.9 | 59 MB | 6.6% 个核 |

- 线程数从每连接约 3 个降到 2 个。按 2 万个连接算，可以少大约 2 万个线程（以默认 1 MB 线程栈计，可以少预留约 20 GB 的虚拟内存）。
- 写线程不再每 200 毫秒醒一次，两组测试都包含这项改动，所以空闲 CPU 的差别不大。空闲 CPU 主要来自测试进程里 1000 个客户端读线程本身。

### 3.2 DR：收到响应后确认消费 vs 发出后确认消费

10 个客户，每个 1 个连接，立即应答。用 10 个线程写入 3000 条 DR。

| 方式 | 总耗时 | 其中写入 DR 耗时 | 写完之后多久全部送达并确认消费 |
|---|---|---|---|
| 收到响应后（第 1 次） | 9670 ms | 9393 ms | 277 ms |
| 发出后（第 1 次） | 10369 ms | 10013 ms | 356 ms |
| 收到响应后（第 2 次） | 10064 ms | 9694 ms | 370 ms |
| 发出后（第 2 次） | 10175 ms | 9689 ms | 486 ms |

两种方式下，Server 都能跟上写入速度：写完后 0.3 到 0.5 秒内全部送达并确认消费，没有可测量的差别。本机的瓶颈是测试程序往 Redis 写 DR 的速度。这和设计中的分析一致：每条 DR 的 Redis 访问次数不变，只是确认消费换了线程。

### 3.3 submit 吞吐（推荐配置，回归对比）

| 负载 | 第 1 批 | 第 2 批 |
|---|---|---|
| 20 连接 × 窗口 10 | 1368 TPS，p99 271 ms | 1349 TPS，p99 251 ms |
| 100 连接 × 窗口 20 | 1284 TPS，p99 2006 ms | 1312 TPS，p99 2027 ms |

两批之间的差异在测量误差范围内，没有退化。

### 3.4 每条消息的日志量（INFO，pduLogger 打开）

在同一台机器上，用改动前的提交和改动后的代码各跑同一个测试（1 条 submit、1 条 DR）：

| | 改动前 | 改动后 |
|---|---|---|
| submit | 4 行，1452 字节 | 4 行，1074 字节（−26%） |
| DR | 11 行，3374 字节 | 9 行，2726 字节（−19%） |

测试消息只有十几个字符。原来的"整个消息"日志会随着消息内容和字段变长，实际消息的节省会更多。现在每个关键点都有一行 INFO，pduLogger 的两行之外，其余都带 msgId。

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-S21 | Server 在发送 DR 的过程中被 kill -9 | 测试环境给一个不应答的客户发 DR，kill -9 后重启，确认这些 DR 在 5 分钟内（自动重投的扫描周期）被重新发出 |
| X-S22 | 真实的慢客户（不读 socket） | 观察 `Send queue full` 日志：连接被关闭，堆内存不再增长；同一 Server 上其他客户的 DR 时延不受影响 |
| X-S23 | 2 万个连接的线程数 | 确认约为 4 万个线程（加上固定的 worker） |

## 5. 回归结果

执行时间 2026-10-29 20:51（AEDT）；`run-all.sh` 从头全量编译后执行 18 个套件。

| 套件 | 检查数 | 通过 | 失败 |
|---|---|---|---|
| Batch1Check | 10 | 10 | 0 |
| InlineDrCheck | 7 | 7 | 0 |
| DrShardCheck | 10 | 10 | 0 |
| CoreDrFlowIT | 26 | 26 | 0 |
| Batch3IT | 32 | 32 | 0 |
| CoreE2EIT | 23 | 23 | 0 |
| ChannelRateIT | 40 | 40 | 0 |
| StartupIT | 2 | 2 | 0 |
| DoorbellRaceIT | 3 | 3 | 0 |
| ConfigReloadIT | 68 | 68 | 0 |
| Batch4IT | 25 | 25 | 0 |
| Batch5IT | 19 | 19 | 0 |
| Batch6IT | 22 | 22 | 0 |
| Batch7IT | 39 | 39 | 0 |
| ClientBatch1IT | 19 | 19 | 0 |
| ClientBatch2IT | 12 | 12 | 0 |
| ServerBatch1IT | 16 | 16 | 0 |
| **ServerBatch2IT（新增）** | 21 | 21 | 0 |
| **合计** | **394** | **394** | **0** |

## 6. 执行过程中的发现和调整

| 问题 | 原因 | 处理 |
|---|---|---|
| S2-04 第一次运行时卡住，直到超时 | **代码问题（原来就存在）**：客户不读 socket 时，写线程阻塞在 `write()` 里并持有写锁；`close()` 要先拿到这把锁才关闭 socket，结果永远拿不到。调用 `stop()` 的线程会被永久卡住 | `close()` 先关闭 socket，让阻塞的 `write()` 抛出异常，再拿锁清理输入输出流 |
| 第一版 S2-03c 只收到 1 次 enquire_link | 原有的计数逻辑：刚发出 enquire_link 时也算一次"未回"，所以下一次检查要等 `FailEnquireLinkTime`（默认 10 秒）。这是原来的行为，没有改 | 测试客户配置 `FailEnquireLinkTime: 1` |
| 第一版 DR 吞吐测试只有约 170 条/秒 | 测试程序单线程写 DR，瓶颈在测试程序本身 | 改为 10 个线程写入，并单独统计写入耗时 |
