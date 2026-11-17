# Redis CPU 瓶颈分析

> 日期：2026-11-17。测试服务器 8.219.93.186（16 核，Redis 7.4.9：主 8386 state、8387 submit、8388 report，从 8376～8378 在同一台机器上）。起因：5000 TPS 混合流量（《高TPS一致性和内存测试设计》的 PF002：普通 MT、改道、3 段长短信、无路由、假 DR）跑不住，3 个主 Redis 一直在 95%～100% CPU。本文只做分析，**没有改代码**。

## 1. 结论

1. **瓶颈是 Redis 的单线程 CPU**。1000 TPS 实测，每条 MT 在各实例上耗的 CPU（扣掉空闲基线）：

   | 实例 | 每条 MT | 折算到 5000 TPS |
   |---|---|---|
   | 8388 report（结果、通道 DR、给客户的 DR、MQM 报告） | 651 µs | 325% 个核 |
   | 8387 submit（MT、外发） | 573 µs | 287% |
   | 8386 state（会话登记、DR 查找、长短信、假 DR） | 315 µs | 158% |
   | 从库 8377 / 8378 | 287 / 285 µs | 143% / 143% |

   单线程最多用满一个核，按 90% 算，8388 每秒只能支撑约 1400 条 MT；负载高时批量变大、每条摊薄一些，5000 TPS 试跑里实际处理能力约每秒 2000 条。**要到 5000 TPS，每条 MT 的 Redis 开销要降到现在的 1/3 以下，或者把负载分到更多 Redis 实例上。**
2. **大头是 Lua 脚本**：EVAL 占命令执行时间的 75%～79%（8387、8388）。每次 EVAL 平均 30～39 µs，脚本里的命令本身只有约 10 µs，**每次调用有 20 多 µs 是脚本调用本身的开销**：每次都把脚本正文（140～1070 字节）发过去并算 SHA1、进出 Lua、把写操作打包成事务复制给从库和 AOF。8388 每条 MT 收到约 12 KB 数据，大部分是脚本正文。
3. **每个 stream 条目的固定开销很高**，而每条 MT 会产生很多条目。8388 上每条 MT 约 9 个条目，8387 上约 3 个。每个条目要约 4～5 次 EVAL、10 次左右往返（第 4 节）。
4. **命令执行只占 Redis CPU 的一半左右**，另一半是网络收发（sys 态占 35%）、复制给同机的从库、AOF，以及 BGSAVE/AOF 重写的 fork（8388 116 秒里 fork 了 11 次）。
5. **后台 SCAN 白白占 CPU**：Server 每秒对 8386 做约 480 次 SCAN（每次 208 µs），空闲时也一样，低负载时占 8386 命令时间的 82%。stream 列表也是靠 SCAN 整个 keyspace 得到的，而 8387 上积了约 500 万个外发"已发送"标记。key 越多，每次扫描越贵，执行期间还会阻塞别的命令，造成延迟尖刺。

## 2. 方法

| 数据 | 怎么取 | 文件（测试服务器） |
|---|---|---|
| 每种命令的次数和耗时 | `INFO commandstats` 前后两次相减（`CONFIG` 被 `rename-command` 禁用，不能 `RESETSTAT`） | `/usr/local/a2p/log/rediscpu_*` |
| 进程实际 CPU、网络字节、fork | `INFO cpu` / `stats` / `persistence` 前后相减；先取 30 秒空闲基线 | 同上 |
| 每条命令、每个脚本、是哪个模块发的 | 100 TPS、30 秒，3 个主库同时 `MONITOR`（Lua 内部命令带 `[lua]` 标记，紧跟在所属 EVAL 之后）；连接端口用 `ss -tnp` 对应到模块 | `/usr/local/a2p/log/redismon_*` |
| 流量 | 测试工具压测模式，PF002 的混合（1000 TPS × 60 秒，60000 条 MT、71988 段；100 TPS × 30 秒，2999 条 MT） | `routetest/cases/PF002_cmd` |

1000 TPS 时系统跟得上（客户提交 → 通道收到 p50 52 ms、p95 188 ms），所以这组数是"正常处理"的开销，没有积压的干扰。

## 3. 数据

### 3.1 CPU 构成（1000 TPS，116 秒窗口：60 秒流量 + 排空）

| 实例 | CPU | 用户态 | 系统态 | 命令执行 | 其中 EVAL | 网络收 | 网络发 | fork |
|---|---|---|---|---|---|---|---|---|
| 8386 | 21% | 13% | 8% | 9% | 2% | 158 MB | 219 MB | 3 |
| 8387 | 31% | 20% | 11% | 15% | 10% | 458 MB | 473 MB | 3 |
| 8388 | 37% | 24% | 13% | 18% | 12% | 741 MB | 774 MB | 11 |
| 8377（8387 的从库） | 15% | | | 9% | | 343 MB | | 3 |
| 8378（8388 的从库） | 16% | | | 10% | | 516 MB | | 10 |

主库发出的字节大部分是复制流（8388 发出 774 MB，从库 8378 收到 516 MB）。

### 3.2 每条 MT 的命令（100 TPS MONITOR，扣除空闲基线）

| 实例 | 每条 MT 的命令 | 主要来自 |
|---|---|---|
| 8388 | 92 | Core（约 30）、Server1（4）、Client（4） |
| 8387 | 64 | Client（9）、Core（14）、Server（1） |
| 8386 | 6（另有空闲时每秒 1648 条后台命令） | Core 的 DR 查找 GET/SETEX、长短信 |

低 TPS 时每个任务往往只读到 1 个条目，`XREADGROUP`、`XGROUP CREATE`、取门铃这类按任务计的命令分摊不开，所以这里的次数比高 TPS 时偏多；按条目计的 Lua 调用不受影响。

### 3.3 Lua 脚本（100 TPS 窗口，2999 条 MT）

| 脚本（代码位置） | 实例 | 调用 | 每条 MT | 正文 | 脚本内的命令 |
|---|---|---|---|---|---|
| 写入 stream + 门铃（`StreamQueueManager` 364/425，`produce`） | 8388 / 8387 | 24139 / 4047 | 8.0 / 1.3 | 268～308 B | XADD、ZADD（结果写回时再加 XACK、XDEL） |
| StreamAttempt.FLUSH（记录处理进度，`StreamAttempt:85`） | 8388 | 16168 | 5.4 | 140 B | HSET |
| StreamAttempt.CLAIM（开始处理：检查已完成、租约，`StreamAttempt:64`） | 8388 / 8387 | 15837 / 3547 | 5.3 / 1.2 | 608 B | HEXISTS、HGET、HSET、EXPIRE、HGETALL |
| StreamAttempt.ACK（确认 + 删除条目 + 删除标记，`StreamAttempt:93`） | 8388 / 8387 | 15837 / 3547 | 5.3 / 1.2 | 158 B | XACK、XDEL、DEL |
| 未读检查（`StreamQueueManager.unreadEntries`，本次新增） | 8388 / 8387 | 10893 / 4881 | 3.6 / 1.6 | 171 B | XLEN、XPENDING |
| 只写一次（`PRODUCE_ONCE_LUA`，`StreamQueueManager:420`） | 8387 / 8388 | 4240 / 3750 | 1.4 / 1.3 | 766 B | HGET、HSET、XADD、ZADD、SETEX |
| Client 发送标记 CLAIM / ACK（`ClientSendMarker:48/66`） | 8387 | 4240 / 4240 | 1.4 / 1.4 | 561 / 329 B | GET、SET / EXISTS、SET、XACK、XDEL、DEL |
| Core 外发 outbox CLAIM（`CoreOutboundOutboxManager:68`） | 8387 | 4240 | 1.4 | 318 B | EXISTS、DEL、ZREM、SETEX |
| 确认 + 删除（`RedisConnection:1086`） | 8388 | 4545 | 1.5 | 135 B | XACK、XDEL |
| Server 收 MT 计数（`RedisConnection:230`） | 8386 | 3547 | 1.2 | 153 B | INCR、TTL |
| 长短信存段（`RedisCsmReassembler:45`） | 8386 | 822 | 0.3 | 1071 B | HGET、HSET×2、TTL×2… |
| 假 DR 到期扫描（`FakeDrTimeoutScheduler:31`）、延迟队列（`RedisConnection:1204`） | 8386 | 14012 / 12170 | 定时 | 203 / 278 B | ZRANGEBYSCORE（空闲时也每秒约 120 次） |

### 3.4 后台（没有流量时也在跑）

| 来源 | 实例 | 频率 | 说明 |
|---|---|---|---|
| Server 的 SCAN（`SmppServerSessionRegistry` 等） | 8386 | 约 480 次/秒，每次 208 µs | 按模式扫整个 keyspace（8386 上有 90 万个 key） |
| 各模块按模式列 stream（`keysSubmitMq` / `keysReportMq` → `scanAll`：自动认领每 5 分钟、积压监控每 10 分钟、启动时） | 8387、8388 | 每次扫完整个 keyspace | 8387 有约 500 万个 key：99% 是 `core:outbox:submit:{MsgID}:{段}:0:done`（TTL 超过 1 小时），扫一遍要约 5000 次 SCAN、每次约 430 µs |
| 假 DR、延迟 DR 的到期扫描 | 8386 | 各 Core 每秒约 60 次 | ZRANGEBYSCORE 的 Lua |
| MQM 报告重发 | 8388 | MQM1 每秒约 50 条 | Core 日志 `Reject stale-fence MQM message`：MQM 的任期（fence）过期，Core 拒收后 MQM 一直重发（**疑似功能问题，另查**） |
| 消费者空转（已修，见 5.2） | 8388 | 修之前空闲时每秒 2 万次 XREADGROUP | 只剩在途条目的 stream 被不停重按门铃 |

另外：8386 上约 30 万个 `RTVS1-*` 键（通道 MsgID → 原消息，用于匹配 DR），TTL 超过 1 天，是之前测试中没收到 DR 的 MT 留下的。

### 3.5 Redis 配置里加重负担的项

| 配置 | 影响 |
|---|---|
| `appendonly yes`、`appendfsync everysec`，同时 `save 900 1 / 300 10 / 60 10000` | 写入多时每分钟触发一次 BGSAVE（fork 一个 GB 级进程），再加上 AOF 重写；8388 在 116 秒里 fork 11 次 |
| 从库 8376～8378 和主库在同一台机器 | 复制流占主库发出字节的大部分，从库在 5000 TPS 时也要 1.4 个核 |
| `maxmemory 2048mb` + `noeviction` | 高 TPS 积压时一旦写满，所有写入都报错（5000 TPS 试跑 3 分钟，8386 从 166 MB 涨到 657 MB） |
| `CONFIG` 被改名禁用 | 无法在线调整（慢日志、持久化）；改配置要重启 |

## 4. 每个 stream 条目的开销

一条普通 MT（模板 M1）经过的 stream：Server → `mt:pending` → Core → `mt:routed` → Client → `core:results`（submit 结果）→ Core；通道 DR：Client → `core:inbound-dr` → Core → `dr:pending` → Server → `core:results`（DR 的结果）→ Core，还有 MQM 报告。合计约 7～9 个条目。每个条目：

| 步骤 | Redis 往返 | 其中 EVAL |
|---|---|---|
| 写入：XADD + ZADD 门铃（或只写一次版） | 1 | 1 |
| 取门铃：ZPOPMIN / ZRANGE（+ ZREM） | 1～2 | |
| `XGROUP CREATE`（每个任务都建一次组） | 1 | |
| `XREADGROUP` | 1 | |
| StreamAttempt.CLAIM | 1 | 1 |
| 业务处理里的读写（DR 查找、outbox、发送标记、CDR 相关） | 2～4 | 0～2 |
| StreamAttempt.FLUSH | 1 | 1 |
| StreamAttempt.ACK（XACK + XDEL + DEL） | 1 | 1 |
| 任务结束：未读检查 | 1 | 1 |

即每个条目约 10 次往返、4～6 次 EVAL；每条 MT 合计约 150 条命令、30 次 EVAL。

## 5. 可以优化的地方（只是评估，没有改）

按"不改变消息流程的语义、风险从低到高"排序。节省比例是根据上面的数据估的，要逐项实测。

| # | 优化 | 依据 | 估计效果 | 风险 |
|---|---|---|---|---|
| 1 | **EVAL 改为 EVALSHA**（启动时 `SCRIPT LOAD`，遇到 `NOSCRIPT` 回退到 EVAL）。只改 `RedisConnection.evalScript`，脚本内容和调用方都不变 | 每条 MT 约 30 次 EVAL，脚本正文占 8388 收到字节的大部分；每次 EVAL 有 20 多 µs 不在脚本命令上 | 网络收的字节减少约 70%，EVAL 的固定开销减少一部分；估计 Redis CPU 降 10%～25% | 低：语义不变，Redis 重启或主从切换后脚本缓存会丢，靠 NOSCRIPT 回退 |
| 2 | **Redis 持久化和复制的设置**：繁忙实例去掉 RDB 的 `save`（保留 AOF everysec），或者反过来；调大 AOF 重写的阈值；从库放到别的机器 | fork 频繁、复制流大、从库和主库争 CPU | 命令之外那一半 CPU 中的一部分；从库不再占本机 CPU | 低～中：运维变更，要评估数据可靠性的要求 |
| 3 | **去掉整个 keyspace 的 SCAN**：stream 列表改为维护一个索引集合（写入时登记），或只用门铃 zset 加上低频的全量扫描；Server 会话登记的扫描换成按会话的集合 | 8386 低负载时 82% 的命令时间是 SCAN；8387 每次全扫 5000 次 SCAN | 8386 的后台 CPU 基本消除；消除 SCAN 造成的延迟尖刺 | 中：要保证所有写 stream 的地方都登记，否则自动认领可能漏掉 |
| 4 | **缩短 outbox done 标记的 TTL，或改为按时间分桶的 hash** | 8387 上 500 万个标记，内存和扫描都随它涨 | key 数下降 1～2 个数量级 | 中：TTL 决定重投时的防重窗口，要和自动认领、重投的最长间隔对齐 |
| 5 | **按批处理 StreamAttempt**：一个批次（最多 100 条）的 CLAIM、FLUSH、ACK 各合成一次脚本调用（或用 pipeline），ACK 一次 XACK 多个 id、一次 XDEL | 每个条目 3 次 Attempt EVAL，按条目计、高 TPS 时也摊不薄 | 高 TPS 时 8388 每个条目从约 4～6 次 EVAL 降到约 1～2 次，估计 Redis CPU 降 30%～50% | 中～高：改动可靠性机制的核心，要重做 Batch6IT 这类重投、并发的测试 |
| 6 | **合并 FLUSH**：没有要记录的进度时不调用 FLUSH；有的话并进 ACK 脚本 | FLUSH 每个条目都调，每条 MT 5.4 次 | 每条 MT 少约 5 次 EVAL | 中：要确认 FLUSH 的数据在 ACK 之前不会被别的节点用到 |
| 7 | **减少每条 MT 的条目数**：看 `core:results`、`mqm:report` 有没有可以合并或省掉的（例如 MQM 报告只在需要时写） | 8388 每条 MT 约 9 个条目 | 每少一个条目，约少 10 次往返 | 高：涉及模块之间的协议 |
| 8 | **横向扩展**：report 实例拆成多个（例如结果、DR 各一个实例），或者 stream 按分片分到多个 Redis 实例 | 单实例单线程是硬上限 | 近似线性 | 高：架构改动，配置和故障切换都要跟着改 |
| 9 | **任务级开销**：每个任务都 `XGROUP CREATE`（可以只在第一次或 NOGROUP 时建）；取门铃和 ZREM 合成一次 | 低 TPS 时每个条目都付一次 | 低 TPS 时明显，高 TPS 时小 | 低 |

**要单独查的功能问题**（不是性能优化）：MQM 任期过期后报告被 Core 拒收、一直重发（3.4）；8386 上 TTL 超过 1 天的 `RTVS1-*` DR 查找键，以及 `maxmemory 2048mb` 写满时的行为。

## 6. 到 5000 TPS 的估算

现在 8388 每条 MT 651 µs，5000 TPS 需要压到每条 180 µs 以下（留 10% 余量）：

- 只做 1、2、3、9（低风险）：估计降到 450～550 µs，单实例约 1700～2000 TPS；
- 再做 5、6（批量化 StreamAttempt）：估计降到 250～350 µs，约 2600～3600 TPS；
- 要稳定 5000 TPS，**还需要横向扩展（8）**，或者大幅减少每条 MT 的条目数（7）。

这些数都是估计，建议先做 1（EVALSHA）和 2（持久化设置），用同样的方法（1000 TPS 下每条 MT 的 CPU）实测，再决定后面的项。

## 7. 本次已经改了、还没提交的两处

在这次分析之前改过，已部署到测试服务器的 a2p.jar 上；分析数据就是在这个版本上测的。两处都不改变每条消息的命令构成：

1. 通道 DR 的 stream 分片（`CoreInboundDRShardEnable`，默认 `true`）：原来一个通道的 DR 全在一个 stream 里，同一时刻基本只有一个 worker 逐条处理，3 个 Core 合计约 720 条/秒。
2. Core 的 4 个消费者（MT、结果、通道 DR、MQM）任务结束时只为未读条目重按门铃（`StreamQueueManager.unreadEntries`）：原来只要 stream 里还有在途条目就重按，空闲时 8388 也被刷到 100%。

改动前的 jar 备份在 `Gmms/WEB-INF/lib/a2p.jar.20261117c`（这两处之前）、`a2p.jar.20261117d`（只有第 1 处）。

## 8. 已实施的优化（2026-11-17：第 5 节的 1、2、3、9；不拆分 Redis）

### 8.1 Redis 配置（第 2 项）

6 个实例（主 8386/8387/8388、从 8376/8377/8378）的配置文件 `/usr/local/redis-7.4.9/conf/redis*.conf`，改动前备份为 `*.conf.bak.20261117`：

| 改动 | 原来 | 现在 |
|---|---|---|
| RDB 快照 | `save 900 1`、`save 300 10`、`save 60 10000` | `save ""`（不做快照；AOF `appendonly yes`、`appendfsync everysec` 不变；复制用的是 `repl-diskless-sync yes`，不需要 RDB 文件） |
| `CONFIG` 命令 | `rename-command CONFIG <改名>` | 不再改名（注释掉），可以用 `CONFIG GET` 检查、`CONFIG RESETSTAT` 清零统计；`FLUSHDB`、`FLUSHALL` 仍然改名 |

`rename-command` 只能在启动时生效，所以重启了 Redis。步骤：停 a2p 的 9 个模块 → 依次 `SHUTDOWN` 从库、主库（AOF 刷盘）→ 改配置 → 启动主库（`cd /usr/local/redis-7.4.9 && src/redis-server conf/{文件}`）→ 启动从库 → 等从库 `master_link_status:up` → 启动 a2p。注意：Redis 配置文件里**不能写行尾注释**（`save ""  # ...` 会启动失败，注释要单独一行）。

回退：把 `*.conf.bak.20261117` 拷回，按同样的顺序重启。

### 8.2 代码

| 项 | 改动 | 位置 |
|---|---|---|
| 1 EVALSHA | `RedisConnection` 里所有 Lua 调用（`evalScript` 和其余 11 处）改走 `evalCached`：用脚本的 SHA1 发 `EVALSHA`，`NOSCRIPT`（Redis 重启、主从切换后脚本缓存没了）时回退到 `EVAL`（同时把脚本放回缓存）。脚本内容、参数、返回值都不变 | `RedisConnection.evalCached` |
| 3 stream 列表 | 写 stream 的 5 段 Lua（写入、只写一次、结果写回 + 确认、暂存放行、死信）在 XADD 后 `SADD idx:streams {stream}`；列 stream 的地方（自动认领、积压监控、重按门铃、跨节点认领、DR 转移、`a2pctl`）读这个集合按模式过滤，不再 SCAN 整个 keyspace。每个模块第一次和以后每 `RedisStreamIndexFullScanMinutes`（60）分钟做一次全量 SCAN 校正集合 | `StreamQueueManager.streamKeys` |
| 3 Server 会话登记 | 会话明细 key 写入时同时 `SADD smpp:server:idx:{模块}`，删除时 `SREM`；每轮清理只检查这个集合里的 key。原来的 SCAN 只在旧数据清理（`SmppServerRegistryLegacyCleanupIntervalMs`，5 分钟）时做，扫到的、本模块保留的 key 补进集合 | `SmppServerSessionRegistry.cleanupCurrentModule` |
| 9 建消费组 | `createGroup` 在本 JVM 里记住建过的组，以后不再发 `XGROUP CREATE`；组被删掉时（stream 被删）`XREADGROUP` 报 NOGROUP，`recoverGroups` 绕过记录重新建 | `StreamQueueManager.createGroup` |
| 9 取门铃 | 原来 `ZRANGE` 之后对每个候选分别 `XLEN`、`ZREM`；现在 `ZRANGE` 之后一次 Lua：按顺序检查本节点负责的候选，空的删门铃，第一个有条目的删门铃并返回 | `StreamQueueManager.popResponsibleDoorbell` |

### 8.3 验证

**回归**：全量回归 73 个套件、1972 项全部通过（第一次跑出 11 项失败，原因和修改见下），基线更新为 Batch4IT 26 项（新增 SCAN-03）、ServerFuncIT KNOWN 23（有一项时序相关的已知缺陷有时通过、有时不通过，和这次修改无关）。

第一次全量回归的 11 项失败：

| 失败 | 原因 | 处理 |
|---|---|---|
| GracefulStopIT MS-32b（Core 接管后少了 20 条 MT）、Batch4IT DLQ-07、Batch5IT R5-03a/R5-07 | **漏改**：大部分 stream 是 `RedisConnection.xaddWithDoorbell` 写的，它的 Lua 没有登记到 `idx:streams`，自动认领列不出这些 stream，Core 接管时在途的 MT 没被认领 | 这段 Lua 也加上 SADD（共 5 段写 stream 的 Lua 都登记） |
| Batch4IT SCAN-01/02 | 测试直接用 jedis 写 20000 个 stream（不经过生产方），不在索引里 | 测试先调用新的 `scheduleStreamIndexFullScan()`；新增 SCAN-03：下一次列表读索引，不 SCAN、不 KEYS |
| ServerBatch3IT S3-01a、ClientPendingIT CP-02b | 测试只数 `EVAL` | 同时数 `EVALSHA` |
| RegistryBatchIT RB-09a | 测试直接写会话明细 key，不在模块索引里 | 测试同时写索引（和登记代码一样） |
| Batch5IT R5-06b | 文字说"by SCAN" | 改文字 |

**每条 MT 的 Redis 开销**（1000 TPS × 60 秒，方法同第 2 节，扣除空闲基线）：

| 实例 | 改前 CPU/MT | 改后 CPU/MT | 变化 | 命令/MT | Lua/MT | 每次 Lua | SCAN（窗口内） | 收到字节/MT | fork |
|---|---|---|---|---|---|---|---|---|---|
| 8386 | 315 µs | 196 µs | **−38%** | 19.4 → 18.7 | 2.5 → 2.5 | 15.4 → 13.9 µs | 29751 → 5405 | 2638 → 1961 | 3 → 0 |
| 8387 | 573 µs | 516 µs | −10% | 52.1 → 55.0 | 9.4 → 10.5 | 20.3 → 18.3 µs | 0 → 0 | 7626 → 4263 | 3 → 1 |
| 8388 | 651 µs | 636 µs | −2% | 73.1 → 75.4 | 17.0 → 17.1 | 13.4 → 13.1 µs | 10 → 0 | 12355 → 7353 | 11 → 9 |
| 8377 / 8378 | 287 / 285 µs | 268 / 310 µs | | | | | | | |

1000 TPS 时的延迟（客户提交 → 通道收到）：p95 188 → 84 ms，p99 1370 → 96 ms，max 2436 → 148 ms（全 keyspace SCAN 造成的停顿没有了）。

**5000 TPS 试跑**（3 分钟）：仍然跟不上，和优化前差不多（工具发出计划的 85%，`mt:pending` 最多积 38 万条，8387/8388 负载时 91%～99%）。

**结论**：

1. 去掉 SCAN 对 8386 效果明显（−38%），也消除了延迟尖刺；
2. **EVALSHA 只减少了网络字节**（−40%），每次 Lua 的耗时几乎没变（13.4 → 13.1 µs）：Lua 的开销主要在脚本执行本身（进出 Lua、脚本里的每条命令、写操作的事务复制），不在传脚本正文和算 SHA1。要降 Lua 的开销，只能减少调用次数（第 5 节的 5、6，按批处理 StreamAttempt、合并 FLUSH）；
3. 8388 的 fork 只少了 2 次：RDB 关掉后，fork 来自 **AOF 重写**。8388 数据只有 23 MB，但 stream 写了就删，AOF 涨得快，按 `auto-aof-rewrite-percentage 100`、`auto-aof-rewrite-min-size 64mb`，从重启到现在（约 1.5 小时）重写了 65 次。可以把 `auto-aof-rewrite-min-size` 调大（例如 1gb，现在 CONFIG 能用，可以在线改并写回配置文件），本次没有改；
4. **8387 内存已到 985 MB**（`maxmemory 2048mb`、`noeviction`），主要是约 500 万个 outbox done 标记（第 5 节第 4 项），写满后所有写入会失败，需要尽快处理；
5. 不拆分 Redis 的前提下，要到 5000 TPS，需要继续做第 5 节的 5、6（减少每个条目的 Lua 调用），估计 8388 每条 MT 能降到 300 µs 左右，单实例约 3000 TPS；要再高，还需要第 7 项（减少每条 MT 的条目数）。

## 9. 8387 的内存：outbox 已完成标记（2026-11-17 16:30）

8387（submit）在 5000 TPS 试跑之后用了 985 MB（`maxmemory 2048mb`、`noeviction`）：

| 项 | 值 |
|---|---|
| key 数 | 608 万个（`db0:keys=6082856,expires=6074270`），**99.9% 是 `core:outbox:submit:{MsgID}:{段}:0:done`** |
| 每个标记 | 值是结果在 stream 里的 id（如 `1794903251236-0`），`MEMORY USAGE` 约 120 字节；加上 key 字典、过期表的开销，平均约 160 字节（985 MB / 608 万） |
| TTL | 随机抽 3000 个：都在 4 小时以内（1 小时内 44%、1～2 小时 40%、3～4 小时 16%），**没有超过 4 小时的**：它们在按时过期，不是泄漏 |

**这些标记是什么、为什么要留着**（`CoreOutboundOutboxManager`）：Core 把 MT 写进 `stream:mt:routed` 时同时写一份 outbox（消息体 + meta + 超时 zset），Client 回的 submit 结果到 Core 时，`CLAIM_LUA` 删掉消息体和 meta，**写一个 `:done` 标记，TTL = `CoreOutboundOutboxPayloadTTLSeconds`（14400 秒，4 小时）**。标记用来去重：同一个结果条目重投（自动认领）时认成同一个（FIRST），超时扫描先产生了超时结果、之后真实结果才到时认成多余（STALE）。所以它们**不会在处理完马上删掉，要等 4 小时自动过期**。

**数量 = 每秒外发段数 × 14400**，内存约每个 160 字节：

| 持续速率 | 4 小时内的标记 | 内存 |
|---|---|---|
| 100 TPS（约 120 段/秒） | 173 万 | 约 0.28 GB |
| 500 TPS | 864 万 | 约 1.4 GB |
| 1000 TPS | 1730 万 | **约 2.8 GB，超过 maxmemory 2 GB** |
| 5000 TPS | 8640 万 | 约 14 GB |

即**持续 800 TPS 左右 4 小时，8387 就会写满**；写满后（`noeviction`）所有写入都报 OOM，MT 写不进 stream。原计划的 5000 TPS × 1 小时测试，大约 30 分钟就会写满 8387。

4 小时是为了盖住 outbox 的硬超时（`CoreOutboundOutboxHardTimeoutMs` 3 小时）：消息体要等到硬超时以后。但 `done` 标记只需要活到"同一次发送的第二个结果还可能到达"为止：Client 自己的 submit 超时（会产生超时结果）、结果条目被自动认领重投（`RedisStreamAutoClaimIdleMs` 60 秒 × 最多 `RedisStreamMaxDeliveries` 10 次）都在分钟级。现在它和消息体共用一个 TTL 配置，**只调小 `CoreOutboundOutboxPayloadTTLSeconds` 会让还没超时的消息体先过期**，所以不能只改配置。

可选的处理（都要改代码，这次没有改）：

1. 新增 `done` 标记自己的 TTL（例如默认 1 小时），和消息体的 TTL 分开：内存降到现在的 1/4；
2. 标记改为按时间分桶的 hash（每 10 分钟一个 hash，field = 段，整个 hash 过期）：每个标记约 30～40 字节，再降 3～4 倍，同时 key 数降几个数量级（SCAN、过期扫描都更轻）；
3. 不改代码时的权宜：调大 8387 的 `maxmemory`（机器有 61 GB 内存），并监控 `used_memory`。

### 9.1 `:done` 标记防的是什么

同一次外发（outbox key `{MsgID}:{段}:{重试次数}`）可能有两个结果：Client 回的真实结果（通道的 submit_sm_resp，或 Client 自己的超时），和 Core 的超时扫描生成的超时结果（交给通道 3 分钟没结果 `CoreOutboundRespTimeoutMs`、Client 读了 3 小时没确认 `CoreOutboundOutboxHardTimeoutMs` 等）。只能有一个算数：先到的删掉 outbox、写 `:done`（值是这个结果的 id），后到的查到 `:done` 且 id 不同，丢弃（`[RESULT-DROPPED]`）。例如：

```
10:00:00  Core 把 MT 交给 Client，Client 发给通道
10:03:00  3 分钟没有结果 → Core 的超时结果先到，写 :done，按失败处理 → 重试策略改道重发
10:03:20  原通道的成功 submit_sm_resp 才回来 → 查到 :done、id 不同 → 丢弃
```

如果 10:03:20 时 `:done` 已经没了，迟到的结果会当成 UNTRACKED 正常处理：同一次外发既按失败改道重发、又按成功记一次（OUT_SUBMIT CDR 重复，DR 也会乱）。同一个结果条目被自动认领重投另有 StreamAttempt 的标记，不靠 `:done`。

`:done` 是第一个结果到达时才写的，硬超时的 3 小时已经在它之前过去了，它只需要盖住"第一个结果之后第二个结果还可能到达"的间隔：结果 stream 的积压（正常秒级，过载时几分钟）、Client 宕机后待处理的 submit 被别的 Client 接管并判超时（分钟级）。

### 9.2 修改（2026-11-17）

- 新增 `CoreOutboundOutboxDoneTTLSeconds`，默认 **1800 秒（30 分钟）**（热加载），`:done` 标记用它；消息体和 meta 仍用 `CoreOutboundOutboxPayloadTTLSeconds`（4 小时）。按每个标记约 160 字节：持续 1000 TPS（约 1200 段/秒）30 分钟内约 216 万个、约 0.35 GB；5000 TPS 约 1.7 GB；
- 6 个 Redis 实例 `maxmemory` 2048mb → **8gb**（在线 `CONFIG SET`，同时写入配置文件；改前备份 `*.conf.bak.20261117b`），策略仍是 `noeviction`。6 个实例上限合计 48 GB，机器 61 GB，a2p 和 MySQL 约用 20 GB：实际用量远低于上限，但要监控 `used_memory`；
- 已经写下的标记保持原来的 4 小时 TTL，到期自然过期；
- 回归：Batch3IT 新增两项（标记 TTL 30 分钟、消息体仍 4 小时；改配置后下一个标记用新值）。

## 10. StreamAttempt 按批处理的设计（方案 a，第 5 节第 5、6 项；还没有改代码）

### 10.1 StreamAttempt 现在保证什么

stream 是至少一次投递：处理完却没确认的条目（确认失败、处理中崩溃、被自动认领拿走）会再处理一次。StreamAttempt 给每个条目（MT 按 MsgID：`core:mt:{msgId}`；通道 DR、结果按条目：`core:att:{stream}:{id}`）一个标记 hash，保证重投时不重复已做过的副作用：

| 字段 | 作用 | 什么时候写 |
|---|---|---|
| `_lease` | 处理者和开始时间；别的消费者看到租约新鲜（`CoreMtLeaseMs`，60 秒）就让开（BUSY） | CLAIM：每个条目开始处理时 |
| `{类别}#{n}:{详情}` | 第 n 个 CDR、DR、写库、计数等已经做过 | 随下一次写 stream 的 Lua 一起写；没有写 stream 的在 FLUSH 时写 |
| `_done` | 整条已处理完，重投时只确认 | FLUSH：**确认之前**单独写 |
| 确认 | XACK + XDEL + 删除标记 | ACK：一段 Lua |

每个条目 CLAIM、FLUSH、ACK 各一次 Lua，8388 上每条 MT 约 16 次（第 3.3 节）。

### 10.2 方案 a：CLAIM、ACK 按批，FLUSH 每个条目一次但不用 Lua

| 调用 | 改成 | 对功能的影响 |
|---|---|---|
| CLAIM | 一批（`consumeBatch` 读出的 N 个条目）一次 Lua：对每个标记做现在的检查（`_done` → DONE，租约新鲜 → BUSY，否则写租约 → RUN），返回每个条目的状态和已记录的字段 | 同一条目的判断规则不变。**租约从批开始时算**：一批的处理时间必须远小于 `CoreMtLeaseMs`（60 秒）；处理每个条目前检查已用时间，超过一半就单独续租。一批里 BUSY、DONE 的条目分别按现在的方式处理（BUSY 留在 pending，DONE 只确认） |
| FLUSH | **仍然每个条目处理完时写一次**，内容和时机都不变，只把 `FLUSH_LUA` 换成普通 `HSET`（放弃时再加 `HDEL _lease`，两个命令放一个 pipeline） | 不影响：写入的字段、顺序、时机和现在一样，只是不用 Lua。崩溃时重复副作用的范围和现在相同（最多一个条目里还没写标记的那部分） |
| ACK | 一批处理完，一次 Lua 对所有 RUN 和 DONE 的条目 XACK（一次多个 id）、XDEL、删除标记 | 不影响：确认前每个条目都已写了 `_done`，批量确认失败时整批重投，都只被确认。变化只是条目在"待确认"里多停一个批的处理时间 |
| `afterDone` | 仍在该条目的 `_done` 写入之后执行（例如删 DR 查找记录），不等批结束 | 不变 |
| 处理失败 | 只放弃这一个条目（释放它的租约、写已做的副作用标记），批里其他条目照常 | 不变 |

**不做方案 b**（一批结束时才一起写 `_done`）：Core 在一批中间崩溃时，这一批里已处理、还没写 `_done` 的条目，重投时会重复 CDR、写库、发 DR。

### 10.3 改动范围

- `StreamAttempt`：现在用 ThreadLocal 绑定"当前条目"，副作用代码（`once`、`markDoneNow`、写 stream 的 Lua 带上标记）都通过它；按批以后每个条目处理前把它的 attempt 设成当前的，处理完写 FLUSH，批结束统一 ACK。
- 4 个使用它的消费者：`MTStreamConsumer`、`ResultStreamConsumer`、`InboundDRStreamConsumer`、`CoreMQMStreamConsumer`（批循环、BUSY/DONE 分支、失败放弃）。
- 自动认领（`RedisStreamAutoClaimRunner`）一次处理一批，同样适用。
- 估计收益：8388 每条 MT 的 Lua 从约 16 次降到约 0.3 次，另加约 5 次普通 HSET；按第 8 节的每次开销估算，每条 MT 少约 150～200 µs（25%～30%）。

### 10.4 要测的

- Batch6IT（重投、并发认领、`_done` 之后确认失败）；
- CoreFailoverIT、GracefulStopIT、ShardZeroIT（压力下杀 Core 或换节点，检查每条 MT 只处理一次）；
- Batch4IT（重投到上限进死信）；
- 新增：一批处理到一半时杀 Core，检查重投后没有重复 CDR、DR、写库；一批的处理时间接近租约时，另一个消费者不会并发处理同一条；
- 用第 2 节的方法重测每条 MT 的 Redis CPU。

## 11. io-threads 实测（2026-11-17 19:25）

配置：3 个主库 `io-threads 3`、`io-threads-do-reads yes`（主线程 + 2 个 I/O 线程；从库不变），改前备份 `*.conf.bak.20261117c`，重启 Redis。测试前 Redis 只留 `thcon`、MySQL 清空；Server 已经打开共享收包池（《Server发DR慢的原因分析》第 5 节）。

**1000 TPS × 60 秒**（主线程 CPU 用 `used_cpu_*_main_thread`）：

| 实例 | 主线程（改前 → 改后） | 整个进程（改前 → 改后） | 空闲时整个进程（改后） |
|---|---|---|---|
| 8386 | 17% → 15% | 17% → 29% | 1% |
| 8387 | 28% → 29% | 28% → 75% | 57% |
| 8388 | 34% → 35% | 34% → 118% | 137% |

主线程没有变轻，I/O 线程（8388 窗口内 61 万次读、77 万次写交给了 I/O 线程）额外占了大量 CPU，空闲时也在忙等。

**5000 TPS × 3 分钟**：

| | 没有 io-threads（19:02） | io-threads 3（19:29） |
|---|---|---|
| 客户收到的 DR | 32.3 万 | **7.0 万** |
| 客户 submit 被拒（`ESME_RTHROTTLED`） | 0 | 1.6 万 |
| `core:inbound-dr` / `core:results` 积压 | 基本为 0 | 13 万 / 23.6 万 |
| Core 的 CPU | 100%～120% | 40%～70% |
| Redis 主线程 | 95% 以上 | 86%～100% |
| Redis 整个进程 | 约 1 核 | 8387 约 2.6 核、8388 约 1.9 核、8386 约 1.7 核 |

**结论：在这台机器、这种负载下，io-threads 让吞吐下降。** 原因：

1. Redis 7 的 I/O 线程和主线程同步交接：每轮事件循环，主线程分发读、等所有 I/O 线程做完，执行命令，再分发写、再等。命令（尤其是 Lua）仍然只在主线程执行，我们的负载以执行为主，主线程省下的时间有限，每次请求的往返却因为交接变长；
2. a2p 访问 Redis 都是同步的（每个线程发一条命令等返回），往返变长，吞吐就下降；
3. I/O 线程忙等，3 个主库多占约 4～5 个核，和 a2p 争 CPU（16 核的机器本来就紧）。

**已改回（2026-11-17 19:40）**：清理数据（Redis 只留 `thcon`、MySQL 清空）后恢复 `*.conf.bak.20261117c`（开 io-threads 时的配置另存为 `*.conf.iothreads.20261117`），重启 6 个 Redis 实例和 a2p；现在 `io-threads 1`、`maxmemory 8gb`、不做 RDB、AOF everysec。要从 Redis 侧提高上限，还是要减少每条消息的命令数（第 10 节），或者增加主库数量（《Redis集群方案设计》）。
