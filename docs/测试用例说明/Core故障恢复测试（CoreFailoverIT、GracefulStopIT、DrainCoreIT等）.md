# Core 故障恢复测试（CoreFailoverIT、ShardZeroIT、GracefulStopIT、DrainCoreIT、ModuleOpsIT、RedisSwitchIT、Batch3IT、Batch4IT、Batch7IT A）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | CoreFailoverIT | it-multicore | 14 | 两个 Core 进程，kill -9，重启，手动移分片 |
> | ShardZeroIT | it-multicore | 21 | 没有分片的 Core（`none`），启动命令里的 NodeID |
> | GracefulStopIT | it-multicore | 17 | kill（SIGTERM）时内存里的东西写完，有时间上限；和 kill -9 对比 |
> | DrainCoreIT | it-multicore | 23 | `a2pctl drain`：下线一个 Core |
> | ModuleOpsIT | it-multicore | 36 | `a2pctl add / remove / resume / cancel`：加减 Core、移分片 |
> | RedisSwitchIT | it | 8 | `SWITCH_REDIS` 主备切换 |
> | Batch3IT | it | 37 | MT 处理中崩溃的去重、outbox、提交超时 |
> | Batch4IT | it | 26 | 死信、流的扫描、消费公平、分片检查、停机时写完 CDR |
> | Batch7IT A 部分 | it | 10 | 路由失败时的数据库兜底（B、C、P 部分在《长短信测试》） |
>
> Client、Server 的故障恢复（ClientFailoverIT、DrainClientIT、DrainServerIT、DrTransferIT 等）放在 Client、Server 两组里。

## 1. 测什么

Core 按**分片**读 MT：`stream:mt:pending:{客户}:{分片}`，每个 Core 只读分给自己的分片（`system:core:shards:{模块}`，或 GmmsConfig 的 `CoreShard.{模块}` / `MyShards`）。**分片是静态配置**：一个 Core 挂了，别的 Core 不会自动接管它的分片，它的积压等它重启，或者人工用 `RELOAD_CORE_SHARDS` 把分片交给别的 Core。

| 场景 | 要保证的 |
|---|---|
| Core 被 kill -9（空闲时、处理中） | 消息不丢；重启后积压和处理到一半的（待确认）都处理掉；**每条 MT 只发出一次** |
| 人工移分片 | 接手的 Core 处理掉积压和待确认的，只发一次 |
| Core 正常停止（kill） | 内存里排队的 SQL、CDR、SDQ 都写完；数据库卡住时也在时间上限内退出 |
| 下线（drain）、加减 Core | 移分片过程中 MT 不丢不重；新加的 Core 拿到分片前不碰流量；旧的 Core 没有回应时的处理 |
| 处理中崩溃 | 重投时已做过的事（CDR、计数、发出、写库）不再做，没做的照做 |
| 一条一直处理失败 | 超过次数进死信流，不无限重投 |
| Redis 主备切换 | 所有 Redis 都在新的一边是主时才切；切换后控制通道还能收到命令 |

## 2. 怎么跑、怎么判定

```
regression/run.sh CoreFailoverIT     # 其余套件同样
cat regression/.work/results/CoreFailoverIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- it-multicore 的套件：本进程扮演 Server（写 MT）和 Client（读出向流），另起**真实的 Core 进程**（`java … -Dmodule=CoreEngine1 -DNodeID=1 it.CoreNode`，每个进程复制一份自己的 a2p_home）。kill -9 用 `Process.destroyForcibly()`，kill 用 `kill -TERM`。
- “每条 MT 只发出一次”的判定：本程序记下发出的每个消息 ID，从各通道出向流 `stream:mt:routed:*` 里数每个 ID 出现几次，输出 `N MTs, missing 0, sent twice 0` 才算通过。
- DrainCoreIT、ModuleOpsIT 运行**真实的 `A2pCtl` 工具**（`a2pctl drain / add / remove / resume / cancel / status`），打开模块登记（`ModuleRegistryEnable=true`）。

## 3. 公共配置

### 3.1 cm.cfg 和路由（多 Core 的套件）

```
[V901]
CCBSSID: 901
A2PPlayerType: operator
Protocol: SMPP

[R900]
CCBSSID: 900
A2PPlayerType: operator
Protocol: SMPP
```

路由 `R900_routingInfo.cfg`：

```
CCBSSID: 900
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V901)
```

Batch3IT、Batch4IT、Batch7IT、RedisSwitchIT 用回归环境的基础配置（CUST8888 → VENDOR8，见《长短信测试》3.1）。

### 3.2 GmmsConfig

回归环境基础的 `Gmms/GmmsConfig.properties` 里：`NodeID=1`、`TotalShards=2`、`MyShards=0-1`。

多 Core 的套件追加：

```
RedisStreamAutoClaimEnable=true
CoreMtLeaseMs=10000
RedisStreamAutoClaimIdleMs=10000
RedisStreamAutoClaimIntervalMs=1000
```

auto-claim 的空闲时间设成 10 秒，比一个新启动的进程处理第一条 MT 的时间长，否则会把还在处理的条目领走（DEF-C2）。

| 套件 | 再追加 |
|---|---|
| DrainCoreIT | `AsyncSqlWriterEnable=false`、`ModuleStatusHeartbeatSeconds=1`、`ModuleStatusTTLSeconds=3`；子进程另加 `ModuleRegistryEnable=true` |
| ModuleOpsIT | `TotalShards=4`，`ModuleShardIntervalSeconds=0`、`ModuleShardConfirmSeconds=5`、`ModuleOpLockSeconds=6`，以及 DrainCoreIT 的那几项 |
| GracefulStopIT | `AsyncSqlMaxWaitMs=100000`、`AsyncSqlBatchSize=100`（让 SQL 积在内存里），MS-05c 加 `ModuleStopMaxWaitSeconds=2` |
| Batch3IT | `CoreOutboundUnreadStallMs=1500`、`CoreOutboundRespTimeoutMs=2500`、`CoreOutboundOutboxHardTimeoutMs=6000`、`CoreOutboundOutboxRecheckMs=1000`、`CoreMtLeaseMs=1500` |
| Batch4IT | `RedisStreamMaxDeliveries=3`（DLQ-06 用 0），auto-claim 空闲 300 毫秒、间隔 700 毫秒；FAIR 部分 `RedisStreamConsumer.CoreMT.WorkerCorePoolSize=1` |
| Batch7IT | `CoreMtLeaseMs=800`；A4 用 `CoreDbFallbackInline=false` |

`conf/a2p_modules.conf`（DrainCoreIT、ModuleOpsIT）：

```
1|CoreEngine2|core|x|0|core|10||
1|CoreEngine3|core|x|0|core|10||
1|MultiSmppServer1|smpp-server|x|0|server|40||
1|MultiSmppClient1|smpp-client|x|0|client|20||
```

（ModuleOpsIT 是 CoreEngine1、CoreEngine2，再加 MultiSmppClient2。）

## 4. 用例

### 4.1 Core 被 kill -9（CoreFailoverIT：CoreEngine1 分片 0，CoreEngine2 分片 1）

| ID | 步骤 | 预期 / 2026-11-27 实测 |
|---|---|---|
| FT-06a | 发 100 条 | 每条发出一次 |
| FT-06b | — | 两个分片都处理完 |
| FT-06c/d | 看日志 | Core 1 只读分片 0，Core 2 只读分片 1 |
| FT-06e | kill -9 Core 1（空闲），再发 100 条 | 分片 1 照常；分片 0 等着，没有自动接管（剩 52 条） |
| FT-06f | — | 没有丢：发出 48 + 等待 52 = 100 |
| FT-06g | 重启 Core 1 | 积压发出，每条一次 |
| FT-06h | 发 1500 条的过程中 kill -9 Core 1 | 分片 0 剩下：待确认 100 条，流里 503 条 |
| FT-06i | 重启 | 待确认的和积压都处理完，1500 条每条一次 |
| FT-06j | — | 分片 0 空了，没有待确认 |
| FT-06k | 再发 1500 条，过程中 kill -9 Core 1 | 分片 0 等着（248 条） |
| FT-06l | `system:core:shards:CoreEngine2` 设为 `0-1`，发 `RELOAD_CORE_SHARDS@CoreEngine2` | Core 2 接手，积压和待确认都处理完，每条一次 |
| FT-06m | — | 分片 0 空了 |
| FT-06n | 再发 50 条 | Core 2 处理两个分片 |

### 4.2 没有分片的 Core、NodeID（ShardZeroIT）

| ID | 目的 | 预期 |
|---|---|---|
| MS-01a/b | 分片值的校验 | 热加载接受 `none`（大小写、空格都行）；空值和错的值（`x`、超过 TotalShards 的 `5`）照样拒绝 |
| MS-31a/b | 启动命令 `-DNodeID` 优先于文件里的 `NodeID=1` | 两个进程是节点 5、6；消费者名字按节点不同 |
| MS-01c | 两个 Core 各一个分片 | 100 条每条一次 |
| MS-01d | Core 1 改成 `none`，`RELOAD_CORE_SHARDS` | 马上公布（不等 10 秒的心跳） |
| MS-01e | — | 日志写着 none |
| MS-01f | — | 分片 0 没人读，不会退回读分片 0（剩 52 条） |
| MS-02 | 对照：完全没有值 | 退回 `MyShards`（0），分片 0 被读 |
| MS-01g～j | Core 2 改成 `0,1` | 两次变化都公布了；Core 2 处理两个分片，100 条每条一次；没有分片的 Core 一条都没碰 |
| MS-01k | 分片检查 | 没有分片的 Core 不算重叠、也不算缺失 |
| MS-01l～n | 文件里 `CoreShard.x=none` 启动 | 启动时就没有分片；Core 2 照常处理 50 条，都在 Core 2 上 |
| MS-31c～f | 处理中 kill -9，换一个节点号重启 | 剩 709 条；旧消费者名字下待确认的被接过来，1500 条每条一次；不带 `-DNodeID` 时用文件里的 |

### 4.3 正常停止（GracefulStopIT，`it.StopNode` 起一个 CoreEngine1 进程）

| ID | 目的 | 预期 / 实测 |
|---|---|---|
| MS-03z | 对照：kill -9 | SQL 丢失：只写了 5000 / 5050 |
| MS-03a | kill（SIGTERM） | 5050 条 SQL 全部写完，包括一个写线程正拿着的那一批 |
| MS-03b | — | 4 秒内退出，退出码 143（实测 371 毫秒） |
| MS-03c | CDR 队列 | 900 条全部写完（kill -9 时这里也是 900） |
| MS-03d | SDQ | 900 条全部写完 |
| MS-03e | 刚分配完消息 ID 就 kill | 位置保存下来了 |
| MS-05a | 数据库卡住 | 到 4 秒上限就退出，在停止脚本的 kill -9 之前（4139 毫秒，退出码 143） |
| MS-05b | — | 日志说明哪些没做完 |
| MS-05c | `ModuleStopMaxWaitSeconds=2` | 2 秒退出（2137 毫秒） |
| MS-04a | `SHUTDOWN@模块` 命令 | 不结束进程；kill 才结束 |
| MS-04b | SHUTDOWN 以后再 kill | SHUTDOWN 写完了队列，kill 没有重复写（900 / 900） |
| MS-04c | SHUTDOWN 以后新来的 CDR | 走同步写，不进已经关掉的队列 |
| MS-03f～h | 真实的 `GmmsLauncher` 起一个 Server，kill | 335 毫秒退出，和以前一样快；所有停止任务做完（含停止读流），日志 `[STOP] SIGTERM: 3 of 3 jobs finished` |
| MS-32a/b | 负载下 kill 一个 Core | 358 毫秒退出；另一个 Core 接手，1500 条每条一次 |

### 4.4 下线一个 Core（DrainCoreIT：CoreEngine2、CoreEngine3 两个进程）

| ID | 目的 | 预期 |
|---|---|---|
| DC-00a/b | 同一台机器上两个模块，一个下线（`STATUS_OFFLINE`） | 另一个还在，节点留在 `system:server:nodes`；最后一个下线，节点去掉 |
| DC-01a/b | 初始化登记；下线前 | 提交结果两个 Core 都读，和分片无关 |
| DC-02a | drain 唯一的 Server | 拒绝：它是最后一个在线的 |
| DC-02b | drain CoreEngine2 | 状态 DRAINING |
| DC-02c | 再 drain 最后一个在线的 Core | 拒绝 |
| DC-03a | — | 等它的分片移走（阶段 `WAIT_SHARDS`） |
| DC-03b | 分片移走，没有处理中的 | `DONE` |
| DC-03c | — | 它不再读提交结果、供应商 DR、暂存的 MT |
| DC-04a/b | 下线以后 | 另一个 Core 读了全部 200 条提交结果、100 条供应商 DR；下线的读了 0 条 |
| DC-04c | 期间的 MT | 100 条每条一次 |
| DC-05a | 运行中 remove | 拒绝 |
| DC-05b～d | cancel | 回到 ACTIVE，下线进度清掉，又开始读结果（200 条里读了 59） |
| DC-06a/b | DRAINING 时重启 | 重新进入下线；启动时开的读线程又停掉（读了 0） |
| DC-07a | drain 一个正在下线的 | 拒绝（不是 ACTIVE） |
| DC-07b | `drain --wait` | 做完才返回，DRAINED |
| DC-07c/d | 停掉以后 | 移除；status 里能看到 |

### 4.5 加减 Core（ModuleOpsIT：TotalShards=4，CoreEngine1 分片 0-1，CoreEngine2 分片 2-3）

| ID | 目的 | 预期 |
|---|---|---|
| MO-00a/b | 初始化 | 两个 Core：0-1、2-3 |
| MO-01 | `add --dry-run` | 显示平均分配，移一个分片，什么都不写 |
| MO-02 | 用已经在用的模块号 | 拒绝 |
| MO-03a | 加第三个 Client：现有 2+2 个连接，再加 2 个，超过通道允许的 4 个 | 拒绝；显示出向 TPS 200 → 300 |
| MO-03b | `--force` | 只显示，不拒绝 |
| MO-03c | 去掉一个 Client | 4 → 2，要重启另一个才生效 |
| MO-04 | add CoreEngine3 | JOINING，没有分片，提示启动进程 |
| MO-05a～c | 同时第二个操作；同时 drain；另一个 a2pctl 拿着锁 | 都拒绝 |
| MO-06 | 进程还没起来就 resume | 再提示一次，别的不做 |
| MO-06b | status | 显示这个操作 |
| MO-07a/b | JOINING 的 Core | 不读提交结果（100 条里 0 条），日志说明原因 |
| MO-08a～c | resume | 分片 3 移过去，ACTIVE，操作完成；三个 Core 报 0-1、2、3；打印要写进配置的行（`CoreShard.CoreEngine3=3` 等） |
| MO-08d | 移分片期间的 MT | 78 条每条一次 |
| MO-08e/f | — | ACTIVE 后读提交结果（60 条）；锁和维护标记去掉 |
| MO-09 | `remove --dry-run` | 它的 2 个分片分给别人，什么都不改 |
| MO-10a/b | `remove --shards-to CoreEngine2` | 分片给 CoreEngine2，下线完，提示停进程；期间 94 条每条一次 |
| MO-10c/d | 运行中 resume；停掉后 resume | 再提示一次；REMOVED，它的分片值删掉，打印要删的配置行 |
| MO-11a/b | add CoreEngine4（分片 2 从 CoreEngine2 移过来），CoreEngine2 不回应（被停住） | 分片 2 不给 CoreEngine4，操作保留 |
| MO-11c | `resume --force` | 当作 CoreEngine2 已经没了，分片 2 给 CoreEngine4 |
| MO-11d | CoreEngine2 又回来 | 重新读它的分片值，只保留 0-1，不会有两个主人 |
| MO-12a～c | add CoreEngine5 后、运行前 cancel | REMOVED，什么都不留；模块号马上可以再用（它从没接过流量） |
| MO-13a～c | remove CoreEngine4，下线完、等停止时 cancel | 回到 ACTIVE，分片留在现在的位置（CoreEngine3 保持 2,3） |

### 4.6 处理中崩溃（Batch3IT，客户 8888 → VENDOR8）

| ID | 目的 | 预期 |
|---|---|---|
| A | 一条 MT 第一次处理：领取、写 IN_SUBMIT、号码计数（1）、发出第 0 部分 | 标记里记下第 0 部分的流 ID，CDR 和计数的标记和发出在同一个 Lua 里写；outbox `core:outbox:submit:8888_A:0:0` 和它的超时项写好 |
| A | 处理失败 | 释放占用，保留标记 |
| A | 重投 | 再领取；跳过 CDR；计数只读不加；不再发第 0 部分（出向流长度还是 1）；之前没做的照做；完成后确认、删标记、XACK+XDEL |
| B | 另一个消费者 | 占用没过期：BUSY；过期（原主人死了）：接手并完成 |
| C | 3 部分 | 3 个 outbox 项，不互相覆盖 |
| D | 提交结果 | 第 2 部分的第一个结果领取 outbox，不动第 1 部分的；`:done` 标记存 30 分钟（`CoreOutboundOutboxDoneTTLSeconds`），不是 4 小时；改成 600 后下一个用 600；第二个结果（超时后迟到的回应）STALE；同一个结果条目重试 FIRST；重发（retriedNumber 1）有新的 outbox 项；没有 outbox 的结果 UNTRACKED |
| E1 | 没有 Client 读 | 从出向流删掉，结果 2500 `COMMUNICATION_ERROR` |
| E2 | Client 确认了但没有结果 | 应答超时后 2700 `SUBMIT_RESP_ERROR` |
| E3 | 在 Client 的待确认里 | 到硬超时前不出结果；到硬超时 2700 |
| E4 | 排在一个正在读的 Client 后面 | 不拉回；Client 停止读以后，没读的拉回并回 2500 |
| E5 | 排队 3.2 秒（超过应答超时）后才发出 | 发出后不马上 2700；扫描看到发出后再过一个应答超时才 2700 |
| E6 | 超时用哪个值 | 通道 8 的 `BufferTimeout` 300 秒 → 等 360 秒；没配的通道用 `CoreOutboundRespTimeoutMs` |
| F | 真实的 `MTStreamConsumer` | 处理、确认、删标记 |

### 4.7 路由失败时的数据库兜底（Batch7IT A）

| ID | 目的 | 预期 |
|---|---|---|
| B7-A1a～c | 路由失败 | 处理返回 true（确认）；返回时 CDR 和失败 DR 已经写好（不再交给线程池），不进 `DBBackupHandler` 线程池 |
| B7-A2a/b | 确认丢了，重投 | 处理；不写第二条 OUT_SUBMIT，不发第二条失败 DR |
| B7-A2c | 标记写进去之前崩溃 | 失败 DR 再发一次（至少一次，不丢） |
| B7-A3a～c | 写 RMQ 失败 | 兜底没完成，留在待确认里；标记保留 CDR 的标记、去掉写 RMQ 的标记；重试时写 RMQ，不再写 CDR |
| B7-A4 | `CoreDbFallbackInline=false` | 交给线程池，失败 DR 照样到（异步） |

### 4.8 死信、扫描、公平、分片检查、停机写 CDR（Batch4IT）

| ID | 目的 | 预期 |
|---|---|---|
| DLQ-01 | 没超过次数 | 待确认的 MT 再领取（第 2、3 次投递） |
| DLQ-02 | 到 3 次 | 不再领取，移进 `stream:dlq:submit` |
| DLQ-03 | 死信内容 | 原因、来源流和组、投递次数、原消息 |
| DLQ-04 | — | 从原来的流和待确认里删掉 |
| DLQ-05 | — | ERROR 日志 `[STREAM-DLQ]`，带消息 ID |
| DLQ-06 | `RedisStreamMaxDeliveries=0` | 不限制（以前的行为） |
| DLQ-07/08 | 供应商 DR 每次都处理失败 | 3 次后由 auto-claim 移进 `stream:dlq:report`；不回失败 DR |
| SCAN-01～03 | 2 万个流 | 都找到；用 SCAN，不用 KEYS；之后读流的索引（SMEMBERS），不再扫描 |
| FAIR-01～03 | 一个大流（1500 条）和一个小流共用一个工作线程 | 小流 104 毫秒就处理了（大流还剩 1300），每 2 批把流让出来；大流之后处理完 |
| FAIR-04/05 | 线程池满（1 个线程、队列 20、60 个流） | 每个流都处理；分发线程没出错 |
| FAIR-06a/b | 满的线程池，30 个流 3000 条 | 都处理；没有一条在分发线程上处理 |
| SHARD-01～06 | 分片检查 | Server 的 TotalShards 是 1、别的是 16：ERROR；Core 分片 6、7 有两个主人：WARN；Client 分片 8～15 没人：ERROR；都打 `[SHARD-CHECK]`；配置一致：没有问题；每个节点公布 `TotalShards|它的分片`，带 TTL |
| CDR-01/02 | 停机时 CDR 队列里有 50 条，后台写线程已停 | 停机事件把 50 条都写进文件 |

### 4.9 Redis 主备切换（RedisSwitchIT）

回归环境只有一个 Redis，备用一边的地址也指向它，用来模拟两边。

| ID | 目的 | 预期 |
|---|---|---|
| RS-01 | 启动 | 在主的一边，它的 state Redis 回答 `role:master` |
| RS-02 | `SWITCH_REDIS:slave`，备用一边还不是主 | 拒绝，留在主的一边 |
| RS-03 | 备用一边变成主后再切 | 所有 Redis 都切到备用一边，`ha/RedisStatus` 写 `Used=S` |
| RS-04 | — | 切换后 Redis 正常使用 |
| RS-05 | 再发 `SWITCH_REDIS:master` | 控制通道还能收到（在新的 state Redis 上重新订阅了），切回主的一边 |
| RS-06 | 再发一次 `SWITCH_REDIS:master` | 什么都不做 |
| RS-07 | 启动时 `ha/RedisStatus` 是 S，主的一边回答 master | 用 M |
| RS-08 | 启动时哪边的 state Redis 都不是 master | 用 `ha/RedisStatus` 记的那边（S） |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 9 组共 192 项（Batch7IT 只算 A 部分 10 项），全部通过；表里的条数、毫秒数取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 整台机器断电、网络分区 | 只测了进程被 kill |
| Redis 真的挂掉（不是切换） | Core 这边没有用例；Client 有 ClientFaultIT（断开 Redis 的 TCP 代理） |
| Redis 主备切换时正在处理的消息 | RS 系列只测了切换本身 |
| 数据库主备切换（`DatabaseStatus`） | 没有用例 |
| 生产上 3 个 Core 同时挂掉一个以上 | 只测了两个 Core |
