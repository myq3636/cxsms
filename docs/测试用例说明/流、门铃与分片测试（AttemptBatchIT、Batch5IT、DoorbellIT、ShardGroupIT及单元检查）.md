# 流、门铃与分片测试（AttemptBatchIT、Batch5IT、DoorbellRaceIT、BacklogDoorbellIT、DoorbellBatchIT、ShardGroupIT、ShardPlanCheck、ShardHashCheck、Batch1Check、DataManagerCheck）

> 日期：2026-11-27。用例代码在 `regression/test/it/`（`*Check` 在 `regression/test/com/king/...`，unit 模式），都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | AttemptBatchIT | it | 29 | 一批条目的处理标记按批领取、确认 |
> | Batch5IT | it | 21 | 提交结果流按分片 |
> | DoorbellRaceIT | it | 3 | 门铃标记的竞争：流在排空时又写进来 |
> | BacklogDoorbellIT | it | 9 | 积压监控的“门铃丢失”判断 |
> | DoorbellBatchIT | it | 4 | 门铃按批弹出 |
> | ShardGroupIT | it | 8 | 分片检查按流的类别分组（SMPP 和 HTTP Client） |
> | ShardPlanCheck | unit | 11 | 加减模块时的分片分配方案（a2pctl 用） |
> | ShardHashCheck | unit | 7 | 分片的哈希分布 |
> | Batch1Check | unit | 10 | 分片公式的负数修复、DR 匹配数据在处理失败时保留 |
> | DataManagerCheck | unit | 9 | 数据库连接坏掉后不再留着用 |
>
> 设计见《StreamAttempt按批处理设计》《Redis多主分片设计》《Redis消息操作次数统计》《5000TPS多主测试慢的环节分析》。

## 1. 基本概念

| 名称 | 是什么 |
|---|---|
| 流 | Redis Stream，按 `{类型}:{ssid}:{分片}` 分开，例如 `stream:mt:pending:8888:0`、`stream:core:results:8:1` |
| 分片 | `TotalShards` 个，按消息的 key（如接收号码）哈希；每个模块只读分给自己的分片 |
| 门铃 | 一个有序集合，流里写进东西时把流名放进去，消费者从门铃里取有活的流（不用扫所有流）；取到后流排空了才去掉 |
| 处理标记 | `core:att:{流}:{条目id}`，记一个条目被谁占着（租约）、做过哪些事（标志）、是否做完（`_done`），重投时据此跳过做过的事 |

## 2. 怎么跑、怎么判定

```
regression/run.sh AttemptBatchIT     # 其余套件同样
cat regression/.work/results/AttemptBatchIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）；it 模式的套件进程内启动 Core，用回归环境基础配置（CUST8888 → VENDOR8 等，见《长短信测试》3.1）。
- 判定 Redis 往返次数时数 Lua（EVAL/EVALSHA）调用次数。
- unit 模式的套件直接调用类，不起进程。

## 3. 用例

### 3.1 处理标记按批（AttemptBatchIT，`CoreStreamAttemptBatch`）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| AB-01a/b | 读出 3 条供应商 DR | 3 条一次 Lua 领取 |
| AB-01c～e | 一批里三种状态 | 有 `_done` 的：DONE，不绑定；别人租约还新的：BUSY；新的：RUN，绑定到线程 |
| AB-01f～h | 处理新的那条 | 给客户发 DR；做完后标志和 `_done` 留在内存，不 HSET；做完后的动作（删 DR 匹配数据）等确认 |
| AB-01i～l | 确认 | DONE 和做完的一起确认（2 条），然后删匹配数据，标记随确认删掉；BUSY 的留在待确认里，带标记；只发出 1 条 DR（新的那条） |
| AB-02 | 同一个消息 ID 的第二条 MT | 不在这批领取里，确认后单独处理 |
| AB-03a～c | 400 毫秒的租约过了 260 毫秒才开始处理 | 还是 RUN；开始时续约（又 260 毫秒）；照常处理 |
| AB-04a0～c0 | 一批里处理完第一条就崩溃 | 两条都在待确认，发出了 1 条 DR；重投时已发出的不再发（它的 `p:` 标志随写出一起写了），另一条发一次；都确认，各 1 条 OUT_DELI_REP |
| AB-04a1～c1 | 同上，开关关 | 同样的结果 |
| AB-07 | `p:`、`delivery#` 标志 | 做完时和 `_done` 一起写，在确认之前 |
| AB-08 | 400 毫秒的租约过了 260 毫秒才做完 | `_done` 马上写 |
| AB-06 | 处理时条目 ID 变了 | 按读出时的 ID 确认，没有待确认，标记删掉 |
| AB-05a～c | 30 条 DR，开关开 / 关 | 都是每条发一次、都确认、没有残留标记；开的时候 Lua 少：38 次对 124 次 |

### 3.2 提交结果流按分片（Batch5IT，`CoreResultStreamSharded`）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| R5-01a | submit_resp | 写进 `stream:core:results:{rssid}:{分片}`（如 `stream:core:results:8:0`） |
| R5-01b | DR 应答 | 写进 `stream:core:results:{ossid}:{分片}`（如 `stream:core:results:8888:1`） |
| R5-01c | 没有 rssid | 用 ossid |
| R5-01d | 拆分消息的各部分 | 在同一个流 |
| R5-01e | 一个通道的结果 | 分布在 TotalShards=2 个流 |
| R5-01f | 没有消息 ID | 用 inMsgID 算分片 |
| R5-02 | `CoreResultStreamSharded=false` | 旧的 `stream:core:results` |
| R5-06a～c | 5 条结果在分片流里，门铃标记已经删掉 | 消费者启动时从流的索引找到并正常处理（写 OUT_SUBMIT） |
| R5-03a/b | 旧的流和分片的流都有 | 都被消费，两种结果都处理 |
| R5-09 | — | 分片流的跟踪日志是 `flow=result->core` |
| R5-04a/b | 性能 | 分片结果 10 个线程处理（旧的 2 个）；1200 条 250 毫秒，旧的 651 毫秒 |
| R5-05a～c | 通道 8 还有 700 条在排队 | 小通道的结果 101 毫秒就处理；之后积压也处理完 |
| R5-07 | 死掉的 Core 留在分片流里的待确认 | auto-claim 取回 |
| R5-08 | 200 条 3 段的长短信，64 个分片 | 每条在一个分片上，用到 62 个分片 |
| R5-10 | `StreamShardsDefault 4` | 客户的 200 条长短信在 4 个分片上（51、4、21、38），每条在一个分片 |

### 3.3 门铃（DoorbellRaceIT、BacklogDoorbellIT、DoorbellBatchIT）

| ID | 情况 | 预期 |
|---|---|---|
| RACE-02 | 在 XLEN 和去掉门铃之间又写进一条 | 门铃保留（旧代码 ZREM 掉了，这条就没人读） |
| RACE-03 | 真的空了 | 去掉门铃 |
| RACE-01 | 一边排空、一边写进 300 条 | 全部消费（剩 0），最后门铃不在 |
| BD-01 | 有条目、没门铃，第一次看到 | 不算丢失（多半是正在处理，消费者拿走了门铃） |
| BD-02/03 | 下次检查还是同一个第一条、还没门铃 | 算丢失；再下次也是 |
| BD-04/05 | 第一条变了（期间有人读）；之后又不变 | 不算；又算丢失 |
| BD-06 | 门铃响了 | 不算 |
| BD-07 | 门铃又被拿走 | 第一次看到，不算 |
| BD-08/09 | 排空了 | 不算，并从监视的流里去掉 |
| DB-01 | 门铃按批弹出，最多 4 个 | 按门铃的顺序（等得最久的先）弹出 4 个 |
| DB-02 | 其中一个流是空的 | 只去掉它的门铃 |
| DB-03 | 下一次 | 弹出剩下的 2 个，门铃空了 |
| DB-04 | 弹出过的流又写进来 | 又响 |

以前（2026-11-26 之前）积压监控在每次检查时把所有正在处理的流都算成门铃丢失，1000 TPS 时每分钟 10～20 次（PF009），BacklogDoorbellIT 测的是修正后的判断。

### 3.4 分片检查分组（ShardGroupIT）

| ID | 情况 | 预期 |
|---|---|---|
| SG-01 | SMPP Client 0-7 / 8-15，HTTP Client 8-15 / 0-7 | 不算重叠 |
| SG-02/03 | 分片的主人汇总 | 按组写成范围；没变化时不重复打 |
| SG-04 | 没有 HTTP Client 负责 0-7 | 对 httpclient 报 ERROR（负责 0-7 的 SMPP Client 不再把它掩盖掉） |
| SG-05 | 有变化 | 再打一次汇总 |
| SG-06 | 两个 SMPP Client 都负责 7、8 | 只在 client 组里报重叠 WARN |
| SG-07 | HTTP Client | 分片公布在 httpclient 下（模块角色还是 client） |
| SG-08 | 范围格式 | `0,1,2,3,8,9` 写成 `0-3,8-9` |

### 3.5 分片分配方案（ShardPlanCheck）

| ID | 情况 | 预期 |
|---|---|---|
| SP-01a/b | 3 个 Core（16 个分片）加到 4 个 | 新的拿 4、5、10、15，只移 4 个分片 |
| SP-01c/d | 4 个减到 3 个 | 只移走的那个的分片，分给别人 |
| SP-02 | 2 个 Client 加到 3 个 | 多出的一个分片留在拿得最多的那个上，移 5 个 |
| SP-03 | 1～9 个模块，1/2/16/36 个分片，随机起点，加一个或减一个 | 没有重叠、都有主人、多少最多差 1、移得最少 |
| SP-04a | 没人负责的分片 | 也分出去 |
| SP-04b | `--shards 4-5` | 从现在的主人那里拿 |
| SP-04c | `--shards-to` | 全给一个 |
| SP-04d/e | 解析 | 找出重叠、空洞、超过 TotalShards 的；`none` 和范围能读能写 |

### 3.6 哈希分布（ShardHashCheck）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| SH-01 | 以前的 `String.hashCode % 64`，手机号 | 最多 / 最少的分片 = 3.28 |
| SH-02 | 现在的 ShardHash | 1.06 |
| SH-03 | — | 同一个 key 同一个分片；只有一个分片或没有 key 时是 0 |
| SH-04 | `StreamShards 4`，一个 ssid 的 1 万个 key | 在它的 4 个分片上（从偏移 18 开始每隔 17：5、18、35、52），同一个 key 同一个分片 |
| SH-07 | 300 个 ssid（S = 4、8、16、32） | 在 2 个、4 个主上都均匀 |
| SH-05 | 2000 个 ssid × 4 个分片 | 64 个分片都用到，最多 / 最少 = 1.83，8000 个流 |
| SH-06 | `StreamShards` 为 0 或不小于 TotalShards | 和以前同一个分片 |

### 3.7 分片公式和 DR 匹配数据（Batch1Check）

| 情况 | 预期 |
|---|---|
| 新的分片公式 | 除了 `Integer.MIN_VALUE`，每个哈希都和旧公式同一个分片（n = 16、10、7、32） |
| `Integer.MIN_VALUE` | 不再得到负数分片（对照：旧公式在 n=10 时是负数） |
| 原消息 | 能序列化 |
| DR 处理失败 | 返回 false（不确认）；匹配数据保留；抛异常时也保留 |
| 重试 | 找到原消息，成功 |
| 成功后 | 删掉匹配数据 `drl:OUT1` |
| ACCEPT 状态的 DR（不是最终状态） | 保留匹配数据 |

### 3.8 数据库连接（DataManagerCheck）

| ID | 情况 | 预期 |
|---|---|---|
| DM-01/02 | 第一次用；还开着 | 拿一个连接，线程持有；再用还是它，不新开 |
| DM-03a | 连接关了 | 新开一个，死的从线程的列表里去掉 |
| DM-04a/b | `isClosed()` 本身出错 | 新开一个，旧的从列表去掉，并还给连接池一次 |
| DM-05 | 100 次断开 | 线程始终只持有 1 个连接 |
| DM-06 | 数据库没了 | 报错，但死连接还回去、不留着 |
| DM-07 | 数据库恢复 | 新开一个 |
| DM-08 | `closeSession` | 还回去并清空列表，和以前一样 |

## 4. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 全部通过 |
| 2026-11-27 | 测试服务器重跑（取上面的实测输出） | 10 组共 111 项，全部通过 |

## 5. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 生产规模的流数量下门铃的开销 | Batch4IT 测了 2 万个流的扫描（见《Core故障恢复测试》），门铃本身没有压测 |
| 分片分配方案在真实迁移中的执行 | 在 ModuleOpsIT（《Core故障恢复测试》4.5） |
| 数据库连接在真实 MySQL 断开时 | DataManagerCheck 用模拟的连接 |
