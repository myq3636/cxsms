# Core 优化测试用例与执行报告

> 覆盖范围：Core 优化第 1 到 3 批，以及 Server DR 分片修复。
>
> | 批次 | 内容 | 提交 |
> |---|---|---|
> | 第 1 批 | DR 反查缓存延后删除、收件人计数原子化、分片计算、`putMsg` 判空 | `f1428aa` |
> | 第 2 批 | DR 同步转发、`CoreDrInlineDispatch` | `7b9a4cc` |
> | 第 3 批 | MT 幂等、外发 Lua、outbox 改造、按状态超时 | 未提交 |
> | Server DR 分片 | Server 按模块消费所有 DR 分片 | `81c55bf` |
>
> 第 1 到 4 节是用例设计，第 5 节是本地执行结果。

## 1. 测试环境

| 项 | 说明 |
|---|---|
| Redis | 7.2.5，官网源码本地编译，127.0.0.1:16379；state、submitMq、reportMq 三个实例指向同一个 Redis |
| JDK | Temurin 17.0.21，`--release 17` 全量编译 `src/` |
| Core 启动 | 真实的 `GmmsUtility.initUtility`（读取 `GmmsConfig.properties`、`cm.cfg`、`conf/routing/*`）、`initRedisClient`、`initCoreShardAssignment`、`initCDRManager`、`initDBManager` |
| 客户配置 | A2P 自身 56；客户 8888（SMPP，`SMSOptionRecipientMaxSendCountIn24H: 100`）；供应商 8；客户 7001 |
| 路由 | `conf/routing/CUST8888_routingInfo.cfg`：`A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR8)` |
| 数据库 | **没有真实数据库**：`DataControl` 标记为已初始化，只注册了 `MessageStoreManager`。会落库的兜底路径（RMQ、WDQ、SDQ 等写入）不在本地验证范围 |
| Client / Server | 不启动。由测试程序扮演：往 `stream:core:results`、`stream:core:inbound-dr:*` 写结果和 DR，按消费组语义读取或确认 `stream:mt:routed:*` |
| 测试代码 | 本地临时目录 `scratchpad/test/`，不进仓库。一键执行脚本 `scratchpad/it/run-all.sh` |

测试套件：

| 套件 | 类型 | 内容 |
|---|---|---|
| `Batch1Check` | 单元（桩对象） | 分片公式、DR 反查缓存的失败与重试语义 |
| `InlineDrCheck` | 单元 | 同步转发开关在线程上的作用范围 |
| `DrShardCheck` | 单元 | Server DR stream 归属判断 |
| `CoreDrFlowIT` | 集成（真实 Redis + 真实 Core） | 收件人计数 Lua、结果 → 反查缓存 → 供应商 DR → 转发、失败重试、同步转发开关 |
| `Batch3IT` | 集成 | MtAttempt、外发 Lua、outbox、结果判定、四种超时 |
| `CoreE2EIT` | 集成（真实消费者 + 路由） | MT 全链路、崩溃后重投、幂等开关、真实结果消费、超时后迟到的结果 |

## 2. 第 1 批用例

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| B1-01 | 计数新建时带过期时间 | 对不存在的 key 调用 `incrWithExpire(key, 86400)` | 返回 1，TTL > 86000 | CoreDrFlowIT：`incrWithExpire: new counter gets the TTL` | P0 |
| B1-02 | 再次计数时保持过期时间 | 第二次调用 | 返回 2，TTL 仍然存在 | CoreDrFlowIT：`second increment keeps the TTL` | P0 |
| B1-03 | 修复没有过期时间的计数 | 用 SET 造一个没有 TTL 的 key（模拟老代码的残留），再调用 | 计数加 1，补上 TTL | CoreDrFlowIT：`repairs a counter that had no TTL` | P0 |
| B1-04 | 不改动已有的过期时间 | key 已有 TTL 100 秒 | 调用后 TTL 仍 ≤ 100 | CoreDrFlowIT：`leaves an existing TTL alone` | P1 |
| B1-05 | 分片号与原公式一致 | n = 16、10、7、32，各 200 万个随机 hash | `abs(h % n) == abs(h) % n` | Batch1Check：`new shard formula gives the old shard` | P0 |
| B1-06 | `Integer.MIN_VALUE` 不再算出负数 | n = 16、10、7、32 | 分片号 ≥ 0；对照组：旧公式在 n=10 时为负 | Batch1Check：`MIN_VALUE no longer gives a negative shard`、`(control)` | P1 |
| B1-07 | DR 处理失败时保留反查缓存 | `processInnerAck` 返回 false | `handleInboundDRInternal` 返回 false，缓存仍在 | Batch1Check：`failed processing returns false`、`lookup entry kept after failed processing` | P0 |
| B1-08 | DR 处理抛异常时保留缓存 | `processInnerAck` 抛异常 | 返回 false，缓存仍在 | Batch1Check：`kept when processing throws` | P0 |
| B1-09 | 重试成功后按日期哈希删除缓存 | 重试时 `processInnerAck` 返回 true | 删除 `OUT1` 和 `DR{dateIn秒}` | Batch1Check：`retry finds the original again`、`deleted after success` | P0 |
| B1-10 | ACCEPT 状态的 DR 不删缓存 | DR 状态为 ACCEPT | 缓存保留 | Batch1Check：`ACCEPT DR keeps the lookup entry` | P1 |
| B1-11 | `putMsg` 遇到未知客户不抛空指针 | ossid 9999 不在 cm.cfg 中 | 返回 true，不抛 NPE | CoreE2EIT：`B1-11` | P1 |
| B1-12 | outbox 登记日志降为 DEBUG | 代码审查 | INFO 级别下没有 `outbox registered` 日志 | 代码审查（第 3 批已重写该类） | P2 |

## 3. 第 2 批用例

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| B2-01 | 供应商 DR 端到端转发 | Client 写 submit_resp → Core 写反查缓存 → Client 写供应商 DR | 写入 `stream:dr:pending:8888:MultiSmppServer1:*` | CoreDrFlowIT：`Core produced the DR to stream:dr:pending` | P0 |
| B2-02 | 写出后才确认消费 | 同上 | 入向 DR 被 XACK+XDEL，反查缓存删除 | CoreDrFlowIT：`inbound DR acked`、`lookup entry deleted` | P0 |
| B2-03 | 在入向 DR 消费线程上同步写出 | 同上 | 日志中 `Send Delivery Report to stream` 的线程不是 `MessageProcessorThread` | CoreDrFlowIT：`DR produced on the inbound-DR consumer thread` | P0 |
| B2-04 | 真实的失败与重试 | 原消息缺少 `timeStamp`，处理时抛空指针 | 消息留在待确认列表，缓存保留，不产生 DR；修复后重放成功并删除缓存 | CoreDrFlowIT：`3b:` 共 5 项 | P0 |
| B2-05 | 配置为 false：普通 DR 走线程池 | `CoreDrInlineDispatch=false`，直接调用 `sendDRMessage` | 由 `MessageProcessorThread#N` 写出 | CoreDrFlowIT：`flag false: unscoped DR goes through the processor pool` | P1 |
| B2-06 | 配置为 false：拒绝 DR 仍同步 | 调用 `sendDRMessageForServerReject` | 调用返回时已写入 stream，线程为 main | CoreDrFlowIT：`flag false: reject DR ...` 共 2 项 | P0 |
| B2-07 | 默认配置：所有 DR 同步 | 不配置该项 | 调用返回时已写入，线程为 main | CoreDrFlowIT：`default: ...` 共 2 项 | P1 |
| B2-08 | 开关的作用范围 | 嵌套调用、抛异常、其他线程 | 只在包住的范围内打开；退出和抛异常后关闭；其他线程不受影响 | InlineDrCheck 共 7 项 | P1 |

## 4. 第 3 批用例

### 4.1 MT 幂等（MtAttempt）

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| B3-01 | 第一次处理 | `begin` → `once(cdr)` → `countOnce` → 外发第 0 段 | RUN；CDR 和计数都执行；routed 1 条；标记里有 `p:0`，CDR 和计数的标记与外发在同一段 Lua 里写入 | Batch3IT：`A: first attempt ...`、`A: part 0 ...`、`A: marker records`、`A: CDR and counter flags` | P0 |
| B3-02 | 处理失败 | `abandon` | 释放占用，保留已做的标记 | Batch3IT：`A: failed attempt releases its lease` | P0 |
| B3-03 | 重投时跳过已做的副作用 | 再次 `begin` | CDR 跳过；计数只读不加；第 0 段不再写入（id 相同，routed 仍 1 条）；之前没做过的副作用照常执行 | Batch3IT：`A: redelivery ...` 共 5 项 | P0 |
| B3-04 | 成功后确认消费并删除标记 | `finish` | 待处理条目被 XACK+XDEL，标记被删除 | Batch3IT：`A: finish ...`、`A: pending entry deleted` | P0 |
| B3-05 | 并发占用 | 另一个线程占用后不释放 | 第二个消费者得到 BUSY | Batch3IT：`B: second consumer sees a fresh lease` | P0 |
| B3-06 | 占用者挂掉后被接手 | 占用超过 `CoreMtLeaseMs` | 接手成功，可以 `finish` | Batch3IT：`B: expired lease ...`、`B: taken-over MT finished` | P0 |
| B3-07 | 真实处理后崩溃再重投 | 处理线程执行完 `processSubmitFromStream` 后不确认消费就退出；占用过期后，消费者的 `processMessage` 接手处理 | routed 仍 1 条；IN_SUBMIT CDR 仍 1 条；收件人计数仍为 1；确认消费并删除标记 | CoreE2EIT：`E2E-02a` 到 `E2E-02h` | P0 |
| B3-08 | 关闭幂等后恢复原逻辑 | `CoreMtIdempotency=false`，同一条消息处理两次 | 不写标记；第二次再次外发（routed 2 条，原来的行为） | CoreE2EIT：`E2E-03a`、`E2E-03b` | P1 |
| B3-09 | 真实消费者处理正常 MT | 启动 `MTStreamConsumer`，写入一条 8888 → 86 号码的 MT | 路由到 rssid 8；写 outbox；确认消费并删除标记；IN_SUBMIT CDR 只有 1 条 | CoreE2EIT：`E2E-01a` 到 `E2E-01d` | P0 |
| B3-10 | 真实消费者处理未知客户 | ossid 7777 | 写 CDR 后确认消费，删除标记 | Batch3IT：`F:` | P1 |

### 4.2 outbox 与结果判定

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| B3-11 | 拆分后各段的 outbox 互不覆盖 | 同一个 msgId 外发第 1、2、3 段 | 存在 `:1:0`、`:2:0`、`:3:0` 三条 | Batch3IT：`C:` | P0 |
| B3-12 | 第一个结果有效 | 第 2 段的结果（id 1-1） | FIRST，只删除第 2 段的 outbox | Batch3IT：`D: first result ...`、`D: part 1 outbox untouched` | P0 |
| B3-13 | 第二个结果作废 | 同一段换一个结果 id（2-1） | STALE | Batch3IT：`D: second result ...` | P0 |
| B3-14 | 同一条结果重试仍有效 | 再次用 id 1-1 | FIRST | Batch3IT：`D: retry of the same result entry` | P0 |
| B3-15 | 重发生成新的 outbox | `retriedNumber=1` 再外发 | 生成 `:2:1` | Batch3IT：`D: a resend ...` | P0 |
| B3-16 | 没有 outbox 的结果照常处理 | 未知的 msgId | UNTRACKED | Batch3IT：`D: result without outbox` | P1 |
| B3-17 | 真实结果先到 | `ResultStreamConsumer` 收到 submit_resp | 取走 outbox；OUT_SUBMIT CDR 只有 1 条；写反查缓存 | CoreE2EIT：`E2E-05a`、`E2E-05b` | P0 |
| B3-18 | 超时后迟到的结果被丢弃 | 超时结果已处理完，再写一条 SUCCESS 的 submit_resp | 判为过期结果并确认消费；不再写 OUT_SUBMIT CDR | CoreE2EIT：`E2E-06a` 到 `E2E-06c` | P0 |

### 4.3 超时（测试配置：Client 停止读取 1.5 到 4 秒，等结果 2.5 到 5 秒，硬超时 6 秒）

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| B3-19 | Client 不存在（没有消费组），消息未读 | 外发到 rssid 101 | 从 routed 收回（XDEL），回 2500 | Batch3IT：`E1:` | P0 |
| B3-20 | 已确认消费但一直没有结果 | rssid 102，模拟 Client 读取并确认消费 | 超过等结果时长后回 2700 | Batch3IT：`E2:` | P0 |
| B3-21 | 已读未确认 | rssid 103，读取但不确认消费 | 硬超时前不判；到硬超时后回 2700 | Batch3IT：`E3:` 共 2 项 | P0 |
| B3-22 | Client 正在读前面的消息时不收回 | rssid 104 有 6 条；Client 每 0.7 秒读 1 条，读 3 条后停止 | 读取期间最后一条不被收回；停止后剩下的消息被收回并回 2500 | Batch3IT：`E4:` 共 2 项 | P0 |
| B3-23 | 超时结果交给 Core 处理 | 没有 Client，MT 一直未读 | 收回；结果消费者取走 outbox，写 OUT_SUBMIT CDR（2500），给客户的失败 DR 写入 `stream:dr:pending` | CoreE2EIT：`E2E-04a` 到 `E2E-04c` | P0 |

## 5. Server DR 分片与回归

| ID | 场景 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|
| S-01 | 默认消费本模块的所有 DR 分片 | MyShards=0 时，本模块 1 号分片也消费 | DrShardCheck：`default: own module shard 1 consumed` 等 | P0 |
| S-02 | 不消费其他模块的 DR | `MultiSmppServer2` 的 stream 不消费 | DrShardCheck：`other module not consumed` | P0 |
| S-03 | 兼容旧格式 key | 不带模块名、不带分片号的 key 都消费 | DrShardCheck：`legacy key ...`、`key without shard` | P1 |
| S-04 | 打开分片过滤后恢复原行为 | `ServerDrStreamShardFilter=true` 时按 MyShards 过滤 | DrShardCheck：`filter on: ...` 共 3 项 | P1 |
| S-05 | MT 分片过滤不受影响 | MT 仍然按分片过滤 | DrShardCheck：`MT shard filter unchanged` 共 2 项 | P0 |
| R-01 | 真实 Core 加载 cm.cfg | 客户 8888、8 存在，当前 A2P 为 56 | CoreDrFlowIT：`Core loaded cm.cfg` | P0 |

## 6. 不在本地范围、需要上线前在测试环境验证的项

| ID | 项 | 原因 | 建议做法 |
|---|---|---|---|
| X-01 | 会落库的兜底路径：RMQ 重试、WDQ、SDQ、DB 兜底写库，以及 `once("db")` 是否防止了重复写库 | 本地没有数据库 | 测试环境发 MT，造成路由失败、供应商重试码等情况，确认 DB 里只有一条记录 |
| X-02 | 真实 Client 和 Server 模块配合 | 本地由测试程序扮演 | 三个模块全部部署，跑完整的 MT 和 DR 流程，对账 CDR |
| X-03 | 拆分后的完整外发循环（真实的 `processBinaryOrTextMessage`） | 本地没有配置拆分 | 配置 `allowSplit` 后发长短信，看 routed 条数与 outbox 条数一致 |
| X-04 | 确认消费本身失败（网络中断） | 本地很难精确制造 | 用 B3-07 的"崩溃后重投"间接覆盖；测试环境可以在确认消费时断开 Redis 连接 |
| X-05 | 多个 Core 节点同时抢同一条消息 | 本地只有单进程 | 两个 Core 共享分片，观察 `MT is being processed by another consumer` 日志和 `ack_returned_zero` |
| X-06 | 收件人计数和黑洞计数的拒绝分支 | 本地只验证了计数只加一次 | 把 `SMSOptionRecipientMaxSendCountIn24H` 配成 1，同一号码发 2 条，第 2 条被拒 |
| X-07 | 性能 | 本地是单机单 Redis | 压测时对比每条 MT 的 Redis 往返次数和时延 P99 |

## 7. 执行结果

执行时间 2026-10-28 16:36（AEDT）；Redis 7.2.5；OpenJDK 17.0.21 (Temurin)；代码为 `main@7b9a4cc` 加上第 3 批未提交的改动；全量编译通过。

### 7.1 各套件汇总

| 套件 | 检查数 | 通过 | 失败 |
|---|---|---|---|
| Batch1Check | 10 | 10 | 0 |
| InlineDrCheck | 7 | 7 | 0 |
| DrShardCheck | 10 | 10 | 0 |
| CoreDrFlowIT | 24 | 24 | 0 |
| Batch3IT | 32 | 32 | 0 |
| CoreE2EIT | 23 | 23 | 0 |
| **合计** | **106** | **106** | **0** |

### 7.2 用例结果

| 用例 | 结果 | 用例 | 结果 | 用例 | 结果 |
|---|---|---|---|---|---|
| B1-01 | ✅ | B2-01 | ✅ | B3-09 | ✅ |
| B1-02 | ✅ | B2-02 | ✅ | B3-10 | ✅ |
| B1-03 | ✅ | B2-03 | ✅ | B3-11 | ✅ |
| B1-04 | ✅ | B2-04 | ✅ | B3-12 | ✅ |
| B1-05 | ✅ | B2-05 | ✅ | B3-13 | ✅ |
| B1-06 | ✅ | B2-06 | ✅ | B3-14 | ✅ |
| B1-07 | ✅ | B2-07 | ✅ | B3-15 | ✅ |
| B1-08 | ✅ | B2-08 | ✅ | B3-16 | ✅ |
| B1-09 | ✅ | B3-01 | ✅ | B3-17 | ✅ |
| B1-10 | ✅ | B3-02 | ✅ | B3-18 | ✅ |
| B1-11 | ✅ | B3-03 | ✅ | B3-19 | ✅ |
| B1-12 | 代码审查 ✅ | B3-04 | ✅ | B3-20 | ✅ |
| S-01 | ✅ | B3-05 | ✅ | B3-21 | ✅ |
| S-02 | ✅ | B3-06 | ✅ | B3-22 | ✅ |
| S-03 | ✅ | B3-07 | ✅ | B3-23 | ✅ |
| S-04 | ✅ | B3-08 | ✅ | R-01 | ✅ |
| S-05 | ✅ | | | | |

第 6 节的 X-01 到 X-07 需要在测试环境执行，本次未执行。

### 7.3 执行过程中的调整

| 项 | 说明 |
|---|---|
| E2E-05b 第一次执行失败 | 检查时机太早：反查缓存比 OUT_SUBMIT CDR 晚几毫秒写入。改为等待最多 5 秒后通过。不是产品问题 |
| E2E-04 / E1 的超时时长 | 为了让测试在秒级完成，测试里把等待时长配得很短；线上默认值：Client 停止读取 120 秒、等结果 180 秒、硬超时 3 小时、复查间隔 60 秒 |

## 8. 测试中发现的问题

| # | 问题 | 位置 | 影响 | 建议 |
|---|---|---|---|---|
| F-1 | DR 处理先计算延迟，后判断是否为空：原消息的 `timeStamp` 为空，或者供应商客户已从配置删除时，会抛空指针 | `MessageStoreManager.handleOutDeliveryReportResForInnerAck`（`timeDate.getTime()`、`rCustomerInfo.getMaxRerouteDRDelayTimeInSec()`） | 这条 DR 会一直失败、一直被重投 | 先判断再计算；和重投次数上限、死信队列放在同一批修复 |
| F-2 | Core 启动依赖 `senderReplacement.cfg` 等文件，缺少时直接空指针、启动失败 | `A2PCustomerManager.initPhonePrefixBlocks` | 新环境部署时容易踩到 | 文件不存在时当作空配置处理，或者在部署清单里写明 |
| F-3 | **更正之前的分析**：路由支持按百分比的负载均衡（mix routing，`getRandomValue` / 轮询） | `A2PCustomerManager` 约 5378 行 | 配置了 mix routing 时，UDH 长短信的各段可能被路由到不同通道，手机拼不起来 | 对带 UDH 的分段，按"ossid + 发送号码 + 接收号码 + 参考号"固定选择通道；已同步更正《A2P消息链路整体分析与改进》第 6.2 节 |

## 9. 复跑方法

```bash
bash /tmp/claude-1000/-home-dev-claude-newsms/f538c348-c3e8-49f4-8a48-2770f66c4840/scratchpad/it/run-all.sh
# 每个套件的完整输出在 scratchpad/it/results/<套件>.txt，汇总在 summary.txt
```
