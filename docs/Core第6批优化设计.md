# Core 第 6 批优化设计：供应商 DR 与出向结果的重投去重

## 1. 问题

供应商 DR 消费者（`InboundDRStreamConsumer`）和出向结果消费者（`ResultStreamConsumer`）收到的同一条 stream 记录被重投时（确认消费失败、确认前崩溃、被自动重投领走），整条记录会被重新处理一遍：

| 路径 | 会重复的副作用 | 后果 |
|---|---|---|
| 供应商 DR | OUT_DELI_REP CDR 与 OUTDR 统计；写 `stream:dr:pending`；DR 失败后改道重发 MT；SDQ/WDQ 写入 | 客户收到重复 DR；向备用通道重复下发 |
| 供应商 DR | 第一次处理成功时已删除 DR 反查缓存，重投时改走查 DB，查不到就写 SDQ | 客户事后再收到一条迟到的重复 DR |
| 出向结果 | OUT_SUBMIT/IN_DELI_REP CDR；写入重试队列 RMQ/RDQ/SMQ；改道；给客户回失败 DR | 向供应商或客户重复重发 |

第 3 批的 outbox 判定只拦"同一次外发的**不同**结果"，同一条结果记录被重投时仍判为有效。

## 2. 方案

### 2.1 `MtAttempt` 改名为 `StreamAttempt` 并通用化

| 项 | MT（第 3 批，不变） | 供应商 DR、出向结果（本批） |
|---|---|---|
| 标记 key | `core:mt:{msgId}` | `core:att:{stream}:{条目id}`（这两类消息的 msgId 不唯一，条目 id 在重投时不变） |
| 标记所在实例 | submitMq | reportMq（和 stream 在同一实例，确认消费与删除标记一段 Lua 完成） |
| "已全部完成"标记 `_done` | 不用 | 处理成功后，**先**写入 `_done`（与待写的副作用标记一起，1 次访问），再确认消费并删除标记；重投时看到 `_done` 就**不处理**、直接确认消费，避免因反查缓存已删除而走到 SDQ 分支 |
| 占用（lease）、失败时保留已做标记、释放占用 | 同第 3 批 | 同第 3 批 |

去重**以 stream 里的一条记录为单位**。正常的重试（例如给客户发 DR 失败后从 RDQ 重发、MT 按重试策略重发）每次都会产生**新的**结果记录，标记互不相关，所以每次都照常写 CDR、照常写重试队列，行为不变。

### 2.1A DR 反查缓存的删除推迟到"已全部完成"之后

如果处理完一条供应商 DR、但还没写入 `_done` 时进程崩溃，而 DR 反查缓存已经删掉了，重投时就查不到原消息，会改走 DB/SDQ 分支，后面仍会发出重复的 DR。

所以在一次处理中，删除反查缓存的动作通过 `StreamAttempt.afterDone` 推迟到 `_done` 写入之后再执行（没有处理过程时立即执行，与原来一样）。这样在 `_done` 写入之前，重投仍能找到原消息、走同样的处理分支，由各个检查点跳过已经做过的副作用。

### 2.2 新增去重检查点（只在一次处理过程中生效，处理之外的调用不受影响）

| 检查点 | 规则 |
|---|---|
| `MessageStoreManager.insertMessageToDB(msg, queue)` | 按"队列、段号、状态"只写一次（覆盖 RMQ、RDQ、SMQ、WDQ、QDQ 等所有 DB 队列） |
| `SdqMessageManager.insertSDQMessage` | 只写一次 |
| `CDRManager` 中的消息统计（IN / OUT / OUTDR，共 10 处 `StMsgCount` 累加） | 只累加一次 |
| 改道（结果路径的 `rerouteDispatcher.dispatch`、DR 路径的 `rerouteOutMsg`） | 改道**成功**后立即写入标记（立即落到 Redis，不等处理结束）；重投时发现已改道，就按"已改道"直接返回 |
| 写 CDR、发 DR、延迟 DR、DB 兜底 | 第 3 批已有的检查点，对这两条路径同样生效 |

### 2.3 DR 写出与标记原子完成

处理供应商 DR 或出向结果时写出的 DR（`stream:dr:pending:*`、`stream:http:dr:*`）和标记都在 reportMq 上，复用第 3 批外发 Lua 的写法：写出 DR 和写入标记在同一段 Lua 里完成。

标记与 stream 不在同一个实例时（例如 MT 处理中被拒绝而发出的 DR、DR 路径改道到 routed），就退回为"先写出，再记标记"，标记随处理结束一起落盘。

### 2.4 开关

`CoreDrResultIdempotency`（GmmsConfig，默认 `true`，支持热加载），同时控制供应商 DR 和出向结果两个消费者；false 时恢复原来的行为。

### 2.5 不在本批

供应商对同一个 outMsgID 重复发送 DR：这是两条不同的记录，不属于重投，本批不做。

## 3. 性能

每条供应商 DR / 出向结果多 2 次 Redis 访问（开始时占用 + 结束时写 `_done`）。写出 DR 和确认消费的访问次数不变。本地实测见测试报告。
