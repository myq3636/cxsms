# DR 流程测试（CoreDrFlowIT、EarlyDrIT、DrLookupIT、DrBatchLookupIT、Batch6IT、DrLivenessIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`，it 模式：
>
> | 套件 | 项数 |
> |---|---|
> | CoreDrFlowIT | 27 |
> | EarlyDrIT | 24 |
> | DrLookupIT | 16 |
> | DrBatchLookupIT | 36 |
> | Batch6IT | 22 |
> | DrLivenessIT | 9 |
>
> 这一份讲**供应商 DR 怎么匹配回原来的 MT、怎么送到客户所在的 Server**。客户收到什么样的 DR（假 DR、改道、错误码）在《DR 业务测试》里。
> 设计见《DR匹配数据迁移设计》《DR早到重试与outbox应答超时改进设计》《DR消费减少Redis往返设计》。

## 1. 测什么

一条供应商 DR 在 Core 里的过程：

1. 通道接受 MT（提交结果成功）时，Core 把原消息存成 **DR 匹配数据** `drl:{供应商消息ID}`，同时登记到 `drl:due:at`（到时间由 MQM 搬进 WDQ）。
2. Client 收到供应商 DR，写进 `stream:core:inbound-dr:{通道}:{分片}`。
3. Core 的 `InboundDRStreamConsumer` 读出，按供应商消息 ID 找匹配数据，生成客户的 DR，选一个客户在线的 Server，写进 `stream:dr:pending:{客户}:{Server}:*`，然后确认、删掉匹配数据。

| 部分 | 套件 | 内容 |
|---|---|---|
| 基本流程 | CoreDrFlowIT | 提交结果写匹配数据 → 供应商 DR 匹配 → 客户 DR → 删匹配数据、确认；处理失败时匹配数据保留；DR 在哪个线程写出（`CoreDrInlineDispatch`） |
| 匹配数据 | DrLookupIT | 存在哪、TTL、`drl:due:at` 的时间（`Redis_RetrieveRedisInterval` 和有效期哪个早）；MQM 把到时间的搬进 WDQ；升级前的旧数据 |
| DR 早到 | EarlyDrIT | DR 比提交结果先到（还没有匹配数据）：排队，在 T/2、T 时再找，找不到就丢弃（不写 SDQ） |
| 批量 | DrBatchLookupIT | 一批 DR 的匹配数据用一次 MGET 读、一次 Lua 删；客户 DR 一次 Lua 写；客户在 Server 上的会话一次 Lua 查、一批只查一次 |
| 重投去重 | Batch6IT | 处理完了但确认丢了、写出后崩溃，重投时不重复发 DR、不重复写 CDR、不重复写重试队列、不重复改道 |
| 选 Server | DrLivenessIT | 原来的 Server 被 kill -9（心跳没了、会话登记还在）时，DR 改发给客户在线的另一个 Server |

## 2. 怎么跑、怎么判定

```
regression/run.sh CoreDrFlowIT     # 其余套件同样
cat regression/.work/results/CoreDrFlowIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379），一个 Redis 同时充当 submit、report、state 所有角色。
- 进程内启动真实的 Core（Core 的初始化、`ResultStreamConsumer`、`InboundDRStreamConsumer`）；本程序扮演 Client（写提交结果、写供应商 DR）。
- **不连数据库**：
  - CoreDrFlowIT 用真实的 `MessageStoreManager`，但没有数据源，只走不碰数据库的路径（Redis 匹配、流、CDR 文件）；
  - EarlyDrIT、DrLookupIT 把按供应商消息 ID 查 WDQ、写 WDQ 换成内存里的假实现，可以模拟“WDQ 里有”“WDQ 写失败”。
- EarlyDrIT、DrLookupIT、DrBatchLookupIT、Batch6IT 关掉了 `CoreInboundDRShardEnable`，直接按名字 `stream:core:inbound-dr:8` 读流；有的用例自己调用一次“读出 → 处理 → 确认”，以便在中间插入崩溃、确认丢失。
- DrBatchLookupIT 统计每个 Redis 命令（MGET、GET、Lua、ZRANGE）的次数，用来判定往返次数。

## 3. 公共配置

### 3.1 cm.cfg 和路由

用回归环境的基础配置，不追加：

```
[CUST8888]
CCBSSID: 8888
A2PPlayerType: operator
Protocol: SMPP
SMSOptionRecipientMaxSendCountIn24H: 100

[VENDOR8]
CCBSSID: 8
A2PPlayerType: operator
Protocol: SMPP
```

路由 `CUST8888_routingInfo.cfg`：`A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR8)`。

原消息：客户 8888 → 通道 8，发送方 `18667991226`，接收号码 `8613800000000`，要 DR，有效期 24 小时，从 `MultiSmppServer1` 进来。供应商 DR：通道 8，状态 DELIVERED，带供应商消息 ID `OUT-…`。

### 3.2 Redis key

| key | 内容 |
|---|---|
| `drl:{供应商消息ID}` | DR 匹配数据（原消息） |
| `drl:due:at` | 有序集合，分数是该搬进 WDQ 的时间 |
| `drl:due` | 升级前的有序集合（分数是 dateIn） |
| EarlyDrQueue 的 ready / processing 有序集合和条目 hash | 早到的 DR，条目里有 `body`、`end`（最后一次找的时间） |
| `stream:core:inbound-dr:8` | 供应商 DR 流 |
| `stream:dr:pending:8888:MultiSmppServer1:*` | 发给客户的 DR |
| `smpp:server:active:{客户}:{模块}`、`smpp:server:session:{客户}:{会话}`、`smpp:server:module:{客户}` | 客户在各 Server 上的会话登记 |
| `module:status:server:{模块}:{节点}` | Server 的心跳 |
| `core:att:{流}:{条目id}` | 一次处理的标记（重投去重用） |

### 3.3 GmmsConfig 和测试里改的值

| 配置 | 默认 | 测试里 | 套件 |
|---|---|---|---|
| `CoreDrInlineDispatch` | true | 先 false、再默认 | CoreDrFlowIT |
| `EarlyDr.Enable` | true | 最后改成 false | EarlyDrIT |
| 通道 8 的应答超时 T（`BufferTimeout`） | — | 2000 毫秒 | EarlyDrIT |
| `Redis_RetrieveRedisInterval` | 30（分钟） | 30，DL-09 用 1000 | DrLookupIT |
| `DrLookup.ClaimTimeoutMs` | — | DL-05 用 1 毫秒 | DrLookupIT |
| `CoreInboundDrBatchLookup` | true | 最后改成 false | DrBatchLookupIT |
| `CoreInboundDrBatchProduce` | true | DB-09 改成 false | DrBatchLookupIT |
| `SmppServerSessionRegistryLuaCheck` | true | DB-05f 改成 false | DrBatchLookupIT |
| `SmppServerSessionRegistryBatchMemo` | true | DB-06e 改成 false | DrBatchLookupIT |
| `CoreDrResultIdempotency` | true | B6-03、B6-09 改成 false | Batch6IT |
| `CoreMtLeaseMs` | 60000 | 800（B6-08 用 60000） | Batch6IT |
| `SmppServerDrLivenessCheck` | true | LV-06 改成 false | DrLivenessIT |
| `CoreInboundDRShardEnable` | true | false（按名字读流） | 除 CoreDrFlowIT 外 |

## 4. 用例

### 4.1 基本流程（CoreDrFlowIT）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| — | Core 加载了配置 | 启动 | 有客户 8888、通道 8，当前 A2P 56 |
| — | 号码计数的 Lua（`incrWithExpire`） | 新计数器、再加一次、没有 TTL 的旧计数器、已有短 TTL 的计数器 | 新的带 TTL（86400 秒），再加保持 TTL；没有 TTL 的补上；已有的 TTL 不动 |
| — | 提交结果写匹配数据 | Client 写成功的 submit_resp（`OUT-IT1`） | 有 `drl:OUT-IT1` |
| — | 供应商 DR 进分片流 | Client 写供应商 DR | 写进 `stream:core:inbound-dr:8:{分片}`，没有不分片的 key |
| — | 匹配、转发 | — | 客户 DR 写进 `stream:dr:pending:8888:MultiSmppServer1:*` |
| — | 收尾 | — | `drl:OUT-IT1` 删掉；供应商 DR 已确认（XACK + XDEL） |
| — | 在哪个线程写出 | — | 在供应商 DR 消费线程上写出，不经过 `MessageProcessorThread` 线程池 |
| 3b | 处理失败（把消息存储设为不可用） | 供应商 DR `OUT-IT5` | 日志 `STREAM-NACK`，留着等重试；匹配数据保留，没有写客户 DR |
| 3b | 恢复后重试 | 恢复消息存储，再处理这条 DR | 找到原消息、转发；删掉匹配数据 |
| NPE-01a/b | 原消息没有 timeStamp（以前会空指针） | 提交结果、供应商 DR | 正常转发，匹配数据删掉 |
| — | `CoreDrInlineDispatch=false` | 普通 DR（`sendDRMessage`） | 照样发出，但经过 `MessageProcessorThread` 线程池 |
| — | 同上 | Server 拒绝的 DR（`sendDRMessageForServerReject`，状态 SDBL） | 调用返回时已写进流，在调用线程（main）上 |
| — | 默认（true） | 普通 DR | 调用返回时已写进流，在调用线程上 |

### 4.2 匹配数据（DrLookupIT）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| DL-01a | 存在哪 | 写一条（dateIn 5 秒前） | `drl:OUT-DL2`（不是 state Redis 上的 `OUT-DL2`） |
| DL-01b | 什么时候搬 | — | `drl:due:at` 的分数是 dateIn + 30 分钟（有效期 1 小时更晚）；TTL 是那之后再 10 分钟（不是传入的 600 秒） |
| DL-01c | 查 TTL（假 DR 超时用） | — | 有的返回正数，没有的返回 -2 |
| DL-02a | 匹配 | 供应商 DR | 客户 DR 写出 |
| DL-02b | 收尾 | — | 最终 DR 以后，匹配数据和 `drl:due:at` 的项都删掉 |
| DL-03a | 早到 DR 排队时发现匹配数据已经有了 | 先写匹配数据，再交给早到队列 | 返回 FOUND，不排队 |
| DL-03b | 没有匹配数据 | — | 和以前一样排队 |
| DL-04 | MQM 搬进 WDQ | 一条 dateIn 31 分钟前、一条 1 分钟前 | 只搬旧的那条进 WDQ 并删掉；新的留着 |
| DL-05a | WDQ 写失败 | 再来一条旧的，WDQ 不可用 | 保留，处于“已领取” |
| DL-05b | 领取过期（1 毫秒）后 | WDQ 恢复，下一轮 | 搬走 |
| DL-06 | — | — | `drl:due:at` 空了 |
| DL-07 | 有效期比 30 分钟早 | 有效期还剩 15 分钟 | 分数是有效期（约 15 分钟后，不是 30）；TTL 不变 |
| DL-08 | 已经过期的 | 有效期已过（dateIn 1 分钟前） | MQM 马上搬进 WDQ（过期 DR 能按时回）；还剩 15 分钟的留着 |
| DL-09 | `Redis_RetrieveRedisInterval=1000`（测试服务器的值），还剩 15 分钟 | — | 按有效期搬；匹配数据的 TTL 比有效期长（以前会在 Redis 里过期消失，没搬进 WDQ） |
| DL-10a | 升级前写的（`drl:due`，分数是 dateIn） | 一条旧的、一条新的 | 旧的搬走，新的留着（升级时不会一下子全搬） |
| DL-10b | — | 新的那条收到最终 DR | 从 `drl:due` 里也删掉 |

### 4.3 DR 早到（EarlyDrIT，T = 2 秒，t0 = Client 收到 DR 的时间）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| ED-00 | T 是什么 | — | 通道 8 的应答超时 |
| ED-01a/b | 还没有原消息 | 刚收到的 DR | Core 处理完（可以确认）；排进队列，t0+T/2 到期，`end` 是 t0+T，带 TTL |
| ED-01c | — | — | 还没给客户 DR |
| ED-02a | 没到期 | 马上分发 | 不处理 |
| ED-02b | t0+T/2 还没有原消息 | 到时间分发 | 改成 t0+T 到期，不写回 Core |
| ED-03a | 原消息来了 | 写匹配数据，t0+T 时分发 | 写回供应商 DR 流，带着原来的 t0 |
| ED-03b | — | — | 条目删掉 |
| ED-03c | Core 再处理 | — | 匹配上，客户 1 条 DR |
| ED-04a～c | 一直没有原消息 | DR 收到时已过 T/2 | 只在 t0+T 找一次；还没有就丢弃，不写回，不给客户 DR |
| ED-05a/b | DR 过了 T 才处理（积压） | 收到 T+0.5 秒以后才处理 | 马上丢弃，不排队 |
| ED-06 | 同一条 DR 重投 | 交给队列两次 | 只有一个条目，到期时间不变 |
| ED-07 | 停掉的 MQM 领取了一条 | 把条目挪到 processing（领取已过期），写匹配数据 | 放回去、找到原消息、写回 |
| ED-08a/b | 原消息在 WDQ 里（不在 Redis） | MQM 按批查 WDQ 找到 | 写回 |
| ED-09 | 升级前的条目（有 `tries`，没有 `end`） | — | 再找一次就丢弃 |
| ED-11a～c | 什么时候 Core 自己查 WDQ | 一条刚收到、一条超过 T | 刚收到的不查 WDQ 直接排队；超过 T 的查一次 WDQ，然后丢弃 |
| ED-10a/b | `EarlyDr.Enable=false` | — | 不排队，和以前一样 |

### 4.4 批量（DrBatchLookupIT）

| ID | 目的 | 预期 |
|---|---|---|
| DB-00 | `CoreInboundDrBatchLookup` 默认 | 打开 |
| DB-01a | 一批 3 条，开始时读匹配数据 | 1 次 MGET，0 次 GET |
| DB-01b～e | 之后取匹配数据 | 用开始时读到的；同一条第二次取会读 Redis；开始时没有的再读一次（期间写进来的能找到）；这批结束后和以前一样读 Redis |
| DB-02a～c | 一批里的删除 | 先记下，到这批结束才删；删过的再取返回空（同一条 MT 的第二条最终 DR）；结束时 1 次 Lua 删掉匹配数据和 `drl:due` 的项 |
| DB-02d | 不在批里 | 马上删 |
| DB-03a～e | 消费者的顺序：领取、预读、处理、确认、删除 | 3 条用 1 次 MGET 处理；删除等确认和这批结束；确认后 3 条匹配数据都删掉；每条客户 DR 1 条 |
| DB-07 | `CoreInboundDrBatchProduce` 默认 | 打开 |
| DB-07a～d | 一批 3 条，第 3 条处理失败 | 客户 DR 等这批结束；处理成功的 2 条用 1 次 Lua 写出；第 3 条不写；确认 2 条 |
| DB-08a/b | 写出后、确认前崩溃 | 重投处理后还是 1 条 DR（标记里记着），然后确认 |
| DB-09a/b | `CoreInboundDrBatchProduce=false` | 马上写出 |
| DB-04 | `CoreInboundDrBatchLookup=false` | 一条一条读、删 |
| DB-05a | 客户在 MultiSmppServer1 上有会话 | true，1 次 Lua（ZRANGE 和各会话明细的 GET 都在里面） |
| DB-05b～e | 会话明细过期；模块上没有会话；唯一的会话过期；明细写的是别的模块 | 去掉没有明细的项；false；false 并把模块从客户的模块列表里去掉；不算这个模块的 |
| DB-05f | `SmppServerSessionRegistryLuaCheck=false` | 每次 1 个 ZRANGE 和 GET，结果一样 |
| DB-06a～d | 一批里同一个客户、同一个 Server | 只查一次（2 条 1 次 Lua）；另一个 Server 单独查；批中间会话没了，这批还用查到的结果；下一批重新查 |
| DB-06e | `SmppServerSessionRegistryBatchMemo=false` | 每条都查（2 条 2 次 Lua） |

### 4.5 重投去重（Batch6IT）

| ID | 情况 | 预期 |
|---|---|---|
| B6-01a～e | 供应商 DR 处理完，确认丢了，重投 | 第一次：客户 1 条 DR、1 条 OUT_DELI_REP，处理完删匹配数据。重投：不再发 DR、不再写 CDR，不去数据库或早到队列找（日志里没有 `redis miss`），确认并删掉标记 |
| B6-02a～d | 写出客户 DR 以后、处理完以前崩溃 | 匹配数据还在（处理完才删）；重投时找到原消息，不再发 DR，删掉匹配数据，不写第二条 CDR，确认 |
| B6-03 | `CoreDrResultIdempotency=false` 时的同一情况 | 重投找不到匹配数据，进了早到队列（日志 `[VDR-EARLY]`，以前的问题） |
| B6-04a～c | 提交失败的结果：崩溃、确认丢失两种 | 第一次写了 OUT_SUBMIT 和失败 DR；重投不再写；确认 |
| B6-05 | 同一条消息的两次 DR 应答结果（客户 DR 发了两次都失败） | 是两个不同的条目：都处理，2 条 IN_DELI_REP |
| B6-06a/b | 写 RMQ | 同一条目重投：跳过（日志 `Redelivered, already inserted into RMQ`）；另一个条目照常写 |
| B6-07 | 之前的处理已经改道 | 报告已改道，不再发（日志 `Redelivered, already rerouted`） |
| B6-08 | 另一个消费者正在处理同一个条目（占用 60 秒） | 不处理，留在待确认里 |
| B6-09 | 性能：800 条供应商 DR，开关关、开 | 2026-11-27：关 269 毫秒，开 242 毫秒 |

### 4.6 选 Server（DrLivenessIT）

两个 Server（MultiSmppServer1 节点 1、MultiSmppServer2 节点 2）的会话登记和心跳由用例按 Server 的格式写进 Redis；客户原来从 MultiSmppServer1 进来。

| ID | 情况 | 预期 |
|---|---|---|
| LV-01 | 两个都在 | DR 发给原来的 MultiSmppServer1 |
| LV-02 | MultiSmppServer1 被 kill（心跳没了，会话登记还没过期） | 发给 MultiSmppServer2 |
| LV-03 | — | 一条 INFO：`[DR-ROUTE] server module=MultiSmppServer1 node=1 has no heartbeat` |
| LV-04 | — | 被 kill 的节点的会话登记跳过，但不删 |
| LV-05 | 心跳恢复 | 又发给 MultiSmppServer1 |
| LV-06 | `SmppServerDrLivenessCheck=false` | 和以前一样发给原来的模块 |
| LV-07 | 客户哪里都没有会话，原模块没有心跳 | 不发给原模块 |
| LV-09 | 节点从来没登记过心跳（状态上报没启动） | 和以前一样照常用 |
| LV-08 | 1000 条 DR 的开销 | 一共 1001 次 GET（每条 1 次会话明细，心跳每秒最多 1 次，有 1 秒的本地缓存），93 毫秒 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 6 组共 134 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 匹配数据在多个 report Redis 主上按 key 选主 | 回归环境只有一个 Redis |
| 真实 MySQL 上按供应商消息 ID 查 WDQ | 这里是内存里的假实现；WDQ 过期回 DR 在 WdqExpiryIT（真实 MySQL） |
| 真实的 Client、Server 进程之间的 DR 全链路 | 在 PathChainIT（每个模块一个进程） |
| Server 被 kill 后已经写进它的 DR 流的 DR | 在 DrTransferIT（`A2pCtl drain-dr`），放在故障恢复里 |
