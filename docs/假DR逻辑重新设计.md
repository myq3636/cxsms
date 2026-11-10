# 假 DR 逻辑重新设计

> 日期：2026-11-10。状态：**已实现**（第 12 节是实现说明和测试结果）。
> 已确认（2026-11-10）：四种方式的需求；假 DR 状态按比例随机（带 base 随机，默认成功）；方式 4 配在客户上；方式 2 由通道开、客户可拒绝；方式 3 改写所有非 DELIVERED，可按前缀；去掉“外发前回”；方式 1 的客户外发最终失败只记 CDR；**旧逻辑全部去掉，配置名能沿用旧的就沿用**。
> 第 10 节的两点按本稿的写法实现（客户 `SMSOptionSendFakeDR` 不再读取；方式 4 保留 `ratio`、不加号码前缀）。方式 1 也按 `SMSOptionFakeDRSuccessRatio` 加浮动随机（已确认）。现状和缺陷见 `docs/假DR可疑点测试报告.md`。

## 1. 现状的问题

- 5 种回假 DR 的方式加 1 种改写真实 DR，散在 `SyncSubmitRouter`、`MessageProcessorThread`、`AutoSendInDRProcessorThread`、`MessageStoreManager` 的十几处，靠消息上的假 DR 标志（`milterActionCode` 第 13 位）和每处重复写的条件组合互相避让。条件抄错就出错：DEF-C8、DEF-C9。
- 假 DR 的状态和改写真实 DR 共用 `SMSOptionDRSuccessRatio`，默认 0，不配时假 DR 全是失败。
- “外发成功后回假 DR”开关在客户上（`SMSOptionSendFakeDR`），号码前缀在通道上（`SMSOptionRecPrefixForFakeDR`）。
- 延迟的假 DR 默认写进旧的 Redis 延迟键，依赖 MQM 模块发送。
- 没有“通道多久不回 DR 就自动回”。

## 2. 现在所有给客户回 DR 的地方（审计）

新设计只改“假 DR”（第 3 节的方式 1、2、4）和“改写真实 DR”（方式 3）。**下表其余的 DR 状态逻辑都不变**，特别是各种失败 DR 不会因为“假 DR 默认成功”而变成成功：

| DR 来源 | 代码位置 | 状态怎么定 | 新设计 |
|---|---|---|---|
| Core 检查不通过（黑白名单、内容、号码、计费等）的拒绝 DR | `InternalCoreEngineSession.processSubmitFromStream` → `sendDRMessageForServerReject` → `sendRejectDR` | `SubmitConvertErrorDRStatus(状态文本)`，取不到时 REJECTED | **不变，仍是失败**。方式 1 在路由成功之后才判断，检查不通过的消息根本走不到 |
| 路由失败、没有通道、DB 兜底 | `SyncSubmitRouter.dbFallback` → `handleMessageError` | 失败状态（UNDELIVERABLE 等） | 不变 |
| 外发失败、不重试 / 最后一次重试失败 | `MessageStoreManager.handleOutSubmitRes` 失败分支、`checkLastRetry` | `SubmitConvertErrorDRStatus`，取不到时 UNDELIVERABLE；下一次重试已过期时 EXPIRED | 不变（`SMSOptionSubmitErrorCodesForFakeDR` 见下） |
| 外发失败、错误码在客户的 `SMSOptionSubmitErrorCodesForFakeDR` 里 | 同上 | DELIVERED | 保留，这是已有的“指定错误码回成功”功能，不受假 DR 比例影响 |
| 长短信没集齐就过期 | `CsmExpirySweeper` | 客户支持过期 DR 时 EXPIRED，否则 UNDELIVERABLE | 不变 |
| 消息在 RMQ/WDQ 里过期（MQM） | MQM → `handleMessageError` / `checkLastRetry` | EXPIRED 或转换后的失败状态 | 不变 |
| 内容模板替换失败、客户配了 `DrSuccForTemplateReplaceFailed` | `MessageProcessorThread.transactTemplateFailedMessageAndReturnSuccessDR` | DELIVERED | 保留，已有功能，不受假 DR 比例影响 |
| 黑洞（`blackhole` 通道、`BlackholePercent`） | `MessageProcessorThread.transactBlackholedMessage` | `BlackholeDRSuccessRatio` / `BlackholeDrStatusCode` | 保留，不受假 DR 比例影响 |
| CMCC 测试短信 | `handleOutSubmit4CmccTestSms` | DELIVERED | 不变 |
| 通道真实 DR | `handleOutDeliveryReportRes` / `ForInnerAck` | 真实状态 | 方式 3 可改写（4.3） |
| DR 查询客户（`InClientPull=2`） | `updateMsgForDRQuery` | 同对应的 DR | 不变，只是写查询表而不是推送 |
| **方式 1：收到客户消息就回** | 现在 `SyncSubmitRouter.spawnFakeDrIfNeeded` → `buildAutoInDR` | 现在按 `SMSOptionDRSuccessRatio`，默认 0 = 全失败 | **改**：按 `SMSOptionFakeDRSuccessRatio`，默认 100（4.0） |
| **方式 2：外发成功后回** | 现在 `handleOutSubmitRes` 成功分支（`ForInnack` 里有一份同样的） | 同上 | **改**：同上 |
| 外发前回（`SMSOptionSendDRBeforeOutSubmitInDC`） | `MessageProcessorThread.transactNewMessage`、`AutoSendInDRProcessorThread` | 同上 | **去掉** |
| **方式 4：超时自动回** | 新增 | — | 按 `SMSOptionFakeDRTimeoutStatus`（4.4） |

**延迟配置的共用**：`SMSOptionDRDelayTimeMinMaxInSec` 现在除了延迟假 DR，**还延迟 Core 拒绝 DR 和黑洞 DR**（`sendRejectDR`、`transactBlackholedMessage`，`SMSOptionSendDelayDRImmediately=1` 时不延迟）。这部分不属于假 DR，保持不变。

## 3. 四种方式和配置

配置名能沿用旧名字的都沿用（意思一样，或者新语义是旧语义的自然延伸），只有旧版没有的才新加。所有配置都在 cm.cfg 的客户/通道块里，`RELOAD_CUSTOMER` 后对新消息生效。

| # | 方式 | 配在 | 配置 | 默认 |
|---|---|---|---|---|
| 1 | 收到客户消息就回 | 客户 | `SMSOptionSendDRByInSubmitInCU`（全部号码） | `false` |
| | | 客户 | `SMSOptionSendFakeDRByInSubmitRecPrefixInCU`（按号码前缀/国家，逗号分隔） | 空 |
| | | 客户 | `SMSOptionDRDelayTimeMinMaxInSec`（延迟秒数，`最小,最大` 或一个数） | `0` |
| 2 | 收到通道 submit_sm_resp 成功就回 | 通道 | `SMSOptionSendDRByOutSubmit`（旧版是客户上的配置，现在改为配在通道上） | `false` |
| | | 通道 | `SMSOptionRecPrefixForFakeDR`（按号码前缀，不配是全部） | 空 |
| | | 通道 | `SMSOptionDRDelayTimeMinMaxInSec`（配在通道上就是方式 2 的延迟） | `0` |
| | | 客户 | `SMSOptionFakeDRDisable`（**新**：这个客户不接受方式 2） | `false` |
| 3 | 真实 DR 不是 DELIVERED 时按比例改成成功 | 客户 | `SMSOptionDRSuccessRatio`（0～100） | `0`（不改写） |
| | | 客户 | `SMSOptionDRSuccessRatioPrefix`（**新**：按号码前缀，不配是全部） | 空 |
| 4 | 通道 N 秒不回 DR 就自动回 | 客户 | `SMSOptionFakeDRTimeoutSec`（**新**） | `0`（不启用） |
| | | 客户 | `SMSOptionFakeDRTimeoutStatus`（**新**：`success` / `fail` / `ratio`） | `success` |
| 共用 | 方式 1、2（以及方式 4 选 `ratio` 时）的假 DR 状态 | 客户 | `SMSOptionFakeDRSuccessRatio`（**新**，0～100） | `100` |
| 共用 | 比例的随机浮动（base 随机），方式 3 和假 DR 比例都用 | 客户 | `SMSOptionDRSuccessBiasValue` | `0` |

**优先级**：同一条消息，方式 1 命中就不再看方式 2、4；方式 2 命中就不再计时（方式 4）。方式 3 只作用在真实 DR 上，和假 DR 互不影响。

## 4. 每种方式的规则

### 4.0 假 DR 的状态（方式 1、2、4）

- 不配 `SMSOptionFakeDRSuccessRatio` 就是 100：全部回 DELIVERED。
- 不是 100 时随机，沿用 `GmmsUtility.isModifySuccessDR`：先在比例上随机加减一个 `0～SMSOptionDRSuccessBiasValue` 的数，得到这一条的比例，再按它决定 DELIVERED 或 UNDELIVERABLE。例：比例 90、浮动 5，每条按 85～95 之间的某个比例决定。
- **只用于假 DR**。第 2 节表里的拒绝、失败、过期 DR 都不经过这个比例。

### 4.1 方式 1：收到客户消息就回

- 条件：`SMSOptionSendDRByInSubmitInCU=true`，或接收号码匹配 `SMSOptionSendFakeDRByInSubmitRecPrefixInCU`。
- 时机：`SyncSubmitRouter.routeSubmitSync` 里，**路由成功**、发给 Client 之前（长短信在拼好之后）。在消息上记下“客户 DR 已决定：IN_SUBMIT”（第 5 节），按客户的延迟安排发送；**消息照常外发**。
- 之后：通道真实 DR、外发最终失败，客户都不再收到 DR，只记 CDR。
- 重试、重路由的消息带着这个标记，不会再回一次。

### 4.2 方式 2：收到通道 submit_sm_resp 成功就回

- 条件：通道 `SMSOptionSendDRByOutSubmit=true`；接收号码匹配通道的 `SMSOptionRecPrefixForFakeDR`（不配就是全部）；客户没有 `SMSOptionFakeDRDisable=true`；消息还没被方式 1 决定过。
- 时机：`handleOutSubmitRes` 成功分支，第 1 条外发（`splitStatus ≤ 1`）。记下“客户 DR 已决定：OUT_SUBMIT”，**然后**才把消息存进 DR 查找记录（Redis 按 outMsgId），按通道的延迟（通道没配就是 0）安排发送。
- 外发失败不回假 DR，走第 2 节的失败 DR。
- 之后的真实 DR 只记 `OUT_DELI_REP`。

### 4.3 方式 3：真实 DR 不是 DELIVERED 时按比例改成成功

- 条件：真实 DR 是最终状态且不是 DELIVERED（UNDELIVERABLE、EXPIRED、REJECTED、UNKNOWN 等）；接收号码匹配客户的 `SMSOptionDRSuccessRatioPrefix`（不配是全部）；按 `SMSOptionDRSuccessRatio`（加浮动）命中。
- 处理：给客户的 DR 和 `IN_DELI_REP` 记 DELIVERED；`OUT_DELI_REP` 记真实状态。
- 只对“要转发给客户”的真实 DR 生效：已经被方式 1、2、4 决定过的消息，真实 DR 只记 CDR，不改写。
- 触发 DR 失败重路由（`NeedDRReroute`）的真实 DR 不改写、不转发，等重路由的结果。

### 4.4 方式 4：通道 N 秒不回 DR 就自动回

- 条件：客户 `SMSOptionFakeDRTimeoutSec > 0`；消息外发成功（第 1 条外发）且还没被方式 1、2 决定过。
- 外发成功时开始计时；到时间还没收到通道的最终 DR，就给客户回 DR，状态按 `SMSOptionFakeDRTimeoutStatus`：
  - `success`（默认）：DELIVERED；
  - `fail`：UNDELIVERABLE；
  - `ratio`：按 4.0 的假 DR 比例随机。
- 回过之后，通道的真实 DR 来了只记 `OUT_DELI_REP`。
- 真实 DR 在超时前到：转发（方式 3 照常生效），计时取消。
- 触发 DR 失败重路由：计时取消，重路由的外发成功后重新计时。
- 机制和性能见第 6 节。

## 5. 每条消息给客户只回一条最终 DR

旧设计靠“假 DR 标志 + 各处条件”避让；新设计用一个**明确的决定记录**加**超时场景下的原子仲裁**：

**决定记录随消息走**：用消息的 `milterActionCode` 第 15 位表示“客户的最终 DR 已经是假 DR”（第 16 位表示“方式 4 正在计时”）。`milterActionCode` 本来就随消息写进 Stream、Redis 的 DR 查找记录、DB 的各个队列表，所以决定跟着消息走，不需要新的序列化字段。原来的第 13 位（旧的假 DR 标志）不再使用。

- 方式 1 在发给 Client 之前打上，所以 Client 回来的外发结果、存进 Redis 的 DR 查找记录、写进 DB（RMQ、WDQ）的消息都带着它。
- 方式 2 在存 DR 查找记录之前打上，所以真实 DR 匹配到的那份消息带着它。
- 给客户发 DR 的三个入口（`MessageStoreManager.sendDRMessage`、`updateMsgForDRQuery`、`insertMsgForDRDelay`）统一检查：带着这一位就不发，打一行 `[DR-SKIP]`（CDR 照常写）。各处原来的避让条件全部删掉。
- 好处：方式 1、2 **不需要额外的 Redis 键和 Redis 操作**。

**超时场景（方式 4）需要仲裁**：计时到期和真实 DR 到达可能同时发生，两边都在 Core 上、可能在不同节点。用 Redis 上的原子操作决定谁回：

| 键 | 内容 |
|---|---|
| `zset:fakedr:timeout:{分片}` | member = outMsgId，score = 到期时间（ms）。分片 = outMsgId 的哈希对总分片数取模 |
| `zset:fakedr:timeout:processing:{分片}` | 已被某个 Core 认领、正在处理的计时（带认领超时，Core 挂了会回到上面） |
| `indr:once:{outMsgId}` | 只在“超时已经回了”或“真实 DR 和计时撞在一起”时才创建，值是谁赢了（`TIMEOUT` / `REAL`） |

- **外发成功、开始计时**：`ZADD` 到期时间；消息打上第 16 位（随 DR 查找记录存下）。实现上是单独的一次 `ZADD`（多一次往返，约 0.1 ms）。
- **计时到期**（Core 上按本节点的分片扫描）：认领 → Lua：计时还在 processing 里（没被真实 DR 删掉）且 `SET indr:once NX` 成功 → 回超时 DR；否则丢弃。
- **真实 DR 到达**（只对标记了“有计时”的消息）：Lua：从两个 ZSET 删掉这个 outMsgId；如果是从等待的 ZSET 里删掉的（计时还没到点），直接转发，不建键；如果计时已被认领，`SET indr:once NX`，成功就转发，失败（超时已经回了）就只记 CDR，并删掉 `indr:once`。
- `indr:once` 的过期时间同 DR 查找记录；真实 DR 来了就删，所以只有“超时回了、真实 DR 一直没来”的消息会留着这个键直到过期。

## 6. 方式 4 的性能

| 方面 | 估算 |
|---|---|
| 没配方式 4 的客户 | **没有任何额外开销**（不计时，真实 DR 不走仲裁） |
| 每条外发成功的消息 | 1 次 `ZADD`（约 0.1 ms） |
| 每条真实 DR | 1 次 Lua（约 0.1～0.3 ms），只对有计时的消息 |
| 每条超时的消息 | 认领（批量 Lua，一次认领多条）+ 1 次仲裁 Lua + 读查找记录 + 写 DR 流，和回一条普通 DR 的开销相当 |
| 空闲扫描 | 每个分片每 200 ms 一次 `ZRANGEBYSCORE`（和 `DelayedInDRDispatcher` 一样），可以忽略 |
| Redis 内存 | 每条在等的消息一个 ZSET 成员，约 80 字节。**在等的数量 ≈ 外发 TPS × 超时秒数 × 没在超时前回 DR 的比例**。最坏情况（通道完全不回 DR）：1000 TPS × 600 秒 = 60 万条 ≈ 50 MB；超时配 3600 秒时约 300 MB |
| Core CPU | 一个扫描线程，正常情况下大部分计时在真实 DR 到达时就删掉了，很少到点 |

**注意的点：**
- 超时设得越长、通道越不回 DR，ZSET 越大；建议超时不超过 1 小时，并监控 `fake_dr_timeout_pending`。
- 认领要批量做（一次 Lua 认领最多 N 条），现在 `DelayedInDRDispatcher` 是一次认领一条，大量同时到点时往返太多；新的计时器从一开始就批量。
- 计时和 DR 查找记录都在 STATE Redis 上，要求 `maxmemory-policy noeviction`（和长短信合并一样）。

## 7. 发送方式

- 所有延迟回复的假 DR、超时 DR 都走 Core 侧的 `DelayedInDRDispatcher`（`zset:delay:outdr:{分片}`）；延迟为 0 时直接写 `stream:dr:pending:*`（HTTP 客户写 `stream:http:dr:*`）。
- **不再依赖 MQM**：去掉 `AutoInDR.DirectStreamEnable`、`AutoInDR.FallbackToLegacyDelayDR` 两个开关，固定走 Core。
- 旧的 Redis 延迟键（`DELAYDR_{inMsgId}` + `delayDR_{ossid}` ZSET + `delayDR` 集合）不再写入。升级后 Core 启动时由一个节点（加锁）把旧键里还没发的 DR 按剩余延迟搬进新 ZSET，再删掉旧键；MQM 的 `SendDelayRedisDRThread` 随之去掉。

## 8. 去掉的旧逻辑和配置

| 去掉 | 说明 |
|---|---|
| 消息上的假 DR 标志（`milterActionCode` 第 13 位，`setFakeDR` / `hasFakeSendDR`） | 由第 15 位（客户的最终 DR 已是假 DR）代替 |
| `sendDRMessage` 里“配了 `SMSOptionSendFakeDR` 就只在标志还在时才发”的检查 | 由第 5 节代替 |
| 各处重复的避让条件（`isNullFakeDRByInSubmitRecPrefixInCU`、`isNeedFakeDRForRecPrefix` 在 DR 路径上的用法等） | 统一成第 5 节；DEF-C8、DEF-C9 随之消失 |
| `MessageProcessorThread` / `AutoSendInDRProcessorThread` 里的“外发前回” | 已确认去掉；`SMSOptionSendDRBeforeOutSubmitInDC` 不再读取，读到时打一次 WARN |
| 客户 `SMSOptionSendFakeDR` | 不再读取（第 10 节第 1 条），读到时打一次 WARN |
| `SMSOptionDRSuccessRatio` 决定假 DR 状态的用法 | 只用于方式 3；假 DR 用 `SMSOptionFakeDRSuccessRatio` |
| `handleOutSubmitResForInnack` 里和 `handleOutSubmitRes` 重复的假 DR 代码 | 两处调用同一个方法 |
| `AutoInDR.DirectStreamEnable`、`AutoInDR.FallbackToLegacyDelayDR`、MQM 旧延迟路线 | 第 7 节 |

## 9. 升级影响（上线前要核对的配置）

| 现在的配置 | 升级后 | 需要怎么做 |
|---|---|---|
| 客户 `SMSOptionSendFakeDR=true`（或 `SMSOptionSendDRByOutSubmit=true` 配在客户上） | **不再回外发成功后的假 DR** | 在这些客户路由到的通道上配 `SMSOptionSendDRByOutSubmit=true`；不想让其他客户也收到的，给那些客户配 `SMSOptionFakeDRDisable=true` |
| 客户 `SMSOptionDRSuccessRatio=N` 并且用了假 DR | 假 DR 变成全部成功（新比例默认 100）；N 只用于改写真实 DR | 想保持原来的比例，配 `SMSOptionFakeDRSuccessRatio=N`；不想改写真实 DR 的，把 `SMSOptionDRSuccessRatio` 去掉。**注意：现在 N 已经同时在改写真实 DR**，所以只配新比例时行为最接近现状 |
| 客户用了假 DR、没配任何比例 | 假 DR 从全部失败变成全部成功 | 确认这是想要的（大概率原来是配错了） |
| 通道 `SMSOptionSendDRBeforeOutSubmitInDC=true` | 不再生效 | 改用方式 2（等通道 submit_sm_resp） |
| 升级时正在路上的消息 | 带的是旧的第 13 位，没有第 15 位 | 已经回过假 DR 的消息，真实 DR 可能再转发一次给客户（只在升级那段时间） |
| 旧 Redis 延迟键里的 DR | 由 Core 启动时迁移 | 无 |

建议在实现里加一个检查（启动日志汇总或 a2pctl），列出用了 `SMSOptionSendFakeDR`、`SMSOptionSendDRBeforeOutSubmitInDC`、只配了 `SMSOptionDRSuccessRatio` 的客户和通道，按上表逐个改配置后再升级。

## 10. 需要再确认

1. **客户 `SMSOptionSendFakeDR` 直接不再读取**：上一轮确认的是“旧的客户 `SMSOptionSendFakeDR=true` 理解成在所有通道上按方式 2 回”，这一轮又定了“旧逻辑全部去掉”。两者冲突，本稿按“去掉”写（第 8、9 节）。如果想让现有客户升级后不用改配置，可以保留这一条兼容：客户 `SMSOptionSendFakeDR=true` 等于这个客户在所有通道上都开了方式 2。
2. **方式 4 的状态选项**：按“可配成功或失败”设计了 `success` / `fail`，另外加了 `ratio`（按假 DR 比例随机）。不需要 `ratio` 可以去掉。另外上一稿给方式 4 加的号码前缀（`SMSOptionFakeDRTimeoutPrefix`）这一稿去掉了，需要再加。

## 11. 日志、指标、测试

**日志：**
- 决定时：`[FAKE-DR] mode=IN_SUBMIT|OUT_SUBMIT|TIMEOUT delaySec=... status=...`
- 被跳过的 DR：`[DR-SKIP] source=REAL|FAIL decided=IN_SUBMIT|OUT_SUBMIT|TIMEOUT`（DEBUG；每个客户 10 分钟一条 INFO 汇总）
- 方式 3 改写：`[DR-REWRITE] from=10400 to=10000`
- 启动和 reload 时每个客户一行 INFO：实际生效的方式、比例、延迟、超时；第 9 节列的旧配置打 WARN。

**指标：** `fake_dr`（按方式）、`dr_skip`（按来源）、`dr_rewrite`、`fake_dr_timeout_pending`。

**测试**（真实 Core，测试进程扮演 Client 和供应商，延用 `CoreDrBizIT` / `FakeDrSuspectIT` 的做法；现有 `CoreDrBizIT` 里依赖旧语义的用例按新语义改写）：

| 编号 | 场景 | 预期 |
|---|---|---|
| FN-01 | Core 检查不通过、路由失败、外发最终失败、长短信过期，客户配了各种假 DR | 都是失败 DR，状态和现在一样（第 2 节） |
| FN-02 | 方式 1 全部 / 按前缀，前缀内、前缀外 | 前缀内路由后回 1 条；前缀外不回 |
| FN-03 | 方式 1 后外发失败、真实 DR 失败、重试到最后一次失败 | 客户只有 1 条；前缀外的号码收到失败 DR（DEF-C8 不再出现） |
| FN-04 | 假 DR 比例：不配、100、90 + 浮动 5、0 | 不配和 100 全部成功；90 约 85%～95%；0 全部失败 |
| FN-05 | 方式 2：通道开 / 前缀 / 通道延迟；客户 `SMSOptionFakeDRDisable` | 外发成功后按延迟回 1 条；客户拒绝时不回；外发失败回失败 DR |
| FN-06 | 方式 2 后真实 DR 失败、EXPIRED | 只记 OUT_DELI_REP，不回第二条（DEF-C9 的场景不再有分支） |
| FN-07 | 方式 3：比例 0、70、100；真实 DR DELIVERED / UNDELIV / EXPIRED / REJECTED；按前缀和全部 | 非 DELIVERED 按比例改成功，前缀外不改；OUT_DELI_REP 记真实状态 |
| FN-08 | 方式 4：超时前真实 DR 到 | 转发真实 DR（方式 3 照常），计时取消，到点不回 |
| FN-09 | 方式 4：超时后真实 DR 到；状态 `success` / `fail` / `ratio` | 到点回对应状态；真实 DR 只记 CDR |
| FN-10 | 方式 4：到点和真实 DR 同时（多线程、两个 Core） | 客户恰好 1 条 |
| FN-11 | 方式 4：DR 失败重路由 | 计时取消，新外发成功后重新计时 |
| FN-12 | 方式 4：Core 重启、分片迁移 | 计时由新 Core 接着处理，不丢不重 |
| FN-13 | 长短信 3 段 + 方式 1 / 2 / 4 | 客户 3 条 DR（每段一条），不重复 |
| FN-14 | 旧配置 `SMSOptionSendFakeDR`、`SMSOptionSendDRBeforeOutSubmitInDC` | 不生效，打 WARN |
| FN-15 | 升级：旧 Redis 延迟键里有未发的 DR | Core 启动迁移后按时发出，旧键删除 |
| FN-16 | 性能：方式 4 打开，1000 TPS、通道不回 DR | ZSET 大小、Redis 和 Core CPU 在第 6 节估算范围内 |

## 12. 实现说明和测试结果

**新增：**
- `processor/FakeDr`：方式 1～4 的判断和假 DR 生成（`onRouted`、`onOutSubmitOk`、`onVendorDr`、`rewriteRealDr`、`fakeStatus`）。
- `processor/FakeDrTimeoutScheduler`：方式 4 的计时（第 5 节的三段 Lua：批量认领、到点、真实 DR 取消）。Core 启动时随 `DelayedInDRDispatcher` 一起启动，调优参数共用 `AutoInDR.DelayBatchSize`、`AutoInDR.DelayClaimTimeoutMs`、`AutoInDR.DispatcherIdleSleepMs`，不新增 GmmsConfig 配置。
- `processor/LegacyDelayDrMigrator`：Core 启动时把旧 Redis 延迟键里没发的 DR 搬到 Core 的延迟发送（加锁，一个 Core 做）。

**删除：** `AutoSendInDRProcessorThread`、MQM 的 `SendDelayRedisDRThread`、`MessageBase.setFakeDR` / `hasFakeSendDR`、`DirectInDRDispatchService.buildAutoInDR` / `isEnabled`、`MessageStoreManager` 的旧延迟键写入、`MessageProcessorThread` 的“外发前回”、各处旧的避让条件；GmmsConfig 的 `AutoInDR.DirectStreamEnable`、`AutoInDR.FallbackToLegacyDelayDR`（同时移出热加载白名单）。延迟 DR 发不出去时改为直接进 RDQ。

**和设计稿不同的地方：**
- 决定记录用 `milterActionCode` 的第 15、16 位，不是新的序列化字段（第 5 节）。
- 方式 1、2、4 都要求客户在 submit 里要了 DR（`registered_delivery`）才回；原来方式 1 不检查。
- 实现中发现：`GmmsMessage` 的复制构造函数（`MessageBase(MessageBase)`）和原消息**共用同一个 `milterActionCode` 数组**，改一边另一边也变（`setMilterActionCode(位, 值)` 原地改数组）。已改为复制一份数组，所有复制消息的地方都不再共用。

**测试：**
- `FakeDrIT`（新，`suites.txt`）25 项全部通过：未路由的消息仍回失败 DR；方式 1 全部 / 比例 0；DEF-C8 的场景（前缀外号码最后一次重试失败收到失败 DR，前缀内只有 1 条）；方式 2 通道开、客户拒绝、之后 EXPIRED 不回第二条也不进 WDQ（DEF-C9 的场景）；方式 3 改写 UNDELIV、REJECTED，前缀外不改，`OUT_DELI_REP` 记真实状态，外发失败不改；方式 4 真实 DR 先到、超时后回成功再来真实 DR 只记 CDR、`fail` 状态、计时清空；旧配置 `SMSOptionSendFakeDR` 不生效并打 WARN。
- `CoreDrBizIT` 按新语义改写（通道开方式 2、比例 0 / 70 / 不配、通道延迟、外发前回去掉），39 项全部通过。
- 原来的 `FakeDrSuspectIT` 和 `known-defects.txt` 里的 DEF-C8、DEF-C9 去掉，场景并入 `FakeDrIT`。

