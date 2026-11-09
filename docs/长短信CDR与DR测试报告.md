# 长短信 CDR 与 DR 测试报告

> 日期：2026-11-09。用例 `regression/test/it/CsmCdrIT.java`（在 `suites.txt`）。`CsmReassembleMode=redis`（默认）。

## 1. 结论

- **入向 CDR 按分段记，没有“合并后的一条”：** 每个分段一条 `IN_SUBMIT_PARTIAL`、一条 `IN_SUBMIT`。拼好的整条消息不再写入向 CDR。和旧的内存组装模式（`legacy`）相同。
- **外发 CDR 按实际发给供应商的条数记：** 每条外发的结果一条 `OUT_SUBMIT`，供应商每个 DR 一条 `OUT_DELI_REP`。通道会按自己的长度重新拆分（`ChlMsgSplit`、`MsgSplitLen`），所以条数可能和入向分段数不同。
- **发现并修复了一个 bug：** 长短信回给客户的 DR 只有第 1 段送出，第 2 段起的 DR 都丢了（第 3 节）。

## 2. 测试方法和结果

一条 3 段的长短信（每段 60 个字符，拼起来 180），每段走 Core 真实的入口 `processSubmitFromStream`；测试进程扮演 Client，给每条外发写一个成功的 submit_resp 和一个供应商 DR，由 Core 真实的结果消费者、入向 DR 消费者处理。统计所有相关 CDR 的类型和条数，以及写给 Server 的 DR 条数。

| | A 通道不拆 | B 通道按 100 拆 | C 单条短信（对照） |
|---|---|---|---|
| `IN_SUBMIT_PARTIAL` | 3 | 3 | 0 |
| `IN_SUBMIT` | 3 | 3 | 1 |
| 外发条数 | 1（180 字符） | 2（100 + 80） | 1 |
| `OUT_SUBMIT` | 1 | 2 | 1 |
| `OUT_DELI_REP` | 1 | 2 | 1 |
| 回给客户的 DR（修复前） | **1** | **1** | 1 |
| 回给客户的 DR（修复后） | 3 | 3 | 1 |

- 入向 CDR 用各分段自己的 `InMsgID`；外发和 DR 的 CDR 记在拼好的消息上：msgId 是第 1 段的，`InMsgID` 是 3 个分段用逗号连起来的。
- 外发被拆分时，只有第 1 段（`splitStatus < 2`）的 DR 回给客户，第 2 段的 DR 只记 CDR（`MessageStoreManager.java:2772`），这是原有设计。
- 写 CDR 的位置：分段的 `IN_SUBMIT_PARTIAL` 在 `processInnerAck` → `handleInSubmit(msg, false)`（分段带 `inCsm`）；`IN_SUBMIT` 在 `SyncSubmitRouter.routeCsmSegmentSync` → `handleInSubmit4Csm`；`OUT_SUBMIT` 在 `handleOutSubmitRes`（`logOutSubmitRes`）。

## 3. 修复：长短信的分段 DR 被当成重复丢掉

**现象：** 供应商的 DR 回来后，Core 用 `CsmUtility.splitCsmDr` 按客户的分段拆成 3 条（日志 `[DR-TO-SERVER]` 3 次），但 3 次是同一个 `redisId`，只写进 1 条；后两条日志 `Redelivered, dr:...:0:0:10000 already produced`。客户只收到第 1 段的 DR。

**原因：** `StreamQueueManager.produceDrOnce` 防重投重复写入的键是 `"dr:" + 流名 + ":" + splitStatus + ":" + statusCode`。拆出来的 DR 这几项都一样（分片按 msgId 算，它们 msgId 也一样），只差 `InMsgID` 和分段序号。

**修复：** 分段序号大于 0 时，键后面加上 `":" + 分段序号`。单条短信、拼好的整条消息分段序号都是 0，键不变，行为不变。

**升级时：** 正在处理的长短信 DR 如果在升级重启时被重投，第 1 段的键变了，可能再发一次（最多一次）。
