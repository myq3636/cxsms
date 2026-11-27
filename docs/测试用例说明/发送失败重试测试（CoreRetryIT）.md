# 发送失败重试测试（CoreRetryIT）

> 日期：2026-11-27。用例代码：`regression/test/it/CoreRetryIT.java`，套件 `CoreRetryIT`（`regression/suites.txt`，it 模式），47 项检查。
> 功能清单见《Core模块功能与测试用例清单》第六节“结果与重试”。发送失败后**改道到备份路由**的逐级规则在《路由规则测试（RoutingRuleIT）》；这组测的是**重试**（同一个通道过一段时间再发）和 MQM 的配合。

## 1. 测什么

通道发送失败（Client 回给 Core 失败的 submit_resp）以后：

| 部分 | 内容 |
|---|---|
| 是否重试 | 失败码在通道的 `SMSOptionSubmitRetryCode` 里才重试，否则马上回失败 DR |
| 重试的过程 | 写 RMQ（重试队列）→ MQM 到时间取出、写 MQM 流 → Core 再发 → 再失败更新 RMQ → 策略用完删掉 RMQ、回失败 DR |
| 重试策略的优先级 | 通道 `SMSOptionMTRetryPolicy` → 客户的发送方策略 `SMSOptionSenderRetryPolicy` → 客户的 MO 策略 `SMSOptionMORetryPolicy` → 全局 `DefaultRetryPolicy`（10 分钟） |
| 过期 | 下次重试会超过有效期时，不再安排重试，由 MQM 的过期任务处理 |
| 最后一次失败回成功 DR | 客户的 `SMSOptionSubmitErrorCodesForFakeDR` |
| 失败后改道 | 有备份路由时先改道；备份也失败就回失败 DR |
| 只重试 DR | 客户的 `SMSOptionOnlySupportDRRetry` |
| MQM 主备 | 备用 MQM 不发；旧任期（fence）的消息被拒绝，行放回去给现在的主 MQM |
| DR 重发 | MQM 重发 DR（RDQ）、客户收到后删 RDQ 的行、发送失败再写 RDQ |

## 2. 怎么跑、怎么判定

```
regression/run.sh CoreRetryIT
cat regression/.work/results/CoreRetryIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core：`MTStreamConsumer`（路由）、`ResultStreamConsumer`（发送结果）、`CoreMQMStreamConsumer`（MQM 重发的消息）。
- **不连数据库**：消息存储换成记录 SQL 的 `RecordingStore`，RMQ、RDQ、SMQ 的 SQL 只记下来不执行。用例从记下的 SQL 里读 `RetriedNumber`、`NextRetryTime`、`RSsID`，检查写了什么。
- 模拟的动作：

| 动作 | 做法 |
|---|---|
| 发一条 MT | 写进 `stream:mt:pending:{ossid}:*`（发送方见各用例，接收号码 `8613800000000`），等它出现在通道的出向流 `stream:mt:routed:{通道}:*` |
| 通道发送失败 | 把这条 MT 作为 submit_resp 写进结果流，状态 **2500**（`COMMUNICATION_ERROR`，用例里的通道都把它配成重试码） |
| MQM 到时间重发 | 按 RMQ 的 SQL 还原消息（`RetriedNumber`、来源 `RMQ`），写进 MQM 流，就像 MQM 从 RMQ 取出来一样 |
| 看 DR | 客户 DR 流 `stream:dr:pending:{ossid}:*` |
| 看 CDR | IN_SUBMIT、OUT_SUBMIT 的条数 |

## 3. 公共配置

### 3.1 cm.cfg

每个块的公共部分：

```
[名称]
CCBSSID: ssid
A2PPlayerType: operator
Protocol: SMPP
（额外配置）
```

**通道**：

| 块 | CCBSSID | 额外配置 |
|---|---|---|
| V601 | 601 | `SMSOptionSubmitRetryCode: 2500`<br>`SMSOptionMTRetryPolicy: 5S,1M,2M` |
| V602 | 602 | —（没有重试码） |
| V603 | 603 | `SMSOptionSubmitRetryCode: 2500`（没有 MT 策略） |
| V604 | 604 | `SMSOptionSubmitRetryCode: 2500`<br>`SMSOptionMTRetryPolicy: 1S` |
| V605 | 605 | `SMSOptionSubmitRetryCode: 2500`<br>`SMSOptionMTRetryPolicy: 2D,2D` |
| V606 | 606 | `SMSOptionSubmitRetryCode: 2500`（失败时改道） |
| V607 | 607 | —（备份通道，没有重试码） |

重试策略的写法：`5S,1M,2M` 表示一共发 3 次，第 1 次失败后等 5 秒、第 2 次失败后等 1 分钟（第 3 个点在第 3 次失败时用不到，因为已经发满 3 次）；`7S*5` 表示每次等 7 秒、共 5 个点；单位 `S` 秒、`M` 分、`D` 天。策略有 N 个点就一共发 N 次（重试 N-1 次）。

**客户**：

| 块 | CCBSSID | 额外配置 |
|---|---|---|
| R700 | 700 | — |
| R701 | 701 | — |
| R702 | 702 | `SMSOptionSenderRetryPolicy: (prefix=SEND,policy=7S*5)`<br>`SMSOptionMORetryPolicy: 9S*5` |
| R703 | 703 | — |
| R704 | 704 | `SMSOptionSubmitErrorCodesForFakeDR: 2500` |
| R705、R706 | 705、706 | — |
| R707 | 707 | `SMSOptionOnlySupportDRRetry: true` |
| R708 | 708 | `SMSOptionSenderRetryPolicy: (prefix=SEND,policy=7S*5)` |

### 3.2 路由

| 客户 | 路由 |
|---|---|
| R700、R708 | `A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V601)` |
| R701 | `... Relay=V602` |
| R702、R703、R707 | `... Relay=V603` |
| R704、R705 | `... Relay=V604` |
| R706 | `A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V606)`<br>`A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V607)` |

（`...` 是 `A2PMainRouting: (Priority=30,PhonePrefix=86,`。）

### 3.3 其他

| 配置 | 值 |
|---|---|
| `Gmms/GmmsConfig.properties` | 追加 `AsyncSqlWriterEnable=false`（SQL 同步执行，便于检查） |
| 发送方黑白名单 | 不配（回归环境的 `senderactiondir` 是空的） |

## 4. 用例

### 4.1 一条 MT 的完整重试过程（客户 R700 → V601，策略 `5S,1M,2M`）

下面每一步接着上一步：

| ID | 步骤 | 预期 |
|---|---|---|
| RE-02a | V601 第 1 次失败（2500） | 写 RMQ（INSERT） |
| RE-02b | 看 RMQ 的 SQL | `RetriedNumber=1`，`NextRetryTime` 约 5 秒后（策略第 1 个点） |
| RE-02c | — | 还在重试，**不回 DR** |
| RE-02d | — | 写了这次失败的 OUT_SUBMIT CDR |
| RE-07a | MQM 重发（写 MQM 流） | 写入成功 |
| RE-07b | Core 处理重发 | 又发到 V601（第 2 次出现在 V601 的出向流） |
| RE-07c | 看第 2 次发出的消息 | 带着 `RetriedNumber=1`、来源 `RMQ` |
| RE-07d | — | 从 MQM 流读出并确认（日志 `flow=mqm->core-submit`） |
| RE-07e | — | 没有第 2 条 IN_SUBMIT CDR |
| RE-04a | 第 2 次失败（2500） | 更新 RMQ 的那一行（UPDATE），不再插入新行 |
| RE-04b | 看 UPDATE 的 SQL | `RetriedNumber=2`，`NextRetryTime` 约 1 分钟后（第 2 个点）；INSERT 仍只有 1 条 |
| RE-04c | — | 还不回 DR |
| RE-04d | MQM 再重发 | 写入成功 |
| RE-04e | Core 处理 | 第 3 次发出 |
| RE-04f | 第 3 次失败（2500） | 策略 3 个点用完（3 次发送、2 次重试）：删除 RMQ 的行（DELETE） |
| RE-04g | — | 给客户回失败 DR：**10405**（`UNDELIVERY_NETWORK_ERROR`，由提交状态 2500 转来） |
| RE-04h | — | 失败 DR 里的通道是 601 |
| RE-04i | — | OUT_SUBMIT CDR 3 条（每次发送 1 条），IN_SUBMIT 1 条 |

### 4.2 不重试

| ID | 目的 | 客户 → 通道 | 步骤 | 预期 |
|---|---|---|---|---|
| RE-03a | 失败码不在通道的重试码里 | R701 → V602 | 失败（2500） | 马上回失败 DR 10405 |
| RE-03b | — | 同上 | — | 不写 RMQ |

### 4.3 用哪个重试策略

| ID | 目的 | 客户 → 通道 | 发送方 | 预期的第一次重试间隔 |
|---|---|---|---|---|
| RE-05a | 通道没有 MT 策略：用客户的发送方策略（发送方以 `SEND` 开头） | R702 → V603 | `SENDER1` | 7 秒 |
| RE-05d | 通道的 MT 策略优先于客户的发送方策略 | R708 → V601 | `SENDER1` | 5 秒 |
| RE-05b | 发送方不匹配：用客户的 MO 策略 | R702 → V603 | `18667991226` | 9 秒 |
| RE-05c | 都没有：全局默认 | R703 → V603 | `18667991226` | 600 秒（10 分钟） |

### 4.4 下次重试会超过有效期

| ID | 目的 | 做法 | 预期 |
|---|---|---|---|
| RE-06a | MT 指定发往 V605（策略 `2D,2D`，2 天） | R703 的 MT，`rssid` 设为 605 | 发到 V605 |
| RE-06b | V605 失败，2 天后已经超过有效期 | 失败（2500） | 写 RMQ，但**不设 `NextRetryTime`**（重发查询永远选不到它） |
| RE-06c | — | — | 这里不回 DR，由 MQM 的过期任务回（在 WdqExpiryIT 等用例里） |

### 4.5 最后一次失败回成功 DR

| ID | 目的 | 客户 → 通道 | 预期 |
|---|---|---|---|
| RE-10a | 失败码在客户的 `SMSOptionSubmitErrorCodesForFakeDR` 里，最后一次失败（V604 策略只有 1 个点） | R704 → V604 | 回 **DELIVERED（10000）** |
| RE-10b | 同一个通道，客户没配这个选项 | R705 → V604 | 回失败 DR 10405 |

### 4.6 失败后改道

| ID | 目的 | 客户 → 通道 | 步骤 | 预期 |
|---|---|---|---|---|
| RE-12a | 有备份路由：先改道 | R706 → V606 | V606 失败（2500） | 改发备份通道 V607，不回 DR |
| RE-12b | 备份也失败 | 同上 | V607 失败（2500，V607 没有重试码） | 不再改道，回失败 DR |

### 4.7 只重试 DR

| ID | 目的 | 客户 → 通道 | 预期 |
|---|---|---|---|
| RE-13a | 客户配了 `SMSOptionOnlySupportDRRetry: true`，MT 发送失败 | R707 → V603 | MT 照样写 RMQ |
| RE-13b | — | 同上 | 用默认策略，600 秒后重试 |

### 4.8 MQM 主备

| ID | 目的 | 做法 | 预期 |
|---|---|---|---|
| RE-07f | 备用 MQM 不发 | 进程内起一个 `MQMMessageSender`，设成备用，交给它一条要重发的 MT | 1.5 秒内没有发出 |
| RE-07g | 变成主 MQM 后发 | 设成主 | 这条 MT 发出 |
| RE-09a | 旧任期（fence）的 MQM 消息 | Redis 的 `mqm:active:fence` 设为 6，本进程的任期是 5；MQM 写一条 DR（RDQ 第 11 行） | 写入成功 |
| RE-09b | Core 拒绝它，把行放回去 | — | SQL `UPDATE RDQ SET ActionCode=-1 WHERE ID=11 AND ActionCode=0`（只在行还被占着时放回），日志 `Reject stale-fence MQM message` |
| RE-09c | — | — | 客户收不到这条 DR |
| RE-09d/e | 旧任期，数据库这时出错 | 让第 13 行的 UPDATE 失败一次 | 行没放回去，消息留在待确认里等重试（不确认），日志 `its row not released (left pending)` |
| RE-09f/g | 旧任期，消息没有对应的行 | 来源为空、行号 0 | 照样拒绝，日志 `rowReleased=false` |

### 4.9 DR 重发（RDQ）

| ID | 目的 | 做法 | 预期 |
|---|---|---|---|
| RE-08a | MQM 重发 DR | Redis 里先登记一个在线的 Server（`system:server:nodes`、`module:status:server:MultiSmppServer1:1`），再把一条 DELIVERED DR（来源 RDQ，第 9 行）写进 MQM 的 report 流 | 写入成功 |
| RE-08b | DR 送到客户的流 | — | 客户 DR 流里有它，状态 DELIVERED |
| RE-08c | — | — | 从 MQM report 流读出并确认（日志 `flow=mqm->core-report`） |
| RE-08d | 客户收到了 | 模拟 Server 回成功的 DR 应答 | 删除 RDQ 第 9 行 |
| RE-08e | DR 发给客户失败 | 模拟 Server 回失败（`FAIL_SENDOUT_DELIVERYREPORT`）的 DR 应答 | 写进 RDQ 等下次重发 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7`） | 全部通过 |
| 2026-11-27 | 测试服务器全量回归（本次修改后） | 47 项全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| MQM 真的从数据库 RMQ 里按 `NextRetryTime` 选出要重发的消息 | 不连数据库，`getResendMessage` 的 SELECT 没有执行过（《Core模块功能与测试用例清单》RE-06 记为部分） |
| 按 service type id 的重试策略（`SMSOptionServiceTypeIDRetryPolicy`） | 没有用例 |
| 重试次数达到上限前客户端主动取消、消息过期后的处理 | 过期后的 DR 在 WdqExpiryIT（WDQ），RMQ 的过期任务没有在数据库上测 |
| 通道在 ROP 失败模式（写 SMQ 等恢复） | 没有用例 |
