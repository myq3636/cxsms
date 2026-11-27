# MQM 测试（WdqExpiryIT，以及 CoreRetryIT、DrLookupIT、EarlyDrIT、CoreDrBizIT 里的 MQM 用例）

> 日期：2026-11-27。
>
> MQM（MsgQueueMonitor）没有单独的回归套件。它的功能分在下面几处测，这份文档把它们汇总到一起：
>
> | 套件 | 模式 | MQM 的部分 | 详细文档 |
> |---|---|---|---|
> | WdqExpiryIT | **手动**（`regression/manual-suites.txt`），连真实 MySQL | WDQ 过期任务：通道不回 DR 时由网关回 DR，17 项 | 本文 4.1 |
> | CoreRetryIT | it-multi | RMQ 重试的重发、主备、旧任期拒绝、RDQ 重发 DR | 《发送失败重试测试（CoreRetryIT）》 |
> | DrLookupIT | it | 把到时间的 DR 匹配数据从 Redis 搬进 WDQ | 《DR流程测试》4.2 |
> | EarlyDrIT | it | 早到 DR 的再查找 | 《DR流程测试》4.3 |
> | CoreDrBizIT | it-multi | 没有 Server 在线时 DR 留在 RDQ，之后重发 | 《DR业务测试》4.10 |

## 1. MQM 做什么

| 任务 | 表 / Redis | 说明 |
|---|---|---|
| MT 重试 | RMQ | 发送失败、要重试的 MT 存在 RMQ，到 `NextRetryTime` 由 MQM 取出，写进 MQM 流交给 Core 再发 |
| DR 重发 | RDQ | 发给客户失败的 DR、没有 Server 在线时的 DR 存在 RDQ，由 MQM 取出重发 |
| 等 DR | WDQ | 通道接受了但还没回 DR 的 MT：先在 Redis（`drl:*`），到 `Redis_RetrieveRedisInterval` 或快过期时由 MQM 搬进 WDQ |
| 过期 DR | WDQ | 过了有效期通道还没回 DR，由 MQM 给客户回 DR（EXPIRED 或 DELIVERED） |
| 早到 DR | Redis 早到队列 | DR 比提交结果先到时，MQM 在 T/2、T 时再找原消息 |
| 主备 | `mqm:active:*` | 两个 MQM 只有主的干活；每次当主有一个任期（fence），Core 拒绝旧任期写来的消息 |

## 2. 怎么跑、怎么判定

各套件见对应的文档。WdqExpiryIT：

```
export WDQ_DB_HOST=… WDQ_DB_USER=… WDQ_DB_PASSWORD=…     # 一个有 gmms.WDQ 的 MySQL
regression/run.sh WdqExpiryIT
cat regression/.work/results/WdqExpiryIT.txt
```

- 没设这三个环境变量时输出 SKIP，算通过。
- 测试服务器上从 `/usr/local/a2p/Gmms/GmmsConfig.properties` 的 `DS_gmms_URL`、`DS_gmms_Login`、`DS_gmms_Password` 取（不要打印密码）。
- 它建一个临时库 `gmms_wdq_it`，`CREATE TABLE gmms_wdq_it.WDQ LIKE gmms.WDQ`，结束时 `DROP DATABASE`，不碰 `gmms` 库里的数据。
- 进程内用真实的 `MessageStoreManager` 连这个临时库，直接调用过期任务用的 `getExpiredMessage("WDQ")`，以及通道接受时的 `handleOutSubmitRes`。

## 3. 配置（WdqExpiryIT）

cm.cfg 追加：

```
[E61]
CCBSSID: 61
A2PPlayerType: operator
Protocol: SMPP
SmsOptionIsSupportExpiredDR: yes

[E62]
CCBSSID: 62
A2PPlayerType: operator
Protocol: SMPP
```

GmmsConfig：`DescendingTime=10`（和服务器一样，查别的 A2P 的 WDQ 时用，没有默认值）；有效期用回归环境的 `MessageExpireTime`、`MessageFinalExpireTime`，**最终有效期比有效期晚 60 分钟**。

WDQ 里放 4 条通道已接受、在等 DR 的 MT（通道 8，过期时间按网关的做法存成 GMT 墙上时间）：

| MsgID | 客户 | 过期时间 |
|---|---|---|
| WA | E61（支持过期 DR） | 1 分钟前 |
| WB | E61 | 5 分钟后 |
| WC | E62（不支持） | 1 分钟前 |
| WD | E61 | 过了最终有效期（70 分钟前） |

## 4. 用例

### 4.1 WDQ 过期任务（WdqExpiryIT）

| ID | 情况 | 预期 |
|---|---|---|
| WQ-00/01 | 写进 4 条 | 4 条在等 |
| WQ-02 | 扫描：WA 过期，客户支持过期 DR | 回 DR EXPIRED（10200） |
| WQ-03 | WC 过期，客户不支持 | 回 DR DELIVERED（10000） |
| WQ-04 | WB 还有 5 分钟 | 还不回（以前用真实的现在时间比，在东八区看起来已经过了 8 小时） |
| WQ-05 | WD 过了最终有效期 | 不回 |
| WQ-06 | 回的 DR | 是这条 MT 的：ossid、rssid、outMsgId 都保留 |
| WQ-07 | 回过 DR 的行 | 标成 Retrieved（12000，ActionCode 0），其他的不变 |
| WQ-08a～c | 之后通道的 DR 才到 | 找到 Retrieved 的 MT；删掉这一行；不再发给客户（只写 CDR） |
| WQ-09 | 还在等的 MT | 它的 DR 来了照样找得到 |
| WQ-10 | 再扫一次 | 没有了 |
| WQ-11 | WB 的 5 分钟过了（把过期时间往前改 6 分钟） | 回 EXPIRED |
| WQ-12/13 | 通道接受一条只剩 5 分钟的 MT | 直接写进 WDQ（10 分钟内过期的不放 Redis）；5 分钟前不回 DR |
| WQ-14 | 通道接受一条还有 60 分钟的 MT | 留在 Redis 等，不进 WDQ（以前用真实时间算出 -420 分钟，进了 WDQ） |

### 4.2 RMQ 重试和主备（CoreRetryIT）

| ID | 内容 | 预期 |
|---|---|---|
| RE-07a～e | MQM 从 RMQ 取出、写 MQM 流 | Core 读出并确认，同一个通道再发；带着重试次数和来源 RMQ；不写第二条 IN_SUBMIT |
| RE-04a～i | 再失败、策略用完 | 更新 RMQ 的行，最后删掉，回失败 DR |
| RE-06b | 下次重试会超过有效期 | 写 RMQ 但不设 `NextRetryTime`，重发查询永远选不到，由过期任务回 |
| RE-07f | 备用 MQM | 不发 |
| RE-07g | 变成主 | 发 |
| RE-09a～c | 旧任期的 MQM 写来的 DR | Core 拒绝，把 RDQ 的行放回去（`ActionCode 0 → -1`，只在还被占着时），客户收不到 |
| RE-09d/e | 旧任期，这时数据库出错 | 行放不回去，消息留在待确认里重试，不确认 |
| RE-09f/g | 旧任期，消息没有对应的行 | 照样拒绝，没有要放回的 |

### 4.3 RDQ 重发 DR（CoreRetryIT、CoreDrBizIT）

| ID | 内容 | 预期 |
|---|---|---|
| RE-08a～c | MQM 重发一条 DR | 送到客户的 DR 流，Core 从 MQM 的 report 流读出并确认 |
| RE-08d | 客户收到了 | 删掉 RDQ 的行 |
| RE-08e | 发给客户失败 | 写进 RDQ 等下次 |
| DR-06a～e | 没有 Server 在线 | DR 写进 RDQ，不进 DR 流；Server 上线后 MQM 重发，送到 |

### 4.4 搬进 WDQ、早到 DR（DrLookupIT、EarlyDrIT）

| ID | 内容 | 预期 |
|---|---|---|
| DL-04 | 搬到时间的匹配数据 | 超过 `Redis_RetrieveRedisInterval` 的搬进 WDQ 并删掉，新的留着 |
| DL-05a/b | WDQ 写失败 | 保留、处于已领取；领取过期后下一轮搬走 |
| DL-07～09 | 有效期比 `Redis_RetrieveRedisInterval` 早 | 按有效期搬（过期 DR 能按时回） |
| DL-10a/b | 升级前的 `drl:due` | 照常搬，最终 DR 来了也删掉 |
| ED-02～09 | MQM 的早到 DR 任务 | t0+T/2、t0+T 找原消息，找到写回 Core，找不到丢弃；停掉的 MQM 领取的会放回；按批查 WDQ |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器，WdqExpiryIT 单独运行，测试服务器的 MySQL（172.19.4.38，临时库 `gmms_wdq_it`，用完已删） | 17 项全部通过（时区 Asia/Shanghai） |
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | CoreRetryIT 47、DrLookupIT 16、EarlyDrIT 24、CoreDrBizIT 41 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| RMQ 在真实数据库上按 `NextRetryTime` 选出要重发的（`getResendMessage` 的 SELECT） | CoreRetryIT 不连数据库，由本程序扮演 MQM 的查询 |
| RDQ 在真实数据库上的重发查询 | 同上 |
| 两个真实的 MQM 进程主备切换 | 主备只在进程内用 `MqmActiveState` 切换；测试服务器上跑着 MsgQueueMonitor1、2，但没有自动化的切换用例 |
| MQM 进程被 kill 后接手 | 同上 |
| WdqExpiryIT 不在全量回归里 | 要数据库账号，手动跑 |
