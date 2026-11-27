# 端到端与日志链测试（CoreE2EIT、PathChainIT、MultiMasterIT、LogChainCoreIT、StartupIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | CoreE2EIT | it | 23 | Core 里一条 MT 的完整路径：路由、outbox、提交结果、超时、迟到的回应 |
> | PathChainIT | it-mod:CoreEngine1 | 14 | Server、Core、Client 各一个进程，消息经过的模块路径（日志和 CDR） |
> | MultiMasterIT | it | 12 | 两个 submit Redis 主、两个 report Redis 主，三个模块各一个进程 |
> | LogChainCoreIT | it | 12 | Core 的日志链：每一步一行 INFO |
> | StartupIT | it-nosr | 2 | 没有 `conf/senderReplacement.cfg` 时 Core 能启动 |
>
> Client、Server 自己的日志链在《Client发送速率与发送过程测试》（LogChainClientIT）、《Server连接与DR发送测试》（ServerBatch2IT S2-01）。

## 1. 测什么

| 部分 | 内容 |
|---|---|
| Core 的完整路径 | 真实的 `MTStreamConsumer` 路由（`conf/routing`）、写出向流、outbox 扫描、`ResultStreamConsumer`；重投去重；Client 不读时拉回；迟到的回应丢弃 |
| 跨进程 | 客户绑定真实的 Server，真实的 Core 路由，真实的 Client 发给假 SMSC，供应商 DR 原路回来 |
| 模块路径 | `GmmsMessage.modulePath`：每个模块写出时加上自己；日志行和 Core 的 CDR（DeliveryChannel 列）里能看到 |
| 多个 Redis 主 | `Redis_submit_masters`、`Redis_report_masters` 各两个：流按分片放在各自的主上；每条 MT 出去一次、每条 DR 回来一次；最后哪个主上都没有残留 |
| 日志链 | 一条 MT、一个提交结果、一条供应商 DR 在 Core 里每一步一行 INFO，正常流程没有 WARN/ERROR |

## 2. 怎么跑、怎么判定

```
regression/run.sh PathChainIT     # 其余套件同样
cat regression/.work/results/PathChainIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。MultiMasterIT 用到的另外两个 Redis（submit 16382、report 16383）由 `run.sh` 启动：submit 主是 16379 + 16382，report 主是 16379 + 16383。
- CoreE2EIT、LogChainCoreIT、StartupIT：进程内启动 Core，本程序扮演 Server 和 Client。
- PathChainIT、MultiMasterIT：Server、Core、Client 各起一个进程，供应商是假 SMSC（`FakeSmsc`），本程序用 SMPP 客户端以客户身份提交、收 DR，再读各进程的日志、CDR 和 Redis。
- StartupIT 用 `it-nosr` 模式：回归环境里没有 `senderReplacement.cfg`。

## 3. 配置

- CoreE2EIT、LogChainCoreIT、StartupIT：回归环境基础配置，CUST8888 → VENDOR8（见《长短信测试》3.1）。
- CoreE2EIT 测超时时把 `CoreOutboundUnreadStallMs`、`CoreOutboundRespTimeoutMs` 调小（同《Core故障恢复测试》Batch3IT）。
- PathChainIT、MultiMasterIT：一个客户（SMPP 绑定到 Server）、一个通道（连假 SMSC），路由 86 → 这个通道。
- MultiMasterIT 的 GmmsConfig：

```
Redis_submit_masters=127.0.0.1:16379,127.0.0.1:16382
Redis_submit_slaves=127.0.0.1:16379,127.0.0.1:16382
Redis_report_masters=127.0.0.1:16379,127.0.0.1:16383
Redis_report_slaves=127.0.0.1:16379,127.0.0.1:16383
```

## 4. 用例

### 4.1 Core 的完整路径（CoreE2EIT，CUST8888 → VENDOR8）

| ID | 情况 | 预期 |
|---|---|---|
| E2E-02a～d | 第一次处理（处理完不确认） | 路由到 rssid 8（1 条）；1 条 IN_SUBMIT；号码计数 1；MT 还在待确认 |
| E2E-02e～h | 重投 | 不再写出（还是 1 条）；不写第二条 IN_SUBMIT；号码不再计数；确认并删掉标记 |
| E2E-03a/b | `CoreMtIdempotency` 关 | 不写标记；重投会再写出（以前的行为） |
| E2E-01a～d | 正常一条 | 写进 `stream:mt:routed:8:*`；outbox `core:outbox:submit:8888_E2E01:0:0`；确认、删标记；正好 1 条 IN_SUBMIT |
| E2E-05a/b | 提交结果 | 领取 outbox，OUT_SUBMIT 只写一次；按供应商消息 ID 存 DR 匹配数据 |
| E2E-04a～c | Client 一直不读 | 超过停滞时间后从出向流拉回，由超时结果领取 outbox；`ResultStreamConsumer` 处理超时结果（OUT_SUBMIT，状态 2500）；给客户的失败 DR 写进 `stream:dr:pending:8888:MultiSmppServer1` |
| E2E-06a～c | 之后迟到的 submit_resp | STALE 丢弃；不写第二条 OUT_SUBMIT；确认（结果流空了） |
| B1-11 | `MessageProcessorHandler.putMsg` 遇到不认识的客户 | 照样入队，不出空指针 |

### 4.2 模块路径（PathChainIT）

| ID | 情况 | 预期 |
|---|---|---|
| PC-00a | 写进流时加上本模块再读回 | `MultiSmppServer1>CoreEngine1` |
| PC-00b | 以前写的条目（没有字段 57） | 能读，没有路径 |
| PC-01/02 | — | Client 绑定供应商，客户绑定 Server |
| PC-03/04 | 客户提交 | 接受，供应商收到 |
| PC-05 | Client 的 `[MT-OUT]` | `path=MultiSmppServer1>CoreEngine1>MultiSmppClient1` |
| PC-06 | Core 的 `[MT-TO-CLIENT]` | `path=MultiSmppServer1>CoreEngine1` |
| PC-07 | — | 客户收到 DR |
| PC-08 | Core 的 `[DR-TO-SERVER]` | `path=MultiSmppClient1>CoreEngine1`（DR 自己的路径，从收到它的 Client 开始） |
| PC-09 | Server 的 `[DR-OUT]` | `path=MultiSmppClient1>CoreEngine1>MultiSmppServer1` |
| PC-10 | IN_SUBMIT CDR | `MultiSmppServer1>CoreEngine1` |
| PC-11 | OUT_SUBMIT CDR（结果回来时写） | `MultiSmppServer1>CoreEngine1>MultiSmppClient1>CoreEngine1` |
| PC-12 | OUT_DELI_REP CDR | `MultiSmppClient1>CoreEngine1` |

### 4.3 多个 Redis 主（MultiMasterIT）

| ID | 情况 | 预期 |
|---|---|---|
| MM-00a | — | 2 个 submit 主、2 个 report 主 |
| MM-00b | 放在哪个主 | 分片的流放在“分片号 % 主的个数”上；暂存流在主 0；只有一个主时都是 0 |
| MM-00c | 长短信的分段 | 放在它那一组的 submit 主上（两个主都用到了），`csm:index` 也在那里；完成后删掉 |
| MM-01/02 | — | Client 绑定供应商，客户绑定 Server |
| MM-03～05 | 客户提交 40 条 | 都接受；供应商收到 40 条，每条一次（40 个接收号码） |
| MM-06 | — | 分片 0 的 MT 流在 submit 主 0，分片 1 的在主 1 |
| MM-07 | — | 客户收到 40 条 DR，对应 40 条 MT，没有重复 |
| MM-08 | — | 结果流和 DR 流在两个 report 主上都有 |
| MM-09 | 最后 | 哪个主上都没有残留：积压 0/0/0，处理标记、发送标记、outbox 都空 |

### 4.4 日志链（LogChainCoreIT）

| ID | 情况 | 预期的日志顺序 |
|---|---|---|
| L-C1a～c | 一条 MT | `STREAM-GET` → `CDR IN_SUBMIT` → `MT-ROUTE` → `STREAM-PUT` → `MT-TO-CLIENT` → `STREAM-ACK`；没有 WARN/ERROR |
| L-C2a/b | 提交结果 | `STREAM-GET` → `RESULT`（outbox=FIRST）→ `CDR OUT_SUBMIT` → `STREAM-ACK` |
| L-C3a～c | 供应商 DR | `STREAM-GET` → `VDR-MATCH`（两个 ID 在同一行）→ `CDR OUT_DELI_REP` → `DR-TO-SERVER` → `STREAM-ACK`；正常的 DR 没有 WARN（会话登记的兜底是 INFO） |
| L-C4a/b | 匹配不上的 DR | 作为早到 DR 排队，一行 INFO `VDR-EARLY`（不再有带发送方、接收号码、内容的 `TMP-DRLOOKUP` WARN） |
| L-C5a/b | 重复的 submit_resp | `[RESULT-DROPPED]`，INFO；不写第二条 OUT_SUBMIT |

### 4.5 启动（StartupIT）

| ID | 情况 | 预期 |
|---|---|---|
| NPE-03a | 准备 | 没有 `senderReplacement.cfg` |
| NPE-03b | 启动 Core | 正常启动并加载 cm.cfg（以前空指针） |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 全部通过 |
| 2026-11-27 | 测试服务器重跑（取上面的实测输出） | 5 组共 63 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 生产的多机部署（每个模块在不同的机器上） | 都在同一台机器 |
| 多个 Redis 主时某个主挂掉 | 没有用例 |
| MO 经过三个进程 | PathChainIT 只走 MT 和 DR，MO 分段测（见《MO测试》） |
| 长短信经过三个进程 | MultiMasterIT 只检查长短信分段放在哪个主（MM-00c），没有走真实的 Server、Client |
