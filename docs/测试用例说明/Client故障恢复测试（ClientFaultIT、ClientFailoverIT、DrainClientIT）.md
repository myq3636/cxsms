# Client 故障恢复测试（ClientFaultIT、ClientFailoverIT、DrainClientIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | ClientFaultIT | it-mod:MultiSmppClient1 | 24 | Redis 真的断开、供应商回应迟到 |
> | ClientFailoverIT | it-multiclient | 23 | 两个 Client 进程，kill -9，重启，手动移分片 |
> | DrainClientIT | it-multiclient | 25 | `a2pctl drain / remove / cancel` 下线一个 Client |
>
> Core 的故障恢复在《Core故障恢复测试》，Client 的连接断开、重连在《Client SMPP连接与PDU测试》。

## 1. 测什么

Client 按分片读出向流 `stream:mt:routed:{通道}:{分片}`，每个 Client 只读分给自己的（`system:client:shards:{模块}`）。和 Core 一样，**分片是静态配置**，一个 Client 挂了别的 Client 不会自动接管。

| 场景 | 要保证的 |
|---|---|
| Redis 断开（TCP 被切断） | 写不进发送标记就不发；收到的回应等 Redis 恢复后写一次结果；供应商的 DR 回 `ESME_RX_T_APPN` 让它重发；恢复后每条 MT 到达供应商**正好一次** |
| 回应迟到（超过 `BufferTimeout`） | 先回 2700，迟到的回应不再写第二个结果，连接不断 |
| Client 被 kill -9 | 不丢；重启后积压发出；已经发出、还没回应的不再发（由 Core 的 outbox 回答）；每条一次 |
| 人工移分片 | 接手的 Client 发出积压，每条一次 |
| 下线一个 Client | 先移走分片，再等窗口里的回应，再留一段时间收回执，然后断开连接、不再重连 |

## 2. 怎么跑、怎么判定

```
regression/run.sh ClientFaultIT     # 其余套件同样
cat regression/.work/results/ClientFaultIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）；供应商是假 SMSC（`FakeSmsc`，见《Client SMPP连接与PDU测试》）。
- ClientFaultIT：Client 连 Redis 经过一个 TCP 代理（`RedisGate`，16381 → 16379），切断代理就是 Redis 真的不通；用例自己直连 16379 检查。
- ClientFailoverIT、DrainClientIT：另起两个**真实的 Client 进程**（MultiSmppClient1、MultiSmppClient2），本程序扮演 Core（往出向流写 MT、读结果）。DrainClientIT 运行真实的 `A2pCtl`，打开模块登记。
- “每条一次”：假 SMSC 数每个消息 ID 收到几次，`missing 0, twice 0` 才算通过。

## 3. 公共配置

### 3.1 ClientFaultIT（假 SMSC 端口 17794）

通道块的公共部分（同《Client SMPP连接与PDU测试》3.1：`ConnectionType: MC`、`IsInit: true`、`ReconnectInterval: 1`、`EnquireLink: false`，1 个会话）：

| 块 | 额外配置 |
|---|---|
| FV501、FV504、FV505 | `WindowSize: 20` |
| FV502 | `WindowSize: 1` |
| FV503 | `WindowSize: 20`、`SMSOptionOutgoingThrottlingNum: 10` |

GmmsConfig 追加：

```
Redis_master_port=16381
RedisStreamAutoClaimEnable=true
RedisStreamAutoClaimIdleMs=2000
RedisStreamAutoClaimIntervalMs=500
CoreMtLeaseMs=2000
ClientSendLeaseMs=2000
SMPPSubmitPendingResultMaxRetry=4
```

### 3.2 ClientFailoverIT、DrainClientIT（假 SMSC 端口 17795）

```
[FOV601]
CCBSSID: 601
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
IsInit: true
ReconnectInterval: 1
EnquireLink: false
BufferTimeout: 30
WindowSize: 500
OutgoingConnection: (ConnName=FOV601_c1,userName=u601,password=p,URL=127.0.0.1,Port=17795,SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)
```

DrainClientIT 的 FOV601 是 `WindowSize: 300`、`BufferTimeout: 60`。ClientFailoverIT 另有：

```
[FOV602]
CCBSSID: 602
…（同上）
WindowSize: 300
BufferTimeout: 60
SMSOptionOutgoingThrottlingNum: 20
OutgoingConnection: (ConnName=FOV602_c1,userName=u602,…,SessionNumber=2,…)
```

`SessionNumber=2`：两个 Client 进程各绑定其中一个。

GmmsConfig 追加：

```
RedisStreamAutoClaimEnable=true
RedisStreamAutoClaimIdleMs=10000
RedisStreamAutoClaimIntervalMs=1000
ClientSendLeaseMs=10000
```

DrainClientIT 再加 `ModuleStatusHeartbeatSeconds=1`、`ModuleStatusTTLSeconds=3`、`ModuleDrainDrWaitSeconds=4`，以及 `conf/a2p_modules.conf`：

```
1|CoreEngine1|core|x|0|core|10||
1|MultiSmppClient1|smpp-client|x|0|client|20||
1|MultiSmppClient2|smpp-client|x|0|client|20||
```

分片：MultiSmppClient1 是 0，MultiSmppClient2 是 1（`system:client:shards:*`）。

## 4. 用例

### 4.1 Redis 断开、回应迟到（ClientFaultIT）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| CF-00 | — | 都绑定上 |
| ID-09a/b | Redis 断开，读不到发送标记 | 不发；条目留在待确认里 |
| ID-09c | Redis 恢复 | 发一次，确认 |
| RS-06a～c | 发出后供应商不回应，`BufferTimeout` 3 秒 | 失败结果 2700 |
| RS-06d～f | 回应迟到 | 不写第二个结果；日志“没有等待中的消息的回应”；连接不断 |
| RS-07a～d | 发出后扣住回应，Redis 断开，再放出回应 | 先不写结果；Redis 恢复后由重试写一次：成功，一个结果 |
| RS-07e～g | `WindowSize: 1`（FV502），回应扣住，重试次数（`SMPPSubmitPendingResultMaxRetry` 4）用完 | 不写结果，由 Core 的 outbox 之后回答；窗口释放，下一条发出 |
| VD-02e～g | Redis 真的断开时供应商发 DR | 回 `ESME_RX_T_APPN`（0x64，即 100），什么都不写；恢复后供应商再发被接受 |
| FT-03a～d | 发送中 Redis 断开 3 秒后恢复 | 每条都到达供应商，正好一次（twice 0、missing 0）；没有待确认，流空了；每条正好一个结果 |

### 4.2 Client 被 kill -9（ClientFailoverIT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| FC-00 | — | 每个 Client 绑定 `SessionNumber=2` 里的一个 |
| CS-02a～d | 两个 Client 发 100 条 | 都到达供应商，没有重复；Client 1 只读分片 0，Client 2 只读分片 1 |
| TW-08a | 第二个通道 FOV602 | 每个 Client 一个连接 |
| TW-08b | FOV602 限速 20 TPS | **已知限制**：每个 Client 各守自己的限速，两个 Client 加起来约 40（3 秒 119 条） |
| FT-01a/b | kill -9 Client 1（空闲） | 分片 1 照常；分片 0 等着（51 条）；不丢不重 |
| FT-01c/d | 重启 | 积压发出，每条一次 |
| FT-01e/f | 40 条发出、还没回应时 kill -9，再重启 | 不再发（0 条重复） |
| FT-01g～j | 负载下 kill -9（分片 0 剩 655 条） | 重启后没有重复；被 kill 打断的那次发送不重发，由 Core 的 outbox 回答；分片 0 空了，没有待确认 |
| FT-06a | 再 kill -9 | 分片 0 等着（56 条） |
| CS-03a / FT-06b、FT-06c | `system:client:shards:MultiSmppClient2` 设为 `0-1`，`RELOAD_CLIENT_SHARDS` | Client 2 接手，积压发出，每条一次 |
| CS-03b | — | Client 2 处理两个分片 |
| MS-02a | Client 2 的分片设为 `none` | 什么都不发，也不退回读分片 0（40 条都留着） |
| MS-02b | 分片改回来 | 积压发出，每条一次 |

### 4.3 下线一个 Client（DrainClientIT）

| ID | 情况 | 预期 |
|---|---|---|
| DL-00 | — | 登记初始化 |
| DL-01a/b | — | 每个 Client 绑定一个连接，都在发 |
| DL-02a | 40 条发出，供应商先不回应 | — |
| DL-02b/c | `drain MultiSmppClient1` | DRAINING，提示移走 Client 的分片；等分片移走 |
| DL-03a/b | 分片移走了，还有回应没到 | 阶段 `WAIT_WINDOW`（处理中 18），连接保持 |
| DL-03c | 回应都到了 | 阶段 `WAIT_DR`，再保持连接 4 秒（`ModuleDrainDrWaitSeconds`）收回执 |
| DL-03d | — | 每条都有结果，没有 2700（不确定） |
| DL-04a | 4 秒后 | 关闭连接，`DONE`（关了 1 个通道） |
| DL-04b/c | — | 不再重连（`ReconnectInterval` 1 秒），`RELOAD_CUSTOMER` 后也不连 |
| DL-04d | — | MT 从另一个 Client 发出，每条一次 |
| DL-05a～d | 连接关闭后 cancel | ACTIVE，提示重启它；下线进度清掉；重启后重新连接；两个都在发，每条一次 |
| DL-06a～d | 再下线，在等窗口时 cancel | 不要求重启；它的 MT 都有结果；连接一直没关 |
| DL-07a | `remove MultiSmppClient1` | 分片 0 移给 MultiSmppClient2（`RELOAD_CLIENT_SHARDS`），下线完，提示停止它 |
| DL-07b | — | 两个分片的 MT 都从 MultiSmppClient2 发出，每条一次 |
| DL-07c | cancel | ACTIVE，它的连接已经关了，提示重启 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | ClientFaultIT 24 项、ClientFailoverIT 23 项、DrainClientIT 25 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 多个 Client 共用一个通道的速率 | TW-08b 记为已知限制：每个 Client 各自限速 |
| HTTP Client 进程的 kill -9 | 只测了 SMPP Client |
| 供应商端断开和 Redis 断开同时发生 | 没有用例 |
