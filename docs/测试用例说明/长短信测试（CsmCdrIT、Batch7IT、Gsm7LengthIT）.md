# 长短信测试（CsmCdrIT、Batch7IT、Gsm7LengthIT）

> 日期：2026-11-27。用例代码：`regression/test/it/CsmCdrIT.java`（24 项）、`Batch7IT.java` 的 B、C、P 部分、`Gsm7LengthIT.java`（26 项），都在 `regression/suites.txt`，it 模式。
> Batch7IT 的 A 部分（路由失败时的数据库兜底）在《故障恢复测试》里。合并数据的设计见《长短信合并数据迁移设计》，GSM 7-bit 长度见《GSM7bit消息长度计算优化设计》。拆分、编码、UDH 的内容处理在《内容处理测试（CoreContentIT）》。

## 1. 测什么

| 部分 | 套件 | 内容 |
|---|---|---|
| 合并 | Batch7IT B | 客户分段发来的长短信在 Redis 里合并：收齐才发出、乱序、重复投递、同一个参考号的新消息、两个 Core 线程同时处理、发出后崩溃 |
| 过期 | Batch7IT C | 等分段超时（`CsmIntegrityCacheTimeout`）：每个已收到的分段回一条过期 DR；不要 DR、已收齐、DR 写失败、扫描开关 |
| 性能 | Batch7IT P | 1000 条 3 段长短信在单线程上的耗时，和普通 MT 对比 |
| CDR 和 DR | CsmCdrIT | 3 段长短信从进来到 DR 回给客户，每种 CDR 写几条；通道整条收和通道按自己的长度再拆两种情况 |
| 单条长度和拆分长度 | Gsm7LengthIT | GSM 7-bit 消息里有要 ISO-8859-1 或 UCS2 的字符时按 GSM 字符数算长度；8-bit 按字节；通道 `MsgSplitLen`、`SMSOptionAsciiMsgSplitLen`、`SMSOptionSupportOutgoingGSM7bit` 对拆分的影响 |

## 2. 怎么跑、怎么判定

```
regression/run.sh CsmCdrIT
regression/run.sh Batch7IT
regression/run.sh Gsm7LengthIT
cat regression/.work/results/{CsmCdrIT,Batch7IT,Gsm7LengthIT}.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core，消息直接交给 Core 处理流的入口 `CoreMessageProcessor.processSubmitFromStream`，和 `MTStreamConsumer` 读出一条以后做的一样：开始一次处理、处理、成功就确认，失败就放弃（留在待确认里）。
- 看结果：
  - 发出的消息：通道的出向流 `stream:mt:routed:{通道}:*`；
  - 合并数据：Redis 里的 `csm:*`（见 3.3）；
  - 客户 DR：`stream:dr:pending:{ossid}:*`；
  - CDR：`cdr/` 下的文件，按类型计数（CDR 是异步写的，等 2.5～3 秒）；
  - 拒绝：日志里的 `status=2250`（消息太长）。
- CsmCdrIT 里本程序扮演 Client：每条发出的 MT 回一个成功的 submit_resp（带供应商的消息 ID），再回一条 DELIVERED 的供应商 DR。

## 3. 公共配置

### 3.1 cm.cfg

回归环境基础的 `regression/home/conf/cm.cfg` 里已有：

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

各套件追加的块（公共部分 `CCBSSID: ssid`、`A2PPlayerType: operator`、`Protocol: SMPP` 不再重复）：

| 套件 | 块 | 额外配置 |
|---|---|---|
| CsmCdrIT | CUST8889 | — |
| CsmCdrIT | VENDOR18 | `ChlMsgSplit: yes`<br>`MsgSplitLen: 100` |
| Gsm7LengthIT | CUST8892、CUST8897～8901 | `NeedCheckMsgSize: false`（只有 8892 用来测这个开关，其他客户用来测拆分，不让单条长度检查挡住） |
| Gsm7LengthIT | VENDOR62 | `ChlMsgSplit: yes`（没有 `MsgSplitLen`） |
| Gsm7LengthIT | VENDOR63 | `ChlMsgSplit: yes`<br>`MsgSplitLen: 160` |
| Gsm7LengthIT | VENDOR64 | `ChlMsgSplit: yes`<br>`SMSOptionSupportOutgoingGSM7bit: 1` |
| Gsm7LengthIT | VENDOR65 | `ChlMsgSplit: yes`<br>`MsgSplitLen: 140`<br>`SMSOptionSupportOutgoingGSM7bit: 1` |
| Gsm7LengthIT | VENDOR66 | `ChlMsgSplit: yes`<br>`MsgSplitLen: 140`<br>`SMSOptionAsciiMsgSplitLen: 160`<br>`SMSOptionSupportOutgoingGSM7bit: 1` |

### 3.2 路由

都是 `A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=通道)`：

| 客户 | 通道 |
|---|---|
| CUST8888（基础配置） | VENDOR8 |
| CUST8889 | VENDOR18 |
| CUST8892 | VENDOR8 |
| CUST8897 | VENDOR62 |
| CUST8898 | VENDOR63 |
| CUST8899 | VENDOR64 |
| CUST8900 | VENDOR65 |
| CUST8901 | VENDOR66 |

### 3.3 合并数据（Redis）

一组长短信的 key 是 `csm:{ossid}:{发送方}:{接收号码}:{参考号}:{总段数}`，例如 `csm:8888:18667991226:86139000001:11:3`：

| key | 内容 |
|---|---|
| `csm:…` | hash，每个收到的分段一项 |
| `csm:index` | 有序集合，按到期时间排的所有组（过期扫描从这里找） |
| `csm:ids:…` | 这组各分段的消息 ID |
| `csm:asm:…` | 发送占用（一个线程在发收齐的组时占着，别的线程这时处理同一组会得到 BUSY） |
| `csm:done:…` | 完成记录，保存这组的消息 ID，用来认出发出后又重复投递来的分段 |

### 3.4 GmmsConfig 和测试里改的值

| 配置 | 值 | 说明 |
|---|---|---|
| `CoreMtLeaseMs` | 800（Batch7IT，默认 60000） | 处理占用多久后算原处理者挂了，测崩溃后的重复投递 |
| `CsmIntegrityCacheTimeout` | 1000（Batch7IT C 部分，默认 3600000） | 等分段的最长时间 |
| `CoreCsmExpiryIntervalSec` | 1（Batch7IT C6，默认 60） | 过期扫描间隔 |
| `CoreCsmExpirySweep` | 先 false、再 true（Batch7IT C6，默认 true） | 扫描开关，热加载 |

## 4. 用例

### 4.1 合并（Batch7IT B，客户 8888 → VENDOR8，每段内容 `part{i} `）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| B7-B1a | 没收齐 | 3 段里的第 1、2 段 | 存进 Redis（hash 2 项、`csm:index` 有这组），都确认，不发出 |
| B7-B1b | 收齐 | 第 3 段 | 发出一条合并好的消息 |
| B7-B1c | 收尾 | — | hash、`csm:ids`、`csm:asm`、`csm:index` 的项都删掉；`csm:done` 记了 3 个消息 ID |
| B7-B1d | 合并的内容 | — | 发出的内容里有 part1 到 part3 |
| B7-B2 | 乱序 | 按 3、1、2 的顺序发 | 最后一段到了才发，只发一次 |
| B7-B3a | 发出后又投递来一个分段 | 重发 B1 的第 2 段 | 确认 |
| B7-B3b | — | — | 不新建一组（否则以后会过期回错的 DR），不再发 |
| B7-B4 | 同一个参考号的新消息（新的消息 ID） | 参考号 11、接收号码同 B1 的新第 1 段 | 新建一组（hash 1 项），没被当成重复吞掉 |
| B7-B5a | 别的线程正在发这组 | 另一个线程存了最后一段、拿到发送占用后没做完；这时第 1 段重复投递来 | BUSY，不确认（留着以后重试） |
| B7-B5b | — | — | 没有发两次 |
| B7-B6a | 发出以后、收尾以前崩溃 | 另一个线程存最后一段、发出合并的消息，然后“死掉” | 发出 1 条，这组还在 Redis |
| B7-B6b | 占用期内重复投递最后一段 | — | BUSY |
| B7-B6c | 占用（800 毫秒）过后重复投递 | 等 900 毫秒再投 | 处理并确认 |
| B7-B6d | — | — | 没有发第二次（这次处理的发出标记还在），这组收尾 |

### 4.2 过期（Batch7IT C，`CsmIntegrityCacheTimeout` = 1 秒）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| B7-C0 | 还没到期 | 3 段里收了 2 段，马上扫描 | 不处理 |
| B7-C1a | 到期 | 1.1 秒后扫描 | 处理了 1 组 |
| B7-C1b | 过期 DR | — | 每个已收到的分段一条（2 条），状态 EXPIRED 或 UNDELIVERABLE（客户支持过期 DR 时是 EXPIRED），在收尾之前写出 |
| B7-C1c | 收尾 | — | 这组和 `csm:index` 的项删掉，留下完成记录 |
| B7-C2 | 过期后又投递来一个分段 | 重发第 1 段 | 确认，不新建一组 |
| B7-C3 | 分段不要 DR | 分段的 `deliveryReport=false`，到期扫描 | 收尾，不回 DR |
| B7-C4 | 到期时已经收齐（发出后崩溃，没人重投） | 另一个线程存了最后一段就“死掉” | 收尾，不回 DR |
| B7-C5a | 过期 DR 写出失败（例如 Redis 断了） | 扫描时写 DR 返回失败 | 这组保留 |
| B7-C5b | 占用期内另一次扫描 | 马上再扫一次 | 看不到这组（已被领取） |
| B7-C5c | 占用过后 | 1.1 秒后再扫 | 回出 DR，这组收尾 |
| B7-C6a | 扫描开关关着 | `CoreCsmExpirySweep=false`，启动 Core 的扫描线程（间隔 1 秒），等 3 秒 | 不处理 |
| B7-C6b | 热加载打开 | 改成 true | 4 秒内扫描线程回出过期 DR |

### 4.3 性能（Batch7IT P）

| ID | 内容 | 预期 / 结果 |
|---|---|---|
| B7-P1 | 1000 条 3 段长短信（参考号在 200 个里循环），单线程走完整的 MT 处理 | 全部合并完成、收尾（不剩 `csm:8888:*:P*`） |
| B7-P2 | 记录耗时 | 2026-11-27：3000 段 1593 毫秒（约 1883 段/秒） |
| B7-P3 | 对照：同一条处理路径上 3000 条普通 MT | 2026-11-27：1077 毫秒 |

P2、P3 只输出不判定。

### 4.4 CDR 和 DR（CsmCdrIT）

每个场景：客户发来（场景 C 是一条普通短信），Core 处理；每条发出的 MT 回成功的 submit_resp 和 DELIVERED 的供应商 DR；然后数 CDR 和客户收到的 DR。分段内容都是 60 个字符。

| 场景 | 客户 → 通道 | 发出 | 预期 |
|---|---|---|---|
| A 通道整条收 | 8888 → VENDOR8，3 段 | 1 条（180 字符，3 个分段的 inMsgId 都在里面） | 见下表 |
| B 通道按 100 再拆 | 8889 → VENDOR18（`MsgSplitLen: 100`），3 段 | 2 条（100、80 字符） | 见下表 |
| C 普通短信（对照） | 8888 → VENDOR8，`hello` | 1 条 | 见下表 |

每个场景都检查：

| 检查 | 预期 |
|---|---|
| 每段（或这条短信）处理并确认 | 是 |
| 发到通道 | 是 |
| 进来的 CDR | 每段一条 `IN_SUBMIT_PARTIAL` 和一条 `IN_SUBMIT`；普通短信只有 1 条 `IN_SUBMIT` |
| `OUT_SUBMIT` | 每条发出的 MT 一条（它的提交结果） |
| `OUT_DELI_REP` | 每条供应商 DR 一条 |
| 客户收到的 DR | 客户发来几条（分段）就回几条：发出的第一部分的 DR 拆回每个分段一条（`CsmUtility.splitCsmDr`） |
| 每条 DR 的 inMsgId | 各是它自己那段的（长短信场景） |

2026-11-27 的实测：

| 场景 | 发出 | 客户收到的 DR | CDR |
|---|---|---|---|
| A | 1 | 3 | IN_SUBMIT 3，IN_SUBMIT_PARTIAL 3，OUT_SUBMIT 1，OUT_DELI_REP 1 |
| B | 2 | 3 | IN_SUBMIT 3，IN_SUBMIT_PARTIAL 3，OUT_SUBMIT 2，OUT_DELI_REP 2 |
| C | 1 | 1 | IN_SUBMIT 1，OUT_SUBMIT 1，OUT_DELI_REP 1 |

### 4.5 单条长度检查（Gsm7LengthIT，客户 8888 → VENDOR8，`NeedCheckMsgSize` 默认打开）

预期是“发出”，或“拒绝”（日志 `status=2250`，消息太长）。GSM 字符数里扩展字符（如 `{`）算 2 个。

| ID | 消息 | 预期 |
|---|---|---|
| G7-01 | GSM 7-bit，160 个 ASCII 字符 | 发出 |
| G7-01b | GSM 7-bit，200 个 ASCII 字符（message_payload） | 发出（ASCII 不检查，和以前一样） |
| G7-02 | GSM 7-bit，带 `é`（ISO-8859-1），160 字符 | 发出（以前是 2250） |
| G7-04 | GSM 7-bit，带 `Δ`（UCS2），160 字符 | 发出（以前是 2250） |
| G7-05 | GSM 7-bit，带 `é`，161 字符 | 拒绝 |
| G7-06a | `é` + 150 个字符 + 5 个 `{`：155 字符、160 个 GSM 字符 | 发出 |
| G7-06b | `é` + 151 个字符 + 5 个 `{`：156 字符、161 个 GSM 字符 | 拒绝 |
| G7-05b | 8-bit（data_coding 3，ISO-8859-1），160 字节 | 拒绝（和以前一样，8-bit 上限 140 字节） |
| G7-05c | 8-bit，140 字节 | 发出 |
| G7-11 | 8-bit，160 字节，客户 8892 `NeedCheckMsgSize: false` | 发出（不检查） |
| G7-12 | G7-02 那条的 IN_SUBMIT CDR | 字符集一栏是 `ASCII`，和以前一样 |

### 4.6 发往通道时的拆分（Gsm7LengthIT）

预期写的是发出的段数和每段大小：

| ID | 客户 → 通道 | 消息 | 预期 |
|---|---|---|---|
| G7-13a | 8897 → VENDOR62（没有 `MsgSplitLen`） | 8-bit ISO，160 字节 | 2 段，每段不超过 140：[140, 20] |
| G7-13b | 同上 | 8-bit ISO，140 字节 | 1 段 |
| G7-13c | 同上 | ASCII，160 | 1 段（和以前一样） |
| G7-14 | 8898 → VENDOR63（`MsgSplitLen: 160`） | 8-bit ISO，160 字节 | 按通道的值：1 段 |
| G7-15 | 8899 → VENDOR64（支持 GSM 7-bit） | GSM 7-bit 带 `é`，160 | 还是 7-bit，1 段 |
| G7-16 | 8897 → VENDOR62（不支持 7-bit） | 同上 | 改成 ISO 8-bit 发，2 段：[140, 20] |
| G7-17a | 8900 → VENDOR65（只有 `MsgSplitLen: 140`） | ASCII，160 | 也按 140 拆：2 段 |
| G7-17b | 同上 | GSM 7-bit 带 `é`，160 | 2 段 |
| G7-18a | 8901 → VENDOR66（再加 `SMSOptionAsciiMsgSplitLen: 160`） | ASCII，160 | 1 段 |
| G7-18b | 同上 | GSM 7-bit 带 `é`，160 | 1 段 |
| G7-18c | 同上 | 8-bit ISO，160 字节 | 按 `MsgSplitLen` 140：2 段 |

### 4.7 Server 解析 GSM 7-bit 内容（Gsm7LengthIT，直接调用 `Smpp.handleGSM7bitMessageContent`，按客户的模式 1）

| ID | 输入字节 | 预期 |
|---|---|---|
| G7-05e | `43 61 66 05`（真正的 GSM 7-bit，`é` 是 0x05），客户 8893 | `Café`，ISO-8859-1，gsm7bit，没有告警 |
| G7-05d | `43 61 66 E9`（模式 1 的客户发了大于 0x7F 的字节），客户 8894 | 和以前一样解析：`Caf?`，ASCII，gsm7bit |
| G7-05f | — | 日志一条告警 `[GSM7] ossid=8894`（这时消息还没有 ossid，用传入的客户 ossid） |
| G7-05g | 同一客户 10 分钟内再来一条（`A3`） | 不再打第二条（限频） |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | CsmCdrIT 24 项、Batch7IT 37 项（含 A 部分）、Gsm7LengthIT 26 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 长短信经过真实的 SMPP Server 进来（UDH 和 SAR TLV 的解析） | 这里分段是直接构造的；Server 的解析在 Server 的用例里 |
| 多个 Core 进程同时处理同一组 | B5、B6 用同一进程里的两个线程模拟 |
| 分段里有的失败、有的成功时客户收到的 DR | CsmCdrIT 只测了全部 DELIVERED |
| submit Redis 多主时合并数据按组 key 选主 | 回归环境只有一个 Redis |
| 长短信的重试、改道 | CoreRetryIT 用的是普通短信 |
