# SMPP 外发 bind 级限速设计

> 日期：2026-11-10。状态：**已实现**（第 9 节是实现和设计稿不同的地方、回归结果）；第 7 节的问题都已确认。第 2 节来自读 `main`（`c997dd4`）的代码。

## 1. 目标

供应商常按 **每个 bind**（每条 SMPP 会话）限速，例如“每个 bind 最多 50 TPS”，超了回 `ESME_RTHROTTLED`（0x58）。现在 Client 只能按 **通道（账号）** 限速，配不出这种规则。加一个配置，按 bind 控制 submit_sm 的发送速度，和现有的账号级限速同时生效。

## 2. 现状

### 2.1 账号级限速

`DistributedThrottlingManager`：每个 Client 进程里每个 rssid 一个 Guava `RateLimiter`，速率取通道的 `SMSOptionOutgoingThrottlingNum`（0 不限），**按单个 Client 节点生效**，节点之间不分摊（配置说明已写明，`ClientFailoverIT` TW-08 有用例）。每次取许可前按当前配置对齐速率，改配置热加载后下一条就生效。

`OutboundStreamConsumer.handleMessageSending`（`OutboundStreamConsumer.java:329`）的顺序：

```text
1. 选会话：MultiSmppClientFactory.getSession(msg)
     -> 通道的连接策略选一条连接（primary / loadbalance ...）
     -> 连接内轮询选会话（AbstractMultiConnection.getSession，IndexBalanceStrategy），不看窗口
2. 会话不在线           -> 失败结果
3. 会话窗口满           -> WINDOW_FULL：本批剩下的推迟 ClientOutboundWindowFullRetryMs（100 ms）
4. 账号级取不到许可      -> THROTTLED：本批剩下的推迟约一个许可的间隔（1000/TPS，10 ms～1 s）
5. 抢发送标记，发 submit_sm
```

推迟时 worker 让出，条目留在本消费者的待确认里，不阻塞线程（`StreamDeferrals`）。

### 2.2 为什么账号级配不出 bind 级

假设供应商要求每个 bind ≤ 50 TPS，`SessionNumber=4`，2 个 Client：每个 Client 2 个 bind。

| 情况 | 账号级只能配成 | 实际每个 bind |
|---|---|---|
| 正常，4 个 bind 都在 | 每个 Client 100（= 50 × 2） | 50，正好 |
| 一个 Client 的一个 bind 断了 | 还是 100 | 剩下那个 bind **100，超限** |
| 扩容到 3 个 Client（每个 ⌈4/3⌉=2 个 bind，见 `模块动态扩缩容设计.md` 10.1 节） | 还是每个 Client 100，总量变 300 | 正确，但总量和连接数都变了，要人工重新算 |
| 轮询偶尔连续落在同一个 bind | 100 | 某一秒可能超过 50 |

账号级的数字是“节点 × 会话数”的人工换算，会话数一变就不对了。

### 2.3 相关代码

- 会话对象（`MultiSmppSession`，客户端）随连接配置建好，重连复用同一个对象（内存排查时确认过）。
- 每条 `OutgoingConnection` 的参数（`A2PMultiConnectionInfo.java:430` 起）：`URL`、`Port`、`SessionNumber`、`BindMode` ……，不认识的参数存进 `ConnectionInfo` 的扩展属性，不会报错。
- 现在没有任何会话级、连接级的限速。`ThrottlingControl` 是入向的系统流控，无关。
- 供应商回 0x58 时，Client 按普通失败写结果，由 Core 的重试策略决定是否重发，没有专门处理。

## 3. 配置

| 配置 | 位置 | 默认 | 作用 |
|---|---|---|---|
| `SMSOptionOutgoingThrottlingNumPerBind` | cm.cfg，通道 | `0`（不限） | 这个通道 **每个 bind** 的 submit_sm 速率（TPS），通道下所有连接的所有 bind 都是这个值 |

```text
[VENDOR8]
SMSOptionOutgoingThrottlingNum: 300            # 账号级：每个 Client 节点 300（原有）
SMSOptionOutgoingThrottlingNumPerBind: 50      # 每个 bind 50（新）
```

**不做连接级的 `BindTps`（已确认）：** 原来考虑过在 `OutgoingConnection` 里加 `BindTps=N`，给同一个通道里不同账号的连接配不同的 bind 速率。现在没有这种需求，不做；以后需要时再加，解析处不认识的参数本来就会存进扩展属性，加起来不难。

- **两层同时生效：** 一条 submit_sm 要同时拿到账号级（本节点）和 bind 级（本会话）的许可。只配其中一个就只有那一层。
- **每个 bind 的速率不随 Client 数、会话数变化：** 某个 bind 断了，剩下的 bind 还是各自 50，总量自然下降，不会把流量压到剩下的 bind 上。
- **热加载：** 和账号级一样，每次取许可前按当前配置对齐速率，改了 `RELOAD_CUSTOMER` 后下一条就生效，不重建会话。
- **只限 submit_sm：** 长短信每一段、重发都各算一条（供应商也是按 PDU 计）；enquire_link、deliver_sm_resp 不算。
- **只对 SMPP Client 生效：** HTTP 外发没有 bind，配了也忽略；Server 侧不受影响。

## 4. 方案

### 4.1 每个会话一个限速器

`MultiSmppSession` 上挂一个 `BindRateLimiter`（新类，很小）：

- 均匀间隔放行，不允许突发：每条之间至少 `1000/TPS` 毫秒。Guava `RateLimiter` 默认会攒最多 1 秒的许可一次放出，供应商按秒计数时可能被判超限，所以 bind 级不用它。账号级保持 Guava 不变。
- 两个操作：`canSend(now)`（只看，不占用）和 `take(now)`（占用）。用一个 `AtomicLong nextAt` 实现，无锁。
- 会话对象在重连时复用，限速器跟着会话走，不会因为重连而清零。

### 4.2 发送顺序

```text
1. 选会话（4.3）：在线、窗口有空、bind 级 canSend
     都没有 -> 有窗口但都没许可：THROTTLED，推迟到最早一个会话可发的时间（10 ms～1 s）
              都没窗口：WINDOW_FULL（和现在一样）
2. 账号级 tryAcquire
     失败 -> THROTTLED（和现在一样）；这时还没占用 bind 许可，不浪费
3. bind 级 take
     失败（并发下被别的 worker 抢先）-> 账号级许可已用掉（少一个），THROTTLED，推迟一个间隔
4. 抢发送标记，发 submit_sm（和现在一样）
```

先“只看”bind、再拿账号许可、最后占用 bind，是为了不浪费许可：Guava 的许可拿了不能退。第 3 步失败只在两个 worker 同时选中同一个会话时发生，最多浪费一个账号许可。

### 4.3 选会话：配了 bind 级限速的通道，在连接里找能发的 bind（已确认选 B）

**现在怎么分配：** 一条连接内按条数轮询（`IndexBalanceStrategy`：每次序号加一、对在线的 bind 数取模），长期看每个 bind 分到的条数是平均的；断开的 bind 不在连接的会话列表里，不参与轮询。

实测（2026-11-10，一个 Client，供应商立即回 resp）：2 个 bind 不限速 `[50, 50]`；账号级 20 TPS 时 2 个 bind `[50, 50]`、3 个 `[31, 30, 29]`、4 个 `[25, 25, 25, 25]`。账号级限速不会让 bind 不平均（虽然轮询在取许可之前，被限速的那次也会推进序号）。

**问题：** 轮询只管“轮到谁”，不管“它现在能不能发”。轮到的 bind 窗口满了，或者（加了 bind 级限速后）还没到下一个许可的时间，整批就推迟，同一连接里其他能发的 bind 闲着。

例：两个 bind，每个 10 TPS（两条之间至少 100 ms），一批 4 条在几毫秒内处理：

| 第几条 | 轮到 | 只看轮到的 | 找能发的 |
|---|---|---|---|
| 1 | A | A 发 | A 发 |
| 2 | B | B 发 | B 发 |
| 3 | A | A 要等 100 ms → 整批推迟 | A、B 都要等 → 推迟到最早可发的时间 |
| 4 | — | 推迟 | 推迟 |

两个 bind 状态相同时结果一样；状态不同时（A 窗口满、或 A 刚发过而 B 空了一会儿），“只看轮到的”让 B 闲着，“找能发的”直接用 B。

**做法（B）：** 只在配了 `SMSOptionOutgoingThrottlingNumPerBind` 的通道上，从轮询的位置开始，在**同一条连接**里最多看一圈，取第一个“在线、窗口有空、bind 级可发”的会话。没配的通道完全保持现在的轮询和窗口满的处理。不跨连接，连接的选择（primary / loadbalance）不动。

### 4.4 日志和指标

- 指标：`client.bind.throttled`（按 rssid），和现有的 `window_full` 同一套。
- 日志：bind 级推迟只记 DEBUG；每个通道 10 分钟一条 INFO 汇总 `[BIND-RATE] rssid=... bind 限速 N TPS，过去 10 分钟推迟 M 次`（`LogRateLimiter`）。
- 启动和热加载时 INFO 打印每条连接生效的 bind 速率。

## 5. 改动范围

| 文件 | 改动 |
|---|---|
| 新增 `BindRateLimiter` | 4.1 |
| `A2PCustomerInfo` | 读 `SMSOptionOutgoingThrottlingNumPerBind` |
| `MultiSmppSession` | 持有限速器；`bindRate()` 取通道的 `SMSOptionOutgoingThrottlingNumPerBind` |
| `AbstractMultiConnection` | 带条件的 `getSession(predicate)`（4.3） |
| `MultiSmppClientFactory`、`CustomerStrategy` | 把条件传到连接 |
| `OutboundStreamConsumer` | 4.2 的顺序；THROTTLED 的推迟时间取 bind 级最早可发时间 |
| `docs/配置项说明.md` | 新配置；`SMSOptionOutgoingThrottlingNum` 的说明补上“和 bind 级同时生效” |

cm.cfg 的配置不在 `GmmsConfig` 里，不涉及 `GmmsUtility` 的热加载白名单。

## 6. 回归用例

真实 Client 进程，假供应商（`FakeSmsc`）记录每个 bind 收到 submit_sm 的时间，持续发送：

| 编号 | 场景 | 预期 |
|---|---|---|
| BR-01 | 只配账号级 100（对照） | 和现在一样 |
| BR-02 | `SessionNumber=2`，每个 bind 20 | 每个 bind 每秒 ≤ 20（任意 1 秒窗口），总量约 40 |
| BR-03 | 同上，账号级 30 | 总量约 30，每个 bind ≤ 20 |
| BR-04 | 一个 bind 断开 | 剩下的 bind 仍 ≤ 20，总量降到约 20，断开的恢复后回到约 40 |
| BR-05 | 两条连接（两个账号） | 两条连接的每个 bind 都按通道配置 |
| BR-06 | 热加载把每个 bind 从 20 改成 5 | 下一秒起 ≤ 5，不重启 |
| BR-07 | 长短信 3 段 | 3 条 submit_sm 各算一条 |
| BR-08 | 每个 bind 均匀发：任意 100 ms 内 ≤ ⌈TPS/10⌉+1 | 不突发 |
| BR-09a | 配了 bind 级，一个 bind 窗口满（供应商不回它的响应），另一个空 | 发到另一个 |
| BR-09b | 没配 bind 级，同上 | 和现在一样整批推迟（行为不变） |
| BR-10 | 不丢不重 | 每条 MT 恰好发一次，结果都写回 |
| BR-11 | 配了 bind 级，HTTP 通道 | 忽略，不影响 |

## 7. 已确认

1. 账号级 `SMSOptionOutgoingThrottlingNum` 保留，新增通道级 `SMSOptionOutgoingThrottlingNumPerBind`，两层同时生效；不做连接级 `BindTps`。
2. 选会话按 B：只在配了 bind 级限速的通道上找能发的 bind。

3. 不允许突发：bind 级按均匀间隔放行（4.1）。

## 8. 附：线上 bind 发送不平均的可能原因

一条连接内的轮询本身是平均的（4.3 的实测）。线上看到不平均，可能是：

| 原因 | 现象 | 怎么确认 |
|---|---|---|
| 某个 bind 上供应商回 resp 慢或不回 | 窗口满，轮到它时整批推迟 100 ms，它分到的条数变少，整个通道也变慢 | PDU 日志按会话统计 `submit_sm` 的 `[SEND]` 到对应 `submit_sm_resp` 的 `[RECV]` 的时间；看有没有 `client_submit_window_full` 的 NACK |
| 多个 Client 模块 | 每个 Client 只用自己的 bind，Client 之间的流量按分片分，不一定一样多 | 按 Client 模块分别统计 |
| 通道有多条连接 | 连接策略（`SubmitPolicy_Conn`，primary 等）决定流量去哪条连接 | 看通道配置 |
| bind 断线重连 | 断开期间它不参与轮询 | PDU 日志里的 bind、unbind 时间 |
| 供应商侧统计口径 | 对方按 bind 统计时可能把 deliver_sm_resp 等也算进去 | 和供应商对一下口径 |

bind 级限速（4.3 的 B）上线后，配了的通道里，回 resp 慢的那个 bind 不会再拖住其他 bind。

实测见 `docs/SMPP外发bind分布实测报告.md`。

## 9. 实现说明和回归结果

**改动的文件：** 新增 `throttle/BindRateLimiter`；`A2PCustomerInfo`（读配置、`getOutgoingThrottlingNumPerBind`）、`A2PMultiConnectionInfo`（复制这个值）；`MultiSmppSession`（`bindRate()`、`bindTps()` 按当前配置读、`connectionSessions()` 取同一连接的在线会话）；`OutboundStreamConsumer`（4.2 的顺序、`BIND_THROTTLED`）；`MetricsNames.ACTION_BIND_THROTTLED`；`A2PCustomerManager`（reload 时这个值变了打一条 `[BIND-RATE]` INFO）。

**和设计稿不同的地方：**

- **没改 `AbstractMultiConnection`、`MultiSmppClientFactory`、`CustomerStrategy`：** 连接策略照常选出一个会话，消费者再用这个会话所在连接的在线会话列表，从它开始找一圈（4.3 的 B）。效果一样，不用把条件穿过 6 个连接策略。
- **均匀间隔允许早到四分之一个间隔：** 推迟的批次被唤醒时会早几毫秒或晚几毫秒，严格按间隔会因为晚到而少发。现在早到不超过 1/4 间隔也放行，并且保持原来的节奏（下一条的时间从应到的时间算）；空闲时不攒许可。任意 1 秒最多 TPS+1 条，任意 100 ms 最多 ⌈TPS/10⌉+1 条。
- **推迟时间 1 ms～1 s（设计稿写 10 ms～1 s）：** 每个 bind 超过 100 TPS 时间隔不到 10 ms，按 10 ms 推迟会发不满。
- **reload 会重建这个通道的会话**（原有行为，改任何通道配置都这样），限速器跟着新会话重新开始；重连那一刻正在发的 MT 拿到失败结果，由 Core 重试（BR-06 实测 60 条里 2～5 条）。
- **启动时没有单独打印每条连接的 bind 速率：** cm.cfg 的通道配置在启动日志里已经有；reload 改了这个值时打一条 `[BIND-RATE] ssid=... SMSOptionOutgoingThrottlingNumPerBind now N TPS per bind`。
- **BR-11（HTTP 通道）没写用例：** HTTP 外发走 `HttpClientStreamConsumer`，没有读这个配置的代码。

**回归 `BindRateIT`（`suites.txt`），20 项全部通过：**

| 编号 | 实测 |
|---|---|
| BR-01 只配账号级 100 | 300 条 `[150, 150]`，3.4 秒 |
| BR-02 2 个 bind 各 20 | 200 条 `[99, 101]`，5.1 秒；每个 bind 任意 1 秒最多 21 条 |
| BR-08 不突发 | 每个 bind 任意 100 ms 最多 3 条 |
| BR-10 不丢不重 | 200 条每条恰好 1 次，结果都写回 |
| BR-03 再加账号级 30 | 150 条 5.0 秒，每个 bind 每秒最多 17 条 |
| BR-04 一个 bind 断开 | 剩下的 bind 每秒最多 21 条，60 条 2.9 秒；恢复后 80 条 1.9 秒 |
| BR-05 两条连接各 1 个 bind，各 10 | 32 + 28 条，每个 bind 每秒最多 11 条 |
| BR-06 reload 20 → 5 | reload 后 20 条 3.8 秒，每秒最多 6 条 |
| BR-07 长短信 3 段，每个 bind 10 | 3 条 submit_sm，相邻至少 76 ms |
| BR-09a 配了 bind 级，bind0 窗口满 | `[5, 95]`，1.7 秒 |
| BR-09b 没配（对照） | `[5, 95]`，9.3 秒（原有行为） |

