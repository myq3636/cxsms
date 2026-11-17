# SMPP 外发各 bind 发送分布实测报告

> 日期：2026-11-10。用例 `regression/test/it/BindSpreadIT.java`（`manual-suites.txt`，按名字运行：`regression/run.sh BindSpreadIT`）。
> 真实 Client 进程（`MultiSmppClient` + `OutboundStreamConsumer`），连本机假供应商 `FakeSmsc`（真实的 SMPP 服务端，TCP 连接、bind、submit_sm/resp 都是真的）。FakeSmsc 记下每条 submit_sm 到达的时间和所在的 bind；可以让某个 bind 的 submit_sm_resp 晚回。
> 每个场景：1 条连接 4 个 bind，一次往 Stream 里放 N 条 MT。

## 1. 结论

1. **轮询本身是平均的。** 供应商及时回 resp 时，4 个 bind 按条数完全平均（`[500, 500, 500, 500]`），每秒、每 100 ms 也基本一样。账号级限速（`SMSOptionOutgoingThrottlingNum`）也不会让 bind 不平均。
2. **一个 bind 回 resp 慢，它就少拿消息，整个通道也会被拖慢。** 慢 bind 的窗口满后，轮到它的那次整批推迟（默认 100 ms），轮询序号已经跳过它。实测这个 bind 少拿约 10%～25%，而其他 bind 也跟着慢下来（被同一批推迟拖住），不会因此多发。
3. **“put buffer”不是原因。**
   - Stream 外发不走旧的阻塞 `bufferMonitor.put` 循环。窗口用的是 `SmppPendingSubmitManager.tryPut`，不阻塞，满了就返回 `WINDOW_FULL`。
   - TCP 发送队列（`TCPIPConnection` 的 `sendQueue`，每个会话一个发送线程，攒一批写进 8KB 缓冲再 flush）不会改变每个 bind 的条数，只可能让同一个 bind 的几条在同一个 TCP 包里连着到。本机测试的速率下，连着到的情况没有出现（每次 1 条）。速率更高时可能出现几条一组，但条数仍然平均。`TCP_NODELAY` 已开，没有 Nagle 延迟。
4. **线上抓包看到不平均，最可能的原因（按可能性）：**
   - 某些 bind 的 submit_sm_resp 回得慢（供应商那边这个 bind 所在的节点忙），见第 3 节 BS-03、BS-04。
   - 抓包时间窗口太短：每秒只有几十条时，100 ms 窗口里各 bind 差 1～2 条很正常（BS-02 账号级 40 TPS：100 ms 窗口里 84% 有某个 bind 是 0 条，但每秒、总数都平均）。
   - 多个 Client 节点：每个节点有自己的 bind 和自己的账号级限速，Stream 分片在节点之间的分配、各节点读得快慢不同，会让不同节点的 bind 之间不平均（同一节点内部仍平均）。抓包时按 bind 来源 IP / 端口区分节点。
5. **新加的 `SMSOptionOutgoingThrottlingNumPerBind` 顺带改善了第 2 点：** 配了它的通道，轮到的 bind 窗口满时改用同一连接里有空的 bind，不再整批推迟（第 4 节 BR-09a/b）。

## 2. 怎么在线上确认

PDU 日志（`SmppPduLogEnable`，默认开）每条 `[SEND]` / `[RECV]` 带会话名（`连接名_序号`）：

- 每个会话的 submit_sm 条数：`grep '\[SEND\].*submit_sm' | 按会话名计数`。
- 每个会话 resp 的回包延迟：同一会话、同一 sequence 的 `[SEND] submit_sm` 和 `[RECV] submit_sm_resp` 时间差。某个会话明显慢，就是第 1 节第 2 点。
- 指标 `window_full`（按 rssid）一直在涨，说明窗口经常满，通道被最慢的 bind 拖住。

## 3. 实测数据（当前代码，没配 bind 级限速）

| 场景 | 配置 | 各 bind 条数 | 说明 |
|---|---|---|---|
| BS-01 | 不限速，窗口 100，立即回 resp，2000 条 | `[500, 500, 500, 500]` | 每秒各 bind 一样；100 ms 窗口平均差 0.30 |
| BS-02 | 账号级 40 TPS，200 条 | `[51, 52, 48, 49]` | 每秒每个 bind 8～12；100 ms 窗口里常有 bind 是 0 条（每个 bind 每 100 ms 才 1 条） |
| BS-03 | 不限速，窗口 10，bind0 的 resp 晚 200 ms（它最多 50 TPS），1000 条 | `[233, 256, 255, 256]` | bind0 少 9%；4 个 bind 都只有约 40 条/秒，被 bind0 拖住 |
| BS-04 | 账号级 100 TPS，窗口 10，bind0 晚 500 ms（最多 20 TPS），400 条 | `[80, 107, 106, 107]` | bind0 少 25%；总量约 92 TPS，没到 100 |
| BS-05 | 不限速，窗口 10，4 个 bind 都晚 20 ms，2000 条 | `[500, 500, 500, 500]` | 都慢但一样慢：仍然平均 |

所有场景每个 bind 的“连着到”（到达间隔 < 1 ms）都是 1 条一组。

**为什么慢的 bind 少拿、还拖慢别人：** 每条消息先按轮询选 bind（`IndexBalanceStrategy`，序号加一取模），选中的 bind 窗口满就返回 `WINDOW_FULL`，这一批剩下的消息全部推迟 `ClientOutboundWindowFullRetryMs`（默认 100 ms）。这次选择已经把序号推过了 bind0，所以 bind0 少一次；同时其他 bind 在这 100 ms 里也没有消息可发。

## 4. 配了 bind 级限速以后（`regression/test/it/BindRateIT.java`）

| 编号 | 场景 | 结果 |
|---|---|---|
| BR-09a | 每个 bind 50，窗口 5，bind0 不回 resp，100 条 | `[5, 95]`，1.7 秒发完（bind1 按 50 TPS） |
| BR-09b | 没配 bind 级，同上（对照） | `[5, 95]`，9.3 秒发完：每次轮到 bind0 整批推迟 100 ms |

其他 BR 用例见 `docs/SMPP外发bind级限速设计.md` 第 9 节。
