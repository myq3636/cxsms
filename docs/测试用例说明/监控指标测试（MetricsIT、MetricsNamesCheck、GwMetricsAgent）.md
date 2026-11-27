# 监控指标测试（MetricsIT、MetricsNamesCheck、GwMetricsAgent）

> 日期：2026-11-27。都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 代码 |
> |---|---|---|---|
> | MetricsIT | it-mod:CoreEngine1 | 41 | `regression/test/it/MetricsIT.java` |
> | MetricsNamesCheck | unit | 5 | `regression/test/com/king/gmms/metrics/MetricsNamesCheck.java` |
> | GwMetricsAgent | mvn（采集工具自己的单元测试） | 24 | `tools/gw-metrics-agent/src/test/java/com/king/tools/metricsagent/` |
>
> 设计见《性能监控埋点评估与设计》《网关指标采集工具设计》，指标含义见《性能监控指标说明》，部署见《网关指标采集工具部署配置说明》。

## 1. 测什么

网关每个模块按窗口（`Metrics.ReportIntervalSeconds`）统计指标，在日志里写 `[METRICS-*]` 的 JSON 行，同时把最近一个窗口写进 Redis `metrics:{模块}:{节点}`；超过阈值写 `[METRICS-ALERT]`。采集工具（gw-metrics-agent）读日志，把每个窗口发给 Portal。

| 部分 | 套件 | 内容 |
|---|---|---|
| 统计器 | MetricsIT（MU） | 延迟的分桶、p50/p95/p99、窗口内最大值、并发计数、仪表值在上报时读取 |
| 输出 | MetricsIT（MU） | JSON 行、超过 4 KB 拆行、告警行、`Metrics.Format=kv` |
| 全链路 | MetricsIT（MI） | Server、Core、Client 各一个进程，一条真实的 MT 和它的 DR：每个模块的延迟、流的积压、Redis 操作、仪表、告警、`A2pCtl metrics` |
| 指标名 | MetricsNamesCheck | 消息类型到指标名的对应 |
| 采集工具 | GwMetricsAgent | 读日志（滚动、截断、压缩包）、组装窗口、发给 Portal（签名、重试、批量大小）、每个窗口正好一次 |

## 2. 怎么跑、怎么判定

```
regression/run.sh MetricsIT MetricsNamesCheck GwMetricsAgent
cat regression/.work/results/{MetricsIT,MetricsNamesCheck,GwMetricsAgent}.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。GwMetricsAgent 用 Maven 跑 `tools/gw-metrics-agent` 的单元测试，每个测试方法算一项。
- MetricsIT 的 MU 部分在本进程里直接用统计器和上报器；MI 部分另起 Server、Core、Client 三个进程，供应商是假 SMSC（`FakeSmsc`，回应故意晚 200 毫秒），用 SMPP 客户端以客户身份提交，然后读每个进程的日志和 Redis 里的窗口。
- 注意：`run.sh` 每次启动会清空 `regression/.work/results/`，单独跑一个套件会把上一次全量回归的输出清掉。

## 3. 配置（MetricsIT）

GmmsConfig 追加：

```
Metrics.ReportIntervalSeconds=2
Metrics.Bind.Enable=true
Metrics.Alert.VendorRttMs=100
Metrics.Alert.DbPoolWaiting=1
RedisStreamMonitor.IntervalSeconds=1
```

cm.cfg 追加：

```
[CUST8891]
CCBSSID: 8891
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
IncomingConnection: (ConnName=CUST8891_c1,userName=mc8891,password=pw8891,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)

[VENDOR81]
CCBSSID: 81
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
IsInit: true
ReconnectInterval: 1
EnquireLink: false
BufferTimeout: 30
WindowSize: 100
OutgoingConnection: (ConnName=VENDOR81_c1,userName=u81,password=p,URL=127.0.0.1,Port=…,SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)
```

路由 `CUST8891_routingInfo.cfg`：

```
CCBSSID: 8891
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR81)
```

假 SMSC 的账号 u81：submit_sm_resp 的消息 ID 是 `MIV1`，回应晚 200 毫秒。

## 4. 用例

### 4.1 统计器和输出（MetricsIT MU）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| MU-00 | 默认 | 指标打开（`Metrics.Enable`） |
| MU-01a | 100 个值，最大 300 毫秒 | 计数 100，最大值 300 毫秒 |
| MU-01b | — | p50 落在 1～5 毫秒的桶（3.22） |
| MU-01c | — | p95、p99 在 250～300 毫秒（不超过最大值）：275.0、295.0 |
| MU-01d | 取快照后 | 最大值按窗口算，下一个窗口是 0 |
| MU-01e | 分桶 | 1、5、10、25、50、100、250、500、1000、5000 毫秒，再加大于 5000 |
| MU-02 | 8 个线程各加 10 万次同一个计数 | 800000 |
| MU-03 | 指标名 | 只建一次（再取是同一个对象） |
| MU-04 | 提供的仪表值 | 上报时才读（5、7） |
| MU-05a～c | 消息上的时间字段 60、61（进入、供应商接受） | 能读回；以前写的没有字段 60 的条目也能读，时间为 0；复制消息时保留 |
| MU-06a～c | `[METRICS-GAUGE]` 行 | JSON，带模块和仪表；超过 4 KB 拆成几行（part 2），没有远超 4 KB 的行 |
| MU-07/07b | 数据库连接池等待 5，大于 `Metrics.Alert.DbPoolWaiting` 1 | 两个窗口只告警一次（300 秒后才重复）；告警行 `[METRICS-ALERT]` 是 JSON，`alert` 是 `db_pool_waiting` |
| MU-08 | `Metrics.Format=kv` | key=value 格式的行 |

### 4.2 全链路（MetricsIT MI）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| MI-01 | — | Client 绑定到供应商 |
| MI-02/03 | 客户提交 3 条 | 都接受，供应商收到 3 条 |
| MI-04 | — | 客户收到 DR |
| MI-05/05b | Redis 里的窗口 | 每个模块写了自己的：`metrics:multismppclient1:1`、`metrics:multismppserver1:1`、`metrics:coreengine1:1`；有 TTL |
| MI-06a | Client：供应商的 submit_sm_resp 晚 200 毫秒 | p50 153、p95 200.7、p99 204.9、平均 205、最大 206 毫秒，3 条 |
| MI-06b～e | Client | 按绑定统计（`Metrics.Bind.Enable`）；`e2e.submit`（Server 收到到发出 submit_sm）；`stream.lag.core_to_client`；`consumer.client_out`（改过名的） |
| MI-07a～d | Core | `stream.lag.server_to_core`、`consumer.mt`；通道 81 的供应商 DR 延迟；结果流和供应商 DR 流的积压；Redis 操作 xadd、read |
| MI-08 | Server | `e2e.dr`、`stream.lag.core_to_server` |
| MI-09 | Client 告警 | 供应商回应 p95 大于 100 毫秒：`[METRICS-ALERT]`，`alert` 是 `vendor_rtt` |
| MI-10/10b | 仪表 | Client 有流监控（默认打开）、消费线程池的仪表；Core 的流监控盯着它的流 |
| MI-11a/b | `A2pCtl metrics` | 每个模块一行；`A2pCtl metrics MultiSmppClient1` 列出它最近一个窗口的每个指标 |

### 4.3 指标名（MetricsNamesCheck）

| ID | 消息类型 | 指标名 |
|---|---|---|
| MN-01 | Submit、Delivery（MO） | `submit` |
| MN-02 | Delivery Report | `dr` |
| MN-03 | 各种应答 | 对应的应答名 |
| MN-04 | HTTP Client 的短名字 | 保留，例如 `business.client.dr.sent_to_customer`，不是 `client.other` |
| MN-05 | 不认识的 | `other` |

### 4.4 采集工具（GwMetricsAgent，24 个单元测试）

| 类 | 测试 | 内容 |
|---|---|---|
| AgentEndToEndTest | everyWindowExactlyOnceThroughRolloversRetriesAndKills | 日志滚动、Portal 出错重试、工具被 kill，每个窗口正好发一次 |
| AssemblerTest | parsesTheFourTagsAndTellsKvAndBadLines | 解析 4 种标签，认出 kv 格式和坏行 |
| | alertTakesTheTimeOfItsLogLine | 告警用它那行日志的时间 |
| | windowIsItsLinesMergedWithoutTheModulePrefixCompleteAtTheNextTs | 一个窗口是它的各行合并，去掉模块前缀，下一个时间戳出现时才算完整 |
| | ledgerMovesOnlyPastWhatPortalTook | 记账位置只移到 Portal 收下的地方 |
| | outboxAcksTheHeadAndSaysWhenFull | 待发队列确认队头，满了会说 |
| CommandRunnerTest | dryRunPrintsTheWindowsAndChangesNothing | 试运行只打印窗口，不改任何东西 |
| | commandArgumentsAreTold | 命令参数有说明 |
| | portalUrlIsRequired | 必须给 Portal 地址 |
| SenderTest | idleSendsAHeartbeatAlone | 空闲时单独发心跳 |
| | tooLargeHalvesTheBatch | 太大时批量减半 |
| | portalTakesTheBatchSignedAndThePositionsAreSaved | 带签名发出，Portal 收下后保存位置 |
| | wrongSecretIs401AndNothingIsTaken | 密钥错是 401，什么都不算收下 |
| | serverErrorsBackOffAndRetryUntilTaken | 服务端错误时退避重试，直到收下 |
| TailerTest | rolloverWhileRunningReadsTheRestOfTheOldFile | 运行中滚动，读完旧文件剩下的 |
| | overLongLinesAreSkipped | 超长行跳过 |
| | reusedInodeWithOtherContentIsReadFromItsStart | inode 被重用、内容不同时从头读 |
| | startAtEndSkipsWhatIsThere | 从末尾开始时跳过已有的 |
| | heldPastMaxHoldReadsTheRestFromTheGz | 停得太久，剩下的从 .gz 里读 |
| | recordedFileGoneStartsFromTheRolledFilesOfThatDayAndMarksAGap | 记录的文件没了，从当天滚动的文件开始读并标记缺口 |
| | rolledFilesAreListedInOrderPlainBeforeItsGz | 滚动的文件按顺序，同名的普通文件在 .gz 前 |
| | rolloversWhileStoppedAreReadInOrderFromTheRecordedPosition | 停着时的滚动，从记录的位置按顺序读 |
| | truncatedFileIsReadAgainFromItsStart | 文件被截断时从头读 |
| | appendsAndPartialLines | 追加和不完整的行 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 三组全部通过 |
| 2026-11-27 | 测试服务器单独重跑（取上面的实测输出） | MetricsIT 41、MetricsNamesCheck 5、GwMetricsAgent 24 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| Portal 收到以后的图表、告警页面 | 不在回归里 |
| 生产流量下的指标开销 | 只有 MU-02 的并发计数 |
| 采集工具和真实 Portal 联调 | 单元测试用的是模拟的 Portal |
