# PM002 热加载：RELOAD_GMMS_CONFIG 后 Metrics.Format 从 json 换成 kv、告警阈值生效

**对应用例清单**：指标说明 6.1、6.2、8（都可以热加载）（`features/04_性能监控与去掉邮件/性能监控指标说明.md`）

**目的**：第 1 阶段 json 格式、没有告警阈值，发 10 条；40 秒后（过了一个窗口）在 GmmsConfig 追加 `Metrics.Format=kv`、`Metrics.Alert.VendorRttMs=100`，发 `RELOAD_GMMS_CONFIG`（不重启），再发 20 条。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTVM1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVM1。

`conf.sed/log4j2.xml`（运行时用这个 sed 脚本改 conf 下的同名文件，结束后恢复）：

```text
s#\${sys:a2p_home}log/metricsname\.log#${sys:a2p_home}log/$${sys:module}_metrics.log#
s#/metrics\.log\.%d#/$${sys:module}_metrics.log.%d#
```

`gmms.append/GmmsConfig.properties`（运行时追加到 Gmms/GmmsConfig.properties（后写的生效），结束后恢复）：

```text
# docs/性能监控指标说明.md 8.1 defaults (this machine has other values)
Metrics.Enable=true
Metrics.ReportIntervalSeconds=30
Metrics.Format=json
Metrics.LogLevel=INFO
Metrics.PrintGauge=true
Metrics.Redis.Enable=true
DB.PoolStats.Enable=true
RedisStreamMonitor.Enable=true
RedisStreamMonitor.IntervalSeconds=30
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvm1（ssid 9921） | respDelayMs=300，drDelayMs=2000 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800005101 | rtvm1 |  | DELIVRD | 热加载前；条数 `10` |

**第 2 阶段**：第 1 阶段的 MT 发完 40 秒后，在 GmmsConfig 后面追加 `phase2/gmms.append/GmmsConfig.properties`（`Metrics.Format=kv`、`Metrics.Alert.VendorRttMs=100`）、发 `RELOAD_GMMS_CONFIG`，再发：

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800005102 | rtvm1 |  | DELIVRD | 热加载后；条数 `20` |

**用例级检查**（这次运行期间）：

- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-TPS] {"module":"`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-TPS] module=`、`window=`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-TIMER] module=`、`submit_rtt.9921`
- `MultiSmppClient1,MultiSmppClient2` 日志里有一行同时包含 `[METRICS-ALERT]`、`"alert":"vendor_rtt"`、`submit_rtt.9921`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

指标每 30 秒一个窗口，MT 发完后等 70 秒（两个窗口）再检查（`mt.settleMs`）。这台机器的 log4j2 把所有模块的 `metrics.logger` 写进同一个 `log/metricsname.log`，每个 JVM 又各自按日期滚动它，指标行会丢（2026-11-17 第一次运行时 PM001 的指标行就被滚动覆盖了）；用例运行时用 `conf.sed/log4j2.xml` 把它改成每个模块一个 `log/{模块}_metrics.log`，结束后恢复。`[METRICS-ALERT]` 在各模块的主日志。通道 RTVM1（9921）晚 300 ms 回 submit_sm_resp、再过 2 秒回 DR。第 1 阶段没有告警阈值，所以出现的 `vendor_rtt` 告警只能来自第 2 阶段热加载的阈值。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/PM002_reload_format_alert`（换配置并重启模块）。
