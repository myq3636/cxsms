# PM001 指标输出：供应商响应、DR 延迟、端到端、Stream 排队、业务计数、消费者耗时、瞬时值、按 ssid / bind、告警、Redis 快照、A2pCtl metrics、没有告警邮件

**对应用例清单**：指标说明 3～7、设计稿 3.1～3.7、7（`features/04_性能监控与去掉邮件/性能监控指标说明.md`）

**目的**：发 40 条 MT（同一个号码，一个 Core 处理），通道晚 300 ms 回响应、晚 2 秒回 DR。两个窗口后检查：每种延迟指标的值（供应商响应 p95 在 300 ms 附近、DR 延迟 p95 在 2 秒附近）、各环节的计数和消费者耗时、瞬时值、按 ssid 和按 bind 的指标、`vendor_rtt` 告警（阈值 100 ms）、Redis 快照和它的 TTL、`A2pCtl metrics` 三种输出；a2p.jar 里没有 MailSender，日志里没有发邮件。

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
Metrics.Ssid.Enable=true
Metrics.Bind.Enable=true
Metrics.Alert.VendorRttMs=100
Metrics.Alert.RepeatSeconds=300
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvm1（ssid 9921） | respDelayMs=300，drDelayMs=2000 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800005001 | rtvm1 | `by main prefix=86 priority=10` | DELIVRD | 40 条，间隔 50 ms；条数 `40` |

**用例级检查**（这次运行期间）：

- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…client.vendor.submit_rtt.9921` 的 `p95` 在 250～700 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…core.vendor.dr_latency.9921` 的 `p95` 在 1500～4000 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…e2e.submit` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…e2e.dr` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…stream.lag.server_to_core` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…stream.lag.core_to_client` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…stream.lag.result_to_core` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…stream.lag.client_to_core` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…stream.lag.core_to_server` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.server.submit.received` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.server.submit.accepted_to_redis` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.core.submit.routing_success` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.core.submit.routed_to_redis` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.client.submit.smpp_sent` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.client.submit_resp.response_received` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.client.dr.received` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.core.dr.processed` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…business.server.dr.sent_to_customer` 的 `value` 在 0.01～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…consumer.mt` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…consumer.result` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…consumer.inbound_dr` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…consumer.client_out` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…consumer.server_dr` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…redis.op.xadd` 的 `count` 在 1～1000000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…jvm.thread.count` 的 `value` 在 5～1000000.0 之间
- 指标日志（`CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics`）里本次运行期间有指标 `…redis.stream.server_to_core.xlen` 的 `value` 在 0～1000000000000.0 之间
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-GAUGE]`、`.db.pool.`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-SSID]`、`.ssid.9921.outsubmit.`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-SSID]`、`.ssid.9801.insubmit.`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-TIMER]`、`client.vendor.submit_rtt.9921.`
- `CoreEngine1_metrics,CoreEngine2_metrics,CoreEngine3_metrics,MultiSmppClient1_metrics,MultiSmppClient2_metrics,MultiSmppServer1_metrics,MultiSmppServer2_metrics,MsgQueueMonitor1_metrics,MsgQueueMonitor2_metrics` 日志里有一行同时包含 `[METRICS-TPS] {"module":"coreengine`
- `MultiSmppClient1,MultiSmppClient2` 日志里有一行同时包含 `[METRICS-ALERT]`、`"alert":"vendor_rtt"`、`submit_rtt.9921`
- 命令 `$RTREDIS ZRANGE metrics:index 0 -1` 的输出包含 `metrics:coreengine1:`
- 命令 `$RTREDIS ZRANGE metrics:index 0 -1` 的输出包含 `metrics:multismppserver1:`
- 命令 `$RTREDIS ZRANGE metrics:index 0 -1` 的输出包含 `metrics:multismppclient`
- 命令 `k=$($RTREDIS ZRANGE metrics:index 0 -1 | grep multismppclient | head -1); for x in $($RTREDIS ZRANGE metrics:index 0 -1 | grep multismppclient); do $RTREDIS HGET $x timer; done` 的输出包含 `submit_rtt.9921`
- 命令 `k=$($RTREDIS ZRANGE metrics:index 0 -1 | grep coreengine1 | head -1); $RTREDIS TTL $k | awk '{print ($1>0 && $1<=90) ? "ttl ok" : "ttl " $1}'` 的输出等于 `ttl ok`
- 命令 `cd $A2P_HOME && java -Da2p_home=$A2P_HOME/ -cp "Gmms/WEB-INF/lib/*" com.king.gmms.ha.A2pCtl metrics` 的输出包含 `rtt p95`
- 命令 `cd $A2P_HOME && java -Da2p_home=$A2P_HOME/ -cp "Gmms/WEB-INF/lib/*" com.king.gmms.ha.A2pCtl metrics` 的输出包含 `coreengine1`
- 命令 `cd $A2P_HOME && java -Da2p_home=$A2P_HOME/ -cp "Gmms/WEB-INF/lib/*" com.king.gmms.ha.A2pCtl metrics --json` 的输出包含 `"module":"multismppclient`
- 命令 `cd $A2P_HOME && java -Da2p_home=$A2P_HOME/ -cp "Gmms/WEB-INF/lib/*" com.king.gmms.ha.A2pCtl metrics MultiSmppServer1` 的输出包含 `server.submit.received`
- 命令 `cd $A2P_HOME/Gmms/WEB-INF/lib && (jar tf a2p.jar 2>/dev/null || unzip -l a2p.jar) | grep -c 'MailSender'` 的输出等于 `0`
- 命令 `grep -a -c 'MailSender\|send mail\|sendMail' $A2P_HOME/log/CoreEngine1.log $A2P_HOME/log/MultiSmppServer1.log $A2P_HOME/log/MultiSmppClient1.log | awk -F: '{s+=$2} END {print s}'` 的输出等于 `0`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

指标每 30 秒一个窗口，MT 发完后等 70 秒（两个窗口）再检查（`mt.settleMs`）。这台机器的 log4j2 把所有模块的 `metrics.logger` 写进同一个 `log/metricsname.log`，每个 JVM 又各自按日期滚动它，指标行会丢（2026-11-17 第一次运行时 PM001 的指标行就被滚动覆盖了）；用例运行时用 `conf.sed/log4j2.xml` 把它改成每个模块一个 `log/{模块}_metrics.log`，结束后恢复。`[METRICS-ALERT]` 在各模块的主日志。通道 RTVM1（9921）晚 300 ms 回 submit_sm_resp、再过 2 秒回 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/PM001_metrics_output`（换配置并重启模块）。
