# Generates the server cases of the metrics and the removed alarm mails PM001..PM002 (regression/routetest/cases/,
# docs/性能监控指标说明.md, docs/性能监控埋点评估与设计.md; case.properties version 2, docs/路由服务器测试说明.md 4.2).
# Run from regression/routetest; rewrites those directories (their result/ too). The account templates and the
# writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SSID["rtvm1"] = 9921
# log4j2 of this machine: metrics.logger of every module -> one log/metricsname.log, rolled by every JVM on its own
# (lines lost): the case gives each module its own log/{module}_metrics.log (conf.sed/log4j2.xml, put back afterwards)
L4SED = r"""s#\${sys:a2p_home}log/metricsname\.log#${sys:a2p_home}log/$${sys:module}_metrics.log#
s#/metrics\.log\.%d#/$${sys:module}_metrics.log.%d#
"""
# the test machine's GmmsConfig has Metrics.LogLevel=DEBUG, PrintGauge, DB.PoolStats and RedisStreamMonitor off
# (log4j2 metrics.logger is info): set back to the documented defaults so the metrics are written
VISIBLE = """# docs/性能监控指标说明.md 8.1 defaults (this machine has other values)
Metrics.Enable=true
Metrics.ReportIntervalSeconds=30
Metrics.Format=json
Metrics.LogLevel=INFO
Metrics.PrintGauge=true
Metrics.Redis.Enable=true
DB.PoolStats.Enable=true
RedisStreamMonitor.Enable=true
RedisStreamMonitor.IntervalSeconds=30
"""
COMMON = dict(refdoc="`features/04_性能监控与去掉邮件/性能监控指标说明.md`", conf_sed={"log4j2.xml": L4SED},
              cm_extra=vendor("RTVM1", 9921), routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTVM1)\n"},
              vendors="rtvm1", vendor_cfg={"rtvm1": {"respDelayMs": "300", "drDelayMs": "2000"}})
M = ",".join(m + "_metrics" for m in ["CoreEngine1", "CoreEngine2", "CoreEngine3", "MultiSmppClient1", "MultiSmppClient2",
                                      "MultiSmppServer1", "MultiSmppServer2", "MsgQueueMonitor1", "MsgQueueMonitor2"])
CTL = 'cd $A2P_HOME && java -Da2p_home=$A2P_HOME/ -cp "Gmms/WEB-INF/lib/*" com.king.gmms.ha.A2pCtl metrics'
NOTE = ("指标每 30 秒一个窗口，MT 发完后等 70 秒（两个窗口）再检查（`mt.settleMs`）。这台机器的 log4j2 把所有模块的 `metrics.logger` 写进同一个 "
        "`log/metricsname.log`，每个 JVM 又各自按日期滚动它，指标行会丢（2026-11-17 第一次运行时 PM001 的指标行就被滚动覆盖了）；用例运行时用 `conf.sed/log4j2.xml` 把它改成每个模块一个 `log/{模块}_metrics.log`，结束后恢复。`[METRICS-ALERT]` 在各模块的主日志。通道 RTVM1（9921）晚 300 ms 回 submit_sm_resp、再过 2 秒回 DR。")

def metric(n, name, field, lo, hi, mod=M):
    return f"check.metric.{n}={mod}|{name}|{field}|{lo}|{hi}"

# ------------------------------------------------------------------------------------------------ PM001
checks = [metric(1, "client.vendor.submit_rtt.9921", "p95", 250, 700),
          metric(2, "core.vendor.dr_latency.9921", "p95", 1500, 4000),
          metric(3, "e2e.submit", "count", 1, 1e9), metric(4, "e2e.dr", "count", 1, 1e9)]
n = 5
for flow in ["server_to_core", "core_to_client", "result_to_core", "client_to_core", "core_to_server"]:
    checks.append(metric(n, "stream.lag." + flow, "count", 1, 1e9)); n += 1
for c in ["server.submit.received", "server.submit.accepted_to_redis", "core.submit.routing_success", "core.submit.routed_to_redis",
          "client.submit.smpp_sent", "client.submit_resp.response_received", "client.dr.received", "core.dr.processed", "server.dr.sent_to_customer"]:
    checks.append(metric(n, "business." + c, "value", 0.01, 1e6)); n += 1
for c in ["consumer.mt", "consumer.result", "consumer.inbound_dr", "consumer.client_out", "consumer.server_dr", "redis.op.xadd"]:
    checks.append(metric(n, c, "count", 1, 1e9)); n += 1
checks.append(metric(n, "jvm.thread.count", "value", 5, 1e6)); n += 1
checks.append(metric(n, "redis.stream.server_to_core.xlen", "value", 0, 1e12)); n += 1
logs = [f"{M}|[METRICS-GAUGE]|.db.pool.", f"{M}|[METRICS-SSID]|.ssid.9921.outsubmit.", f"{M}|[METRICS-SSID]|.ssid.9801.insubmit.",
        f"{M}|[METRICS-TIMER]|client.vendor.submit_rtt.9921.", f'{M}|[METRICS-TPS] {{"module":"coreengine',
        'MultiSmppClient1,MultiSmppClient2|[METRICS-ALERT]|"alert":"vendor_rtt"|submit_rtt.9921']
checks += [f"check.log.{i}={l}" for i, l in enumerate(logs, 1)]
cmds = ["$RTREDIS ZRANGE metrics:index 0 -1 => metrics:coreengine1:",
        "$RTREDIS ZRANGE metrics:index 0 -1 => metrics:multismppserver1:",
        "$RTREDIS ZRANGE metrics:index 0 -1 => metrics:multismppclient",
        "k=$($RTREDIS ZRANGE metrics:index 0 -1 | grep multismppclient | head -1); for x in $($RTREDIS ZRANGE metrics:index 0 -1 | grep multismppclient); do $RTREDIS HGET $x timer; done => submit_rtt.9921",
        "k=$($RTREDIS ZRANGE metrics:index 0 -1 | grep coreengine1 | head -1); $RTREDIS TTL $k | awk '{print ($1>0 && $1<=90) ? \"ttl ok\" : \"ttl \" $1}' => =ttl ok",
        CTL + " => rtt p95", CTL + " => coreengine1", CTL + " --json => \"module\":\"multismppclient",
        CTL + " MultiSmppServer1 => server.submit.received",
        "cd $A2P_HOME/Gmms/WEB-INF/lib && (jar tf a2p.jar 2>/dev/null || unzip -l a2p.jar) | grep -c 'MailSender' => =0",
        "grep -a -c 'MailSender\\|send mail\\|sendMail' $A2P_HOME/log/CoreEngine1.log $A2P_HOME/log/MultiSmppServer1.log $A2P_HOME/log/MultiSmppClient1.log | awk -F: '{s+=$2} END {print s}' => =0"]
checks += [f"check.cmd.{i}={c}" for i, c in enumerate(cmds, 1)]
case(id="PM001", dir="PM001_metrics_output", title="指标输出：供应商响应、DR 延迟、端到端、Stream 排队、业务计数、消费者耗时、瞬时值、按 ssid / bind、告警、Redis 快照、A2pCtl metrics、没有告警邮件",
  refs="指标说明 3～7、设计稿 3.1～3.7、7", props={"mt.settleMs": "70000"}, notes=NOTE, **COMMON,
  gmms_append={"GmmsConfig.properties": VISIBLE + "Metrics.Ssid.Enable=true\nMetrics.Bind.Enable=true\nMetrics.Alert.VendorRttMs=100\nMetrics.Alert.RepeatSeconds=300\n"},
  purpose="发 40 条 MT（同一个号码，一个 Core 处理），通道晚 300 ms 回响应、晚 2 秒回 DR。两个窗口后检查：每种延迟指标的值（供应商响应 p95 在 300 ms 附近、DR 延迟 p95 在 2 秒附近）、各环节的计数和消费者耗时、瞬时值、按 ssid 和按 bind 的指标、`vendor_rtt` 告警（阈值 100 ms）、Redis 快照和它的 TTL、`A2pCtl metrics` 三种输出；a2p.jar 里没有 MailSender，日志里没有发邮件。",
  mts=[("OTP","8613800005001","rtvm1","by main prefix=86 priority=10","DELIVRD","40 条，间隔 50 ms", None, {"count": "40"})],
  checks=checks)

# ------------------------------------------------------------------------------------------------ PM002
case(id="PM002", dir="PM002_reload_format_alert", title="热加载：RELOAD_GMMS_CONFIG 后 Metrics.Format 从 json 换成 kv、告警阈值生效",
  refs="指标说明 6.1、6.2、8（都可以热加载）", props={"mt.settleMs": "70000"}, notes=NOTE + "第 1 阶段没有告警阈值，所以出现的 `vendor_rtt` 告警只能来自第 2 阶段热加载的阈值。", **COMMON,
  gmms_append={"GmmsConfig.properties": VISIBLE},
  phase2={"afterMs": "40000", "gmms": "Metrics.Format=kv\nMetrics.Alert.VendorRttMs=100\n", "reload": "RELOAD_GMMS_CONFIG"},
  purpose="第 1 阶段 json 格式、没有告警阈值，发 10 条；40 秒后（过了一个窗口）在 GmmsConfig 追加 `Metrics.Format=kv`、`Metrics.Alert.VendorRttMs=100`，发 `RELOAD_GMMS_CONFIG`（不重启），再发 20 条。",
  mts=[("OTP","8613800005101","rtvm1","","DELIVRD","热加载前", None, {"count": "10"})],
  mts2=[("OTP","8613800005102","rtvm1","","DELIVRD","热加载后", None, {"count": "20"})],
  checks=[f'check.log.1={M}|[METRICS-TPS] {{"module":"',
          f"check.log.2={M}|[METRICS-TPS] module=|window=",
          f"check.log.3={M}|[METRICS-TIMER] module=|submit_rtt.9921",
          'check.log.4=MultiSmppClient1,MultiSmppClient2|[METRICS-ALERT]|"alert":"vendor_rtt"|submit_rtt.9921'])

exec(_rest.split("# ================================================================================================ write", 1)[1])
