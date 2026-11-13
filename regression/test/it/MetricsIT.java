package it;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.*;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.metrics.MetricsReporter;
import com.king.gmms.metrics.MetricsView;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;
import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;

/**
 * Performance metrics (docs/性能监控埋点评估与设计.md): the collector (buckets, percentiles, largest value, gauges
 * read when reported), the [METRICS-*] JSON lines and [METRICS-ALERT], and over the whole chain, each module its own
 * process: the latencies of a real MT and its DR, the Redis snapshot and A2pCtl metrics.
 */
public class MetricsIT extends ItSupport {
	static final int VPORT = 17808, SPORT = 17809;
	static final String S = "MultiSmppServer1", C = "CoreEngine1", K = "MultiSmppClient1";

	static List<String> lines(String file, String tag) throws Exception {
		List<String> l = new ArrayList<>();
		Path p = Paths.get(file);
		if (Files.exists(p)) for (String s : Files.readAllLines(p)) if (s.contains(tag)) l.add(s);
		return l;
	}
	static JSONObject json(String line) {
		int i = line.indexOf("] {");
		return i < 0 ? null : JSON.parseObject(line.substring(i + 2));
	}
	/** the timers of a module's last window in Redis */
	static JSONObject redisPart(String module, String part) {
		Map<String, String> h = jedis.hgetAll("metrics:" + module.toLowerCase() + ":1");
		return h == null || h.get(part) == null ? new JSONObject() : JSON.parseObject(h.get(part));
	}
	static JSONObject timer(String module, String name) {
		JSONObject t = redisPart(module, "timer");
		String k = module.toLowerCase() + "." + name;
		return t.containsKey(k) ? t.getJSONObject(k) : null;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nMetrics.ReportIntervalSeconds=2\nMetrics.Bind.Enable=true\n"
				+ "Metrics.Alert.VendorRttMs=100\nMetrics.Alert.DbPoolWaiting=1\nRedisStreamMonitor.IntervalSeconds=1\n").getBytes("UTF-8"));
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8")
				+ "\n[CUST8891]\nCCBSSID: 8891\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n"
				+ "IncomingConnection: (ConnName=CUST8891_c1,userName=mc8891,password=pw8891,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n"
				+ "\n[VENDOR81]\nCCBSSID: 81\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\n"
				+ "OutgoingConnection: (ConnName=VENDOR81_c1,userName=u81,password=p,URL=127.0.0.1,Port=" + VPORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/routing/CUST8891_routingInfo.cfg"), "CCBSSID: 8891\nA2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR81)\n".getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(VPORT);
		FakeSmsc.Account v = smsc.account("u81");
		v.submitMessageId = "MIV1";
		v.respDelayMs.put(0, 200L);
		try {
			boot();
			// this process is a CoreEngine1 too: only the node processes write their windows to Redis
			MetricsReporter.getInstance().configureOutput("json", false, new MetricsReporter.Alerts(0, 0, 1, 0, 300));
			// and its windows only when the test asks (reportNow): no scheduled report in between
			MetricsReporter.getInstance().applyConfig(true, 3600, "INFO", false, true, 0.01);
			MetricsCollector mc = MetricsCollector.getInstance();
			check(mc.isEnabled() && MetricsReporter.getInstance().isRunning(), "MU-00 metrics on by default (Metrics.Enable)");

			// ---- the collector ----
			String t1 = MetricsNames.metric("it.timer1");
			for (int i = 0; i < 90; i++) mc.recordMillis(t1, 2);
			for (int i = 0; i < 10; i++) mc.recordMillis(t1, 300);
			Map<String, MetricsCollector.TimerSnapshot> snap = mc.snapshotTimers();
			MetricsCollector.TimerSnapshot ts = snap.get(t1);
			double p50 = MetricsCollector.percentileMs(ts.buckets, 0.5, ts.windowMaxNanos / 1e6);
			double p95 = MetricsCollector.percentileMs(ts.buckets, 0.95, ts.windowMaxNanos / 1e6);
			double p99 = MetricsCollector.percentileMs(ts.buckets, 0.99, ts.windowMaxNanos / 1e6);
			check(ts.count == 100 && ts.windowMaxNanos == 300_000_000L, "MU-01a 100 values, largest 300 ms: " + ts.count + " " + ts.windowMaxNanos);
			check(p50 > 1 && p50 <= 5, "MU-01b p50 in the 1..5 ms bucket: " + p50);
			check(p95 > 250 && p95 <= 300 && p99 > 250 && p99 <= 300, "MU-01c p95, p99 in 250..300 ms (capped by the largest): " + p95 + " " + p99);
			check(mc.snapshotTimers().get(t1).windowMaxNanos == 0, "MU-01d the largest value is per window: 0 after the snapshot");
			String bucketNames = Arrays.toString(MetricsCollector.timerBucketNames());
			check(bucketNames.contains("le_1ms") && bucketNames.contains("le_25ms") && bucketNames.contains("le_5000ms") && bucketNames.contains("gt_5000ms"),
					"MU-01e buckets 1/5/10/25/50/100/250/500/1000/5000 ms: " + bucketNames);

			String c1 = MetricsNames.metric("it.counter1");
			List<Thread> th = new ArrayList<>();
			for (int i = 0; i < 8; i++) { Thread t = new Thread(() -> { for (int j = 0; j < 100000; j++) mc.incrementCounter(c1); }); th.add(t); t.start(); }
			for (Thread t : th) t.join();
			check(mc.getCounter(c1) == 800000, "MU-02 8 threads x 100000 on one counter: " + mc.getCounter(c1));
			check(MetricsNames.metric("it.x", 81) == MetricsNames.metric("it.x", 81)
					&& MetricsNames.build("stream", "in", "submit", "read") == MetricsNames.build("stream", "in", "submit", "read"),
					"MU-03 names are built once (the same instance again)");
			int[] box = { 5 };
			mc.registerGauge(MetricsNames.gauge("it", "supplied"), () -> box[0]);
			long g1 = mc.snapshotGauges().get(MetricsNames.gauge("it", "supplied"));
			box[0] = 7;
			long g2 = mc.snapshotGauges().get(MetricsNames.gauge("it", "supplied"));
			check(g1 == 5 && g2 == 7, "MU-04 a supplied gauge is read when reported: " + g1 + " " + g2);

			GmmsMessage m = mt("MI_SER", 8891, 81, 0);
			m.setInTimeMs(1700000000123L);
			m.setOutTimeMs(1700000000456L);
			String body = SerializableHandler.convertGmmsMessage2RedisMessage(m);
			GmmsMessage back = SerializableHandler.convertRedisMssage2GmmsMessage(body);
			check(back.getInTimeMs() == 1700000000123L && back.getOutTimeMs() == 1700000000456L, "MU-05a fields 60, 61 (entered, vendor accepted) read back");
			GmmsMessage old = SerializableHandler.convertRedisMssage2GmmsMessage(String.join("`", Arrays.copyOf(body.split("`", -1), 60)));
			check(old != null && old.getInTimeMs() == 0 && "MI_SER".equals(old.getMsgID()), "MU-05b an entry written before (no field 60): read, times 0");
			check(new GmmsMessage(m).getInTimeMs() == 1700000000123L, "MU-05c a copy keeps the times");

			// ---- the lines of this process: JSON, a long one split, the alert once per RepeatSeconds ----
			for (int i = 0; i < 150; i++) mc.setGauge(MetricsNames.gauge("it.long_gauge_name_to_fill_the_line_" + i, "value"), i);
			mc.setGauge(MetricsNames.dbGauge("itds", "waiting"), 5);
			String log = home + "log/it.log";
			MetricsReporter.getInstance().reportNow();
			MetricsReporter.getInstance().reportNow();
			List<String> gl = lines(log, "[METRICS-GAUGE]");
			boolean parsed = !gl.isEmpty(), part2 = false;
			for (String l : gl) { JSONObject o = json(l); if (o == null || !o.containsKey("gauge") || !o.containsKey("module")) parsed = false; else if (o.getIntValue("part") == 2) part2 = true; }
			check(parsed, "MU-06a [METRICS-GAUGE] lines are JSON with module, gauge: " + gl.size());
			check(part2, "MU-06b a line over 4 KB is split (part 2)");
			check(gl.stream().allMatch(l -> l.length() < 4400), "MU-06c no line much longer than 4 KB");
			List<String> al = lines(log, "[METRICS-ALERT]");
			long dbAlerts = al.stream().filter(l -> l.contains("db_pool_waiting") && l.contains("itds")).count();
			check(dbAlerts == 1, "MU-07 DB pool waiting 5 > Metrics.Alert.DbPoolWaiting 1: one alert in two windows (repeat after 300 s): " + al);
			check(!al.isEmpty() && json(al.get(0)) != null && json(al.get(0)).getDoubleValue("value") == 5.0, "MU-07b the alert line is JSON");
			MetricsReporter.getInstance().configureOutput("kv", false, new MetricsReporter.Alerts(0, 0, 0, 0, 300));
			MetricsReporter.getInstance().reportNow();
			List<String> kv = lines(log, "[METRICS-GAUGE] module=");
			check(!kv.isEmpty() && kv.get(0).contains(" gauge=\""), "MU-08 Metrics.Format=kv: key=value lines");
			MetricsReporter.getInstance().configureOutput("json", false, new MetricsReporter.Alerts(0, 0, 0, 0, 300));

			// ---- the chain, each module its own process ----
			jedis.set("thcon", "1");
			PathChainIT.launch(C, "core");
			PathChainIT.launch(K, "client");
			PathChainIT.launch(S, "server", String.valueOf(SPORT));
			check(waitFor(() -> v.binds.get() == 1, 20000), "MI-01 the Client bound to the vendor");
			try (R r = new R(SPORT)) {
				r.bind("mc8891", "pw8891");
				for (int i = 0; i < 3; i++) {
					Pdu resp = r.submit(ServerFuncIT.sub("SENDER", "861380000000" + i, 0, 1, 0, ("metrics " + i).getBytes(), null));
					check(resp != null && resp.status == 0, "MI-02 submit " + i + " accepted");
				}
				check(waitFor(() -> v.submits.size() == 3, 20000), "MI-03 the vendor got the 3 MTs");
				Thread.sleep(1500);
				v.deliver("8613800000000", "SENDER", 4, ClientResultIT.receipt("MIV1", "DELIVRD", "000"), null);
				Pdu d = ServerFuncIT.nextDeliver(r, 20000);
				check(d != null, "MI-04 the customer got the DR");
				if (d != null) r.send(0x80000005, d.seq, new byte[] { 0 });
				Thread.sleep(5000);   // two windows of 2 s

				String cl = K.toLowerCase(), co = C.toLowerCase(), se = S.toLowerCase();
				check(jedis.zrange(MetricsReporter.REDIS_INDEX, 0, -1).containsAll(Arrays.asList("metrics:" + cl + ":1", "metrics:" + co + ":1", "metrics:" + se + ":1")),
						"MI-05 each module wrote its window to Redis: " + jedis.zrange(MetricsReporter.REDIS_INDEX, 0, -1));
				check(jedis.ttl("metrics:" + cl + ":1") > 0, "MI-05b the snapshot expires (TTL)");
				// the windows keep moving: a timer is in the window of its events only, so look at the logs too
				String clientLog = PathChainIT.h(K) + "log/it.log", coreLog = PathChainIT.h(C) + "log/it.log", serverLog = PathChainIT.h(S) + "log/it.log";
				JSONObject rtt = lastTimer(clientLog, cl + ".client.vendor.submit_rtt.81");
				check(rtt != null && rtt.getLongValue("count") >= 1 && rtt.getDoubleValue("max") >= 190, "MI-06a Client: vendor submit_sm_resp 200 ms late: " + rtt);
				JSONObject bind = lastTimerPrefix(clientLog, cl + ".client.vendor.submit_rtt.81.");
				check(bind != null, "MI-06b per bind (Metrics.Bind.Enable)");
				check(lastTimer(clientLog, cl + ".e2e.submit") != null, "MI-06c Client: e2e.submit (Server in to submit_sm out)");
				check(lastTimer(clientLog, cl + ".stream.lag.core_to_client") != null, "MI-06d Client: stream.lag.core_to_client");
				check(lastTimer(clientLog, cl + ".consumer.client_out") != null, "MI-06e Client: consumer.client_out (renamed)");
				check(lastTimer(coreLog, co + ".stream.lag.server_to_core") != null && lastTimer(coreLog, co + ".consumer.mt") != null,
						"MI-07a Core: stream.lag.server_to_core, consumer.mt");
				check(lastTimer(coreLog, co + ".core.vendor.dr_latency.81") != null, "MI-07b Core: vendor DR latency of channel 81");
				check(lastTimer(coreLog, co + ".stream.lag.result_to_core") != null && lastTimer(coreLog, co + ".stream.lag.client_to_core") != null,
						"MI-07c Core: stream.lag of the results and the inbound DRs");
				check(lastTimer(coreLog, co + ".redis.op.xadd") != null && lastTimer(coreLog, co + ".redis.op.read") != null, "MI-07d Core: redis.op xadd, read");
				check(lastTimer(serverLog, se + ".e2e.dr") != null && lastTimer(serverLog, se + ".stream.lag.core_to_server") != null,
						"MI-08 Server: e2e.dr, stream.lag.core_to_server");
				List<String> alerts = lines(clientLog, "[METRICS-ALERT]");
				check(alerts.stream().anyMatch(l -> l.contains("vendor_rtt") && l.contains("submit_rtt.81\"")), "MI-09 Client: [METRICS-ALERT] vendor_rtt p95 > 100 ms: " + alerts);
				JSONObject g = redisPart(K, "gauge");
				check(g.containsKey(cl + ".redis.stream.core_to_client.xlen") && g.containsKey(cl + ".consumer.clientoutboundsubmit.pool.active"),
						"MI-10 Client gauges: the stream monitor (on by default), the consumer pool");
				check(redisPart(C, "gauge").containsKey(co + ".redis.stream.server_to_core.streams"), "MI-10b Core: the stream monitor watches its streams");

				PrintStream so = System.out;
				ByteArrayOutputStream buf = new ByteArrayOutputStream();
				System.setOut(new PrintStream(buf, true, "UTF-8"));
				int rc1, rc2;
				try {
					rc1 = MetricsView.print(jedis, null, false);
					rc2 = MetricsView.print(jedis, K, false);
				} finally {
					System.setOut(so);
				}
				String view = buf.toString("UTF-8");
				System.out.println("INFO " + view.replace("\n", "\nINFO "));
				check(rc1 == 0 && view.contains(cl) && view.contains(co) && view.contains(se) && view.contains("rtt p95"), "MI-11a A2pCtl metrics: a line per module");
				check(rc2 == 0 && view.contains("gauges:") && view.contains(cl + ".redis.stream.core_to_client.xlen"),
						"MI-11b A2pCtl metrics " + K + ": every metric of its last window");
			}
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		for (Process p : PathChainIT.nodes) p.destroyForcibly();
		smsc.close();
		exit();
	}

	/** the timer in the last [METRICS-TIMER] line of the log that has it */
	static JSONObject lastTimer(String log, String name) throws Exception {
		JSONObject found = null;
		for (String l : lines(log, "[METRICS-TIMER]")) {
			JSONObject o = json(l);
			if (o != null && o.getJSONObject("timer").containsKey(name)) found = o.getJSONObject("timer").getJSONObject(name);
		}
		return found;
	}
	static JSONObject lastTimerPrefix(String log, String prefix) throws Exception {
		JSONObject found = null;
		for (String l : lines(log, "[METRICS-TIMER]")) {
			JSONObject o = json(l);
			if (o == null) continue;
			for (String k : o.getJSONObject("timer").keySet()) if (k.startsWith(prefix)) found = o.getJSONObject("timer").getJSONObject(k);
		}
		return found;
	}
}
