package it;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.king.gmms.listener.smppserver.MultiSmppServer;

/**
 * SMPP server load test (local, not part of the repo). One JVM per configuration:
 * args: scenario mode workerMax maxInFlight [extra]
 *   scenario = tps   : C customers x K connections, every connection keeps W submits in flight, D seconds
 *            = fair  : 1 heavy customer (10 connections, window 200) + 19 light customers (1 connection, 20 msg/s)
 *            = conns : N idle bound connections, count server threads
 *   mode     = shared | session (SmppServerInboundSharedPoolEnable true / false)
 */
public class ServerPerf extends ItSupport {
	static final int PORT = 17776;
	static String label0 = "";

	public static void main(String[] args) throws Exception {
		String scenario = args[0];
		String mode = args[1];
		int workerMax = Integer.parseInt(args[2]);
		int maxInFlight = Integer.parseInt(args[3]);
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		props.setProperty("SmppServerInboundSharedPoolEnable", "shared".equals(mode) ? "true" : "false");
		props.setProperty("SmppServerInboundWorkerMax", String.valueOf(workerMax));
		props.setProperty("SmppServerInboundMaxInFlightPerSsid", String.valueOf(maxInFlight));
		props.setProperty("RedisStreamMaxLen", "200000");
		String extra = System.getProperty("perf.props", "");
		for (String kv : extra.split(",")) {
			if (kv.contains("=")) props.setProperty(kv.substring(0, kv.indexOf('=')), kv.substring(kv.indexOf('=') + 1));
		}
		if (extra.length() > 0) label0 = " [" + extra + "]";
		Logs.quiet();
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);
		String label = scenario + " mode=" + mode + " workerMax=" + workerMax + " maxInFlightPerSsid=" + maxInFlight + label0;
		if ("tps".equals(scenario)) {
			tps(label, Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
		} else if ("fair".equals(scenario)) {
			fair(label);
		} else if ("dr".equals(scenario)) {
			drs(label, Integer.parseInt(args[4]));
		} else {
			conns(label, Integer.parseInt(args[4]));
		}
		System.exit(0);
	}

	static int serverThreads() {
		int n = 0;
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			String s = t.getName();
			if (s.startsWith("Receiver") || s.startsWith("Sender") || s.startsWith("ServerSessionTrd")
					|| s.startsWith("CustReceiver") || s.startsWith("SmppServerInbound")) n++;
		}
		return n;
	}

	static List<SmppTestClient> connect(int firstCust, int customers, int perCustomer, int window,
			SmppTestClient.LatencyRecorder rec) throws Exception {
		List<SmppTestClient> list = new ArrayList<SmppTestClient>();
		for (int c = 0; c < customers; c++) {
			for (int k = 0; k < perCustomer; k++) {
				SmppTestClient cl = new SmppTestClient("127.0.0.1", PORT, window, rec);
				if (!cl.bind("c" + (firstCust + c), "p" + (firstCust + c))) throw new IllegalStateException("bind");
				list.add(cl);
			}
		}
		return list;
	}

	/** every connection keeps `window` submits in flight for `seconds` (after a 5 s warm-up) */
	static void tps(String label, int customers, int perCustomer, int window) throws Exception {
		final int seconds = 15;
		final SmppTestClient.LatencyRecorder warm = new SmppTestClient.LatencyRecorder();
		final List<SmppTestClient> clients = connect(20, customers, perCustomer, window, warm);
		final AtomicBoolean stop = new AtomicBoolean(false);
		List<Thread> senders = new ArrayList<Thread>();
		for (final SmppTestClient c : clients) {
			Thread t = new Thread(() -> {
				int i = 0;
				try { while (!stop.get()) c.submit("12345", "86138" + (i % 100000), "perf message " + (i++)); } catch (Exception e) { }
			});
			t.setDaemon(true);
			t.start();
			senders.add(t);
		}
		Thread.sleep(5000);
		warm.samples.clear();
		long ok0 = 0, err0 = 0;
		for (SmppTestClient c : clients) { ok0 += c.okResp.get(); err0 += c.errResp.get(); }
		int peakThreads = 0;
		long t0 = System.currentTimeMillis();
		for (int s = 0; s < seconds; s++) {
			Thread.sleep(1000);
			peakThreads = Math.max(peakThreads, serverThreads());
			if (s % 5 == 4) for (String k : jedis.keys("stream:mt:pending*")) jedis.del(k);
		}
		long ms = System.currentTimeMillis() - t0;
		long ok = -ok0, err = -err0;
		for (SmppTestClient c : clients) { ok += c.okResp.get(); err += c.errResp.get(); }
		stop.set(true);
		System.out.println(String.format("RESULT %s connections=%d window=%d | TPS=%d errors=%d | latency p50=%.1fms p95=%.1fms p99=%.1fms | serverThreads=%d",
				label, clients.size(), window, ok * 1000 / ms, err, warm.percentile(50) / 1000.0, warm.percentile(95) / 1000.0,
				warm.percentile(99) / 1000.0, peakThreads));
	}

	/** heavy customer floods; light customers send 20 msg/s each; their latency is what matters */
	static void fair(String label) throws Exception {
		final SmppTestClient.LatencyRecorder heavyRec = new SmppTestClient.LatencyRecorder();
		final SmppTestClient.LatencyRecorder lightRec = new SmppTestClient.LatencyRecorder();
		final List<SmppTestClient> heavy = connect(20, 1, 10, 200, heavyRec);
		final List<SmppTestClient> light = connect(21, 19, 1, 50, lightRec);
		final AtomicBoolean stop = new AtomicBoolean(false);
		for (final SmppTestClient c : heavy) {
			Thread t = new Thread(() -> {
				int i = 0;
				try { while (!stop.get()) c.submit("12345", "86138" + (i % 100000), "heavy " + (i++)); } catch (Exception e) { }
			});
			t.setDaemon(true);
			t.start();
		}
		for (final SmppTestClient c : light) {
			Thread t = new Thread(() -> {
				int i = 0;
				try { while (!stop.get()) { c.submit("12345", "86139" + i, "light " + (i++)); Thread.sleep(50); } } catch (Exception e) { }
			});
			t.setDaemon(true);
			t.start();
		}
		Thread.sleep(5000);
		heavyRec.samples.clear();
		lightRec.samples.clear();
		long h0 = 0;
		for (SmppTestClient c : heavy) h0 += c.okResp.get();
		long t0 = System.currentTimeMillis();
		for (int s = 0; s < 15; s++) {
			Thread.sleep(1000);
			if (s % 5 == 4) for (String k : jedis.keys("stream:mt:pending*")) jedis.del(k);
		}
		long ms = System.currentTimeMillis() - t0;
		long h = -h0, lightErr = 0;
		for (SmppTestClient c : heavy) h += c.okResp.get();
		for (SmppTestClient c : light) lightErr += c.errResp.get();
		stop.set(true);
		System.out.println(String.format("RESULT %s | heavy TPS=%d p99=%.1fms | light (19 x 20/s) p50=%.1fms p99=%.1fms max=%.1fms errors=%d",
				label, h * 1000 / ms, heavyRec.percentile(99) / 1000.0, lightRec.percentile(50) / 1000.0,
				lightRec.percentile(99) / 1000.0, lightRec.percentile(100) / 1000.0, lightErr));
	}

	/** N bound idle connections: server-side threads, and process CPU while idle */
	static void conns(String label, int n) throws Exception {
		int base = serverThreads();
		List<SmppTestClient> list = connect(20, 20, n / 20, 1, null);
		Thread.sleep(5000);
		Runtime rt = Runtime.getRuntime();
		System.gc();
		com.sun.management.OperatingSystemMXBean os =
				(com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
		long cpu0 = os.getProcessCpuTime();
		long t0 = System.nanoTime();
		Thread.sleep(10000);
		double cpuPct = (os.getProcessCpuTime() - cpu0) * 100.0 / (System.nanoTime() - t0);
		System.out.println(String.format("RESULT %s | %d connections: server threads %d (%.1f per connection), heap used %d MB, idle CPU %.1f%% of one core",
				label, list.size(), serverThreads() - base, (serverThreads() - base) / (double) list.size(),
				(rt.totalMemory() - rt.freeMemory()) >> 20, cpuPct));
	}

	/** n DRs to 10 customers (one connection each, answering at once): time until all are answered and acked */
	static void drs(String label, final int n) throws Exception {
		List<SmppTestClient> list = connect(20, 10, 1, 1, null);
		com.king.gmms.messagequeue.StreamQueueManager q = com.king.gmms.messagequeue.StreamQueueManager.getInstance();
		for (int i = 0; i < 200; i++) {                       // warm-up
			com.king.message.gmms.GmmsMessage d = mt("W" + i, 20 + i % 10, 8, 0);
			d.setMessageType(com.king.message.gmms.GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			d.setStatus(com.king.message.gmms.GmmsStatus.DELIVERED);
			q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		}
		final long warmTarget = 200;
		waitFor(() -> { long s = 0; for (SmppTestClient c : list) s += c.delivers.get(); return s >= warmTarget; }, 30000);
		long t0 = System.currentTimeMillis();
		List<Thread> producers = new ArrayList<Thread>();
		for (int p = 0; p < 10; p++) {
			final int part = p;
			Thread t = new Thread(() -> {
				for (int i = part; i < n; i += 10) {
					com.king.message.gmms.GmmsMessage d = mt("P" + i, 20 + i % 10, 8, 0);
					d.setMessageType(com.king.message.gmms.GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					d.setStatus(com.king.message.gmms.GmmsStatus.DELIVERED);
					q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
				}
			});
			t.start();
			producers.add(t);
		}
		for (Thread t : producers) t.join();
		long produceMs = System.currentTimeMillis() - t0;
		final long target = n + 200;
		boolean done = waitFor(() -> {
			long s = 0;
			for (SmppTestClient c : list) s += c.delivers.get();
			if (s < target) return false;
			for (String k : jedis.keys("stream:dr:pending:*")) {
				try { if (jedis.xpending(k, "ServerDrToCustomerGroup_1").getTotal() > 0) return false; } catch (Exception e) { }
			}
			return true;
		}, 120000);
		long ms = System.currentTimeMillis() - t0;
		long total = 0;
		for (SmppTestClient c : list) total += c.delivers.get();
		System.out.println(String.format("RESULT %s | %d DRs to 10 customers: %d ms (%d DR/s, producing took %d ms), delivered %d, all acked=%s",
				label, n, ms, n * 1000L / Math.max(1, ms), produceMs, total - 200, done));
	}

	/** keep the per-message INFO logs out of the measurement */
	static class Logs {
		static void quiet() {
			org.apache.logging.log4j.core.config.Configurator.setAllLevels(org.apache.logging.log4j.LogManager.ROOT_LOGGER_NAME, org.apache.logging.log4j.Level.WARN);
		}
	}
}
