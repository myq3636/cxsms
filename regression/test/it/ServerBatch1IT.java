package it;

import java.lang.reflect.Field;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.king.gmms.connectionpool.session.SmppServerInboundQueueManager;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Server batch 1: shared inbound pool by default, DR window-full put-off, accept/limits/bind timeout; not part of the repo. */
public class ServerBatch1IT extends ItSupport {
	static final int PORT = 17775;

	static SmppTestClient client(int cust, int window) throws Exception {
		SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, window, new SmppTestClient.LatencyRecorder());
		if (!c.bind("c" + cust, "p" + cust)) throw new IllegalStateException("bind failed c" + cust);
		return c;
	}

	static boolean dispatcherRanReceive = false;

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		// a tiny worker pool so it really fills up (S1-03)
		props.setProperty("SmppServerInboundWorkerMax", "2");
		props.setProperty("SmppServerInboundWorkerQueue", "1");
		props.setProperty("SmppServerBindTimeoutMs", "2000");
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);

		// ---- S1-01 shared inbound pool is the default ----
		SmppTestClient a = client(20, 20);
		for (int i = 0; i < 10; i++) a.submit("12345", "8613800000001", "s101-" + i);
		check(waitFor(() -> a.okResp.get() == 10, 5000), "S1-01a 10 submit_sm accepted (ESME_ROK) through the shared pool");
		boolean perSession = false, shared = false;
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			perSession |= t.getName().startsWith("CustReceiver");
			shared |= t.getName().startsWith("SmppServerInboundWorker");
		}
		check(!perSession && shared, "S1-01b no per-connection receiver pool (CustReceiver_*), shared workers in use");
		check(SmppServerInboundQueueManager.getInstance().isEnabled(), "S1-01c SmppServerInboundSharedPoolEnable not configured -> enabled");

		Field wf = SmppServerInboundQueueManager.class.getDeclaredField("workerPool");
		wf.setAccessible(true);
		ThreadPoolExecutor pool = (ThreadPoolExecutor) wf.get(SmppServerInboundQueueManager.getInstance());
		check(pool.getCorePoolSize() == 2 && pool.getMaximumPoolSize() == 2
				&& pool.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy,
				"S1-02 worker pool core = max = WorkerMax (2), full pool rejects (no CallerRuns)");

		// ---- S1-03 pool full: tasks go back to their queue, the dispatcher never runs them ----
		final AtomicInteger samples = new AtomicInteger();
		Thread sampler = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
					if (!e.getKey().getName().startsWith("SmppServerInboundDispatcher")) continue;
					for (StackTraceElement el : e.getValue()) {
						if (el.getMethodName().equals("receive") && el.getClassName().contains("Session")) dispatcherRanReceive = true;
					}
				}
				samples.incrementAndGet();
				try { Thread.sleep(2); } catch (InterruptedException ie) { return; }
			}
		});
		sampler.start();
		Field rf = SmppServerInboundQueueManager.class.getDeclaredField("rejected");
		rf.setAccessible(true);
		long rejectedBefore = ((AtomicLong) rf.get(SmppServerInboundQueueManager.getInstance())).get();
		List<SmppTestClient> flood = new ArrayList<SmppTestClient>();
		for (int cust = 21; cust <= 25; cust++) flood.add(client(cust, 50));
		final int per = 200;
		List<Thread> senders = new ArrayList<Thread>();
		for (final SmppTestClient c : flood) {
			Thread t = new Thread(() -> {
				try { for (int i = 0; i < per; i++) c.submit("12345", "8613800000002", "flood-" + i); } catch (Exception e) { }
			});
			t.start();
			senders.add(t);
		}
		for (Thread t : senders) t.join();
		boolean allOk = waitFor(() -> {
			long n = 0;
			for (SmppTestClient c : flood) n += c.okResp.get();
			return n == 5L * per;
		}, 30000);
		sampler.interrupt();
		long rejectedNow = ((AtomicLong) rf.get(SmppServerInboundQueueManager.getInstance())).get();
		long errs = 0;
		for (SmppTestClient c : flood) errs += c.errResp.get();
		check(allOk && errs == 0, "S1-03a 5 customers x 200 submits with 2 workers: all 1000 accepted, " + errs + " errors");
		check(rejectedNow > rejectedBefore, "S1-03b the pool was full " + (rejectedNow - rejectedBefore) + " times (tasks put back and retried)");
		check(!dispatcherRanReceive && samples.get() > 50,
				"S1-03c dispatcher threads never processed a PDU themselves (" + samples.get() + " stack samples)");
		for (SmppTestClient c : flood) c.close();

		// ---- S1-04 enquire_link is not held behind the customer's submit backlog ----
		props.setProperty("SmppServerInboundMaxInFlightPerSsid", "1");
		final SmppTestClient e = client(26, 100);
		final redis.clients.jedis.Jedis slow = new redis.clients.jedis.Jedis("127.0.0.1", 16379);
		for (int i = 0; i < 30; i++) e.submit("12345", "8613800000003", "backlog-" + i);
		// a Lua script runs alone: Redis serves nobody else for 2 s
		new Thread(() -> { try { slow.eval("local s = redis.call('TIME') local e = tonumber(s[1]) + 2 "
				+ "while tonumber(redis.call('TIME')[1]) < e do end return 1"); } catch (Exception x) { } }).start();
		Thread.sleep(100);
		long enqAt = System.currentTimeMillis();
		e.enquire();
		boolean enqFast = waitFor(() -> e.enquireRespAt > 0, 1500);
		long enqMs = e.enquireRespAt - enqAt;
		long doneSubmits = e.okResp.get();
		check(enqFast && doneSubmits < 30, "S1-04 enquire_link answered in " + enqMs + " ms while " + (30 - doneSubmits)
				+ " submits of the same customer were still queued (Redis paused 2 s, 1 in flight per customer)");
		check(waitFor(() -> e.okResp.get() + e.errResp.get() == 30, 10000),
				"S1-04b the backlog is answered afterwards (" + e.okResp.get() + " ok, " + e.errResp.get()
						+ " ESME_RSYSERR from submits whose Redis call timed out during the 2 s pause)");
		props.remove("SmppServerInboundMaxInFlightPerSsid");
		e.close();

		// ---- S1-05 bind timeout ----
		Socket idle = new Socket("127.0.0.1", PORT);
		idle.setSoTimeout(6000);
		long t0 = System.currentTimeMillis();
		int r;
		try { r = idle.getInputStream().read(); } catch (Exception x) { r = -2; }
		long closedMs = System.currentTimeMillis() - t0;
		check(r == -1 && closedMs >= 1800 && closedMs < 4500,
				"S1-05 connection that never binds closed after " + closedMs + " ms (SmppServerBindTimeoutMs=2000)");
		idle.close();

		// ---- S1-06/07 connection limits ----
		Thread.sleep(500);
		props.setProperty("SmppServerMaxConnectionsPerIp", "3");
		List<SmppTestClient> conns = new ArrayList<SmppTestClient>();
		int liveBefore = com.king.gmms.listener.smppserver.SmppServerConnectionLimiter.getInstance().live("127.0.0.1");
		for (int i = liveBefore; i < 3; i++) conns.add(client(27, 10));
		SmppTestClient extra = new SmppTestClient("127.0.0.1", PORT, 10, null);
		Thread.sleep(500);
		check(extra.isClosedByServer(), "S1-06a 4th connection from the same IP closed at once (SmppServerMaxConnectionsPerIp=3, "
				+ liveBefore + " already open)");
		conns.get(0).close();
		if (conns.size() > 1) conns.remove(0);
		Thread.sleep(1500);
		SmppTestClient again = new SmppTestClient("127.0.0.1", PORT, 10, null);
		check(again.bind("c27", "p27"), "S1-06b after one connection closed, a new one is accepted and binds");
		props.remove("SmppServerMaxConnectionsPerIp");
		props.setProperty("SmppServerMaxConnections", "1");
		SmppTestClient over = new SmppTestClient("127.0.0.1", PORT, 10, null);
		Thread.sleep(500);
		check(over.isClosedByServer(), "S1-07 SmppServerMaxConnections=1 with connections open: new connection closed");
		props.remove("SmppServerMaxConnections");
		for (SmppTestClient c : conns) c.close();
		again.close();
		a.close();

		// ---- S1-08 DR window full: DRs put off, none sent twice ----
		final SmppTestClient d = client(28, 10);
		d.deliverDelayMs = 30;        // a slow customer: the 20-DR window fills up again and again
		StreamQueueManager q = StreamQueueManager.getInstance();
		int n = 120;
		for (int i = 0; i < n; i++) {
			GmmsMessage dr = mt("S108DR" + i, 28, 8, 0);
			dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			dr.setStatus(GmmsStatus.DELIVERED);
			dr.setInMsgID("IN-S108DR" + i);
			q.produceDeliveryReportToQueue(dr, q.getDrPendingQueue(dr));
		}
		check(waitFor(() -> d.delivers.get() >= n, 30000), "S1-08a all " + n + " DRs delivered to a customer whose window keeps filling up ("
				+ d.delivers.get() + " deliver_sm)");
		Thread.sleep(1500);
		int dup = 0;
		for (AtomicInteger v : d.deliverByText.values()) if (v.get() > 1) dup++;
		check(dup == 0 && d.deliverByText.size() == n && d.delivers.get() == n,
				"S1-08b no DR sent twice (" + d.deliverByText.size() + " distinct, " + d.delivers.get() + " deliver_sm)");
		check(!jedis.exists("stream:dlq:report"), "S1-08c put-off DRs not dead-lettered");
		d.close();
		server.stopService();
	}
}
