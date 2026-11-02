package it;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.strategy.SessionStrategyInterface;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.ClientSendMarker;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/** Client batch 1: send once, non-blocking rate limit, slow channel isolation; not part of the repo. */
public class ClientBatch1IT extends ItSupport {
	static final String GROUP = "ClientOutboundSubmitGroup";
	static final Map<String, AtomicInteger> sends = new ConcurrentHashMap<String, AtomicInteger>();
	static final Map<Integer, FakeVendor> vendors = new HashMap<Integer, FakeVendor>();

	/** vendor session: records sends; can be disconnected or hang in submit (a sender that stops) */
	static class FakeVendor extends Session {
		volatile boolean connected = true;
		volatile boolean hang = false;

		public boolean submit(GmmsMessage msg) {
			if (hang) {
				try { Thread.sleep(Long.MAX_VALUE); } catch (InterruptedException e) { return false; }
			}
			sends.computeIfAbsent(msg.getMsgID(), k -> new AtomicInteger()).incrementAndGet();
			return true;
		}
		public ConnectionStatus getStatus() { return connected ? ConnectionStatus.CONNECT : ConnectionStatus.DISCONNECT; }
		public void setConnectionManager(ConnectionManager c) {}
		public void stop() {}
		public void initSenders() {}
		public void initReceivers() {}
		public void updateReceivers() {}
		public int enquireLink() { return 0; }
		public boolean connect() { return true; }
		public boolean createConnection() { return true; }
		public ByteBuffer submitAndRec(GmmsMessage msg) { return null; }
		public ConnectionInfo getConnectionInfo() { return null; }
		public long getLastActivity() { return 0; }
		public void setKeepRunning(boolean k) {}
		public boolean isKeepRunning() { return true; }
		public TransactionURI getTransactionURI() { return null; }
		public String getSessionName() { return "fake"; }
		public void setSessionName(String n) {}
		public int getSessionNum() { return 1; }
		public void setStatus(ConnectionStatus s) {}
		public boolean isServer() { return false; }
		public OperatorMessageQueue getOperatorMessageQueue() { return null; }
		public boolean isFakeSession() { return true; }
		public void parse(ByteBuffer b) {}
		public boolean receive(Object o) { return true; }
		public void connectionUnavailable() {}
	}

	static void vendor(final int rssid) throws Exception {
		final FakeVendor v = new FakeVendor();
		vendors.put(rssid, v);
		Field f = MultiSmppClientFactory.class.getDeclaredField("ssid2SessionStrategies");
		f.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<Integer, SessionStrategyInterface> m = (Map<Integer, SessionStrategyInterface>) f.get(MultiSmppClientFactory.getInstance());
		m.put(rssid, msg -> v);
	}

	static int sent(String msgId) {
		AtomicInteger n = sends.get(msgId);
		return n == null ? 0 : n.get();
	}

	static final Map<String, String> STREAM = new ConcurrentHashMap<String, String>();

	static String routed(GmmsMessage m) {
		return STREAM.get(m.getMsgID());
	}

	/** produce like Core and read it back with the Client group (the entry is now pending for us) */
	static GmmsMessage readOne(GmmsMessage m) {
		StreamQueueManager q = StreamQueueManager.getInstance();
		q.produceOutboundMessage(m);
		for (String key : jedis.keys("stream:mt:routed:" + m.getRSsID() + ":*")) {
			q.createGroup(key, GROUP, true);
			Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
			query.put(key, StreamEntryID.UNRECEIVED_ENTRY);
			for (GmmsMessage g : q.consumeBatch(GROUP, "client-outbound-submit-1", 100, query, true)) {
				if (m.getMsgID().equals(g.getMsgID())) {
					STREAM.put(m.getMsgID(), key);
					return g;
				}
			}
		}
		throw new IllegalStateException("not read: " + m.getMsgID());
	}

	static Object process(GmmsMessage m) throws Exception {
		Method pm = OutboundStreamConsumer.class.getDeclaredMethod("processMessage", String.class, GmmsMessage.class);
		pm.setAccessible(true);
		return pm.invoke(OutboundStreamConsumer.getInstance(), routed(m), m);
	}

	static long pending(GmmsMessage m) {
		return jedis.xpending(routed(m), GROUP).getTotal();
	}

	static int results(String msgId) {
		return inStreams("stream:core:results*", msgId).size();
	}

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
		props.setProperty("ClientSendLeaseMs", "800");
		vendor(12);   // unlimited
		vendor(11);   // SMSOptionOutgoingThrottlingNum 2
		vendor(13);   // SMSOptionOutgoingThrottlingNum 5

		// ---- C1-01 marker states, one entry each (processMessage as the consumer runs it) ----
		GmmsMessage a = readOne(mt("CB1A", 8888, 12, 0));
		process(a);
		check(sent("CB1A") == 1 && pending(a) == 0 && !jedis.exists(ClientSendMarker.key(a)),
				"C1-01a normal: sent once, acked, marker deleted with the ack");

		GmmsMessage b = readOne(mt("CB1B", 8888, 12, 0));
		jedis.set(ClientSendMarker.key(b), "D");
		process(b);
		check(sent("CB1B") == 0 && pending(b) == 0 && !jedis.exists(ClientSendMarker.key(b)),
				"C1-01b redelivered after it was sent (ack lost): not sent again, acked");

		GmmsMessage c = readOne(mt("CB1C", 8888, 12, 0));
		jedis.set(ClientSendMarker.key(c), "S|other:x:1|" + System.currentTimeMillis());
		process(c);
		check(sent("CB1C") == 0 && pending(c) == 1, "C1-01c another consumer is sending it: not sent, left pending");
		Thread.sleep(900);
		process(c);
		check(sent("CB1C") == 0 && pending(c) == 0 && results("CB1C") == 0,
				"C1-01d its sender stopped while sending (lease over): not sent again, acked, no failure result (Core outbox reports it)");

		// a sender that stops inside submit, then the entry is redelivered
		final GmmsMessage d = readOne(mt("CB1D", 8888, 12, 0));
		vendors.get(12).hang = true;
		Thread t = new Thread(() -> { try { process(d); } catch (Exception e) { } });
		t.setDaemon(true);
		t.start();
		Thread.sleep(300);
		vendors.get(12).hang = false;
		process(d);
		check(sent("CB1D") == 0 && pending(d) == 1, "C1-01e redelivered while the first sender is still in submit: BUSY, not sent");
		Thread.sleep(900);
		process(d);
		check(sent("CB1D") == 0 && pending(d) == 0, "C1-01f after the lease: not sent (unknown whether the vendor got it), acked");
		t.interrupt();

		// session down
		vendors.get(12).connected = false;
		GmmsMessage e1 = readOne(mt("CB1E", 8888, 12, 0));
		process(e1);
		check(results("CB1E") == 1 && pending(e1) == 0 && !jedis.exists(ClientSendMarker.key(e1)),
				"C1-01g session down: failure result to Core, acked, marker removed");
		GmmsMessage e2 = readOne(mt("CB1F", 8888, 12, 0));
		jedis.set(ClientSendMarker.key(e2), "D");
		process(e2);
		check(results("CB1F") == 0 && pending(e2) == 0,
				"C1-01h session down but the redelivered MT was sent before: no failure result, only acked");
		vendors.get(12).connected = true;

		props.setProperty("ClientSendIdempotency", "false");
		GmmsMessage f = readOne(mt("CB1G", 8888, 12, 0));
		process(f);
		check(sent("CB1G") == 1 && pending(f) == 0 && !jedis.exists(ClientSendMarker.key(f)),
				"C1-01i ClientSendIdempotency=false: sent without a marker");
		props.remove("ClientSendIdempotency");

		// ---- C1-02 slow channel: no duplicate although auto-claim runs fast ----
		props.setProperty("RedisStreamAutoClaimEnable", "true");
		props.setProperty("RedisStreamAutoClaimIdleMs", "1500");
		props.setProperty("RedisStreamAutoClaimIntervalMs", "500");
		props.setProperty("RedisStreamConsumer.ClientOutboundSubmit.WorkerMaxPoolSize", "4");
		StreamQueueManager q = StreamQueueManager.getInstance();
		int n = 20;
		for (int i = 0; i < n; i++) {
			GmmsMessage m = mt("CB2_" + i, 8888, 11, 0);
			q.produceOutboundMessage(m);
		}
		// another slow channel with a backlog on every shard, and a fast one sent while both are busy
		for (int i = 0; i < 60; i++) {
			q.produceOutboundMessage(mt("CB3S_" + i, 8888, 13, 0));
		}
		long t0 = System.currentTimeMillis();
		OutboundStreamConsumer.getInstance().start();
		Thread.sleep(1500);
		for (int i = 0; i < 20; i++) {
			q.produceOutboundMessage(mt("CB3F_" + i, 8888, 12, 0));
		}
		long tf = System.currentTimeMillis();
		boolean fastDone = waitFor(() -> {
			for (int i = 0; i < 20; i++) if (sent("CB3F_" + i) != 1) return false;
			return true;
		}, 5000);
		long fastMs = System.currentTimeMillis() - tf;
		int slowSoFar = 0;
		for (int i = 0; i < n; i++) slowSoFar += sent("CB2_" + i);
		check(fastDone && slowSoFar < n,
				"C1-03 4 workers, two slow channels with backlogs: the fast channel's 20 MTs sent in " + fastMs
						+ " ms while the slow one is still at " + slowSoFar + "/" + n);
		boolean blocked = false;
		for (Map.Entry<Thread, StackTraceElement[]> en : Thread.getAllStackTraces().entrySet()) {
			for (StackTraceElement el : en.getValue()) {
				if (el.getClassName().contains("RateLimiter") && el.getMethodName().contains("acquire")
						&& !el.getMethodName().contains("try")) blocked = true;
			}
		}
		check(!blocked, "C1-04 no thread waits inside RateLimiter.acquire (workers are released)");

		check(waitFor(() -> {
			for (int i = 0; i < n; i++) if (sent("CB2_" + i) < 1) return false;
			return true;
		}, 20000), "C1-02a all " + n + " MTs of the 2 TPS channel sent");
		long slowMs = System.currentTimeMillis() - t0;
		int dup = 0;
		for (int i = 0; i < n; i++) if (sent("CB2_" + i) > 1) dup++;
		check(dup == 0, "C1-02b no MT sent twice although entries waited longer than the auto-claim idle (1.5 s)");
		check(slowMs >= (n - 3) * 1000L / 2, "C1-02c rate kept: " + n + " MTs at 2 TPS took " + slowMs + " ms");
		check(waitFor(() -> {
			for (int i = 0; i < 60; i++) if (sent("CB3S_" + i) != 1) return false;
			return true;
		}, 20000), "C1-02d the 5 TPS channel: all 60 sent exactly once");
		check(!jedis.exists("stream:dlq:submit"), "C1-02e put-off entries do not count deliveries: nothing in the dead letter stream");
		Thread.sleep(500);
		check(jedis.keys("client:sent:*").isEmpty(), "C1-02f no marker left behind");
		long left = 0;
		for (String k : jedis.keys("stream:mt:routed:1*")) left += jedis.xlen(k);
		check(left == 0, "C1-02g routed streams of the channels empty (all acked and deleted)");

		Field pf = OutboundStreamConsumer.class.getDeclaredField("workerPool");
		pf.setAccessible(true);
		java.util.concurrent.ThreadPoolExecutor pool = (java.util.concurrent.ThreadPoolExecutor) pf.get(OutboundStreamConsumer.getInstance());
		check(pool.getCorePoolSize() == 4 && pool.getMaximumPoolSize() == 4
				&& pool.getRejectedExecutionHandler() instanceof java.util.concurrent.ThreadPoolExecutor.AbortPolicy,
				"C1-05 worker pool: core = max = configured 4 (" + pool.getMaximumPoolSize() + "), full pool rejects (no CallerRuns)");
		OutboundStreamConsumer.getInstance().stop();
	}
}
