package it;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.HttpSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.messagequeue.HttpClientStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;
import com.sun.net.httpserver.HttpServer;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/** Client batch 2: HTTP channel rate limit and per-channel timeouts; not part of the repo. */
public class ClientBatch2IT extends ItSupport {
	static final String CM = "conf/cm.cfg";

	/** failure results written per msgId (channels 15/16 have no HTTP session: each processed MT gives exactly one) */
	static Map<String, Integer> resultsByMsg(String prefix) {
		Map<String, Integer> out = new ConcurrentHashMap<String, Integer>();
		for (String k : jedis.keys("stream:core:results*")) {
			for (StreamEntry e : jedis.xrange(k, (StreamEntryID) null, (StreamEntryID) null)) {
				GmmsMessage m = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && m.getMsgID() != null && m.getMsgID().startsWith(prefix)) {
					out.merge(m.getMsgID(), 1, Integer::sum);
				}
			}
		}
		return out;
	}

	static int timeoutMs(Session s, String method) throws Exception {
		Method m = HttpSession.class.getDeclaredMethod(method);
		m.setAccessible(true);
		return (Integer) m.invoke(s);
	}

	/** the session's real HTTP POST (HttpSession.doPost), where the connect/read timeouts are applied */
	static boolean post(Session s) {
		try {
			Method m = HttpSession.class.getDeclaredMethod("doPost", java.net.URL.class, String.class, String.class, boolean.class);
			m.setAccessible(true);
			m.invoke(s, new java.net.URL("http://127.0.0.1:18080/sms"), "a=1", null, false);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	static void setReadTimeout(int seconds) throws Exception {
		File f = new File(home + CM);
		String s = new String(Files.readAllBytes(f.toPath()), "UTF-8");
		s = s.replaceAll("(\\[HTTP14\\][^\\[]*?SMSOptionHttpReadTimeout: )\\d+", "$1" + seconds);
		Files.write(f.toPath(), s.getBytes("UTF-8"));
		Method m = A2PCustomerManager.class.getDeclaredMethod("loadCustomers");
		m.setAccessible(true);
		m.invoke(GmmsUtility.getInstance().getCustomerManager());
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

		// ---- H-T per-channel timeouts ----
		final AtomicInteger hits = new AtomicInteger();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 18080), 0);
		server.createContext("/", ex -> {
			hits.incrementAndGet();
			try { Thread.sleep(4000); } catch (InterruptedException e) { }
			byte[] b = "OK".getBytes();
			ex.sendResponseHeaders(200, b.length);
			ex.getResponseBody().write(b);
			ex.close();
		});
		server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
		server.start();
		Session s14 = CommonHttpClientFactory.getInstance().getSession(14);
		check(s14 instanceof HttpSession, "H-T0 HTTP14 session created: " + (s14 == null ? null : s14.getClass().getSimpleName()));
		check(timeoutMs(s14, "readTimeoutMs") == 1000 && timeoutMs(s14, "connectTimeoutMs") == 30000,
				"H-T1 HTTP14: SMSOptionHttpReadTimeout 1 -> read 1000 ms; connect not set -> module ConnectTimeout 30000 ms");
		Session s16 = CommonHttpClientFactory.getInstance().getSession(16);
		check(s16 == null, "H-T1b HTTP16 is not initialised (no session), used for the rate tests");
		long t0 = System.currentTimeMillis();
		boolean ok = post(s14);
		long took = System.currentTimeMillis() - t0;
		check(hits.get() == 1 && !ok && took < 2500,
				"H-T2 vendor answers after 4 s: request reached it, gave up after " + took + " ms (read timeout 1 s, not the module 60 s)");
		setReadTimeout(6);
		check(timeoutMs(s14, "readTimeoutMs") == 6000, "H-T3 cm.cfg reload: the same session uses the new read timeout (6000 ms) on its next request");
		t0 = System.currentTimeMillis();
		ok = post(s14);
		took = System.currentTimeMillis() - t0;
		check(hits.get() == 2 && ok && took >= 3500, "H-T4 with 6 s the 4 s answer is received (" + took + " ms, ok=" + ok + ")");
		setReadTimeout(1);
		server.stop(0);

		// ---- H-R rate limit of HTTP submits, isolation, no duplicates ----
		props.setProperty("RedisStreamAutoClaimEnable", "true");
		props.setProperty("RedisStreamAutoClaimIdleMs", "1500");
		props.setProperty("RedisStreamAutoClaimIntervalMs", "500");
		props.setProperty("RedisStreamConsumer.HttpClientSubmit.WorkerMaxPoolSize", "2");
		StreamQueueManager q = StreamQueueManager.getInstance();
		final int n = 20;
		for (int i = 0; i < n; i++) {
			q.produceHttpSubmitMessage(mt("CB2R_" + i, 8888, 15, 0));
		}
		long start = System.currentTimeMillis();
		HttpClientStreamConsumer.getInstance().start();
		Thread.sleep(1500);
		for (int i = 0; i < 20; i++) {
			q.produceHttpSubmitMessage(mt("CB2F_" + i, 8888, 16, 0));
		}
		long tf = System.currentTimeMillis();
		boolean fastDone = waitFor(() -> resultsByMsg("CB2F_").size() == 20, 5000);
		long fastMs = System.currentTimeMillis() - tf;
		int slowSoFar = resultsByMsg("CB2R_").size();
		check(fastDone && slowSoFar < n, "H-R3 2 workers, the 2 TPS HTTP channel backlogged: the unlimited channel's 20 MTs done in "
				+ fastMs + " ms while the limited one is at " + slowSoFar + "/" + n);
		boolean blocked = false;
		for (Map.Entry<Thread, StackTraceElement[]> en : Thread.getAllStackTraces().entrySet()) {
			for (StackTraceElement el : en.getValue()) {
				if (el.getClassName().contains("RateLimiter") && "acquire".equals(el.getMethodName())) blocked = true;
			}
		}
		check(!blocked, "H-R4 no thread waits inside RateLimiter.acquire");
		check(waitFor(() -> resultsByMsg("CB2R_").size() == n, 20000), "H-R1a all " + n + " MTs of the 2 TPS HTTP channel processed");
		long slowMs = System.currentTimeMillis() - start;
		int dup = 0;
		for (int v : resultsByMsg("CB2R_").values()) if (v > 1) dup++;
		check(dup == 0, "H-R1b each processed exactly once although they waited longer than the auto-claim idle (1.5 s)");
		check(slowMs >= (n - 3) * 1000L / 2, "H-R1c rate kept: " + n + " at 2 TPS took " + slowMs + " ms");
		check(!jedis.exists("stream:dlq:submit"), "H-R2 put-off entries do not count deliveries: nothing dead-lettered");
		HttpClientStreamConsumer.getInstance().stop();
	}
}
