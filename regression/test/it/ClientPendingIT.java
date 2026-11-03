package it;

import java.util.concurrent.atomic.AtomicInteger;

import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.connectionpool.session.SmppSubmitResult;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.smpp.pending.SmppPendingSubmitManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

/** Client pending submit and the vendor's mode: Redis only when something was written there. Real SMPP link as in LogChainClientIT. */
public class ClientPendingIT extends ItSupport {
	static final int PORT = 17781;
	static final int SSID = 41;
	static final AtomicInteger pendingCalls = new AtomicInteger();

	static void setMode(String mode) throws Exception {
		Object c = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(SSID);
		for (Class<?> k = c.getClass(); k != null; k = k.getSuperclass()) {
			try {
				java.lang.reflect.Field f = k.getDeclaredField("smppSubmitPendingMode");
				f.setAccessible(true);
				f.set(c, mode);
				return;
			} catch (NoSuchFieldException e) { }
		}
		throw new IllegalStateException("no smppSubmitPendingMode");
	}
	static boolean noKey(String seq) { return jedis.keys("smpp:pending:submit:*:itcp:" + seq).isEmpty(); }

	/** n submit_sm one after the other, each answered by the vendor; true when all results are written */
	static boolean submits(MultiSmppSession cs, String prefix, int n) throws Exception {
		long before = streamLen("stream:core:results*");
		for (int i = 0; i < n; i++) {
			GmmsMessage m = mt("8888_" + prefix + i, 8888, SSID, 0);
			if (cs.submitClientOutbound(m) != SmppSubmitResult.SENT) return false;
			final long want = before + i + 1;
			if (!waitFor(() -> streamLen("stream:core:results*") >= want, 5000)) return false;
		}
		return true;
	}

	static GmmsMessage submit(String id) { return mt("8888_" + id, 8888, SSID, 0); }

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
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);

		Thread monitor = new Thread(() -> {
			try (Jedis j = new Jedis("127.0.0.1", 16379)) {
				j.monitor(new JedisMonitor() {
					public void onCommand(String command) {
						if (command.contains(" \"EVAL\" ") && command.contains("smpp:pending:submit:")) pendingCalls.incrementAndGet();
					}
				});
			} catch (Exception e) { }
		});
		monitor.setDaemon(true);
		monitor.start();
		Thread.sleep(300);

		SingleNodeCustomerInfo vendor = (SingleNodeCustomerInfo) GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(SSID);
		ConnectionInfo info = vendor.getConnectionMap(false).values().iterator().next();
		info.setUserName("c42");
		info.setPassword("p42");
		MultiSmppSession cs = new MultiSmppSession(info);
		cs.setKeepRunning(true);
		check(cs.createConnection() && cs.connect(), "CP-00 client session bound");
		cs.setStatus(ConnectionStatus.CONNECT);

		// ---- real link ----
		setMode("memory");
		pendingCalls.set(0);
		check(submits(cs, "M", 100), "CP-01a memory: 100 submit_sm answered, 100 results written");
		Thread.sleep(300);
		check(pendingCalls.get() == 0, "CP-01b memory: no Redis Lua on a pending key: " + pendingCalls.get() + " for 100 submits");
		check(jedis.keys("smpp:pending:submit:*").isEmpty(), "CP-01c memory: no pending key in Redis");

		setMode("redis");
		pendingCalls.set(0);
		check(submits(cs, "R", 20), "CP-02a redis: 20 submit_sm answered, 20 results written");
		Thread.sleep(300);
		check(pendingCalls.get() >= 20, "CP-02b redis: pending keys written and consumed in Redis: " + pendingCalls.get() + " Lua for 20 submits");
		check(waitFor(() -> jedis.keys("smpp:pending:submit:*").isEmpty(), 5000), "CP-02c redis: no pending key left");
		setMode("memory");

		// ---- manager: modes, switches, a key left by an earlier process ----
		SmppPendingSubmitManager mgr = new SmppPendingSubmitManager("MultiSmppServer1:" + SSID + ":itcp", 100, 60000L, null);
		setMode("hybrid");
		mgr.put("h", submit("CPH"));
		check(!noKey("h"), "CP-03a hybrid: pending key written");
		mgr.remove("h");
		check(noKey("h"), "CP-03b hybrid: pending key deleted on the response");

		setMode("redis");
		mgr.put("sw", submit("CPSW"));
		setMode("memory");
		check(!noKey("sw"), "CP-04a written in redis mode, vendor switched to memory: key still there");
		mgr.remove("sw");
		check(noKey("sw"), "CP-04b its response still deletes the key");
		mgr.put("sw2", submit("CPSW2"));
		setMode("redis");
		mgr.remove("sw2");
		mgr.put("sw3", submit("CPSW3"));
		check(noKey("sw2") && !noKey("sw3"), "CP-05 switched from memory to redis: removing the old one is harmless, the next one is written");
		mgr.remove("sw3");

		setMode("memory");
		String owner = "MultiSmppServer1:1";
		String key = "smpp:pending:submit:" + owner + ":" + SSID + ":MultiSmppServer1:" + SSID + ":itcp:left";
		RedisClient.getInstance().setPendingMessage(key, SerializableHandler.convertGmmsMessage2RedisMessage(submit("CPLEFT")), 60,
				"zset:smpp:pending:submit:timeout:" + owner + ":" + SSID, System.currentTimeMillis() + 2000L,
				"set:smpp:pending:submit:timeout:zsets:" + owner);
		mgr.put("left", submit("CPNEW"));
		mgr.remove("left");
		check(jedis.exists(key), "CP-06a memory: an unrelated leftover key is not deleted by the response");
		check(waitFor(() -> !jedis.exists(key) && !inStreams("stream:core:results*", "8888_CPLEFT").isEmpty(), 8000), "CP-06b the scanner reports its timeout result, nothing lost");
		mgr.shutdown(false, "it");
		server.stopService();
	}
}
