package com.king.gmms.connectionpool.session;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

import sun.reflect.ReflectionFactory;

/** Scratch checks for Core optimisation batch 1 (not part of the repo). */
public class Batch1Check {
	static int failures = 0;
	static A2PCustomerManager manager;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	@SuppressWarnings("unchecked")
	static <T> T alloc(Class<T> type) throws Exception {
		Constructor<Object> objCons = Object.class.getDeclaredConstructor();
		Constructor<?> c = ReflectionFactory.getReflectionFactory().newConstructorForSerialization(type, objCons);
		return (T) c.newInstance();
	}

	static void set(Class<?> owner, Object target, String name, Object value) throws Exception {
		Field f = owner.getDeclaredField(name);
		f.setAccessible(true);
		f.set(target, value);
	}

	/** In-memory stand-in for the state Redis connection. */
	public static class StubRedis extends RedisConnection {
		Map<String, String> store;
		List<String> deletes;

		StubRedis() {
			super(null, null, 0, false, null);
		}

		public String getString(String key) {
			return store.get(key);
		}

		public void delPipeline(String key, String hashKey) {
			deletes.add(key + "|" + hashKey);
			store.remove(key);
		}

		public boolean delObject(String key) {
			deletes.add(key);
			return store.remove(key) != null;
		}
	}

	/** Session whose processInnerAck result is chosen by the test. */
	public static class StubSession extends InternalCoreEngineSession {
		Boolean innerAckResult;
		int innerAckCalls;

		public boolean processInnerAck(GmmsMessage msg) {
			innerAckCalls++;
			if (innerAckResult == null) {
				throw new IllegalStateException("simulated failure");
			}
			return innerAckResult;
		}
	}

	public static void main(String[] args) throws Exception {
		shardChecks();
		drLookupChecks();

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	static void shardChecks() {
		Random r = new Random(7);
		int[] totals = { 16, 10, 7, 32 };
		boolean same = true, nonNegative = true;
		for (int n : totals) {
			for (int i = 0; i < 2000000; i++) {
				int h = r.nextInt();
				if (h == Integer.MIN_VALUE) continue;
				if (Math.abs(h) % n != Math.abs(h % n)) same = false;
			}
			if (Math.abs(Integer.MIN_VALUE % n) < 0) nonNegative = false;
		}
		check(same, "new shard formula gives the old shard for every non-MIN hash (n=16,10,7,32)");
		check(nonNegative, "Integer.MIN_VALUE no longer gives a negative shard");
		check(Math.abs(Integer.MIN_VALUE) % 10 < 0, "(control) old formula was negative for MIN_VALUE, n=10");
	}

	static void drLookupChecks() throws Exception {
		Field pf = GmmsUtility.class.getDeclaredField("properties");
		pf.setAccessible(true);
		if (pf.get(GmmsUtility.getInstance()) == null) {
			pf.set(GmmsUtility.getInstance(), new java.util.Properties());
		}
		StubRedis stub = alloc(StubRedis.class);
		stub.store = new HashMap<String, String>();
		stub.deletes = new ArrayList<String>();
		set(RedisClient.class, null, "stateRedis", stub);
		Field initField = RedisClient.class.getDeclaredField("init");
		initField.setAccessible(true);
		RedisClient redisClient = (RedisClient) initField.get(null);

		A2PCustomerManager ctm = alloc(A2PCustomerManager.class);
		Class<?> snap = Class.forName("com.king.gmms.domain.CustomerSnapshot");
		Constructor<?> sc = snap.getDeclaredConstructor();
		sc.setAccessible(true);
		java.lang.reflect.Method freeze = snap.getDeclaredMethod("freeze");
		freeze.setAccessible(true);
		Object empty = freeze.invoke(sc.newInstance());
		set(A2PCustomerManager.class, ctm, "customers", new AtomicReference<Object>(empty));
		set(A2PCustomerManager.class, ctm, "connectionCustomers", new ConcurrentHashMap<Integer, Object>());
		set(A2PCustomerManager.class, ctm, "ssidsInDRStoreMode",
				java.util.Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));

		manager = ctm;
		GmmsUtility.getInstance().setGmmsCustomerManager(ctm);
		StubSession session = alloc(StubSession.class);
		set(InternalCoreEngineSession.class, session, "redis", redisClient);
		set(InternalCoreEngineSession.class, session, "isRedisEnable", true);
		set(InternalCoreEngineSession.class, session, "isStoreDRModeEnable", false);
		set(InternalCoreEngineSession.class, session, "ctm", ctm);
		set(AbstractInternalSession.class, session, "gmmsUtility", GmmsUtility.getInstance());

		Date dateIn = new Date(1793069850000L);
		GmmsMessage original = new GmmsMessage();
		original.setMsgID("8888_1");
		original.setOutMsgID("OUT1");
		original.setOSsID(8888);
		original.setRSsID(8);
		original.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		original.setDateIn(dateIn);
		String body = SerializableHandler.convertGmmsMessage2RedisMessage(original);
		check(body != null, "original message serialises");
		String hashKey = "DR" + dateIn.getTime() / 1000;

		// 1. processing fails: lookup entry must stay for the retry
		stub.store.put("OUT1", body);
		session.innerAckResult = Boolean.FALSE;
		boolean r1 = session.handleInboundDRInternal(dr(GmmsStatus.DELIVERED), null);
		check(!r1 && session.innerAckCalls == 1, "failed processing returns false (not acked)");
		check(stub.deletes.isEmpty() && stub.store.containsKey("OUT1"), "lookup entry kept after failed processing");

		// 2. processing throws: same
		session.innerAckResult = null;
		boolean r2 = session.handleInboundDRInternal(dr(GmmsStatus.DELIVERED), null);
		check(!r2 && stub.deletes.isEmpty() && stub.store.containsKey("OUT1"), "lookup entry kept when processing throws");

		// 3. retry succeeds: entry deleted with its date hash
		session.innerAckResult = Boolean.TRUE;
		boolean r3 = session.handleInboundDRInternal(dr(GmmsStatus.DELIVERED), null);
		check(r3, "retry finds the original again and succeeds");
		check(stub.deletes.size() == 1 && stub.deletes.get(0).equals("OUT1|" + hashKey) && !stub.store.containsKey("OUT1"),
				"lookup entry deleted after success " + stub.deletes);

		// 4. ACCEPT (intermediate) DR: never deletes
		stub.deletes.clear();
		stub.store.put("OUT1", body);
		boolean r4 = session.handleInboundDRInternal(dr(GmmsStatus.ACCEPT), null);
		check(r4 && stub.deletes.isEmpty() && stub.store.containsKey("OUT1"), "ACCEPT DR keeps the lookup entry");
	}

	static GmmsMessage dr(GmmsStatus status) {
		GmmsMessage dr = new GmmsMessage();
		dr.setMsgID("8_2");
		dr.setOutMsgID("OUT1");
		dr.setRSsID(8);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(status);
		return dr;
	}

	static void putMsgNullCustomerCheck() throws Exception {
		// getCustomerBySSID returns null for every ssid with the empty snapshot set above
		MessageProcessorHandler handler = alloc(MessageProcessorHandler.class);
		set(MessageProcessorHandler.class, handler, "gmmsUtility", GmmsUtility.getInstance());
		final List<Runnable> ran = new ArrayList<Runnable>();
		java.util.concurrent.ExecutorService pool = new java.util.concurrent.AbstractExecutorService() {
			public void execute(Runnable command) { ran.add(command); }
			public void shutdown() {}
			public List<Runnable> shutdownNow() { return null; }
			public boolean isShutdown() { return false; }
			public boolean isTerminated() { return false; }
			public boolean awaitTermination(long t, java.util.concurrent.TimeUnit u) { return true; }
		};
		set(MessageProcessorHandler.class, handler, "handlerThreadPool", pool);
		GmmsMessage msg = new GmmsMessage();
		msg.setOSsID(9999);
		msg.setRSsID(9998);
		msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		boolean ok;
		try {
			ok = handler.putMsg(msg);
		} catch (Throwable t) {
			ok = false;
		}
		check(ok && ran.size() == 1, "putMsg with an unknown o-customer queues the message instead of failing");
	}
}
