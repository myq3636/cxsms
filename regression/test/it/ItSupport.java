package it;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/** Shared bootstrap and helpers of the local Core integration tests (not part of the repo). */
public class ItSupport {
	static int failures = 0;
	static String home;
	static Jedis jedis;
	static Properties props;
	static MessageStoreManager msm;

	/** check ids of known-defects.txt: their failure prints KNOWN, their pass prints FIXED; neither fails the run */
	static final java.util.Set<String> KNOWN = knownDefects();

	static java.util.Set<String> knownDefects() {
		java.util.Set<String> ids = new java.util.HashSet<String>();
		String f = System.getProperty("known.defects");
		if (f == null) return ids;
		try {
			for (String l : java.nio.file.Files.readAllLines(java.nio.file.Paths.get(f))) {
				if (l.length() > 0 && !l.startsWith("#")) ids.add(l.split(" ")[0]);
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return ids;
	}

	static void check(boolean ok, String what) {
		boolean known = KNOWN.contains(what.split(" ")[0]);
		System.out.println((ok ? (known ? "FIXED " : "PASS ") : (known ? "KNOWN " : "FAIL ")) + what);
		if (!ok && !known) failures++;
	}

	interface Cond { boolean ok() throws Exception; }

	static boolean waitFor(Cond c, long ms) throws Exception {
		long end = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < end) {
			if (c.ok()) return true;
			Thread.sleep(100);
		}
		return c.ok();
	}

	static void exit() {
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	/** creates the message store used by boot(); a suite can swap it to see the SQL without a database */
	static java.util.function.Supplier<MessageStoreManager> msmFactory = MessageStoreManager::new;

	/** Real Core initialisation on the local Redis; the DB layer is registered without a data source. */
	static void boot() throws Exception {
		home = System.getProperty("a2p_home");
		jedis = new Jedis("127.0.0.1", 16379);
		if (!Boolean.getBoolean("it.keepRedis")) jedis.flushAll();
		GmmsUtility gu = GmmsUtility.getInstance();
		Field lf = GmmsUtility.class.getDeclaredField("log");
		lf.setAccessible(true);
		if (lf.get(null) == null) lf.set(null, com.king.framework.SystemLogger.getSystemLogger(GmmsUtility.class));
		gu.initUtility(home + "Gmms/GmmsConfig.properties");
		gu.initRedisClient("M");
		gu.initCoreShardAssignment();
		gu.initCDRManager();
		msm = msmFactory.get();
		Field dmf = com.king.db.DataControl.class.getDeclaredField("dataManagers");
		dmf.setAccessible(true);
		@SuppressWarnings("unchecked")
		java.util.Hashtable<Object, Object> dms = (java.util.Hashtable<Object, Object>) dmf.get(null);
		dms.put(MessageStoreManager.class.getName(), msm);
		dms.put(com.king.db.DBLockConnection.class.getName(), new com.king.db.DBLockConnection());
		Field inf = com.king.db.DataControl.class.getDeclaredField("initialized");
		inf.setAccessible(true);
		inf.set(null, Boolean.TRUE);
		gu.initDBManager(com.king.db.DatabaseStatus.MASTER_USED);
		Field pf = GmmsUtility.class.getDeclaredField("properties");
		pf.setAccessible(true);
		props = (Properties) pf.get(gu);
	}

	static GmmsMessage mt(String msgId, int ossid, int rssid, int split) {
		GmmsMessage m = new GmmsMessage();
		m.setMsgID(msgId);
		m.setInMsgID("IN-" + msgId);
		m.setOSsID(ossid);
		m.setRSsID(rssid);
		m.setOA2P(56);
		m.setRA2P(56);
		m.setCurrentA2P(56);
		m.setSenderAddress("18667991226");
		m.setRecipientAddress("8613800000000");
		m.setTextContent("Your code is 123456");
		m.setContentType("ASCII");
		m.setMessageSize(19);
		m.setGmmsMsgType("text");
		m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		m.setDeliveryReport(true);
		m.setSplitStatus(split);
		m.setDateIn(new Date());
		m.setTimeStamp(new Date());
		m.setExpiryDate(new Date(System.currentTimeMillis() + 3600 * 1000L));
		m.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
		return m;
	}

	static long streamLen(String pattern) {
		long n = 0;
		for (String k : jedis.keys(pattern)) n += jedis.xlen(k);
		return n;
	}

	/** messages currently in streams matching pattern whose msgId equals msgId */
	static List<GmmsMessage> inStreams(String pattern, String msgId) {
		List<GmmsMessage> found = new ArrayList<GmmsMessage>();
		for (String k : jedis.keys(pattern)) {
			for (StreamEntry e : jedis.xrange(k, (StreamEntryID) null, (StreamEntryID) null)) {
				GmmsMessage m = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && msgId.equals(m.getMsgID())) found.add(m);
			}
		}
		return found;
	}

	/** CDR lines of a type for a msgId, across the CDR files written so far */
	static int cdrCount(String msgId, String type) throws Exception {
		return cdrLines(msgId, type).size();
	}

	static List<String> cdrLines(String msgId, String type) throws Exception {
		List<String> lines = new ArrayList<String>();
		File dir = new File(home + "cdr");
		List<File> files = new ArrayList<File>();
		collect(dir, files);
		for (File f : files) {
			for (String l : Files.readAllLines(f.toPath())) {
				if (l.contains("\"" + msgId + "\"") && l.contains("," + type + ",")) lines.add(l);
			}
		}
		return lines;
	}

	private static void collect(File dir, List<File> out) {
		File[] fs = dir.listFiles();
		if (fs == null) return;
		for (File f : fs) {
			if (f.isDirectory()) collect(f, out);
			else out.add(f);
		}
	}

	static String logLine(String msgId, String text) throws Exception {
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath())) {
			if (l.contains(msgId) && l.contains(text)) return l;
		}
		return null;
	}
}
