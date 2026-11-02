package com.king.gmms.domain;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.king.gmms.GmmsUtility;
import com.king.gmms.RetryPolicyManager;

import sun.reflect.ReflectionFactory;

/**
 * Scratch check (not part of the repo): every CustomerSnapshot index, incremental
 * reload == fresh build, plus the non-connection effects of loadCustomers().
 */
public class SnapshotFullCheck {
	static int failures = 0;
	static File cmFile;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	static void set(Object o, String name, Object v) throws Exception {
		Field f = A2PCustomerManager.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(o, v);
	}

	@SuppressWarnings("unchecked")
	static <T> T get(Object o, String name) throws Exception {
		Field f = o.getClass().getDeclaredField(name);
		f.setAccessible(true);
		return (T) f.get(o);
	}

	static Object call(Object o, String name, Class<?>[] types, Object... args) throws Exception {
		Method m = o.getClass().getDeclaredMethod(name, types);
		m.setAccessible(true);
		return m.invoke(o, args);
	}

	static A2PCustomerManager newManager() throws Exception {
		Constructor<?> c = ReflectionFactory.getReflectionFactory()
				.newConstructorForSerialization(A2PCustomerManager.class, Object.class.getDeclaredConstructor());
		A2PCustomerManager m = (A2PCustomerManager) c.newInstance();
		set(m, "customers", new AtomicReference<CustomerSnapshot>(new CustomerSnapshot().freeze()));
		set(m, "connectionCustomers", new ConcurrentHashMap<Integer, A2PCustomerInfo>());
		set(m, "vpConnectRelayinfo", new ConcurrentHashMap<Integer, Integer>());
		set(m, "routingInfoMap", new ConcurrentHashMap<String, A2PCustomerConfig>());
		set(m, "phonePrefixMap", new ConcurrentHashMap<String, A2PCustomerConfig>());
		set(m, "policyInfo", new ConcurrentHashMap<String, Boolean>());
		set(m, "allowRoutingInfoMap", new ConcurrentHashMap<Integer, List<Integer>>());
		set(m, "connectionNodeMap", Collections.synchronizedMap(new HashMap<String, String>()));
		set(m, "connectionIPMap", Collections.synchronizedMap(new HashMap<String, String>()));
		set(m, "serverInfoMap", Collections.synchronizedMap(new HashMap<String, ArrayList<ConnectionInfo>>()));
		set(m, "serverInfoMapLock", new Object());
		set(m, "a2pIP", "10.0.0.1");
		set(m, "confFile", cmFile.getPath());
		return m;
	}

	static CustomerSnapshot snap(A2PCustomerManager m) throws Exception {
		AtomicReference<CustomerSnapshot> r = get(m, "customers");
		return r.get();
	}

	static void write(String text) throws Exception {
		FileWriter w = new FileWriter(cmFile);
		w.write(text);
		w.close();
	}

	/** a fresh build of the same cm.cfg: buildSnapshot only, writes no global state */
	static CustomerSnapshot freshBuild(String text) throws Exception {
		write(text);
		A2PCustomerManager m = newManager();
		Method b = A2PCustomerManager.class.getDeclaredMethod("buildSnapshot", CustomerSnapshot.class, Map.class, String.class);
		b.setAccessible(true);
		Object changes = b.invoke(m, new CustomerSnapshot().freeze(), A2PCustomerConfig.parse(cmFile), "fresh");
		CustomerSnapshot snapshot = get(changes, "snapshot");
		return snapshot.freeze();
	}

	/** a fresh manager loading the same cm.cfg through the startup path */
	static A2PCustomerManager fresh(String text) throws Exception {
		write(text);
		A2PCustomerManager m = newManager();
		call(m, "initCustomerInfo", new Class<?>[0]);
		return m;
	}

	// ---------- value normalization for comparing two snapshots ----------
	static String norm(Object v) throws Exception {
		if (v == null) return "null";
		if (v instanceof A2PCustomerInfo) {
			A2PCustomerInfo c = (A2PCustomerInfo) v;
			return "Cust(" + c.getSSID() + "," + c.getShortName() + ")";
		}
		if (v instanceof A2PCustomerConfig) {
			A2PCustomerConfig c = (A2PCustomerConfig) v;
			return "Cfg(" + c.getShortName() + "," + new TreeMap<String, String>(c.getLines()) + ")";
		}
		if (v instanceof Pattern) return "P(" + ((Pattern) v).pattern() + ")";
		if (v.getClass().isArray()) {
			List<String> l = new ArrayList<String>();
			for (int i = 0; i < Array.getLength(v); i++) l.add(norm(Array.get(v, i)));
			return l.toString();
		}
		if (v instanceof Map) {
			TreeMap<String, String> t = new TreeMap<String, String>();
			for (Object e : ((Map<?, ?>) v).entrySet()) {
				Map.Entry<?, ?> en = (Map.Entry<?, ?>) e;
				t.put(norm(en.getKey()), norm(en.getValue()));
			}
			return t.toString();
		}
		if (v instanceof Collection) {
			// order inside lists is not relied on by any reader: compare as sorted multisets
			List<String> l = new ArrayList<String>();
			for (Object o : (Collection<?>) v) l.add(norm(o));
			Collections.sort(l);
			return l.toString();
		}
		if (v.getClass().getName().equals("com.king.gmms.ContentScan")) {
			StringBuilder sb = new StringBuilder("Scan(");
			for (Field f : v.getClass().getDeclaredFields()) {
				if (Modifier.isStatic(f.getModifiers())) continue;
				f.setAccessible(true);
				sb.append(f.getName()).append('=').append(f.get(v)).append(',');
			}
			return sb.append(')').toString();
		}
		return String.valueOf(v);
	}

	static List<Field> indexFields() {
		List<Field> l = new ArrayList<Field>();
		for (Field f : CustomerSnapshot.class.getDeclaredFields()) {
			if (Modifier.isStatic(f.getModifiers())) continue;
			String n = f.getName();
			if (n.equals("generation") || n.equals("frozen")) continue;
			f.setAccessible(true);
			l.add(f);
		}
		return l;
	}

	static void compare(String label, CustomerSnapshot inc, CustomerSnapshot fre) throws Exception {
		List<String> diff = new ArrayList<String>();
		for (Field f : indexFields()) {
			if (f.getName().equals("confFileVersion")) continue; // both read the same file, checked separately
			String a = norm(f.get(inc)), b = norm(f.get(fre));
			if (!a.equals(b)) diff.add(f.getName() + "\n      incremental: " + a + "\n      fresh:       " + b);
		}
		check(diff.isEmpty(), label + ": incremental reload == fresh build on all " + (indexFields().size() - 1) + " indexes"
				+ (diff.isEmpty() ? "" : "\n    " + diff));
	}

	// ---------- cm.cfg used by the checks ----------
	static String blk(String name, int ssid, String... lines) {
		StringBuilder sb = new StringBuilder("[").append(name).append("]\nCCBSSID: ").append(ssid).append('\n');
		for (String l : lines) sb.append(l).append('\n');
		return sb.toString();
	}

	static final String A2P = blk("A2P_HK", 1, "A2PPlayerType: A2P", "ChlURL: http://10.0.0.1:8080", "PeeringTcpVersion: V2", "Protocol: Peering2");

	static String opA(int ssid, String... extra) {
		List<String> l = new ArrayList<String>();
		Collections.addAll(l, "A2PPlayerType: operator", "MCCMNC: 460001", "SPID: spA", "ServerID: srvA", "ChlInit: true",
				"ShortCode: code=1101", "ChlMsgSplit: yes", "SplitSuffix: yes", "SMSOptionAsciiMsgSplitLen: 160", "MsgSplitLen: 70",
				"ChlIsSupportDeliveryReport: No", "DeliveryReportMode: 1", "LocalDNSPrefix: a.com,b.com",
				"ContentScan: (Prefix=86,Charset=ASCII,Except=UCS2)", "SmsOptionIsSupportUDH: No", "SmsOptionIsSupport7Bit: yes",
				"SmsOptionIsSupportExpiredDR: yes", "SMSOptionCoreProcessorThrottlingNum: 10", "Protocol: SMPP",
				"SMSOptinIsSupportHttpQueryMessage: MtDr", "NoticePrefix: 1,2", "PhonePrefix: 8613,8615", "CustomerID: 5001",
				"IOSMSSSID: 9001", "Service: GMMS", "RequiredCharsets: (R_RELAY=ChanB_x,Charset=UCS2)",
				"SMSOptionOutgoingThrottlingNum: 50", "SMSOptionMORetryPolicy: 1,2,3");
		Collections.addAll(l, extra);
		return blk("OpA", ssid, l.toArray(new String[0]));
	}

	static final String OTHERS = blk("VOp", 102, "A2PPlayerType: VirtualOperator", "Protocol: SMPP")
			+ blk("ChanB", 103, "A2PPlayerType: channel", "Protocol: HTTP", "SMSOptionCoreProcessorThrottlingNum: 5")
			+ blk("HubC", 104, "A2PPlayerType: 3rdPartyHUB", "Protocol: SMPP", "ShortCode: code=1104")
			+ blk("PartD", 105, "A2PPlayerType: partition", "Protocol: SMPP")
			+ blk("VFKK2G_JP", 106, "A2PPlayerType: operator", "Protocol: SMPP", "MCCMNC: 440010");

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		cmFile = new File(System.getProperty("user.dir"), "cm_check.cfg");
		cmFile.deleteOnExit();
		GmmsUtility.getInstance().setCmTempFile(new File(System.getProperty("user.dir"), "cmtemp/cm_temp.cfg").getPath());

		String v1 = A2P + opA(101) + OTHERS;
		// v2: OpA options changed (DR/UDH/7bit/expired off, other MCCMNC, no ShortCode, other SPID), HubC removed, NewE added
		String opA2 = opA(101).replace("SmsOptionIsSupportUDH: No", "SmsOptionIsSupportUDH: yes")
				.replace("SmsOptionIsSupport7Bit: yes", "SmsOptionIsSupport7Bit: no")
				.replace("SmsOptionIsSupportExpiredDR: yes", "").replace("ChlIsSupportDeliveryReport: No", "")
				.replace("MCCMNC: 460001", "MCCMNC: 460002").replace("ShortCode: code=1101\n", "").replace("SPID: spA", "SPID: spA2")
				.replace("SMSOptionCoreProcessorThrottlingNum: 10\n", "").replace("SMSOptionOutgoingThrottlingNum: 50", "SMSOptionOutgoingThrottlingNum: 80");
		String v2 = A2P + opA2 + OTHERS.replace(blk("HubC", 104, "A2PPlayerType: 3rdPartyHUB", "Protocol: SMPP", "ShortCode: code=1104"), "")
				+ blk("NewE", 107, "A2PPlayerType: channel", "Protocol: SMPP", "SMSOptionCoreProcessorThrottlingNum: 7");
		// v3: OpA moves to CCBSSID 111, ChanB changes role
		String v3 = v2.replace(opA2, opA2.replace("CCBSSID: 101", "CCBSSID: 111"))
				.replace("A2PPlayerType: channel\nProtocol: HTTP", "A2PPlayerType: 3rdPartyHUB\nProtocol: HTTP");
		String[] versions = { v1, v2, v3, v1 };
		String[] labels = { "v1 startup", "v2 modify/remove/add", "v3 ssid change + role change", "v4 back to v1" };

		// ----- coverage: every index has data after v1 -----
		A2PCustomerManager m = fresh(v1);
		CustomerSnapshot s1 = snap(m);
		List<String> empty = new ArrayList<String>();
		for (Field f : indexFields()) {
			Object v = f.get(s1);
			if (v == null || (v instanceof Map && ((Map<?, ?>) v).isEmpty()) || (v instanceof Collection && ((Collection<?>) v).isEmpty()))
				empty.add(f.getName());
		}
		check(empty.isEmpty(), "v1 fills every index (" + indexFields().size() + " fields)" + (empty.isEmpty() ? "" : ", empty: " + empty));
		check(s1.currentA2P == 1 && s1.peeringTcpVersion == 2, "currentA2P / peeringTcpVersion");
		check(s1.complete, "v1 complete");

		// ----- set up routing ALLOW for OpA and the effects checks -----
		Map<String, A2PCustomerConfig> routing = get(m, "routingInfoMap");
		Map<Integer, ?> retry = RetryPolicyManager.getInstance().getSsid_policy_map();
		Map<Integer, ?> limiters = get(com.king.gmms.throttle.DistributedThrottlingManager.getInstance(), "limiters");
		check(retry.containsKey(101) && retry.containsKey(103), "startup publishes retry policies");

		// ----- incremental reloads through the real loadCustomers(), each compared with a fresh build -----
		for (int i = 1; i < versions.length; i++) {
			write(versions[i]);
			if (i == 1) {
				File rf = new File(System.getProperty("user.dir"), "routing_check.cfg");
				FileWriter w = new FileWriter(rf);
				w.write("[OpA]\nCCBSSID: 101\nALLOW: ChanB_x\n");
				w.close();
				routing.putAll(A2PCustomerConfig.parse(rf));
				for (A2PCustomerConfig b : routing.values()) b.setSsid(b.getMandatoryInt("CCBSSID"));
				rf.delete();
			}
			Object ret = call(m, "loadCustomers", new Class<?>[0]);
			check(Integer.valueOf(0).equals(ret), labels[i] + ": loadCustomers returns 0");
			// effects outside the snapshot, checked before anything else touches the singletons
			if (i == 1) {
				check(!retry.containsKey(104) && retry.containsKey(107), "v2: retry policy of removed HubC dropped, NewE added");
				check(Boolean.TRUE.equals(((Map<String, Boolean>) get(m, "policyInfo")).get("101:103")), "v2: routing ALLOW of changed OpA re-applied");
				check(limiters.containsKey(101) && !limiters.containsKey(1), "v2: updateRate for the changed customer only");
			} else if (i == 2) {
				check(retry.containsKey(111) && !retry.containsKey(101), "v3: retry policy moved from 101 to 111");
			} else {
				check(retry.containsKey(101) && !retry.containsKey(111) && retry.containsKey(104), "v4: retry policy back on 101, HubC back");
			}
			CustomerSnapshot f = freshBuild(versions[i]);
			compare(labels[i], snap(m), f);
			check(snap(m).complete, labels[i] + ": complete");
		}
		// the startup path itself against the incremental result of the same file
		compare("startup path vs incremental v4", snap(m), snap(fresh(v1)));

		// ----- the comparison must be able to fail: v1 state against a v2 build -----
		int before = failures;
		compare("NEGATIVE (expected to differ)", snap(m), freshBuild(v2));
		boolean detected = failures == before + 1;
		failures = before;
		check(detected, "comparison detects a real difference");
		write(v1);

		// ----- the same file again: nothing to do -----
		Object again = call(m, "loadCustomers", new Class<?>[0]);
		check(Integer.valueOf(1).equals(again), "unchanged cm.cfg is skipped");

		// ----- OnEvent: 20 concurrent RELOAD_CUSTOMER events -> merged, one publish, no error -----
		set(m, "reloadExecutor", java.util.concurrent.Executors.newSingleThreadExecutor());
		set(m, "customerReloadPending", new java.util.concurrent.atomic.AtomicBoolean(false));
		write(v2);
		long gen = snap(m).generation;
		final A2PCustomerManager mm = m;
		final java.util.concurrent.atomic.AtomicInteger notZero = new java.util.concurrent.atomic.AtomicInteger();
		Thread[] notify = new Thread[20];
		for (int i = 0; i < notify.length; i++) {
			notify[i] = new Thread() {
				public void run() {
					if (mm.OnEvent(new com.king.framework.lifecycle.event.ReloadCustomerEvent()) != 0) notZero.incrementAndGet();
				}
			};
		}
		for (Thread t : notify) t.start();
		for (Thread t : notify) t.join();
		java.util.concurrent.ExecutorService ex = get(m, "reloadExecutor");
		ex.shutdown();
		ex.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
		check(notZero.get() == 0, "OnEvent returns 0 to every Notify Thread at once");
		check(snap(m).generation == gen + 1, "20 concurrent events -> exactly one publish (generation " + gen + " -> " + snap(m).generation + ")");
		compare("after concurrent events", snap(m), freshBuild(v2));

		new File(System.getProperty("user.dir"), "cmtemp/cm_temp.cfg").delete();
		new File(System.getProperty("user.dir"), "cmtemp").delete();
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
