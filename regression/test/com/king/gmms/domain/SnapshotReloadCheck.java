package com.king.gmms.domain;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import sun.reflect.ReflectionFactory;

/**
 * Scratch check of A2PCustomerManager snapshot build/publish (not part of the repo).
 * The manager is allocated without its constructor (which needs the whole runtime),
 * only buildSnapshot + publish are exercised.
 */
public class SnapshotReloadCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	static A2PCustomerManager newManager() throws Exception {
		Constructor<Object> objCons = Object.class.getDeclaredConstructor();
		Constructor<?> c = ReflectionFactory.getReflectionFactory()
				.newConstructorForSerialization(A2PCustomerManager.class, objCons);
		A2PCustomerManager m = (A2PCustomerManager) c.newInstance();
		set(m, "customers", new AtomicReference<CustomerSnapshot>(new CustomerSnapshot().freeze()));
		set(m, "connectionCustomers", new ConcurrentHashMap<Integer, A2PCustomerInfo>());
		set(m, "vpConnectRelayinfo", new ConcurrentHashMap<Integer, Integer>());
		set(m, "a2pIP", "10.0.0.1");
		return m;
	}

	static void set(Object o, String name, Object v) throws Exception {
		Field f = A2PCustomerManager.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(o, v);
	}

	@SuppressWarnings("unchecked")
	static AtomicReference<CustomerSnapshot> ref(A2PCustomerManager m) throws Exception {
		Field f = A2PCustomerManager.class.getDeclaredField("customers");
		f.setAccessible(true);
		return (AtomicReference<CustomerSnapshot>) f.get(m);
	}

	static Map<String, A2PCustomerConfig> cfg(String text) throws Exception {
		File f = File.createTempFile("cmcfg", ".cfg", new File(System.getProperty("user.dir")));
		f.deleteOnExit();
		FileWriter w = new FileWriter(f);
		w.write(text);
		w.close();
		return A2PCustomerConfig.parse(f);
	}

	/** buildSnapshot + freeze + CAS publish, like loadCustomers minus the side effects */
	static Object reload(A2PCustomerManager m, String text, String version) throws Exception {
		Method build = A2PCustomerManager.class.getDeclaredMethod("buildSnapshot", CustomerSnapshot.class, Map.class,
				String.class);
		build.setAccessible(true);
		CustomerSnapshot old = ref(m).get();
		Object changes = build.invoke(m, old, cfg(text), version);
		Field sf = changes.getClass().getDeclaredField("snapshot");
		sf.setAccessible(true);
		CustomerSnapshot next = (CustomerSnapshot) sf.get(changes);
		check(ref(m).compareAndSet(old, next.freeze()), "publish generation " + next.generation);
		return changes;
	}

	@SuppressWarnings("unchecked")
	static <T> T field(Object o, String name) throws Exception {
		Field f = o.getClass().getDeclaredField(name);
		f.setAccessible(true);
		return (T) f.get(o);
	}

	static String block(String name, int ssid, String extra) {
		return "[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\n"
				+ "SMSOptionCoreProcessorThrottlingNum: 10\nShortCode: code=1" + ssid + "\n" + extra + "\n";
	}

	public static void main(String[] args) throws Exception {
		final A2PCustomerManager m = newManager();
		String v1 = block("CustA", 101, "MCCMNC: 460001") + block("CustB", 102, "") + block("CustC", 103, "");
		reload(m, v1, "v1");
		CustomerSnapshot s1 = ref(m).get();
		check(s1.ssid_cust_map.size() == 3, "v1 loads 3 customers");
		check(m.getSSidbyCustomerNameshort("CustB") == 102, "name map resolves CustB");
		check(m.getSsidByProtocol("smpp").size() == 3, "protocol index has 3 ssids");
		check(m.getCorePriority_ssid_list().size() == 3, "core priority list has 3 ssids");
		check(m.isShortcode("1101"), "shortcode index");
		check(m.getSmsSsidByMncMcc("001_460") == 101, "mccmnc index");
		// what CorePrioritrySender holds while the next reload runs
		List<Integer> heldBySender = m.getCorePriority_ssid_list();
		A2PCustomerInfo a1 = m.getCustomerBySSID(101), b1 = m.getCustomerBySSID(102), c1 = m.getCustomerBySSID(103);
		boolean writeBlocked = false;
		try {
			s1.ssid_cust_map.put(999, a1);
		} catch (UnsupportedOperationException e) {
			writeBlocked = true;
		}
		check(writeBlocked, "published snapshot rejects writes");

		// v2: B changed, C removed, D added
		String v2 = block("CustA", 101, "MCCMNC: 460001") + block("CustB", 102, "ChlQueue: changed")
				+ block("CustD", 104, "");
		Object ch2 = reload(m, v2, "v2");
		List<A2PCustomerInfo[]> changed2 = field(ch2, "changed");
		check(changed2.size() == 3, "v2 changes: B modified, C removed, D added (" + changed2.size() + ")");
		check(m.getCustomerBySSID(101) == a1, "unchanged A keeps the same object");
		check(m.getCustomerBySSID(102) != b1, "changed B is a new object");
		check(m.getCustomerBySSID(103) == null && !m.isShortcode("1103"), "removed C is gone from all indexes");
		check(m.getCustomerBySSID(104) != null, "added D is loaded");
		check(!m.getCorePriority_ssid_list().contains(103) && m.getCorePriority_ssid_list().contains(104)
				&& m.getCorePriority_ssid_list().size() == 3,
				"core priority list drops removed C, adds D " + m.getCorePriority_ssid_list());
		check(ref(m).get().complete && "v2".equals(m.getConfFileVersion()), "v2 snapshot complete");

		// v3: B broken (CCBSSID not a number) and D collides with A on CCBSSID
		A2PCustomerInfo b2 = m.getCustomerBySSID(102), d2 = m.getCustomerBySSID(104);
		String v3 = block("CustA", 101, "MCCMNC: 460001") + block("CustB", 102, "ChlQueue: changed").replace("CCBSSID: 102", "CCBSSID: x")
				+ block("CustD", 101, "");
		Object ch3 = reload(m, v3, "v3");
		List<String> failed3 = field(ch3, "failed");
		check(failed3.contains("CustB") && failed3.contains("CustD"), "v3 reports B and D as failed " + failed3);
		check(m.getCustomerBySSID(102) == b2, "broken B keeps its previous config");
		check(m.getCustomerBySSID(104) == d2, "colliding D keeps its previous config");
		check(m.getCustomerBySSID(101) == a1, "A untouched by the failures");
		check(!ref(m).get().complete, "v3 snapshot marked incomplete, next reload retries");

		// v4: same file again after a fix: retried even though A is unchanged
		reload(m, v2, "v2");
		check(ref(m).get().complete, "fixed config completes the snapshot");

		// v5: B stays but its SMSOptionCoreProcessorThrottlingNum is removed
		String v5 = block("CustA", 101, "MCCMNC: 460001")
				+ block("CustB", 102, "ChlQueue: changed").replace("SMSOptionCoreProcessorThrottlingNum: 10\n", "")
				+ block("CustD", 104, "");
		reload(m, v5, "v5");
		check(m.getCustomerBySSID(102) != null && !m.getCorePriority_ssid_list().contains(102)
				&& m.getCorePriority_ssid_list().size() == 2,
				"option removed: B leaves the core priority list " + m.getCorePriority_ssid_list());
		check(heldBySender.contains(103) && m.getCustomerBySSID(103) == null,
				"a list taken before the reload still has C, its customer lookup returns null");

		// readers during repeated reloads: never null, never CME
		final AtomicBoolean stop = new AtomicBoolean(false);
		final AtomicInteger readerErrors = new AtomicInteger();
		Thread[] readers = new Thread[4];
		for (int i = 0; i < readers.length; i++) {
			readers[i] = new Thread() {
				public void run() {
					while (!stop.get()) {
						try {
							if (m.getCustomerBySSID(101) == null) readerErrors.incrementAndGet();
							for (Integer ssid : m.getCorePriority_ssid_list()) {
								if (ssid == null) readerErrors.incrementAndGet();
							}
							if (m.getSsidByProtocol("smpp") == null) readerErrors.incrementAndGet();
						} catch (Throwable t) {
							readerErrors.incrementAndGet();
						}
					}
				}
			};
			readers[i].start();
		}
		for (int i = 0; i < 300; i++) {
			reload(m, i % 2 == 0 ? v1 : v2, "loop" + i);
		}
		stop.set(true);
		for (Thread t : readers) t.join();
		check(readerErrors.get() == 0, "readers saw no null/CME during 300 reloads (" + readerErrors.get() + ")");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
