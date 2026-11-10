package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * SMSOptionOutgoingThrottlingNumPerBind: the rate of each bind as the vendor sees it (arrival time of every
 * submit_sm per bind at the FakeSmsc), with and without the account rate, a bind down, two connections,
 * a reload, a long SMS, a bind whose window is full.
 */
public class BindRateIT extends ItSupport {
	static final int PORT = 17804;
	static StreamQueueManager q;
	static FakeSmsc smsc;
	static int no = 0;
	static Path cmFile;

	static String conn(String name, String user, int sessions) {
		return "(ConnName=" + name + ",userName=" + user + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=" + sessions + ",BindMode=TRX,Version=V34,IsInit=true)";
	}
	static String vendor(String name, int ssid, int window, String opts, String conns) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: " + window + "\n" + opts
				+ "OutgoingConnection: " + conns + "\n";
	}
	static GmmsMessage msg(int rssid) {
		String id = "8888_BR" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		m.setTextContent(id);
		m.setMessageSize(id.length());
		return m;
	}
	static List<String> send(int rssid, int n) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) { GmmsMessage m = msg(rssid); ids.add(m.getMsgID()); q.produceOutboundMessage(m); }
		return ids;
	}
	/** the submits of these messages, once all arrived (or the time is up) */
	static List<FakeSmsc.Submit> got(FakeSmsc.Account a, List<String> ids, long ms) throws Exception {
		Set<String> want = new HashSet<>(ids);
		waitFor(() -> of(a, want).size() >= want.size(), ms);
		return of(a, want);
	}
	static List<FakeSmsc.Submit> of(FakeSmsc.Account a, Set<String> want) throws Exception {
		List<FakeSmsc.Submit> l = new ArrayList<>();
		synchronized (a.submits) { for (FakeSmsc.Submit s : a.submits) if (want.contains(text(s))) l.add(s); }
		return l;
	}
	/** short_message without the UDH of a segment */
	static String text(FakeSmsc.Submit s) throws Exception {
		String t = s.text();
		return (s.esm & 0x40) != 0 && s.sm.length > 0 ? t.substring((s.sm[0] & 0xff) + 1) : t;
	}
	static List<Long> times(List<FakeSmsc.Submit> l, FakeSmsc.Link k) {
		List<Long> t = new ArrayList<>();
		for (FakeSmsc.Submit s : l) if (k == null || s.link == k) t.add(s.at);
		Collections.sort(t);
		return t;
	}
	/** the most submits in any window of this many ms */
	static int maxIn(List<Long> t, long ms) {
		int max = 0;
		for (int i = 0, j = 0; i < t.size(); i++) {
			while (t.get(i) - t.get(j) >= ms * 1_000_000L) j++;
			max = Math.max(max, i - j + 1);
		}
		return max;
	}
	static long spanMs(List<Long> t) { return t.size() < 2 ? 0 : (t.get(t.size() - 1) - t.get(0)) / 1_000_000L; }
	static List<Integer> perLink(FakeSmsc.Account a, List<FakeSmsc.Submit> l) {
		List<Integer> n = new ArrayList<>();
		for (FakeSmsc.Link k : a.links) n.add(times(l, k).size());
		return n;
	}
	static String rates(FakeSmsc.Account a, List<FakeSmsc.Submit> l) {
		StringBuilder b = new StringBuilder();
		for (FakeSmsc.Link k : a.links) {
			List<Long> t = times(l, k);
			if (t.isEmpty()) continue;
			b.append(" [n=").append(t.size()).append(" max/1s=").append(maxIn(t, 1000)).append(" max/100ms=").append(maxIn(t, 100)).append("]");
		}
		List<Long> all = times(l, null);
		return "per bind" + b + "; all n=" + all.size() + " in " + spanMs(all) + " ms, max/1s=" + maxIn(all, 1000);
	}
	static boolean onceEach(List<FakeSmsc.Submit> l, List<String> ids) throws Exception {
		Map<String, Integer> c = new HashMap<>();
		for (FakeSmsc.Submit s : l) c.merge(text(s), 1, Integer::sum);
		for (String id : ids) if (c.getOrDefault(id, 0) != 1) return false;
		return c.size() == ids.size();
	}
	/** results written back for these messages */
	static int results(List<String> ids) {
		Set<String> want = new HashSet<>(ids);
		int n = 0;
		for (String k : jedis.keys("stream:core:results*")) {
			if (!"stream".equals(jedis.type(k))) continue;
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && want.contains(m.getMsgID())) n++;
			}
		}
		return n;
	}
	static void reload() throws Exception {
		jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
		Thread.sleep(2500);
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("BR901", 901, 100, "SMSOptionOutgoingThrottlingNum: 100\n", conn("BR901_c", "u901", 2)));
		cm.append(vendor("BR902", 902, 100, "SMSOptionOutgoingThrottlingNumPerBind: 20\n", conn("BR902_c", "u902", 2)));
		cm.append(vendor("BR903", 903, 100, "SMSOptionOutgoingThrottlingNum: 30\nSMSOptionOutgoingThrottlingNumPerBind: 20\n", conn("BR903_c", "u903", 2)));
		cm.append(vendor("BR904", 904, 100, "SMSOptionOutgoingThrottlingNumPerBind: 20\n", conn("BR904_c", "u904", 2)));
		cm.append(vendor("BR905", 905, 100, "SMSOptionOutgoingThrottlingNumPerBind: 10\n", conn("BR905_a", "u905a", 1) + ";" + conn("BR905_b", "u905b", 1)));
		cm.append(vendor("BR906", 906, 100, "SMSOptionOutgoingThrottlingNumPerBind: 20\n", conn("BR906_c", "u906", 1)));
		cm.append(vendor("BR907", 907, 100, "SMSOptionOutgoingThrottlingNumPerBind: 10\n", conn("BR907_c", "u907", 1)));
		cm.append(vendor("BR908", 908, 5, "SMSOptionOutgoingThrottlingNumPerBind: 50\n", conn("BR908_c", "u908", 2)));
		cm.append(vendor("BR909", 909, 5, "", conn("BR909_c", "u909", 2)));
		cmFile = Paths.get(home + "conf/cm.cfg");
		Files.write(cmFile, (new String(Files.readAllBytes(cmFile), "UTF-8") + cm).getBytes("UTF-8"));
		smsc = new FakeSmsc(PORT);
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();
			final String[][] binds = { { "u901", "2" }, { "u902", "2" }, { "u903", "2" }, { "u904", "2" }, { "u905a", "1" }, { "u905b", "1" }, { "u906", "1" }, { "u907", "1" }, { "u908", "2" }, { "u909", "2" } };
			check(waitFor(() -> { for (String[] b : binds) if (smsc.account(b[0]).binds.get() != Integer.parseInt(b[1])) return false; return true; }, 20000), "BR-00 all bound");
			check(GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(902).getOutgoingThrottlingNumPerBind() == 20
					&& GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(901).getOutgoingThrottlingNumPerBind() == 0, "BR-00b option read: 20, default 0");
			Thread.sleep(500);

			// ---- BR-01 control: account rate only ----
			FakeSmsc.Account a1 = smsc.account("u901");
			List<String> ids = send(901, 300);
			List<FakeSmsc.Submit> l = got(a1, ids, 20000);
			System.out.println("INFO BR-01 " + rates(a1, l));
			List<Integer> pl = perLink(a1, l);
			check(l.size() == 300 && Math.abs(pl.get(0) - pl.get(1)) <= 2, "BR-01 account 100 only: all sent, binds even: " + pl);

			// ---- BR-02 / BR-08 / BR-10: 2 binds at 20 each ----
			FakeSmsc.Account a2 = smsc.account("u902");
			ids = send(902, 200);
			l = got(a2, ids, 20000);
			System.out.println("INFO BR-02 " + rates(a2, l));
			check(onceEach(l, ids), "BR-10 every MT sent exactly once: " + l.size());
			boolean perSec = true, per100 = true;
			for (FakeSmsc.Link k : a2.links) { perSec &= maxIn(times(l, k), 1000) <= 21; per100 &= maxIn(times(l, k), 100) <= 3; }
			long span = spanMs(times(l, null));
			check(perSec, "BR-02a each bind at most 20 (+1) in any second");
			check(span >= 4300 && span <= 6500, "BR-02b both binds together about 40 TPS: 200 in " + span + " ms");
			check(per100, "BR-08 evenly spaced: each bind at most 3 in any 100 ms (no burst)");
			final List<String> sent2 = ids;
			check(waitFor(() -> results(sent2) == sent2.size(), 10000), "BR-10b one result each: " + results(sent2));

			// ---- BR-03: account 30 over binds of 20 ----
			FakeSmsc.Account a3 = smsc.account("u903");
			ids = send(903, 150);
			l = got(a3, ids, 20000);
			System.out.println("INFO BR-03 " + rates(a3, l));
			perSec = true;
			for (FakeSmsc.Link k : a3.links) perSec &= maxIn(times(l, k), 1000) <= 21;
			span = spanMs(times(l, null));
			// Guava lets the account rate start with up to one second stored: 150 at 30 TPS takes 4 to 5 s
			check(l.size() == 150 && perSec && span >= 3800 && span <= 6500, "BR-03 the lower of the two: 150 in " + span + " ms, each bind <= 21/s: " + perSec);

			// ---- BR-04: a bind down ----
			FakeSmsc.Account a4 = smsc.account("u904");
			a4.bindStatus = 0x0D;
			a4.open().get(0).socket.close();
			check(waitFor(() -> a4.open().size() == 1, 5000), "BR-04a one bind down (its rebind refused)");
			Thread.sleep(500);
			ids = send(904, 60);
			l = got(a4, ids, 15000);
			System.out.println("INFO BR-04 one bind " + rates(a4, l));
			span = spanMs(times(l, null));
			check(l.size() == 60 && maxIn(times(l, null), 1000) <= 21 && span >= 2600, "BR-04b the bind left keeps its 20, not 40: 60 in " + span + " ms");
			a4.bindStatus = 0;
			check(waitFor(() -> a4.open().size() == 2, 8000), "BR-04c bound again");
			Thread.sleep(500);
			ids = send(904, 80);
			l = got(a4, ids, 15000);
			System.out.println("INFO BR-04 both " + rates(a4, l));
			span = spanMs(times(l, null));
			check(l.size() == 80 && span <= 2600, "BR-04d both binds again, about 40: 80 in " + span + " ms");

			// ---- BR-05: two connections ----
			FakeSmsc.Account a5a = smsc.account("u905a"), a5b = smsc.account("u905b");
			ids = send(905, 60);
			Set<String> want = new HashSet<>(ids);
			waitFor(() -> of(a5a, want).size() + of(a5b, want).size() >= 60, 15000);
			List<FakeSmsc.Submit> la = of(a5a, want), lb = of(a5b, want);
			System.out.println("INFO BR-05 a " + rates(a5a, la) + " | b " + rates(a5b, lb));
			check(la.size() + lb.size() == 60 && maxIn(times(la, null), 1000) <= 11 && maxIn(times(lb, null), 1000) <= 11,
					"BR-05 each bind of each connection at most 10 (+1) a second: " + la.size() + "+" + lb.size());

			// ---- BR-06: reload 20 -> 5 ----
			FakeSmsc.Account a6 = smsc.account("u906");
			ids = send(906, 60);
			Thread.sleep(1500);
			String now = new String(Files.readAllBytes(cmFile), "UTF-8");
			Files.write(cmFile, now.replace("[BR906]\nCCBSSID: 906\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\nSMSOptionOutgoingThrottlingNumPerBind: 20\n",
					"[BR906]\nCCBSSID: 906\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\nSMSOptionOutgoingThrottlingNumPerBind: 5\n").getBytes("UTF-8"));
			reload();
			check(GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(906).getOutgoingThrottlingNumPerBind() == 5, "BR-06a RELOAD_CUSTOMER: 5 in effect");
			// the reload rebinds the channel: an MT caught in between gets a failure result (Core retries it)
			final List<String> sent6 = ids;
			waitFor(() -> results(sent6) >= sent6.size(), 20000);
			check(waitFor(() -> a6.open().size() == 1, 8000), "BR-06b bound again after the reload");
			ids = send(906, 20);
			l = got(a6, ids, 15000);
			List<Long> t6 = times(l, null);
			System.out.println("INFO BR-06 after the reload " + rates(a6, l));
			check(l.size() == 20 && maxIn(t6, 1000) <= 6 && spanMs(t6) >= 3000, "BR-06c 20 MTs after the reload: at most 5 (+1) a second: " + maxIn(t6, 1000) + ", " + spanMs(t6) + " ms");

			// ---- BR-07: a long SMS, 3 segments: 3 submit_sm, each counted ----
			FakeSmsc.Account a7 = smsc.account("u907");
			ids = new ArrayList<>();
			for (int i = 1; i <= 3; i++) {
				GmmsMessage m = msg(907);
				m.setUdh(new byte[] { 5, 0, 3, 7, 3, (byte) i });
				m.setSarMsgRefNum("07"); m.setSarTotalSegments(3); m.setSarSegmentSeqNum(i); m.setSplitStatus(i);
				ids.add(m.getMsgID());
				q.produceOutboundMessage(m);
			}
			l = got(a7, ids, 10000);
			List<Long> t7 = times(l, null);
			long gap = Long.MAX_VALUE;
			for (int i = 1; i < t7.size(); i++) gap = Math.min(gap, (t7.get(i) - t7.get(i - 1)) / 1_000_000L);
			check(l.size() == 3 && gap >= 70, "BR-07 3 segments at 10 per bind: 3 submit_sm, at least 75 ms apart: " + l.size() + ", " + gap + " ms");

			// ---- BR-09: one bind's window full (its responses never come) ----
			FakeSmsc.Account a8 = smsc.account("u908");
			a8.respDelayMs.put(0, 120000L);
			ids = send(908, 100);
			long t0 = System.currentTimeMillis();
			l = got(a8, ids, 20000);
			long took = System.currentTimeMillis() - t0;
			pl = perLink(a8, l);
			System.out.println("INFO BR-09a " + rates(a8, l) + " in " + took + " ms");
			check(l.size() == 100 && pl.get(0) <= 5, "BR-09a with the bind rate: the other bind takes the rest: " + pl);
			FakeSmsc.Account a9 = smsc.account("u909");
			a9.respDelayMs.put(0, 120000L);
			ids = send(909, 100);
			t0 = System.currentTimeMillis();
			l = got(a9, ids, 30000);
			took = System.currentTimeMillis() - t0;
			pl = perLink(a9, l);
			System.out.println("INFO BR-09b " + rates(a9, l) + " in " + took + " ms");
			check(l.size() == 100 && pl.get(0) <= 5, "BR-09b without it (unchanged): bind0 window full, the rest through bind1 after deferrals: " + pl + " in " + took + " ms");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		smsc.close();
		exit();
	}
}
