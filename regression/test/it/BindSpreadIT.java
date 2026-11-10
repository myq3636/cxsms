package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * How the MTs of one channel spread over its binds, as the vendor sees them: per bind, per 100 ms and per second,
 * and how many submit_sm arrive back to back on one bind (written in one flush). A measurement, run by name.
 * Each scenario: one connection with 4 binds to the FakeSmsc, N MTs put on the stream at once.
 */
public class BindSpreadIT extends ItSupport {
	static final int PORT = 17803;
	static StreamQueueManager q;
	static FakeSmsc smsc;
	static int no = 0;

	static String vendor(String name, int ssid, int window, int sessions, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: " + window + "\n" + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=u" + ssid + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=" + sessions + ",BindMode=TRX,Version=V34,IsInit=true)\n";
	}

	/** puts n MTs on the stream; returns the submits the vendor got, in arrival order */
	static List<FakeSmsc.Submit> burst(FakeSmsc.Account a, int rssid, int n, long waitMs) throws Exception {
		int before = a.submits.size();
		for (int i = 0; i < n; i++) {
			String id = "8888_BS" + (++no);
			GmmsMessage m = mt(id, 8888, rssid, 0);
			m.setTextContent(id);
			m.setMessageSize(id.length());
			q.produceOutboundMessage(m);
		}
		waitFor(() -> a.submits.size() >= before + n, waitMs);
		Thread.sleep(300);
		synchronized (a.submits) { return new ArrayList<>(a.submits.subList(before, a.submits.size())); }
	}

	static void report(String name, FakeSmsc.Account a, List<FakeSmsc.Submit> got, int n) {
		System.out.println("INFO ==== " + name);
		List<FakeSmsc.Link> links = a.links;
		int k = links.size();
		long t0 = got.isEmpty() ? 0 : got.get(0).at;
		long span = got.isEmpty() ? 0 : (got.get(got.size() - 1).at - t0) / 1_000_000;
		int[] total = new int[k];
		int secs = (int) (span / 1000) + 1, tenths = (int) (span / 100) + 1;
		int[][] perSec = new int[k][secs], perTenth = new int[k][tenths];
		for (FakeSmsc.Submit s : got) {
			int b = links.indexOf(s.link);
			long ms = (s.at - t0) / 1_000_000;
			total[b]++; perSec[b][(int) (ms / 1000)]++; perTenth[b][(int) (ms / 100)]++;
		}
		System.out.println("INFO   sent " + n + ", vendor got " + got.size() + " in " + span + " ms; per bind " + Arrays.toString(total));
		for (int b = 0; b < k; b++) System.out.println("INFO   bind" + b + " per second " + Arrays.toString(perSec[b]));
		int show = Math.min(tenths, 20);
		for (int b = 0; b < k; b++) System.out.println("INFO   bind" + b + " per 100ms (first " + show + ") " + Arrays.toString(Arrays.copyOf(perTenth[b], show)));
		// 100 ms windows with traffic: how far the busiest and the quietest bind are apart
		double sumSkew = 0; int windows = 0, idleBind = 0;
		for (int t = 0; t < tenths; t++) {
			int max = 0, min = Integer.MAX_VALUE, sum = 0;
			for (int b = 0; b < k; b++) { max = Math.max(max, perTenth[b][t]); min = Math.min(min, perTenth[b][t]); sum += perTenth[b][t]; }
			if (sum == 0) continue;
			windows++; sumSkew += (max - min) / (sum / (double) k); if (min == 0) idleBind++;
		}
		System.out.println(String.format("INFO   100ms windows %d: mean (max-min)/avg %.2f, with an idle bind %d", windows, windows == 0 ? 0 : sumSkew / windows, idleBind));
		// back to back on one bind: arrival gaps under 1 ms, i.e. one write/flush on the client side
		for (int b = 0; b < k; b++) {
			List<Integer> runs = new ArrayList<>(); long last = -1; int run = 0;
			for (FakeSmsc.Submit s : got) {
				if (s.link != links.get(b)) continue;
				if (last >= 0 && s.at - last < 1_000_000) run++; else { if (run > 0) runs.add(run); run = 1; }
				last = s.at;
			}
			if (run > 0) runs.add(run);
			int max = 0; long sum = 0; for (int r : runs) { max = Math.max(max, r); sum += r; }
			System.out.println(String.format("INFO   bind%d bursts %d, mean %.1f, max %d", b, runs.size(), runs.isEmpty() ? 0 : sum / (double) runs.size(), max));
		}
	}

	static void scenario(String name, int ssid, int n, long waitMs) throws Exception {
		FakeSmsc.Account a = smsc.account("u" + ssid);
		List<FakeSmsc.Submit> got = burst(a, ssid, n, waitMs);
		report(name, a, got, n);
		check(got.size() == n, name + ": all " + n + " sent: " + got.size());
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("BS801", 801, 100, 4, ""));
		cm.append(vendor("BS802", 802, 100, 4, "SMSOptionOutgoingThrottlingNum: 40\n"));
		cm.append(vendor("BS803", 803, 10, 4, ""));
		cm.append(vendor("BS804", 804, 10, 4, "SMSOptionOutgoingThrottlingNum: 100\n"));
		cm.append(vendor("BS805", 805, 10, 4, ""));
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + cm).getBytes("UTF-8"));
		smsc = new FakeSmsc(PORT);
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();
			final int[] ssids = { 801, 802, 803, 804, 805 };
			check(waitFor(() -> { for (int s : ssids) if (smsc.account("u" + s).binds.get() != 4) return false; return true; }, 20000), "BS-00 all bound, 4 binds each");
			Thread.sleep(500);
			scenario("BS-01 no limit, window 100, responses at once", 801, 2000, 30000);
			scenario("BS-02 account 40 TPS, window 100", 802, 200, 15000);
			smsc.account("u803").respDelayMs.put(0, 200L);
			scenario("BS-03 no limit, window 10, bind0 answers 200 ms late (at most 50 TPS)", 803, 1000, 60000);
			smsc.account("u804").respDelayMs.put(0, 500L);
			scenario("BS-04 account 100 TPS, window 10, bind0 answers 500 ms late (at most 20 TPS)", 804, 400, 30000);
			smsc.account("u805").respDelayMs.put(0, 20L);
			smsc.account("u805").respDelayMs.put(1, 20L);
			smsc.account("u805").respDelayMs.put(2, 20L);
			smsc.account("u805").respDelayMs.put(3, 20L);
			scenario("BS-05 no limit, window 10, every bind 20 ms late (at most 500 TPS each)", 805, 2000, 30000);
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		smsc.close();
		exit();
	}
}
