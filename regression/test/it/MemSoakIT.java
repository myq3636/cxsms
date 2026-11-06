package it;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Memory soak of a real SMPP Client process against a fake SMSC (not in suites.txt: minutes long, run by hand with
 * SUITE_TIMEOUT=900 regression/run.sh MemSoakIT). Steady MT traffic, then quiet longer than BufferTimeout, twice;
 * jmap -histo:live of the Client every 15 s: what stays alive while it sends, and whether all of it goes once it is quiet.
 */
public class MemSoakIT extends ClientFailoverIT {
	static final int RATE = Integer.getInteger("soak.rate", 3000);
	static final int LOAD_SECONDS = Integer.getInteger("soak.load", 120);
	static final int QUIET_SECONDS = Integer.getInteger("soak.quiet", 75);
	static final String[] CLASSES = { "com.king.message.gmms.GmmsMessage", "SmppPendingSubmitManager$PendingSubmitTimeoutTask",
			"com.king.gmms.smpp.pending.PendingSubmitEntry" };
	static final int THREADS = Integer.getInteger("soak.threads", 12);
	static Process client;
	static volatile long sent = 0;
	static final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong();
	static final java.util.concurrent.atomic.AtomicLong produced = new java.util.concurrent.atomic.AtomicLong();

	/** thread safe: the ids of ClientFailoverIT.send are not */
	static void produce(String prefix, int n) {
		for (int i = 0; i < n; i++) {
			long k = seq.incrementAndGet();
			String id = "8888_" + prefix + k;
			com.king.message.gmms.GmmsMessage m = mt(id, 8888, RSSID, 0);
			m.setTextContent(id);
			m.setMessageSize(id.length());
			m.setRecipientAddress("86138" + String.format("%08d", k % 100000000L));
			q.produceOutboundMessage(m);
			produced.incrementAndGet();
		}
	}

	/** RATE per second for seconds, spread over THREADS producers; samples every 15 s */
	static long maxLiveDuringLoad = 0;
	static void load(int round, FakeSmsc.Account a) throws Exception {
		final long start = System.currentTimeMillis();
		final long end = start + LOAD_SECONDS * 1000L;
		final int perThreadPerTick = Math.max(1, RATE / THREADS / 10);
		List<Thread> ts = new ArrayList<>();
		for (int t = 0; t < THREADS; t++) {
			Thread th = new Thread(() -> {
				long tick = System.currentTimeMillis();
				while (System.currentTimeMillis() < end) {
					produce("R" + round + "_", perThreadPerTick);
					tick += 100;
					long wait = tick - System.currentTimeMillis();
					if (wait > 0) try { Thread.sleep(wait); } catch (InterruptedException e) { return; }
				}
			});
			th.start();
			ts.add(th);
		}
		long before = produced.get();
		long nextSample = start + 15000;
		while (System.currentTimeMillis() < end) {
			Thread.sleep(200);
			if (System.currentTimeMillis() >= nextSample) {
				sent = produced.get();
				line("load" + round, (int) ((System.currentTimeMillis() - start) / 1000), a.texts.size());
				if (System.currentTimeMillis() - start > 20000) maxLiveDuringLoad = Math.max(maxLiveDuringLoad, last[0]);
				nextSample += 15000;
			}
		}
		for (Thread th : ts) th.join();
		sent = produced.get();
		System.out.println("INFO round " + round + ": " + (sent - before) + " MTs in " + LOAD_SECONDS + " s, " + (sent - before) / LOAD_SECONDS + " per second");
	}

	/** live objects of the Client (a full GC first) and its used heap after it */
	static long[] sample() throws Exception {
		String jmap = System.getProperty("java.home") + "/bin/jmap";
		Process p = new ProcessBuilder(jmap, "-histo:live", String.valueOf(client.pid())).redirectErrorStream(true).start();
		long[] r = new long[CLASSES.length * 2 + 1];
		try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
			String l;
			while ((l = in.readLine()) != null) {
				String[] f = l.trim().split("\\s+");
				if (f.length >= 4 && f[0].endsWith(":")) {
					for (int i = 0; i < CLASSES.length; i++) {
						if (f[3].equals(CLASSES[i]) || f[3].endsWith("." + CLASSES[i])) { r[i * 2] = Long.parseLong(f[1]); r[i * 2 + 1] = Long.parseLong(f[2]); }
					}
				} else if (l.trim().startsWith("Total")) {
					r[CLASSES.length * 2] = Long.parseLong(f[2]);
				}
			}
		}
		p.waitFor();
		return r;
	}
	static void line(String phase, int second, int received) throws Exception {
		long[] s = sample();
		System.out.println(String.format("SOAK %-6s t=%4ds sent=%7d received=%7d backlog=%6d | GmmsMessage %7d (%6d KB) timeoutTasks %7d entries %6d | live heap %6d MB",
				phase, second, sent, received, len(0) + len(1), s[0], s[1] / 1024, s[2], s[4], s[6] / 1024 / 1024));
		last = s;
	}
	static long[] last;

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + "\n[FOV601]\nCCBSSID: " + RSSID + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 60\nWindowSize: 500\n"
				+ "OutgoingConnection: (ConnName=FOV601_c1,userName=u601,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamTraceLogEnable=false\nSmppPduLogEnable=false\n").getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a = smsc.account("u601");
		try {
			boot();
			q = com.king.gmms.messagequeue.StreamQueueManager.getInstance();
			client = start(C1, 1);
			// SessionNumber=2 shared by the 2 Clients of the module list: one connection each
			check(waitFor(() -> a.binds.get() == 1, 15000), "MS-00 the Client binds its connection");
			System.out.println("INFO rate " + RATE + "/s, load " + LOAD_SECONDS + " s, quiet " + QUIET_SECONDS + " s, BufferTimeout 60 s, Client -Xmx512m");

			// ---- warm up ----
			produce("W", 20000);
			sent = produced.get();
			waitFor(() -> a.texts.size() >= sent, 60000);
			line("warm", 0, a.texts.size());
			long[] baseline = last;

			List<long[]> quietEnds = new ArrayList<>();
			for (int round = 1; round <= 2; round++) {
				load(round, a);
				long quietFrom = System.currentTimeMillis();
				for (int s = 15; s <= QUIET_SECONDS; s += 15) {
					long at = quietFrom + s * 1000L;
					while (System.currentTimeMillis() < at) Thread.sleep(200);
					line("quiet" + round, s, a.texts.size());
				}
				quietEnds.add(last);
			}

			// ---- every MT out once ----
			waitFor(() -> a.texts.size() >= sent, 30000);
			List<String> all;
			synchronized (a.texts) { all = new ArrayList<>(a.texts); }
			int distinct = new HashSet<>(all).size();
			check(distinct == sent && all.size() == sent, "MS-01 every MT reached the vendor once: sent " + sent + ", received " + all.size() + ", distinct " + distinct);
			long[] q1 = quietEnds.get(0), q2 = quietEnds.get(1);
			System.out.println("INFO after warm-up: GmmsMessage " + baseline[0] + ", live heap " + baseline[6] / 1024 / 1024 + " MB; after round 1 quiet: " + q1[0] + ", "
					+ q1[6] / 1024 / 1024 + " MB; after round 2 quiet: " + q2[0] + ", " + q2[6] / 1024 / 1024 + " MB");
			long tps = (sent - 20000) / (2L * LOAD_SECONDS);
			check(maxLiveDuringLoad < tps * 5 + 2000, "MS-04 while sending, the live MTs are about those in flight, not TPS x BufferTimeout (" + tps * 60 + "): at most " + maxLiveDuringLoad);
			check(q1[0] < 1000 && q2[0] < 1000, "MS-02 quiet longer than BufferTimeout: the MTs are gone from the Client: " + q1[0] + " / " + q2[0]);
			check(q2[6] <= q1[6] * 1.2 + 8L * 1024 * 1024, "MS-03 the live heap after the second quiet is not above the first (no growth from round to round): "
					+ q1[6] / 1024 / 1024 + " -> " + q2[6] / 1024 / 1024 + " MB");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			if (client != null) client.destroyForcibly();
			smsc.close();
		}
		exit();
	}
}
