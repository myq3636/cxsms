package it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.king.gmms.GmmsUtility;
import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/** Reload commands sent in rapid succession, with traffic, on Core, Server and Client (not part of the repo). */
public class RapidReloadIT extends ItSupport {
	static Path cm;
	static String mod;
	static A2PCustomerManager ctm() { return GmmsUtility.getInstance().getCustomerManager(); }
	static String who(int ssid) { A2PCustomerInfo c = ctm().getCustomerBySSID(ssid); return c == null ? null : c.getShortName(); }
	static String files(String f) { return jedis.hget("config:files:" + mod + ":1", f); }
	static void control(String cmd) { jedis.publish("channel:gmms:control", cmd); }
	static long logs(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(text)) n++;
		return n;
	}
	/** -Datomic=true: temp file + rename, otherwise overwrite in place */
	static void put(String text) throws Exception {
		if (Boolean.getBoolean("atomic")) {
			Path tmp = Paths.get(cm + ".tmp");
			Files.write(tmp, text.getBytes("UTF-8"));
			Files.move(tmp, cm, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		} else {
			Files.write(cm, text.getBytes("UTF-8"));
		}
	}

	static void info(String s) { System.out.println("INFO " + s); }

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		mod = System.getProperty("module");
		cm = Paths.get(home + "conf/cm.cfg");
		byte[] orig = Files.readAllBytes(cm);
		try {
			String text = new String(orig, "UTF-8");
			if (mod.startsWith("Core")) core(text);
			else if (mod.contains("Server")) server(text);
			else client(text);
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		} finally {
			Files.write(cm, orig);
		}
		exit();
	}

	// ---- Core ----

	static final AtomicLong seq = new AtomicLong();

	/** n MTs while burst runs; returns nothing, counts come from the log */
	static void traffic(String prefix, int n, Runnable burst) throws Exception {
		final AtomicBoolean go = new AtomicBoolean(true);
		Thread b = new Thread(() -> { while (go.get()) burst.run(); });
		b.start();
		StreamQueueManager q = StreamQueueManager.getInstance();
		for (int i = 0; i < n; i++) {
			GmmsMessage m = mt("8888_" + prefix + i, 8888, -1, 0);
			m.setRecipientAddress("86138" + String.format("%08d", seq.incrementAndGet()));   // CUST8888 limits per recipient
			q.produceSubmitMessage(m);
			if (i % 20 == 0) Thread.sleep(50);
		}
		go.set(false);
		b.join();
		final String tag = "8888_" + prefix;
		waitFor(() -> streamLen("stream:mt:pending:8888:*") == 0, 60000);
		Thread.sleep(500);
		long acked = 0, routed = 0, noRoute = 0, nack = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) {
			if (!l.contains(tag)) continue;
			if (l.contains("[STREAM-ACK] flow=server->core")) acked++;
			if (l.contains("[MT-TO-CLIENT]") || l.contains("[MT-HOLD]")) routed++;
			if (l.contains("[CDR] type=OUT_SUBMIT status=2120")) noRoute++;
			if (l.contains("[STREAM-NACK]")) nack++;
		}
		info(prefix + ": " + n + " MT, acked=" + acked + " routed=" + routed + " noRoute=" + noRoute + " nack=" + nack);
		check(acked == n && routed == n && noRoute == 0 && nack == 0,
				prefix + " all " + n + " MTs routed during the burst (acked=" + acked + ", routed=" + routed + ", failed as not routed=" + noRoute + ", nack=" + nack + ")");
	}

	static void rejected(String tag, String content, String reason, A2PCustomerInfo v8, A2PCustomerInfo v9, String version) throws Exception {
		Files.write(cm, content.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "failed".equals(files("cm.result")) && String.valueOf(files("cm.failReason")).contains(reason), 8000),
				tag + ": rejected, cm.failReason=" + files("cm.failReason"));
		check(ctm().getCustomerBySSID(8) == v8 && ctm().getCustomerBySSID(9) == v9 && version.equals(files("cm.version")),
				tag + ": nobody removed or rebuilt, cm.version unchanged");
		jedis.hset("config:files:" + mod + ":1", "cm.result", "x");
	}

	static void core(String text) throws Exception {
		boot();
		props.setProperty("RedisStreamTraceLogEnable", "true");
		MTStreamConsumer.getInstance().start();
		ResultStreamConsumer.getInstance().start();
		waitFor(() -> files("cm.version") != null, 15000);
		A2PCustomerInfo vendor8 = ctm().getCustomerBySSID(8);

		// 1. same file, RELOAD_CUSTOMER over and over
		traffic("RA", 600, () -> control("RELOAD_CUSTOMER"));
		check(ctm().getCustomerBySSID(8) == vendor8, "RR-01b nothing changed in the file: VENDOR8 is the same object");

		// 2. RELOAD_ROUTING over and over
		long before = logs("Reload routingInfo completed");
		traffic("RB", 600, () -> { control("RELOAD_ROUTING"); try { Thread.sleep(2); } catch (Exception e) { } });
		info("routing reloads done: " + (logs("Reload routingInfo completed") - before));

		// 3. every command mixed
		String[] all = {"RELOAD_CUSTOMER", "RELOAD_ROUTING", "RELOAD_GMMS_CONFIG", "RELOAD_PHONE_PREFIX", "RELOAD_RECIPIENT_RULE",
				"RELOAD_ANTISPAM", "RELOAD_CONTENT_TEMPLATE", "RELOAD_BLACKLIST", "RELOAD_WHITELIST", "RELOAD_SENDER_BLACKLIST",
				"RELOAD_SENDER_WHITELIST", "RELOAD_CONTENT_BLACKLIST", "RELOAD_CONTENT_WHITELIST", "RELOAD_VENDOR_REPLACE",
				"RELOAD_SYSTEM_REPLACE", "RELOAD_RECIPIENT_BLACKLIST"};
		final long[] i = {0};
		traffic("RC", 600, () -> { control(all[(int) (i[0]++ % all.length)]); try { Thread.sleep(2); } catch (Exception e) { } });

		// 4. the file changes between commands (another customer), traffic to VENDOR8 goes on
		final String changed = text.replace("[VENDOR13]", "[VENDOR13N]");
		final long[] k = {0};
		traffic("RD", 600, () -> {
			try {
				Files.write(cm, (k[0]++ % 2 == 0 ? changed : text).getBytes("UTF-8"));
				control("RELOAD_CUSTOMER");
				Thread.sleep(20);
			} catch (Exception e) { }
		});
		check(ctm().getCustomerBySSID(8) == vendor8, "RR-04b customer not in the changes: VENDOR8 is the same object after " + k[0] + " file changes");

		// 5. after the burst the last file wins
		Files.write(cm, changed.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "VENDOR13N".equals(who(13)), 10000) && "ok".equals(files("cm.result")), "RR-05a last file wins: ssid 13 = VENDOR13N");
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "VENDOR13".equals(who(13)), 10000), "RR-05b commands still work after the bursts (no stuck thread)");
		long t0 = System.currentTimeMillis();
		Files.write(cm, changed.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		waitFor(() -> "VENDOR13N".equals(who(13)), 10000);
		info("one reload after the bursts takes " + (System.currentTimeMillis() - t0) + " ms");

		// 6. guards on cm.cfg
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		waitFor(() -> "VENDOR13".equals(who(13)) && "ok".equals(files("cm.result")), 10000);
		A2PCustomerInfo v8 = ctm().getCustomerBySSID(8), v9 = ctm().getCustomerBySSID(9);
		String goodVersion = files("cm.version");
		String half = text.substring(0, text.indexOf("[VENDOR8]"));
		rejected("RR-07 half-written cm.cfg (most customers missing)", half, "would be removed", v8, v9, goodVersion);
		rejected("RR-08 empty cm.cfg", "", "empty", v8, v9, goodVersion);
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "ok".equals(files("cm.result")) && "".equals(files("cm.failReason")), 8000) && ctm().getCustomerBySSID(8) == v8,
				"RR-09 whole file back: cm.result=ok, nobody was rebuilt");

		final AtomicBoolean writing = new AtomicBoolean(true);
		final byte[] whole = text.getBytes("UTF-8");
		Thread slow = new Thread(() -> {
			try {
				while (writing.get()) { Files.write(cm, whole); Thread.sleep(100); }
			} catch (Exception e) { }
		});
		slow.start();
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "failed".equals(files("cm.result")) && String.valueOf(files("cm.failReason")).contains("being written"), 15000)
				&& ctm().getCustomerBySSID(8) == v8, "RR-10 file keeps changing: rejected as still being written (" + files("cm.failReason") + ")");
		writing.set(false); slow.join();
		Thread.sleep(700);
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> "ok".equals(files("cm.result")), 8000), "RR-10b writer finished: reload ok");

		// a write in chunks is waited for, the reload then reads the whole file
		int changedBefore = (int) logs("changed customers: 3");
		String withNew = text + "\n[CUSTNEW91]\nCCBSSID: 91\nA2PPlayerType: operator\nProtocol: SMPP\n";
		final byte[] nb = withNew.getBytes("UTF-8");
		Thread chunks = new Thread(() -> {
			try (java.io.OutputStream o = Files.newOutputStream(cm)) {
				for (int off = 0; off < nb.length; off += 800) { o.write(nb, off, Math.min(800, nb.length - off)); o.flush(); Thread.sleep(40); }
			} catch (Exception e) { }
		});
		chunks.start();
		Thread.sleep(30);
		control("RELOAD_CUSTOMER");
		chunks.join();
		check(waitFor(() -> "CUSTNEW91".equals(who(91)), 10000) && ctm().getCustomerBySSID(8) == v8 && ctm().getCustomerBySSID(9) == v9 && logs("changed customers: 3") == changedBefore,
				"RR-11 reload while the file is written in chunks: waits, reads the whole file, only the new customer changes");
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		waitFor(() -> who(91) == null, 8000);

		java.lang.reflect.Field pf = GmmsUtility.class.getDeclaredField("properties");
		pf.setAccessible(true);
		java.util.Properties live = (java.util.Properties) pf.get(GmmsUtility.getInstance());   // a config reload replaced the object
		live.setProperty("ReloadCustomerMaxRemovePercent", "100");
		Files.write(cm, half.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> who(8) == null, 8000), "RR-12 ReloadCustomerMaxRemovePercent=100: a bulk removal is allowed");
		live.remove("ReloadCustomerMaxRemovePercent");
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		waitFor(() -> "VENDOR8".equals(who(8)), 8000);

		// 7. guards on the routing files
		Path route = Paths.get(home + "conf/routing/CUST8888_routingInfo.cfg"), route2 = Paths.get(home + "conf/routing/CUST7001_routingInfo.cfg");
		byte[] r1 = Files.readAllBytes(route), r2 = Files.readAllBytes(route2);
		try {
			String rv = files("routing.version");
			Files.write(route, new byte[0]); Files.write(route2, new byte[0]);
			control("RELOAD_ROUTING");
			check(waitFor(() -> "partial".equals(files("routing.result")), 8000) && String.valueOf(files("routing.failed")).contains("CUST8888") && rv.equals(files("routing.version")) && ctm().getCustomerRoutingRelay(8888, mt("8888_G", 8888, -1, 0)) == 8,
					"RR-13 empty routing files: each keeps its routing (" + files("routing.failed") + ")");
			Files.write(route, r1); Files.write(route2, r2);
			control("RELOAD_ROUTING");
			check(waitFor(() -> "ok".equals(files("routing.result")), 8000), "RR-13b files back: routing reload ok");

			// 8. lookups never miss while the routing changes between two channels
			final AtomicBoolean go = new AtomicBoolean(true);
			final AtomicLong total = new AtomicLong(), miss = new AtomicLong(), err = new AtomicLong();
			Thread[] readers = new Thread[4];
			for (int t = 0; t < readers.length; t++) {
				readers[t] = new Thread(() -> {
					GmmsMessage m = mt("8888_P", 8888, -1, 0);
					while (go.get()) {
						try {
							total.incrementAndGet();
							int r = ctm().getCustomerRoutingRelay(8888, m);
							if (r != 8 && r != 12) miss.incrementAndGet();
						} catch (Exception e) { err.incrementAndGet(); }
					}
				});
				readers[t].start();
			}
			for (int n = 0; n < 300; n++) { control("RELOAD_ROUTING"); Thread.sleep(20); }
			long done0 = logs("Reload routingInfo completed");
			byte[] other = "CCBSSID: 8888\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR12)\n".getBytes("UTF-8");
			for (int n = 0; n < 30; n++) {
				Files.write(route, n % 2 == 0 ? other : r1);
				control("RELOAD_ROUTING");
				Thread.sleep(700);
			}
			go.set(false);
			for (Thread t : readers) t.join();
			info("lookups=" + total.get() + " miss=" + miss.get() + " err=" + err.get() + " reloads=" + logs("Reload routingInfo completed"));
			check(miss.get() == 0 && err.get() == 0, "RR-14 " + total.get() + " lookups during 330 RELOAD_ROUTING: no miss, no exception (before: 26 per 21 million)");
		} finally {
			Files.write(route, r1); Files.write(route2, r2);
		}

		info("errors: Handle Command Inner ACK error=" + logs("Handle Command Inner ACK error") + ", snapshot discarded=" + logs("customer snapshot changed during reload")
				+ ", reload failed=" + logs("reload failed") + ", customer reload failed=" + logs("customer reload failed") + ", merged=" + logs("customer reload already queued"));
		check(logs("Handle Command Inner ACK error") == 0 && logs("customer snapshot changed during reload") == 0 && logs("customer reload failed") == 0,
				"RR-06 no NPE in MT processing, no discarded snapshot, no failed reload");
	}

	// ---- Server ----

	static final int SPORT = 17788;

	static void server(String text) throws Exception {
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(SPORT));
		new MultiSmppServer().startService();
		Thread.sleep(1500);
		waitFor(() -> files("cm.version") != null, 15000);
		final SmppTestClient c33 = new SmppTestClient("127.0.0.1", SPORT, 50, null);
		check(c33.bind("c33", "p33"), "RS-00 c33 bound, not changed during the test");

		final AtomicBoolean go = new AtomicBoolean(true);
		final AtomicLong sent = new AtomicLong();
		Thread traffic = new Thread(() -> {
			try { while (go.get()) { c33.submit("18667991226", "8613800000000", "x"); sent.incrementAndGet(); Thread.sleep(5); } } catch (Exception e) { }
		});
		traffic.start();

		String a = text.replace("[SMPPCUST31]", "[SMPPCUST31N]");
		int rounds = 40;
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < rounds; i++) {
			Files.write(cm, (i % 2 == 0 ? a : text).getBytes("UTF-8"));
			control("RELOAD_CUSTOMER");
			Thread.sleep(25);
		}
		// a client of the changed customer keeps trying during the burst
		long binds = 0, rejected = 0;
		for (int i = 0; i < 5; i++) {
			SmppTestClient c = new SmppTestClient("127.0.0.1", SPORT, 5, null);
			if (c.bind("c31", "p31")) binds++; else rejected++;
			c.close();
		}
		info("during the settling: c31 binds ok=" + binds + " rejected=" + rejected + ", burst took " + (System.currentTimeMillis() - t0) + " ms");
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		Thread.sleep(3000);
		go.set(false);
		traffic.join(3000);
		check(!c33.isClosedByServer(), "RS-01 unchanged c33 never disconnected in " + rounds + " reloads of another customer");
		final long expect = sent.get();
		check(waitFor(() -> streamLen("stream:mt:pending:33:*") >= expect, 8000), "RS-02 all " + expect + " submits of c33 written to the stream");
		SmppTestClient f = new SmppTestClient("127.0.0.1", SPORT, 5, null);
		check(f.bind("c31", "p31"), "RS-03 after the burst c31 binds");
		f.close();
		check("SMPPCUST31".equals(who(31)) && "ok".equals(files("cm.result")), "RS-04 last file wins, cm.result=ok");
		info("errors: customer reload failed=" + logs("customer reload failed") + ", break customer sessions error=" + logs("break customer sessions error") + ", init customer sessions error=" + logs("init customer sessions error") + ", merged=" + logs("customer reload already queued"));
		check(logs("customer reload failed") == 0 && logs("break customer sessions error") == 0 && logs("init customer sessions error") == 0, "RS-05 no reload or session error");
	}

	// ---- Client ----

	static void client(String text) throws Exception {
		int port = 17789;
		String blocks = "\n[CLIV41]\nCCBSSID: 141\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nWindowSize: 20\n"
				+ "OutgoingConnection: (ConnName=CLIV41_c1,userName=u141,password=p141,URL=127.0.0.1,Port=" + port + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n"
				+ "\n[CLIV43]\nCCBSSID: 143\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nWindowSize: 20\n"
				+ "OutgoingConnection: (ConnName=CLIV43_c1,userName=u143,password=p143,URL=127.0.0.1,Port=" + port + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
		String base = text + blocks;
		Files.write(cm, base.getBytes("UTF-8"));
		RenameReloadIT.Vendor v = new RenameReloadIT.Vendor(port);
		Thread t = new Thread(v); t.setDaemon(true); t.start();
		boot();
		new MultiSmppClient().startService();
		OutboundStreamConsumer.getInstance().start();
		check(waitFor(() -> v.get(v.binds, "u141") == 1 && v.get(v.binds, "u143") == 1, 15000), "RK-00 both vendors bound");
		waitFor(() -> files("cm.version") != null, 15000);

		String a = base.replace("[CLIV41]", "[CLIV41N]");
		int rounds = 40;
		for (int i = 0; i < rounds; i++) {
			put(i % 2 == 0 ? a : base);
			control("RELOAD_CUSTOMER");
			Thread.sleep(25);
		}
		put(base);
		control("RELOAD_CUSTOMER");
		Thread.sleep(15000);       // reconnects settle (ReconnectInterval 1 s)
		int b = v.get(v.binds, "u141"), c = v.get(v.closed, "u141");
		info("u141 after " + rounds + " reloads and settling: binds=" + b + " closed=" + c + " active=" + (b - c));
		check(b - c == 1, "RK-01 changed vendor has exactly 1 connection after the burst (no leak, no loss): binds=" + b + " closed=" + c);
		check(v.get(v.binds, "u143") == 1 && v.get(v.closed, "u143") == 0, "RK-02 unchanged vendor: never reconnected");
		check(RenameReloadIT.mtReachesVendor(v, "u141", 141) && RenameReloadIT.mtReachesVendor(v, "u143", 143), "RK-03 MT reaches both vendors");
		info("errors: customer reload failed=" + logs("customer reload failed") + ", init customer sessions error=" + logs("init customer sessions error") + ", merged=" + logs("customer reload already queued"));
	}
}
