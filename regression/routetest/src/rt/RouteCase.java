package rt;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;

/**
 * One routing test case on a running a2p, checked message by message (docs/路由服务器测试说明.md 3, 4):
 * plays the vendors (one SMSC port, one account per system_id) and the customers (bind to the Server), sends the MTs
 * of case.properties, and for each MT checks the submit_sm_resp, the vendors it went to in order, the Core log lines,
 * the DR to the customer, the CDRs and the DB rows; then the case level checks (logs, Redis). Writes result/{time}/
 * in the case directory.
 *
 * java -cp routetest.jar rt.RouteCase CASE_DIR [ENV_FILE]
 */
public final class RouteCase {
	// ---------------------------------------------------------------- vendors

	final class Vendor {
		final String sys;
		final int submitStatus;
		/** submitStatusSeq: the status of the n-th attempt of one MT at this vendor */
		final int[] seq;
		/** DELIVRD, UNDELIV ... or none */
		final String dr, drErr;
		final long drDelayMs, respDelayMs;
		/** slowFirstLink: only the first bind that gets a submit_sm answers respDelayMs late */
		final boolean slowFirstLink;
		volatile Integer slowLink;
		final int minLinks;
		final List<Submit> submits = new CopyOnWriteArrayList<>();
		final List<Link> links = new CopyOnWriteArrayList<>();
		final AtomicInteger ids = new AtomicInteger();
		final AtomicLong soakCount = new AtomicLong();
		/** attempt key of an MT -> attempts so far (UDH: the references seen) */
		final Map<String, List<String>> attempts = new HashMap<>();

		Vendor(String sys, Properties p) {
			this.sys = sys;
			submitStatus = Integer.decode(p.getProperty("vendor." + sys + ".submitStatus", "0").trim());
			String s = p.getProperty("vendor." + sys + ".submitStatusSeq");
			if (s == null) seq = null;
			else seq = Arrays.stream(s.split(",")).mapToInt(x -> Integer.decode(x.trim())).toArray();
			dr = p.getProperty("vendor." + sys + ".dr", p.getProperty("vendor.dr", "DELIVRD")).trim();
			drErr = p.getProperty("vendor." + sys + ".drErr");
			drDelayMs = Long.parseLong(p.getProperty("vendor." + sys + ".drDelayMs", p.getProperty("vendor.drDelayMs", "500")).trim());
			respDelayMs = Long.parseLong(p.getProperty("vendor." + sys + ".respDelayMs", "0").trim());
			slowFirstLink = Boolean.parseBoolean(p.getProperty("vendor." + sys + ".slowFirstLink", "false").trim());
			minLinks = Integer.parseInt(p.getProperty("vendor." + sys + ".minLinks", p.getProperty("vendor.minLinks", "1")).trim());
		}

		int openLinks() { int n = 0; for (Link l : links) if (l.open) n++; return n; }

		/** the status for this submit: submitStatusSeq by the attempt number of the MT at this vendor */
		synchronized int statusFor(Submit s) {
			Copy c = s.marker == null ? null : byMarker.get(s.marker);
			int[] seq = this.seq;
			if (c != null && c.spec.vendorSubmitStatus != null)
				seq = Arrays.stream(c.spec.vendorSubmitStatus.split(",")).mapToInt(x -> Integer.decode(x.trim())).toArray();
			if (seq == null) return submitStatus;
			String key = s.udhRef >= 0 ? "udh:" + s.dest : "m:" + s.marker;
			List<String> seen = attempts.computeIfAbsent(key, k -> new ArrayList<>());
			String attempt = s.udhRef >= 0 ? String.valueOf(s.udhRef) : String.valueOf(seen.size());
			int n = seen.indexOf(attempt);
			if (n < 0 || s.udhRef < 0) { seen.add(attempt); n = seen.size() - 1; }
			return seq[Math.min(n, seq.length - 1)];
		}
	}

	static final class Submit {
		String vendor, source, dest, text, vendorMsgId, marker;
		int status, udhRef = -1, udhTotal, udhSeq, link, dc;
		byte[] sm;                                                         // short_message without the UDH
		long at;
	}

	static final class Link {
		final Socket s; final DataOutputStream out; volatile boolean open = true; int id;
		Link(Socket s) throws IOException { this.s = s; out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream())); }
		synchronized void pdu(int cmd, int status, int seq, byte[] body) throws IOException {
			out.writeInt(16 + body.length); out.writeInt(cmd); out.writeInt(status); out.writeInt(seq); out.write(body); out.flush();
		}
	}

	final Map<String, Vendor> vendors = new ConcurrentHashMap<>();
	final ScheduledExecutorService later = Executors.newScheduledThreadPool(8, r -> { Thread t = new Thread(r, "dr"); t.setDaemon(true); return t; });
	final AtomicInteger drSeq = new AtomicInteger(5000);
	Properties props;
	Pattern markerPattern;

	void vendorServer(int port) throws IOException {
		ServerSocket ss = new ServerSocket();
		ss.setReuseAddress(true);
		ss.bind(new InetSocketAddress(port));
		Thread t = new Thread(() -> {
			while (true) {
				try {
					Socket s = ss.accept();
					Thread c = new Thread(() -> serveVendor(s), "vendor-conn");
					c.setDaemon(true);
					c.start();
				} catch (IOException e) { return; }
			}
		}, "vendor-accept");
		t.setDaemon(true);
		t.start();
	}

	void serveVendor(Socket s) {
		Vendor v = null; Link link = null;
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
			Link k = new Link(s);
			while (true) {
				int len = in.readInt(), cmd = in.readInt(), st = in.readInt(), seq = in.readInt();
				byte[] body = new byte[len - 16];
				in.readFully(body);
				if (cmd == 1 || cmd == 2 || cmd == 9) {                      // bind: every system_id is a vendor of the tool
					String sys = cstr(body, new int[] { 0 });
					v = vendors.computeIfAbsent(sys, x -> new Vendor(x, props));
					link = k;
					synchronized (v) { k.id = v.links.size(); v.links.add(k); }
					k.pdu(cmd | 0x80000000, 0, seq, "rtsmsc\0".getBytes(StandardCharsets.US_ASCII));
				} else if (cmd == 0x15) {
					k.pdu(0x80000015, 0, seq, new byte[0]);
				} else if (cmd == 4 && v != null) {                             // submit_sm
					Submit sub = parseSubmit(body);
					sub.vendor = v.sys;
					sub.link = k.id;
					sub.at = System.currentTimeMillis();
					sub.status = v.statusFor(sub);
					sub.vendorMsgId = v.sys.toUpperCase() + "-" + v.ids.incrementAndGet();
					if (soak) v.soakCount.incrementAndGet(); else v.submits.add(sub);
					byte[] resp = sub.status == 0 ? (sub.vendorMsgId + "\0").getBytes(StandardCharsets.US_ASCII) : new byte[] { 0 };
					long respDelay = v.respDelayMs;
					if (v.slowFirstLink) {
						synchronized (v) { if (v.slowLink == null) v.slowLink = k.id; }
						if (v.slowLink != k.id) respDelay = 0;
					}
					if (respDelay > 0) {                                        // a slow vendor (or bind): submit_sm_resp later
						final Link rk = k; final int rs = sub.status, rq = seq;
						later.schedule(() -> { try { rk.pdu(0x80000004, rs, rq, resp); } catch (IOException e) { } }, respDelay, TimeUnit.MILLISECONDS);
					} else {
						k.pdu(0x80000004, sub.status, seq, resp);
					}
					Copy mc = sub.marker == null ? null : byMarker.get(sub.marker);
					String dr = mc != null && mc.spec.vendorDr != null ? mc.spec.vendorDr : v.dr;
					long delay = mc != null && mc.spec.vendorDrDelayMs != null ? Long.parseLong(mc.spec.vendorDrDelayMs.trim()) : v.drDelayMs;
					if (sub.status == 0 && !"none".equalsIgnoreCase(dr)) {
						final Vendor fv = v; final Link fk = k; final Submit fs = sub;
						final long rd = respDelay;
						later.schedule(() -> { try { receipt(fv, fk, fs); } catch (IOException e) { } }, delay + rd, TimeUnit.MILLISECONDS);
					}
				} else if (cmd == 0x80000005 || cmd == 0x80000015) {
					// deliver_sm_resp, enquire_link_resp
				} else {
					k.pdu(cmd | 0x80000000, 0, seq, new byte[0]);
				}
			}
		} catch (Exception e) {
			if (link != null) link.open = false;
		} finally {
			try { s.close(); } catch (IOException e) { }
		}
	}

	Submit parseSubmit(byte[] body) {
		int[] i = { 0 };
		Submit sub = new Submit();
		cstr(body, i); i[0] += 2;                                         // service_type, source ton/npi
		sub.source = cstr(body, i); i[0] += 2;
		sub.dest = cstr(body, i);
		int esm = body[i[0]] & 0xff; i[0] += 3;                           // esm_class, protocol_id, priority
		cstr(body, i); cstr(body, i); i[0] += 2;                          // schedule, validity, registered, replace
		int dc = body[i[0]] & 0xff; i[0] += 2;                            // data_coding, sm_default_msg_id
		int n = body[i[0]++] & 0xff;
		byte[] sm = Arrays.copyOfRange(body, i[0], i[0] + n);
		i[0] += n;
		while (i[0] + 4 <= body.length) {                                  // message_payload, SAR TLVs
			int tag = ((body[i[0]] & 0xff) << 8) | (body[i[0] + 1] & 0xff), l = ((body[i[0] + 2] & 0xff) << 8) | (body[i[0] + 3] & 0xff);
			int v = i[0] + 4;
			if (tag == 0x0424 && sm.length == 0) sm = Arrays.copyOfRange(body, v, v + l);
			if (tag == 0x020C) sub.udhRef = l == 2 ? ((body[v] & 0xff) << 8) | (body[v + 1] & 0xff) : body[v] & 0xff;
			if (tag == 0x020E) sub.udhTotal = body[v] & 0xff;
			if (tag == 0x020F) sub.udhSeq = body[v] & 0xff;
			i[0] += 4 + l;
		}
		int from = 0;
		if ((esm & 0x40) != 0 && sm.length > 0) {                         // UDH: concatenation IE 0x00 or 0x08
			int udhl = sm[0] & 0xff;
			for (int j = 1; j + 1 < 1 + udhl; ) {
				int ie = sm[j] & 0xff, il = sm[j + 1] & 0xff;
				if (ie == 0x00 && il == 3) { sub.udhRef = sm[j + 2] & 0xff; sub.udhTotal = sm[j + 3] & 0xff; sub.udhSeq = sm[j + 4] & 0xff; }
				if (ie == 0x08 && il == 4) { sub.udhRef = ((sm[j + 2] & 0xff) << 8) | (sm[j + 3] & 0xff); sub.udhTotal = sm[j + 4] & 0xff; sub.udhSeq = sm[j + 5] & 0xff; }
				j += 2 + il;
			}
			from = 1 + udhl;
		}
		sub.dc = dc;
		sub.sm = Arrays.copyOfRange(sm, from, sm.length);
		sub.text = dc == 8 ? new String(sm, from, sm.length - from, StandardCharsets.UTF_16BE)
				: new String(sm, from, sm.length - from, StandardCharsets.ISO_8859_1);
		Matcher m = markerPattern.matcher(sub.text);
		sub.marker = m.find() ? m.group() : null;
		if (sub.marker == null && soak) {
			Matcher x = soakPattern.matcher(sub.text);
			if (x.find()) {
				sub.marker = x.group();
				Long sent = soakSentAt.get(sub.marker);
				if (sent != null && soakFirstSeen.add(sub.marker)) soakLatMt.add(System.currentTimeMillis() - sent);
			}
		}
		return sub;
	}

	/** the vendor's DR: deliver_sm with esm_class 4 and the receipt text */
	void receipt(Vendor v, Link k, Submit s) throws IOException {
		if (soak && s.marker != null) soakDrSentAt.put(s.marker, System.currentTimeMillis());
		String now = new SimpleDateFormat("yyMMddHHmm").format(new Date());
		Copy c = s.marker == null ? null : byMarker.get(s.marker);
		String stat = c != null && c.spec.vendorDr != null ? c.spec.vendorDr : v.dr;
		boolean ok = "DELIVRD".equals(stat);
		String err = c != null && c.spec.vendorDrErr != null ? c.spec.vendorDrErr : v.drErr != null ? v.drErr : ok ? "000" : "001";
		String tlv = c != null ? c.spec.vendorTlv : null;
		String text = "id:" + s.vendorMsgId + " sub:001 dlvrd:" + (ok ? "001" : "000") + " submit date:" + now
				+ " done date:" + now + " stat:" + stat + " err:" + err + " text:";
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		b.write(0);
		b.write(1); b.write(1); b.write(s.dest.getBytes(StandardCharsets.US_ASCII)); b.write(0);
		b.write(1); b.write(1); b.write(s.source.getBytes(StandardCharsets.US_ASCII)); b.write(0);
		b.write(4); b.write(0); b.write(0); b.write(0); b.write(0); b.write(0); b.write(0); b.write(0); b.write(0);
		byte[] sm = text.getBytes(StandardCharsets.US_ASCII);
		b.write(sm.length); b.write(sm);
		if (tlv != null && !tlv.isEmpty()) {                               // network_error_code: type and 2-byte value, hex
			byte[] t = hex(tlv);
			b.write(0x04); b.write(0x23); b.write(0); b.write(t.length); b.write(t);
		}
		k.pdu(5, 0, drSeq.incrementAndGet(), b.toByteArray());
	}

	// ---------------------------------------------------------------- customers

	/** one segment (or the whole short MT) the customer submitted */
	static final class Seg {
		final Copy copy; volatile Integer respStatus; volatile String inMsgId;
		Seg(Copy c) { copy = c; }
	}

	final class Customer {
		final String user;
		Socket cs; DataOutputStream cout; volatile int bindStatus = -1;
		final AtomicInteger cseq = new AtomicInteger(1);
		final Map<Integer, Seg> bySeq = new ConcurrentHashMap<>();
		/** the receipts: text, network_error_code TLV (hex) or null, arrival time (ms) */
		final List<String[]> drs = new CopyOnWriteArrayList<>();

		/** customer.{user}.drResp=false: no deliver_sm_resp, the Server's DR to it times out (RDQ) */
		boolean noDrResp;

		Customer(String user) {
			this.user = user;
			noDrResp = props != null && "false".equalsIgnoreCase(props.getProperty("customer." + user + ".drResp", "true").trim());
		}

		synchronized void send(int cmd, int status, int seq, byte[] body) throws IOException {
			cout.writeInt(16 + body.length); cout.writeInt(cmd); cout.writeInt(status); cout.writeInt(seq); cout.write(body); cout.flush();
		}

		void bind(String host, int port, String pwd) throws Exception {
			cs = new Socket(host, port);
			cs.setTcpNoDelay(true);
			cout = new DataOutputStream(new BufferedOutputStream(cs.getOutputStream()));
			DataInputStream in = new DataInputStream(new BufferedInputStream(cs.getInputStream()));
			Thread t = new Thread(() -> {
				try {
					while (true) {
						int len = in.readInt(), cmd = in.readInt(), st = in.readInt(), seq = in.readInt();
						byte[] body = new byte[len - 16];
						in.readFully(body);
						if (cmd == 0x80000009) bindStatus = st;
						else if (cmd == 0x80000004) {
							if (soak) { bySeq.remove(seq); (st == 0 ? soakRespOk : soakRespErr).incrementAndGet(); continue; }
							Seg g = bySeq.get(seq);
							if (g != null) { g.inMsgId = cstr(body, new int[] { 0 }); g.respStatus = st; }
						} else if (cmd == 5 && soak) {
							soakDr.incrementAndGet();
							String text = new String(body, StandardCharsets.ISO_8859_1);
							Matcher x = soakPattern.matcher(text);
							if (x.find()) {
								long now = System.currentTimeMillis();
								Long sent = soakSentAt.remove(x.group()), dr = soakDrSentAt.remove(x.group());
								if (sent != null) soakLatFull.add(now - sent);
								if (dr != null) soakLatDr.add(now - dr);
								soakFirstSeen.remove(x.group());
							}
							if (!noDrResp) send(0x80000005, 0, seq, new byte[] { 0 });
						} else if (cmd == 5) {
							int[] i = { 0 };
							cstr(body, i); i[0] += 2; cstr(body, i); i[0] += 2; cstr(body, i);
							i[0] += 3; cstr(body, i); cstr(body, i); i[0] += 4;
							int n = body[i[0]++] & 0xff;
							String text = new String(body, i[0], n, StandardCharsets.ISO_8859_1);
							String tlv = null;
							for (int j = i[0] + n; j + 4 <= body.length; ) {
								int tag = ((body[j] & 0xff) << 8) | (body[j + 1] & 0xff), l = ((body[j + 2] & 0xff) << 8) | (body[j + 3] & 0xff);
								if (tag == 0x0423 && j + 4 + l <= body.length) tlv = toHex(body, j + 4, l);
								j += 4 + l;
							}
							drs.add(new String[] { text, tlv, String.valueOf(System.currentTimeMillis()) });
							send(0x80000005, 0, seq, new byte[] { 0 });
						} else if (cmd == 0x15) send(0x80000015, 0, seq, new byte[0]);
					}
				} catch (Exception e) { }
			}, "customer-" + user);
			t.setDaemon(true);
			t.start();
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			b.write(user.getBytes(StandardCharsets.US_ASCII)); b.write(0); b.write(pwd.getBytes(StandardCharsets.US_ASCII)); b.write(0);
			b.write(0); b.write(0x34); b.write(0); b.write(0); b.write(0);
			send(9, 0, cseq.getAndIncrement(), b.toByteArray());
			long end = System.currentTimeMillis() + 10000;
			while (bindStatus < 0 && System.currentTimeMillis() < end) Thread.sleep(20);
			if (bindStatus != 0) throw new IllegalStateException("customer " + user + " bind to " + host + ":" + port + " failed, status=" + bindStatus);
		}

		/** the MT, as UDH segments of at most 134 bytes when parts > 1 */
		void submit(Copy c, int parts) throws IOException {
			byte[] all = encode(c);
			int dc = dataCoding(c.spec.enc);
			List<byte[]> chunks = new ArrayList<>();
			if (parts <= 1) chunks.add(all);
			else for (int o = 0; o < all.length; o += 134) chunks.add(Arrays.copyOfRange(all, o, Math.min(all.length, o + 134)));
			int ref = new Random().nextInt(255) + 1;
			for (int n = 0; n < chunks.size(); n++) {
				if (c.spec.dropSeg == n + 1) continue;                      // this segment is not sent
				Seg g = new Seg(c);
				c.segs.add(g);
				int seq = cseq.getAndIncrement();
				bySeq.put(seq, g);
				ByteArrayOutputStream b = new ByteArrayOutputStream();
				b.write(0);
				b.write(5); b.write(0); b.write(c.spec.sender.getBytes(StandardCharsets.US_ASCII)); b.write(0);
				b.write(1); b.write(1); b.write(c.spec.recipient.getBytes(StandardCharsets.US_ASCII)); b.write(0);
				b.write(chunks.size() > 1 && !c.spec.sar ? 0x40 : 0); b.write(0); b.write(0); b.write(0); b.write(0);
				b.write(1); b.write(0); b.write(dc); b.write(0);             // registered_delivery 1: a DR is wanted; data_coding
				byte[] sm = chunks.get(n);
				if (chunks.size() > 1 && c.spec.sar) {                       // SAR TLVs after short_message
					b.write(sm.length); b.write(sm);
					b.write(0x02); b.write(0x0C); b.write(0); b.write(2); b.write(0); b.write(ref);
					b.write(0x02); b.write(0x0E); b.write(0); b.write(1); b.write(chunks.size());
					b.write(0x02); b.write(0x0F); b.write(0); b.write(1); b.write(n + 1);
					send(4, 0, seq, b.toByteArray());
					continue;
				}
				if (chunks.size() > 1) {
					ByteArrayOutputStream u = new ByteArrayOutputStream();
					u.write(5); u.write(0); u.write(3); u.write(ref); u.write(chunks.size()); u.write(n + 1); u.write(sm);
					sm = u.toByteArray();
				}
				b.write(sm.length); b.write(sm);
				send(4, 0, seq, b.toByteArray());
			}
		}
	}

	final Map<String, Customer> customers = new ConcurrentHashMap<>();

	// ---------------------------------------------------------------- soak (docs/内存泄漏测试设计.md)

	volatile boolean soak;
	Pattern soakPattern;
	volatile long soakMeasureFrom;
	final Map<String, Long> soakSentAt = new ConcurrentHashMap<>(), soakDrSentAt = new ConcurrentHashMap<>();
	final Set<String> soakFirstSeen = ConcurrentHashMap.newKeySet();
	final Queue<Long> soakLatMt = new ConcurrentLinkedQueue<>(), soakLatDr = new ConcurrentLinkedQueue<>(), soakLatFull = new ConcurrentLinkedQueue<>();

	static String percentiles(Queue<Long> q) {
		List<Long> v = new ArrayList<>(q);
		if (v.isEmpty()) return "count=0";
		Collections.sort(v);
		long sum = 0;
		for (long x : v) sum += x;
		return String.format("count=%d avg=%d p50=%d p95=%d p99=%d max=%d", v.size(), sum / v.size(), v.get(v.size() / 2),
				v.get(Math.min(v.size() - 1, (int) (v.size() * 0.95))), v.get(Math.min(v.size() - 1, (int) (v.size() * 0.99))), v.get(v.size() - 1));
	}
	final AtomicLong soakRespOk = new AtomicLong(), soakRespErr = new AtomicLong(), soakDr = new AtomicLong();

	/**
	 * Steady traffic for the memory leak test: the mt.N of the case are templates (mt.N.weight: share), sent at
	 * soak.tps for soak.durationSec with random recipients after the template's prefix; a customer is re-bound every
	 * soak.reconnectSec, soak.reloadCmds are published one by one every soak.reloadSec. Only counts are kept. The times of
	 * the phases go to $RT_MEMDIR/phases.txt for bin/memwatch.sh and rt.MemReport.
	 */
	boolean runSoak(Path dir, Properties p, List<Spec> specs, Path out, StringBuilder sum) throws Exception {
		long duration = Long.parseLong(p.getProperty("soak.durationSec").trim()) * 1000, warmup = Long.parseLong(p.getProperty("soak.warmupSec", "300").trim()) * 1000;
		long drain = Long.parseLong(p.getProperty("soak.drainSec", "60").trim()) * 1000, idle = Long.parseLong(p.getProperty("soak.idleSec", "600").trim()) * 1000;
		long reconnectEvery = Long.parseLong(p.getProperty("soak.reconnectSec", "0").trim()) * 1000, reloadEvery = Long.parseLong(p.getProperty("soak.reloadSec", "0").trim()) * 1000;
		double tps = Double.parseDouble(p.getProperty("soak.tps", "100").trim());
		int rlen = Integer.parseInt(p.getProperty("soak.recipientLen", "13").trim());
		String[] reloads = p.getProperty("soak.reloadCmds", "").trim().isEmpty() ? new String[0] : p.getProperty("soak.reloadCmds").split(",");
		List<Spec> pick = new ArrayList<>();
		for (int n = 1; n <= specs.size(); n++) {
			int w = Integer.parseInt(p.getProperty("mt." + n + ".weight", "1").trim());
			for (int i = 0; i < w; i++) pick.add(specs.get(n - 1));
		}
		soak = true;
		soakPattern = Pattern.compile(Pattern.quote(p.getProperty("case.id", "S")) + "-S[0-9]+(?![0-9])");
		Path phases = System.getenv("RT_MEMDIR") == null ? out.resolve("phases.txt") : Paths.get(System.getenv("RT_MEMDIR"), "phases.txt");
		long start = System.currentTimeMillis();
		Files.write(phases, ("loadStart=" + start + "\nwarmupEnd=" + (start + warmup) + "\n").getBytes(StandardCharsets.UTF_8));
		Random rnd = new Random();
		long sent = 0, sendErr = 0, segs = 0, reconnects = 0, reloadsDone = 0, nextReconnect = start + reconnectEvery, nextReload = start + reloadEvery;
		List<String> users = new ArrayList<>(new TreeSet<>(customers.keySet()));
		Map<String, Long> bySpec = new TreeMap<>();
		StringBuilder events = new StringBuilder();
		while (true) {
			long now = System.currentTimeMillis();
			if (now >= start + duration) break;
			long due = (long) ((now - start) * tps / 1000) - sent;
			for (long i = 0; i < due; i++) {
				Spec t = pick.get(rnd.nextInt(pick.size()));
				Spec s = new Spec();
				s.label = t.label; s.sender = t.sender; s.customer = t.customer; s.enc = t.enc; s.parts = t.parts; s.sar = t.sar; s.path = t.path; s.dr = t.dr; s.logs = "";
				StringBuilder r = new StringBuilder(t.recipient);
				while (r.length() < rlen) r.append(rnd.nextInt(10));
				s.recipient = r.toString();
				Copy c = new Copy(s, t.label, p.getProperty("case.id", "S") + "-S" + (sent + 1));
				c.text = c.marker + " " + t.label;
				if (now >= start + warmup) soakSentAt.put(c.marker, now);     // latency after the warm-up only
				if (s.parts > 1) {
					StringBuilder x = new StringBuilder(c.text);
					while (x.length() < 134 * (s.parts - 1) + 100) x.append(" long sms segment test");
					c.text = x.substring(0, 134 * (s.parts - 1) + 100);
				}
				sent++;
				bySpec.merge(t.label, 1L, Long::sum);
				try { customers.get(s.customer).submit(c, s.parts); segs += Math.max(1, s.parts); } catch (Exception e) { sendErr++; }
			}
			if (reconnectEvery > 0 && now >= nextReconnect && !users.isEmpty()) {
				String u = users.get((int) (reconnects % users.size()));
				Customer old = customers.get(u);
				try { old.cs.close(); } catch (IOException e) { }
				Thread.sleep(1000);
				Customer nc = new Customer(u);
				try {
					nc.bind(p.getProperty("server.host"), Integer.parseInt(p.getProperty("customer." + u + ".port", p.getProperty("server.port")).trim()), p.getProperty("customer.password"));
					customers.put(u, nc);
					events.append(new Date()).append(" re-bound ").append(u).append('\n');
				} catch (Exception e) {
					events.append(new Date()).append(" re-bind of ").append(u).append(" failed: ").append(e.getMessage()).append('\n');
				}
				reconnects++;
				nextReconnect += reconnectEvery;
			}
			if (reloadEvery > 0 && now >= nextReload && reloads.length > 0) {
				String cmd = reloads[(int) (reloadsDone % reloads.length)].trim();
				events.append(new Date()).append(" PUBLISH ").append(cmd).append(" -> ").append(String.join(" ", redis("PUBLISH", "channel:gmms:control", cmd))).append('\n');
				reloadsDone++;
				nextReload += reloadEvery;
			}
			Thread.sleep(50);
		}
		long loadEnd = System.currentTimeMillis();
		Files.write(phases, ("loadEnd=" + loadEnd + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
		Thread.sleep(drain);
		long drAfterDrain = soakDr.get();
		Thread.sleep(idle);
		Files.write(phases, ("idleEnd=" + System.currentTimeMillis() + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
		long planned = (long) (tps * duration / 1000);
		long ok = soakRespOk.get(), err = soakRespErr.get();
		boolean pass = sent >= planned * 0.95 && ok + err > 0 && ok >= (ok + err) * 0.99;
		sum.append(String.format("soak: %.0f TPS for %d s (warm-up %d s), then %d s drain and %d s idle%n", tps, duration / 1000, warmup / 1000, drain / 1000, idle / 1000));
		sum.append("MTs sent ").append(sent).append(" of ").append(planned).append(" planned, segments ").append(segs).append(", send errors ").append(sendErr).append('\n');
		sum.append("by template: ").append(bySpec).append('\n');
		sum.append("submit_sm_resp ok ").append(ok).append(", failed ").append(err).append('\n');
		sum.append("DRs to the customers ").append(drAfterDrain).append(" after the drain, ").append(soakDr.get()).append(" at the end\n");
		StringBuilder vs = new StringBuilder();
		for (Vendor v : new TreeMap<>(vendors).values()) vs.append(v.sys).append('=').append(v.soakCount.get()).append(' ');
		sum.append("submit_sm at the vendors: ").append(vs).append('\n');
		sum.append("customer re-binds ").append(reconnects).append(", reload commands ").append(reloadsDone).append('\n').append(events);
		sum.append("TRAFFIC: ").append(pass ? "PASS" : "FAIL").append(" (sent >= 95% of planned, submit_sm_resp ok >= 99%)\n");
		sum.append("latency after the warm-up (ms): customer submit -> vendor: ").append(percentiles(soakLatMt)).append('\n');
		sum.append("                                vendor DR -> customer: ").append(percentiles(soakLatDr)).append('\n');
		sum.append("                                customer submit -> customer DR: ").append(percentiles(soakLatFull)).append('\n');
		sum.append("MTs after the warm-up without a DR at the end: ").append(soakSentAt.size()).append('\n');
		Files.write(out.resolve("soak.txt"), ("sent=" + sent + "\nsegments=" + segs + "\nrespOk=" + ok + "\nrespErr=" + err + "\ndr=" + soakDr.get()
				+ "\nlatMt=" + percentiles(soakLatMt) + "\nlatDr=" + percentiles(soakLatDr) + "\nlatFull=" + percentiles(soakLatFull)
				+ "\nnoDr=" + soakSentAt.size() + "\n").getBytes(StandardCharsets.UTF_8));
		sum.append("RESULT: ").append(pass ? "PASS" : "FAIL").append(" (traffic; the memory verdict is added by rt.MemReport)\n");
		Files.write(out.resolve("summary.txt"), sum.toString().getBytes(StandardCharsets.UTF_8));
		System.out.print(sum);
		System.out.println("result: " + out);
		for (Customer c : customers.values()) try { c.cs.close(); } catch (IOException e) { }
		return pass;
	}
	final Map<String, Copy> byMarker = new ConcurrentHashMap<>();

	// ---------------------------------------------------------------- MTs

	/** mt.N / mt2.N of case.properties */
	static final class Spec {
		String label, sender, recipient, path, logs, dr, text, customer, ratio, cdr, db, segments;
		String vendorDr, vendorDrErr, vendorTlv, vendorDrDelayMs, vendorSubmitStatus, drErr, drTlv, drDelayMs, drRatio;
		Integer drCount;
		String bindMax1s, bindMax100ms, spanMs, bindsUsed;
		String enc, hex, segLens, vendorDc, vendorEnc, vendorText; Integer length; boolean matchDest;
		boolean sar, drEachSeg; int dropSeg; String cdrCount;
		int phase, count = 1, parts = 1; long after, waitMs;
		final List<Copy> copies = new ArrayList<>();
		final List<String> fails = new ArrayList<>();
	}

	/** one MT sent (count copies of a spec) */
	static final class Copy {
		final Spec spec; final String label, marker; String text; long sentAt;
		final List<Seg> segs = new CopyOnWriteArrayList<>();
		final Set<String> msgIds = new LinkedHashSet<>();
		final List<String> evidence = new ArrayList<>(), cdr = new ArrayList<>(), fails = new ArrayList<>();
		List<String> path = new ArrayList<>();
		String drText, drTlv, dbSeen, vendorGot;
		Copy(Spec s, String label, String marker) { spec = s; this.label = label; this.marker = marker; }
		String msgId() { return msgIds.isEmpty() ? null : msgIds.iterator().next(); }
	}

	List<Spec> specs(Properties p, String prefix, int phase, String caseId, String runId) {
		List<Spec> out = new ArrayList<>();
		for (int n = 1; p.containsKey(prefix + n); n++) {
			String k = prefix + n;
			String[] f = p.getProperty(k).split("\\|", -1);
			// the expected path may hold '|' inside (a|b): fields are sender|recipient|path...|log|dr, path is the middle
			Spec s = new Spec();
			s.phase = phase;
			s.label = (phase == 1 ? "M" : "N") + n;
			s.sender = f[0].trim(); s.recipient = f[1].trim();
			s.dr = f[f.length - 1].trim();
			s.logs = f.length >= 5 ? f[f.length - 2].trim() : "";
			s.path = String.join("|", Arrays.copyOfRange(f, 2, Math.max(3, f.length - 2))).trim();
			s.text = p.getProperty(k + ".text");
			s.customer = p.getProperty(k + ".customer", p.getProperty("customer.user"));
			s.after = Long.parseLong(p.getProperty(k + ".after", p.getProperty("mt.gapMs", "200")).trim());
			s.waitMs = Long.parseLong(p.getProperty(k + ".waitMs", p.getProperty("mt.drWaitMs", "60000")).trim());
			s.count = Integer.parseInt(p.getProperty(k + ".count", "1").trim());
			s.ratio = p.getProperty(k + ".ratio");
			s.cdr = p.getProperty(k + ".cdr");
			s.db = p.getProperty(k + ".db");
			s.parts = Integer.parseInt(p.getProperty(k + ".parts", "1").trim());
			s.segments = p.getProperty(k + ".segments");
			s.vendorDr = p.getProperty(k + ".vendorDr");
			s.vendorDrErr = p.getProperty(k + ".vendorDrErr");
			s.vendorTlv = p.getProperty(k + ".vendorTlv");
			s.drErr = p.getProperty(k + ".drErr");
			s.drTlv = p.getProperty(k + ".drTlv");
			s.vendorDrDelayMs = p.getProperty(k + ".vendorDrDelayMs");
			s.vendorSubmitStatus = p.getProperty(k + ".vendorSubmitStatus");
			s.drDelayMs = p.getProperty(k + ".drDelayMs");
			s.drRatio = p.getProperty(k + ".drRatio");
			s.bindMax1s = p.getProperty(k + ".bindMax1s");
			s.bindMax100ms = p.getProperty(k + ".bindMax100ms");
			s.spanMs = p.getProperty(k + ".spanMs");
			s.bindsUsed = p.getProperty(k + ".bindsUsed");
			s.enc = p.getProperty(k + ".enc", "ascii").trim();
			s.hex = p.getProperty(k + ".hex");
			String len = p.getProperty(k + ".length");
			s.length = len == null ? null : Integer.valueOf(len.trim());
			s.segLens = p.getProperty(k + ".segLens");
			s.vendorDc = p.getProperty(k + ".vendorDc");
			s.vendorEnc = p.getProperty(k + ".vendorEnc");
			s.vendorText = p.getProperty(k + ".vendorText");
			s.sar = Boolean.parseBoolean(p.getProperty(k + ".sar", "false").trim());
			s.drEachSeg = Boolean.parseBoolean(p.getProperty(k + ".drEachSeg", "false").trim());
			s.dropSeg = Integer.parseInt(p.getProperty(k + ".dropSeg", "0").trim());
			s.cdrCount = p.getProperty(k + ".cdrCount");
			s.matchDest = "dest".equalsIgnoreCase(p.getProperty(k + ".vendorMatch", "").trim()) || s.segLens != null || "gsm7packed".equals(s.vendorEnc);
			String dc = p.getProperty(k + ".drCount");
			s.drCount = dc == null ? null : Integer.valueOf(dc.trim());
			for (int c = 1; c <= s.count; c++) {
				String label = s.count == 1 ? s.label : s.label + "." + c;
				Copy cp = new Copy(s, label, caseId + "-" + label + "-" + runId);
				cp.text = (s.text == null ? "" : s.text + " ") + cp.marker;
				if (s.length != null) {                                      // padded with 'a' to this many GSM characters (or bytes)
					String head = s.text == null ? "" : s.text, tail = " " + cp.marker;
					int n0 = "gsm7".equals(s.enc) || "gsm7packed".equals(s.enc) ? Gsm7.septets(head + tail) : (head + tail).length();
					StringBuilder pad = new StringBuilder(head);
					for (int i = n0; i < s.length; i++) pad.append('a');
					cp.text = pad + tail;
				}
				if (s.parts > 1) {                                           // long enough for s.parts segments of 134 bytes
					StringBuilder t = new StringBuilder(cp.text);
					while (t.length() < 134 * (s.parts - 1) + 100) t.append(" long sms segment test");
					cp.text = t.substring(0, 134 * (s.parts - 1) + 100);
				}
				s.copies.add(cp);
				byMarker.put(cp.marker, cp);
			}
			out.add(s);
		}
		return out;
	}

	// ---------------------------------------------------------------- helpers

	/** the bytes the customer sends: enc ascii (dc 0), gsm7 (dc 0, a byte per character), gsm7packed (dc 0), latin1 (dc 3), ucs2 (dc 8); hex: these bytes first */
	static byte[] encode(Copy c) {
		Spec s = c.spec;
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		if (s.hex != null) { byte[] h = hex(s.hex.trim()); b.write(h, 0, h.length); }
		String t = s.hex != null ? " " + c.marker : c.text;
		byte[] x;
		switch (s.enc) {
			case "gsm7": x = Gsm7.unpacked(t); break;
			case "gsm7packed": x = Gsm7.pack(Gsm7.unpacked(t)); break;
			case "latin1": x = t.getBytes(StandardCharsets.ISO_8859_1); break;
			case "ucs2": x = t.getBytes(StandardCharsets.UTF_16BE); break;
			default: x = t.getBytes(StandardCharsets.ISO_8859_1);
		}
		b.write(x, 0, x.length);
		return b.toByteArray();
	}

	static int dataCoding(String enc) {
		return "latin1".equals(enc) ? 3 : "ucs2".equals(enc) ? 8 : 0;
	}

	/** a vendor segment as text: by its data_coding, or as enc says for data_coding 0 */
	static String decode(Submit x, String enc) {
		if (x.dc == 8) return new String(x.sm, StandardCharsets.UTF_16BE);
		if (x.dc == 0 && "gsm7".equals(enc)) return Gsm7.text(x.sm);
		if (x.dc == 0 && "gsm7packed".equals(enc)) return Gsm7.text(Gsm7.unpack(x.sm, x.sm.length * 8 / 7));
		return new String(x.sm, StandardCharsets.ISO_8859_1);
	}

	/** GSM 03.38 default alphabet and its extension table */
	static final class Gsm7 {
		static final String BASIC = "@\u00a3$\u00a5\u00e8\u00e9\u00f9\u00ec\u00f2\u00c7\n\u00d8\u00f8\r\u00c5\u00e5\u0394_\u03a6\u0393\u039b\u03a9\u03a0\u03a8\u03a3\u0398\u039e\u001b\u00c6\u00e6\u00df\u00c9"
				+ " !\"#\u00a4%&'()*+,-./0123456789:;<=>?\u00a1ABCDEFGHIJKLMNOPQRSTUVWXYZ\u00c4\u00d6\u00d1\u00dc\u00a7\u00bfabcdefghijklmnopqrstuvwxyz\u00e4\u00f6\u00f1\u00fc\u00e0";
		static final String EXT_CHARS = "\f^{}\\[~]|\u20ac";
		static final int[] EXT_CODES = { 0x0A, 0x14, 0x28, 0x29, 0x2F, 0x3C, 0x3D, 0x3E, 0x40, 0x65 };

		static int septets(String t) {
			int n = 0;
			for (char ch : t.toCharArray()) n += EXT_CHARS.indexOf(ch) >= 0 ? 2 : 1;
			return n;
		}

		static byte[] unpacked(String t) {
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			for (char ch : t.toCharArray()) {
				int e = EXT_CHARS.indexOf(ch);
				if (e >= 0) { b.write(0x1B); b.write(EXT_CODES[e]); continue; }
				int i = BASIC.indexOf(ch);
				b.write(i >= 0 ? i : 0x3F);
			}
			return b.toByteArray();
		}

		static String text(byte[] septets) {
			StringBuilder s = new StringBuilder();
			for (int i = 0; i < septets.length; i++) {
				int v = septets[i] & 0x7F;
				if (v == 0x1B && i + 1 < septets.length) {
					int n = septets[++i] & 0x7F, e = -1;
					for (int j = 0; j < EXT_CODES.length; j++) if (EXT_CODES[j] == n) e = j;
					s.append(e >= 0 ? EXT_CHARS.charAt(e) : '?');
				} else s.append(BASIC.charAt(v));
			}
			return s.toString();
		}

		static byte[] pack(byte[] septets) {
			byte[] out = new byte[(septets.length * 7 + 7) / 8];
			for (int i = 0; i < septets.length; i++) {
				int bit = i * 7, v = septets[i] & 0x7F;
				out[bit / 8] |= (byte) (v << (bit % 8));
				if (bit % 8 > 1 && bit / 8 + 1 < out.length) out[bit / 8 + 1] |= (byte) (v >> (8 - bit % 8));
			}
			return out;
		}

		static byte[] unpack(byte[] packed, int count) {
			byte[] out = new byte[count];
			for (int i = 0; i < count; i++) {
				int bit = i * 7, v = (packed[bit / 8] & 0xFF) >> (bit % 8);
				if (bit % 8 > 1 && bit / 8 + 1 < packed.length) v |= (packed[bit / 8 + 1] & 0xFF) << (8 - bit % 8);
				out[i] = (byte) (v & 0x7F);
			}
			return out;
		}
	}

	static byte[] hex(String h) {
		byte[] b = new byte[h.length() / 2];
		for (int i = 0; i < b.length; i++) b[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
		return b;
	}

	static String toHex(byte[] b, int from, int len) {
		StringBuilder s = new StringBuilder();
		for (int i = from; i < from + len; i++) s.append(String.format("%02X", b[i] & 0xff));
		return s.toString();
	}

	static String cstr(byte[] b, int[] i) {
		int s = i[0];
		while (i[0] < b.length && b[i[0]] != 0) i[0]++;
		String r = new String(b, s, i[0] - s, StandardCharsets.ISO_8859_1);
		i[0]++;
		return r;
	}

	static boolean waitFor(java.util.function.BooleanSupplier c, long ms) throws InterruptedException {
		long end = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < end) { if (c.getAsBoolean()) return true; Thread.sleep(100); }
		return c.getAsBoolean();
	}

	/** lines of the files holding any of the fixed strings (.gz files with zgrep) */
	static List<String> grep(List<String> ids, List<String> paths) throws Exception {
		if (ids.isEmpty() || paths.isEmpty()) return Collections.emptyList();
		Path f = Files.createTempFile("rtids", ".txt");
		Files.write(f, ids);
		List<String> out = new ArrayList<>();
		List<String> plain = new ArrayList<>(), gz = new ArrayList<>();
		for (String x : paths) (x.endsWith(".gz") ? gz : plain).add(x);
		for (List<String> group : Arrays.asList(plain, gz)) {
			if (group.isEmpty()) continue;
			List<String> cmd = new ArrayList<>(Arrays.asList(group == gz ? "zgrep" : "grep", "-a", "-h", "-F", "-f", f.toString()));
			cmd.addAll(group);
			out.addAll(exec(cmd, null));
		}
		Files.delete(f);
		return out;
	}

	static List<String> exec(List<String> cmd, Map<String, String> env) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
		if (env != null) pb.environment().putAll(env);
		Process p = pb.start();
		List<String> out = new ArrayList<>();
		try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
			for (String l; (l = r.readLine()) != null; ) out.add(l);
		}
		p.waitFor();
		return out;
	}

	Properties gmms = new Properties();
	String home;
	long since;

	List<String> redis(String... args) throws Exception {
		List<String> cmd = new ArrayList<>(Arrays.asList(props.getProperty("redis.cli", "/usr/local/redis-7.4.9/src/redis-cli"),
				"-h", gmms.getProperty("Redis_master_host", "127.0.0.1").trim(), "-p", gmms.getProperty("Redis_master_port", "6379").trim(),
				"--no-auth-warning"));
		cmd.addAll(Arrays.asList(args));
		Map<String, String> env = new HashMap<>();
		String pw = gmms.getProperty("Redis_password", "").trim();
		if (!pw.isEmpty()) env.put("REDISCLI_AUTH", pw);
		return exec(cmd, env);
	}

	/** check.cmd: RTREDIS (redis-cli of the state Redis, with its password in REDISCLI_AUTH), A2P_HOME */
	Map<String, String> cmdEnv() {
		Map<String, String> env = new HashMap<>();
		env.put("RTREDIS", props.getProperty("redis.cli", "/usr/local/redis-7.4.9/src/redis-cli") + " -h " + gmms.getProperty("Redis_master_host", "127.0.0.1").trim()
				+ " -p " + gmms.getProperty("Redis_master_port", "6379").trim() + " --no-auth-warning");
		String pw = gmms.getProperty("Redis_password", "").trim();
		if (!pw.isEmpty()) env.put("REDISCLI_AUTH", pw);
		env.put("A2P_HOME", home);
		return env;
	}

	/** one row of SELECT columns FROM table WHERE MsgID=id, tab separated, or empty */
	List<String> db(String table, String columns, String msgId) throws Exception {
		Matcher m = Pattern.compile("jdbc:mysql://([^:/]+)(?::(\\d+))?/(\\w+)").matcher(gmms.getProperty("DS_gmms_URL", ""));
		if (!m.find()) return Collections.emptyList();
		List<String> cmd = Arrays.asList(props.getProperty("mysql.cli", "/usr/local/mysql/bin/mysql"), "-h" + m.group(1),
				"-P" + (m.group(2) == null ? "3306" : m.group(2)), "-u" + gmms.getProperty("DS_gmms_Login", "").trim(), "-N", "-B", m.group(3),
				"-e", "SELECT " + columns + " FROM " + table + " WHERE MsgID='" + msgId.replace("'", "") + "'");
		Map<String, String> env = new HashMap<>();
		env.put("MYSQL_PWD", gmms.getProperty("DS_gmms_Password", "").trim());
		return exec(cmd, env);
	}

	/** the module's log and its rolled files changed since the case started */
	List<String> logFiles(String module) throws IOException {
		List<String> out = new ArrayList<>();
		Path cur = Paths.get(home, "log", module + ".log");
		if (Files.exists(cur)) out.add(cur.toString());
		for (Path d : new Path[] { Paths.get(home, "log"), Paths.get(home, "log", "previous") }) {
			if (!Files.isDirectory(d)) continue;
			try (DirectoryStream<Path> ds = Files.newDirectoryStream(d, module + ".log.*")) {
				for (Path f : ds) if (Files.getLastModifiedTime(f).toMillis() >= since) out.add(f.toString());
			}
		}
		return out;
	}

	static final Pattern TS = Pattern.compile("\\d{6} \\d{2}:\\d{2}:\\d{2},\\d{3}");
	static String ts(String l) { Matcher x = TS.matcher(l); return x.find() ? x.group() : ""; }

	// ---------------------------------------------------------------- run

	public static void main(String[] a) throws Exception {
		Path dir = Paths.get(a[0]).toAbsolutePath();
		Properties p = new Properties();
		Path envFile = a.length > 1 ? Paths.get(a[1]) : dir.getParent().getParent().resolve("env.properties");
		try (Reader r = Files.newBufferedReader(envFile, StandardCharsets.UTF_8)) { p.load(r); }
		try (Reader r = Files.newBufferedReader(dir.resolve("case.properties"), StandardCharsets.UTF_8)) { p.load(r); }
		boolean pass;
		try { pass = new RouteCase().run(dir, p); } catch (Exception e) { e.printStackTrace(); pass = false; }
		System.exit(pass ? 0 : 1);
	}

	final List<String> modules = new ArrayList<>();

	boolean run(Path dir, Properties p) throws Exception {
		props = p;
		String caseId = p.getProperty("case.id", dir.getFileName().toString());
		long startedAt = System.currentTimeMillis();
		String env = System.getenv("RT_SINCE");                              // run_case.sh: before the modules restarted
		since = env != null && env.matches("\\d+") ? Long.parseLong(env) : startedAt;
		String runId = new SimpleDateFormat("MMddHHmmss").format(new Date());
		markerPattern = Pattern.compile(Pattern.quote(caseId) + "-[MN][0-9.]+-" + runId);
		home = p.getProperty("a2p.home", "/usr/local/a2p");
		try (Reader r = Files.newBufferedReader(Paths.get(home, "Gmms", "GmmsConfig.properties"), StandardCharsets.ISO_8859_1)) { gmms.load(r); }
		for (String m : p.getProperty("logs", "MultiSmppServer1,MultiSmppServer2,CoreEngine1,CoreEngine2,CoreEngine3,MultiSmppClient1,MultiSmppClient2,MsgQueueMonitor1,MsgQueueMonitor2").split(","))
			modules.add(m.trim());
		Path out = dir.resolve("result").resolve(new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
		Files.createDirectories(out.resolve("evidence"));
		StringBuilder sum = new StringBuilder();
		sum.append("case ").append(caseId).append(": ").append(p.getProperty("case.title", "")).append('\n');
		sum.append("run ").append(new Date()).append('\n');

		// vendors: every system_id may bind, the listed ones are waited for
		List<String> listed = new ArrayList<>();
		for (String sys : p.getProperty("vendors", "").split(",")) {
			sys = sys.trim();
			if (sys.isEmpty()) continue;
			listed.add(sys);
			vendors.put(sys, new Vendor(sys, p));
		}
		vendorServer(Integer.parseInt(p.getProperty("vendor.port")));
		long bindWait = Long.parseLong(p.getProperty("vendor.bindWaitMs", "120000"));
		int minLinks = Integer.parseInt(p.getProperty("vendor.minLinks", "1"));
		boolean bound = waitFor(() -> listed.stream().allMatch(v -> vendors.get(v).openLinks() >= vendors.get(v).minLinks), bindWait);
		StringBuilder vb = new StringBuilder();
		for (String v : listed) vb.append(v).append('=').append(vendors.get(v).openLinks()).append(" bind(s) ");
		sum.append("vendors: ").append(vb).append('\n');
		if (!bound) {
			sum.append("RESULT: ERROR, not every vendor bound in ").append(bindWait / 1000).append("s\n");
			Files.write(out.resolve("summary.txt"), sum.toString().getBytes(StandardCharsets.UTF_8));
			System.out.print(sum);
			return false;
		}

		List<Spec> specs = specs(p, "mt.", 1, caseId, runId);
		List<Spec> specs2 = specs(p, "mt2.", 2, caseId, runId);
		List<Spec> all = new ArrayList<>(specs); all.addAll(specs2);
		for (Spec s : all) customers.computeIfAbsent(s.customer, Customer::new);
		for (Customer c : customers.values())                               // customer.{user}.port: another Server
			c.bind(p.getProperty("server.host"), Integer.parseInt(p.getProperty("customer." + c.user + ".port", p.getProperty("server.port")).trim()), p.getProperty("customer.password"));
		if (all.isEmpty()) Thread.sleep(Long.parseLong(p.getProperty("mt.settleMs", "5000")));

		if (p.containsKey("soak.durationSec")) return runSoak(dir, p, specs, out, sum);
		send(specs);
		if (!specs2.isEmpty() || p.containsKey("phase.2.afterMs")) {
			Thread.sleep(Long.parseLong(p.getProperty("phase.2.afterMs", "0").trim()));
			sum.append(phase2(dir, p));
			send(specs2);
		}

		// wait: every MT has its DR (or waited its waitMs); meanwhile the msgIds and the DB rows
		List<Copy> copies = new ArrayList<>();
		for (Spec s : all) copies.addAll(s.copies);
		long lastPoll = 0;
		while (true) {
			long now = System.currentTimeMillis();
			boolean done = true;
			for (Copy c : copies) if (!finished(c, now)) done = false;
			if (now - lastPoll > 5000) {
				lastPoll = now;
				resolveMsgIds(copies);
				pollDb(copies);
			}
			if (done) break;
			Thread.sleep(200);
		}
		Thread.sleep(Long.parseLong(p.getProperty("mt.settleMs", "3000")));   // logs and CDRs are written asynchronously
		resolveMsgIds(copies);
		collectEvidence(copies, startedAt);

		// checks
		int ok = 0;
		StringBuilder csv = new StringBuilder("mt,sender,recipient,customer,submit_resp,inMsgId,msgId,expect_path,path,expect_dr,dr,result,reasons\n");
		for (Spec s : all) {
			for (Copy c : s.copies) {
				check(c);
				csv.append(c.label).append(',').append(s.sender).append(',').append(s.recipient).append(',').append(s.customer).append(',')
					.append(c.segs.isEmpty() ? "" : c.segs.get(0).respStatus).append(',').append(c.segs.isEmpty() ? "" : c.segs.get(0).inMsgId).append(',')
					.append(c.msgId()).append(",\"").append(s.path).append("\",\"").append(String.join(">", c.path)).append("\",").append(s.dr).append(',')
					.append(stat(c.drText)).append(',').append(c.fails.isEmpty() ? "PASS" : "FAIL").append(",\"")
					.append(String.join("; ", c.fails).replace("\"", "'")).append("\"\n");
				writeEvidence(out, c);
			}
			if (s.ratio != null) checkRatio(s);
			if (s.drRatio != null) checkDrRatio(s);
			checkBinds(s);
			int bad = 0;
			for (Copy c : s.copies) if (!c.fails.isEmpty()) bad++;
			if (bad > 0) s.fails.add(0, bad + "/" + s.copies.size() + " MT(s) failed: " + String.join("; ", firstFails(s)));
			if (s.fails.isEmpty()) ok++;
			Copy c0 = s.copies.get(0);
			sum.append(String.format("%-4s %-9s %-15s expect %-22s got %-22s dr=%-8s %s%s%n", s.label, s.sender, s.recipient, s.path,
					s.count == 1 ? String.join(">", c0.path) : distribution(s) + (drStates.containsKey(s) ? " DR" + drStates.get(s) : ""), stat(c0.drText), s.fails.isEmpty() ? "PASS" : "FAIL ",
					String.join("; ", s.fails)));
			if (bindStats.containsKey(s)) sum.append("     binds: ").append(bindStats.get(s)).append('\n');
			for (String l : c0.evidence) {
				if (!l.contains("[MT-ROUTE]") && !l.contains("[MT-REROUTE]")) continue;
				int i = l.indexOf("[MT-");
				sum.append("     ").append(l.substring(i).trim()).append('\n');
			}
		}
		List<String> caseChecks = caseChecks(p);
		int caseOk = 0;
		for (String c : caseChecks) if (c.startsWith("PASS")) caseOk++;
		if (!caseChecks.isEmpty()) {
			sum.append("case checks:\n");
			for (String c : caseChecks) sum.append("  ").append(c).append('\n');
		}
		boolean pass = ok == all.size() && caseOk == caseChecks.size();
		sum.append("RESULT: ").append(pass ? "PASS" : "FAIL").append(" (").append(ok).append("/").append(all.size()).append(" MTs");
		if (!caseChecks.isEmpty()) sum.append(", ").append(caseOk).append("/").append(caseChecks.size()).append(" case checks");
		sum.append(")\n");
		Files.write(out.resolve("summary.txt"), sum.toString().getBytes(StandardCharsets.UTF_8));
		Files.write(out.resolve("messages.csv"), csv.toString().getBytes(StandardCharsets.UTF_8));
		System.out.print(sum);
		System.out.println("result: " + out);
		for (Customer c : customers.values()) try { c.cs.close(); } catch (IOException e) { }
		return pass;
	}

	void send(List<Spec> specs) throws Exception {
		boolean first = true;
		for (Spec s : specs) {
			for (Copy c : s.copies) {
				if (!first) Thread.sleep(c == s.copies.get(0) ? s.after : Math.min(s.after, 50));
				first = false;
				c.sentAt = System.currentTimeMillis();
				customers.get(s.customer).submit(c, s.parts);
			}
		}
	}

	/** the files of phase2/ in place, then the reload command */
	String phase2(Path dir, Properties p) throws Exception {
		StringBuilder b = new StringBuilder("phase 2 at ").append(new Date()).append(':');
		for (String[] d : new String[][] { { "phase2/routing", "conf/routing" }, { "phase2/conf", "conf" } }) {
			Path from = dir.resolve(d[0]);
			if (!Files.isDirectory(from)) continue;
			try (DirectoryStream<Path> ds = Files.newDirectoryStream(from)) {
				for (Path f : ds) {
					Files.copy(f, Paths.get(home, d[1], f.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
					b.append(' ').append(d[1]).append('/').append(f.getFileName());
				}
			}
		}
		Path ga = dir.resolve("phase2/gmms.append/GmmsConfig.properties");
		if (Files.exists(ga)) {
			Files.write(Paths.get(home, "Gmms", "GmmsConfig.properties"), Files.readAllBytes(ga), StandardOpenOption.APPEND);
			b.append(" Gmms/GmmsConfig.properties (appended)");
		}
		String cmd = p.getProperty("phase.2.reload", "").trim();
		if (!cmd.isEmpty()) {
			b.append("; PUBLISH channel:gmms:control ").append(cmd).append(" -> ").append(String.join(" ", redis("PUBLISH", "channel:gmms:control", cmd)));
			Thread.sleep(Long.parseLong(p.getProperty("phase.2.settleMs", "5000")));
		}
		return b.append('\n').toString();
	}

	boolean finished(Copy c, long now) {
		if (c.sentAt == 0) return false;
		String dr = c.spec.dr;
		if (c.spec.drCount == null && !dr.isEmpty() && !"none".equalsIgnoreCase(dr) && drOf(c) != null) return true;
		return now > c.sentAt + c.spec.waitMs;
	}

	String drOf(Copy c) {
		String[] d = drPdu(c);
		return d == null ? null : d[0];
	}

	List<String[]> drPdus(Copy c) {
		Customer cu = customers.get(c.spec.customer);
		List<String[]> out = new ArrayList<>();
		for (String[] d : cu.drs) for (Seg g : c.segs)
			if (g.inMsgId != null && !g.inMsgId.isEmpty() && d[0].contains("id:" + g.inMsgId + " ")) { out.add(d); break; }
		return out;
	}

	String[] drPdu(Copy c) {
		Customer cu = customers.get(c.spec.customer);
		for (Seg g : c.segs) {
			if (g.inMsgId == null || g.inMsgId.isEmpty()) continue;
			for (String[] d : cu.drs) if (d[0].contains("id:" + g.inMsgId + " ")) return d;
		}
		return null;
	}

	/** msgIds from the Server's [SUBMIT-IN] lines of the customer's ids */
	void resolveMsgIds(List<Copy> copies) throws Exception {
		Map<String, Copy> byIn = new HashMap<>();
		for (Copy c : copies) for (Seg g : c.segs) if (g.inMsgId != null && !g.inMsgId.isEmpty()) byIn.put(g.inMsgId, c);
		if (byIn.isEmpty()) return;
		List<String> pats = new ArrayList<>();
		for (String id : byIn.keySet()) pats.add("inMsgId=" + id + " ");
		List<String> files = new ArrayList<>();
		for (String m : modules) if (m.startsWith("MultiSmppServer")) files.addAll(logFiles(m));
		for (String l : grep(pats, files)) {
			if (!l.contains("[SUBMIT-IN]")) continue;
			Matcher m = Pattern.compile("inMsgId=(\\S+) ").matcher(l);
			if (!m.find()) continue;
			Copy c = byIn.get(m.group(1));
			if (c == null) continue;
			String[] w = l.trim().split("\\s+");
			for (int i = 0; i + 1 < w.length; i++) if (w[i + 1].equals("[SUBMIT-IN]")) synchronized (c) { c.msgIds.add(w[i]); }
		}
	}

	/** db=table:Col=Val,Col=Val: seen once while waiting */
	void pollDb(List<Copy> copies) throws Exception {
		for (Copy c : copies) {
			if (c.spec.db == null || c.dbSeen != null || c.msgId() == null) continue;
			String[] t = c.spec.db.split(":", 2);
			List<String> cols = new ArrayList<>(), want = new ArrayList<>();
			for (String kv : t[1].split(",")) { String[] x = kv.split("=", 2); cols.add(x[0].trim()); want.add(x[1].trim()); }
			for (String row : db(t[0].trim(), String.join(",", cols), c.msgId())) {
				if (Arrays.asList(row.split("\t", -1)).equals(want)) c.dbSeen = t[0] + ": " + row;
			}
		}
	}

	void collectEvidence(List<Copy> copies, long startedAt) throws Exception {
		List<String> ids = new ArrayList<>();
		for (Copy c : copies) ids.addAll(c.msgIds);
		// grep per module to know where each line comes from
		Map<String, List<String>> lines = new LinkedHashMap<>();
		for (String m : modules) lines.put(m, grep(ids, logFiles(m)));
		List<String> cdrFiles = new ArrayList<>();
		for (Path d : new Path[] { Paths.get(home, "cdr"), Paths.get(home, "cdr", "link") }) {
			if (!Files.isDirectory(d)) continue;
			try (DirectoryStream<Path> ds = Files.newDirectoryStream(d)) {
				for (Path f : ds) if (Files.isRegularFile(f) && Files.getLastModifiedTime(f).toMillis() >= startedAt - 60000) cdrFiles.add(f.toString());
			}
		}
		List<String> cdrLines = grep(ids, cdrFiles);
		Comparator<String> byTime = Comparator.comparing(l -> ts(l.substring(l.indexOf(' ') + 1)));
		for (Copy c : copies) {
			for (Map.Entry<String, List<String>> e : lines.entrySet())
				for (String l : e.getValue()) for (String id : c.msgIds) if (l.contains(id)) { c.evidence.add(e.getKey() + " " + l); break; }
			c.evidence.sort(byTime);   // the MT's way through Server, Core, Client in time order
			for (String l : cdrLines) for (String id : c.msgIds) if (l.contains("\"" + id)) { c.cdr.add(l); break; }
			c.path = pathOf(c);
		}
	}

	/** the vendors the MT went to, in time order (long MTs: one attempt per vendor and UDH reference) */
	List<String> pathOf(Copy c) {
		List<Submit> subs = new ArrayList<>();
		for (Vendor v : vendors.values()) for (Submit s : v.submits) {
			if (byDest(c) ? s.dest.equals(c.spec.recipient) && s.at >= c.sentAt : c.marker.equals(s.marker)) subs.add(s);
		}
		subs.sort(Comparator.comparingLong(s -> s.at));
		List<String> path = new ArrayList<>();
		Set<String> attempts = new HashSet<>();
		for (Submit s : subs) {
			if (byDest(c) && s.udhRef >= 0 && !attempts.add(s.vendor + ":" + s.udhRef)) continue;
			path.add(s.vendor);
		}
		return path;
	}

	static boolean byDest(Copy c) { return c.spec.parts > 1 || c.spec.matchDest; }

	/** the submit_sm of the last attempt of a copy (all UDH segments of it), in segment order */
	List<Submit> lastAttempt(Copy c) {
		List<Submit> subs = new ArrayList<>();
		for (Vendor v : vendors.values()) for (Submit x : v.submits)
			if (byDest(c) ? x.dest.equals(c.spec.recipient) && x.at >= c.sentAt : c.marker.equals(x.marker)) subs.add(x);
		subs.sort(Comparator.comparingLong(x -> x.at));
		if (subs.isEmpty()) return subs;
		Submit last = subs.get(subs.size() - 1);
		List<Submit> out = new ArrayList<>();
		for (Submit x : subs) if (x.vendor.equals(last.vendor) && (last.udhRef < 0 ? x == last : x.udhRef == last.udhRef)) out.add(x);
		out.sort(Comparator.comparingInt(x -> x.udhSeq));
		return out;
	}

	void check(Copy c) {
		Spec s = c.spec;
		for (Seg g : c.segs) if (g.respStatus == null || g.respStatus != 0) { c.fails.add("submit_sm_resp " + g.respStatus); break; }
		if (c.msgId() == null) c.fails.add("msgId not found in the Server log");
		// path
		String exp = s.path;
		if (s.ratio != null || (s.count > 1 && !exp.contains(">"))) exp = "(" + exp + ")";
		if (!"*".equals(s.path.trim()) && !pathMatches(exp, c.path)) c.fails.add("path " + (c.path.isEmpty() ? "none" : String.join(">", c.path)) + ", expected " + s.path);
		// Core log lines
		if (!s.logs.isEmpty()) {
			for (String part : s.logs.split("&&")) {
				String t = part.trim();
				if (t.isEmpty()) continue;
				boolean not = t.startsWith("!");
				if (not) t = t.substring(1);
				String mod = "CoreEngine";                                      // {MultiSmppClient}...: lines of these modules
				if (t.startsWith("{") && t.indexOf('}') > 0) { mod = t.substring(1, t.indexOf('}')); t = t.substring(t.indexOf('}') + 1); }
				Pattern re = t.startsWith("~") ? Pattern.compile(t.substring(1)) : null;   // ~regex
				boolean found = false;
				for (String l : c.evidence) if (l.startsWith(mod) && (re != null ? re.matcher(l).find() : l.contains(t))) { found = true; break; }
				if (!found && !not) c.fails.add("no Core log \"" + t + "\"");
				if (found && not) c.fails.add("Core log has \"" + t + "\"");
			}
		}
		// DR
		String[] pdu = drPdu(c);
		c.drText = pdu == null ? null : pdu[0];
		c.drTlv = pdu == null ? null : pdu[1];
		String got = stat(c.drText);
		List<String[]> all = drPdus(c);
		if (s.drCount != null && all.size() != s.drCount) c.fails.add(all.size() + " DR(s) to the customer, expected " + s.drCount);
		if (s.drDelayMs != null && pdu != null) {
			String[] r = s.drDelayMs.split(",");
			long d = Long.parseLong(pdu[2]) - c.sentAt;
			if (d < Long.parseLong(r[0].trim()) || d > Long.parseLong(r[r.length - 1].trim())) c.fails.add("DR after " + d + "ms, expected " + s.drDelayMs + "ms");
		}
		if (s.drErr != null && c.drText != null && !s.drErr.trim().equals(field(c.drText, "err:")))
			c.fails.add("DR err:" + field(c.drText, "err:") + ", expected " + s.drErr.trim());
		if (s.drTlv != null && c.drText != null) {
			String want = s.drTlv.trim();
			String have = c.drTlv == null ? "none" : c.drTlv;
			if (!want.equalsIgnoreCase(have)) c.fails.add("DR network_error_code " + have + ", expected " + want);
		}
		if ("none".equalsIgnoreCase(s.dr)) { if (c.drText != null) c.fails.add("DR " + got + ", expected none"); }
		else if (s.dr.startsWith("!")) { if (c.drText == null || got.equalsIgnoreCase(s.dr.substring(1))) c.fails.add("DR " + got + ", expected " + s.dr); }
		else if (!s.dr.isEmpty() && s.drRatio == null && !s.dr.equalsIgnoreCase(got)) c.fails.add("DR " + got + ", expected " + s.dr);
		else if (s.drRatio != null && c.drText == null) c.fails.add("no DR to the customer");
		// CDR
		if (!c.path.isEmpty() && c.cdr.stream().noneMatch(l -> cdrType(l).equals("OUT_SUBMIT"))) c.fails.add("no OUT_SUBMIT CDR");
		if (s.cdr != null && c.msgId() != null) {
			List<String> left = new ArrayList<>(c.cdr);
			for (String e : s.cdr.split(";")) {
				String[] kv = e.trim().split("=", 2);
				String v = kv[1].trim();
				Pattern re = null;                                               // {id}~regex: and the line matches it
				int ti = v.indexOf('~');
				if (ti >= 0) { re = Pattern.compile(v.substring(ti + 1)); v = v.substring(0, ti); }
				String[] is = v.split("@", 2);                                   // {id}@10400: and this status
				String id = "\"" + is[0].replace("{id}", c.msgId()) + "\"";
				String st = is.length > 1 ? "," + is[1].trim() + "," : null;
				String hit = null;
				for (String l : left) if (cdrType(l).equals(kv[0].trim()) && l.contains(id) && (st == null || l.contains(st)) && (re == null || re.matcher(l).find())) { hit = l; break; }
				if (hit == null) c.fails.add("no CDR " + kv[0].trim() + " with MsgID " + id + (st == null ? "" : " and status " + is[1].trim()) + (re == null ? "" : " matching " + re));
				else left.remove(hit);
			}
		}
		if (s.db != null && c.dbSeen == null) c.fails.add("DB row " + s.db + " not seen");
		if (s.drEachSeg) {                                                 // every segment sent: exactly one DR with its id
			Customer cu = customers.get(s.customer);
			for (Seg g : c.segs) {
				int n = 0;
				for (String[] d : cu.drs) if (g.inMsgId != null && d[0].contains("id:" + g.inMsgId + " ")) n++;
				if (n != 1) c.fails.add("segment " + g.inMsgId + " got " + n + " DR(s), expected 1");
			}
		}
		if (s.cdrCount != null) {                                          // IN_SUBMIT_PARTIAL:3,OUT_SUBMIT:1: lines of each type
			for (String e : s.cdrCount.split(",")) {
				String[] kv = e.trim().split(":");
				long n = c.cdr.stream().filter(l -> cdrType(l).equals(kv[0].trim())).count();
				if (n != Long.parseLong(kv[1].trim())) c.fails.add(n + " " + kv[0].trim() + " CDR(s), expected " + kv[1].trim());
			}
		}
		// what the vendor got: data_coding, segment sizes, the text decoded
		if (s.segLens != null || s.vendorDc != null || s.vendorText != null) {
			List<Submit> segs = lastAttempt(c);
			List<String> lens = new ArrayList<>();
			Set<Integer> dcs = new TreeSet<>();
			StringBuilder t = new StringBuilder();
			for (Submit x : segs) {
				lens.add(String.valueOf(x.sm.length));
				dcs.add(x.dc);
				t.append(decode(x, s.vendorEnc));
			}
			c.vendorGot = "dc=" + dcs + " segLens=" + String.join(",", lens) + " text=" + t;
			if (s.segLens != null && !String.join(",", lens).equals(s.segLens.replace(" ", ""))) c.fails.add("vendor segments " + lens + ", expected " + s.segLens);
			if (s.vendorDc != null && !(dcs.size() == 1 && dcs.contains(Integer.decode(s.vendorDc.trim())))) c.fails.add("vendor data_coding " + dcs + ", expected " + s.vendorDc);
			if (s.vendorText != null) {
				String want = "same".equals(s.vendorText.trim()) ? c.text                         // fold: é -> e
						: "fold".equals(s.vendorText.trim()) ? java.text.Normalizer.normalize(c.text, java.text.Normalizer.Form.NFD).replaceAll("\\p{M}", "")
						: "ascii?".equals(s.vendorText.trim()) ? c.text.replaceAll("[^\\x00-\\x7F]", "?")   // what an ASCII encoder writes
						: s.vendorText.replace("{text}", c.text);
				if (!t.toString().equals(want)) c.fails.add("vendor text differs: " + t);
			}
		}
		// segments of a long MT
		if (s.segments != null) {
			for (String e : s.segments.split(",")) {
				String[] kv = e.trim().split(":");
				Vendor v = vendors.get(kv[0].trim());
				List<Submit> segs = new ArrayList<>();
				if (v != null) for (Submit x : v.submits) if (x.udhRef >= 0 && x.dest.equals(s.recipient) && x.at >= c.sentAt && x.status == 0) segs.add(x);
				if (segs.size() != Integer.parseInt(kv[1].trim())) { c.fails.add(kv[0] + " got " + segs.size() + " segment(s), expected " + kv[1]); continue; }
				segs.sort(Comparator.comparingInt(x -> x.udhSeq));
				StringBuilder t = new StringBuilder();
				for (Submit x : segs) t.append(x.text);
				if (!t.toString().equals(c.text)) c.fails.add(kv[0] + " segments joined differ from the MT text");
			}
		}
	}

	static boolean pathMatches(String exp, List<String> got) {
		if ("none".equalsIgnoreCase(exp.trim())) return got.isEmpty();
		String[] steps = exp.split(">");
		if (steps.length != got.size()) return false;
		for (int i = 0; i < steps.length; i++) {
			Set<String> alt = new HashSet<>();
			for (String a : steps[i].replace("(", "").replace(")", "").split("\\|")) alt.add(a.trim().toLowerCase());
			if (!alt.contains(got.get(i).toLowerCase())) return false;
		}
		return true;
	}

	/** ratio=v1:70,v2:30,tolerance=10 on the last vendor of each copy */
	void checkRatio(Spec s) {
		Map<String, Integer> n = new HashMap<>();
		for (Copy c : s.copies) if (!c.path.isEmpty()) n.merge(c.path.get(c.path.size() - 1), 1, Integer::sum);
		double tol = 0;
		Map<String, Double> want = new LinkedHashMap<>();
		for (String e : s.ratio.split(",")) {
			String[] kv = e.trim().split("[:=]");
			if (kv[0].trim().equals("tolerance")) tol = Double.parseDouble(kv[1].trim());
			else want.put(kv[0].trim(), Double.parseDouble(kv[1].trim()));
		}
		for (Map.Entry<String, Double> e : want.entrySet()) {
			double pct = 100.0 * n.getOrDefault(e.getKey(), 0) / s.copies.size();
			if (Math.abs(pct - e.getValue()) > tol + 1e-9) s.fails.add(String.format("%s %.1f%%, expected %.1f%% +-%.1f", e.getKey(), pct, e.getValue(), tol));
		}
	}

	/** drRatio=DELIVRD:90,tolerance=10 on the DR of each copy */
	void checkDrRatio(Spec s) {
		Map<String, Integer> n = new HashMap<>();
		for (Copy c : s.copies) n.merge(stat(c.drText), 1, Integer::sum);
		double tol = 0;
		Map<String, Double> want = new LinkedHashMap<>();
		for (String e : s.drRatio.split(",")) {
			String[] kv = e.trim().split("[:=]");
			if (kv[0].trim().equals("tolerance")) tol = Double.parseDouble(kv[1].trim());
			else want.put(kv[0].trim(), Double.parseDouble(kv[1].trim()));
		}
		for (Map.Entry<String, Double> e : want.entrySet()) {
			double pct = 100.0 * n.getOrDefault(e.getKey(), 0) / s.copies.size();
			if (Math.abs(pct - e.getValue()) > tol + 1e-9) s.fails.add(String.format("DR %s %.1f%%, expected %.1f%% +-%.1f", e.getKey(), pct, e.getValue(), tol));
		}
		drStates.put(s, n.toString());
	}

	final Map<Spec, String> drStates = new HashMap<>();
	final Map<Spec, String> bindStats = new HashMap<>();

	/**
	 * bindMax1s / bindMax100ms: the most submit_sm of the spec's MTs one bind got in any 1 s / 100 ms; spanMs=min,max:
	 * first to last submit_sm; bindsUsed: at least this many binds got some
	 */
	void checkBinds(Spec s) {
		Map<String, List<Long>> byLink = new TreeMap<>();
		long first = Long.MAX_VALUE, last = 0;
		for (Copy c : s.copies) for (Vendor v : vendors.values()) for (Submit x : v.submits) {
			if (!c.marker.equals(x.marker)) continue;
			byLink.computeIfAbsent(x.vendor + "#" + x.link, k -> new ArrayList<>()).add(x.at);
			first = Math.min(first, x.at); last = Math.max(last, x.at);
		}
		if (byLink.isEmpty()) return;
		int max1s = 0, max100 = 0;
		StringBuilder b = new StringBuilder();
		for (Map.Entry<String, List<Long>> e : byLink.entrySet()) {
			List<Long> t = e.getValue();
			Collections.sort(t);
			int m1 = maxIn(t, 1000), m2 = maxIn(t, 100);
			max1s = Math.max(max1s, m1); max100 = Math.max(max100, m2);
			b.append(b.length() == 0 ? "" : " ").append(e.getKey()).append('=').append(t.size()).append("(1s<=").append(m1).append(",100ms<=").append(m2).append(')');
		}
		b.append(" span=").append(last - first).append("ms");
		bindStats.put(s, b.toString());
		if (s.bindMax1s != null && max1s > Integer.parseInt(s.bindMax1s.trim())) s.fails.add("a bind got " + max1s + " in 1 s, expected <= " + s.bindMax1s);
		if (s.bindMax100ms != null && max100 > Integer.parseInt(s.bindMax100ms.trim())) s.fails.add("a bind got " + max100 + " in 100 ms, expected <= " + s.bindMax100ms);
		if (s.bindsUsed != null && byLink.size() < Integer.parseInt(s.bindsUsed.trim())) s.fails.add(byLink.size() + " bind(s) used, expected >= " + s.bindsUsed);
		if (s.spanMs != null) {
			String[] r = s.spanMs.split(",");
			long span = last - first;
			if (span < Long.parseLong(r[0].trim()) || span > Long.parseLong(r[r.length - 1].trim())) s.fails.add("sent in " + span + " ms, expected " + s.spanMs + " ms");
		}
	}

	static int maxIn(List<Long> sorted, long window) {
		int best = 0;
		for (int i = 0, j = 0; i < sorted.size(); i++) {
			while (sorted.get(i) - sorted.get(j) >= window) j++;
			best = Math.max(best, i - j + 1);
		}
		return best;
	}

	static String distribution(Spec s) {
		Map<String, Integer> n = new TreeMap<>();
		for (Copy c : s.copies) n.merge(c.path.isEmpty() ? "none" : String.join(">", c.path), 1, Integer::sum);
		StringBuilder b = new StringBuilder();
		for (Map.Entry<String, Integer> e : n.entrySet()) b.append(b.length() == 0 ? "" : ",").append(e.getKey()).append(':').append(e.getValue());
		return b.toString();
	}

	static List<String> firstFails(Spec s) {
		Set<String> out = new LinkedHashSet<>();
		for (Copy c : s.copies) { out.addAll(c.fails); if (out.size() >= 3) break; }
		return new ArrayList<>(out);
	}

	static String cdrType(String l) {
		String[] f = l.split(",", 3);
		return f.length > 1 ? f[1] : "";
	}

	static String stat(String receipt) {
		return receipt == null ? "none" : field(receipt, "stat:");
	}

	static String field(String receipt, String name) {
		int i = receipt.indexOf(name);
		if (i < 0) return "?";
		int e = receipt.indexOf(' ', i);
		return receipt.substring(i + name.length(), e < 0 ? receipt.length() : e);
	}

	void writeEvidence(Path out, Copy c) throws IOException {
		StringBuilder ev = new StringBuilder();
		Spec s = c.spec;
		ev.append(c.label).append(": ").append(s.sender).append(" -> ").append(s.recipient).append(" customer=").append(s.customer).append(" text=").append(c.text).append('\n');
		ev.append("expect: path=").append(s.path).append(" log=").append(s.logs).append(" dr=").append(s.dr);
		if (s.cdr != null) ev.append(" cdr=").append(s.cdr);
		if (s.db != null) ev.append(" db=").append(s.db);
		ev.append('\n');
		for (Seg g : c.segs) ev.append("inMsgId=").append(g.inMsgId).append(" submit_resp=").append(g.respStatus).append('\n');
		ev.append("msgId=").append(c.msgIds).append('\n');
		ev.append("vendors, in order:\n");
		for (Vendor v : vendors.values()) for (Submit x : v.submits) {
			boolean mine = byDest(c) ? x.dest.equals(s.recipient) && x.at >= c.sentAt : c.marker.equals(x.marker);
			if (mine) ev.append("  ").append(new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(x.at))).append(' ').append(x.vendor)
					.append(" bind#").append(x.link).append(" submit_sm_resp=0x").append(Integer.toHexString(x.status)).append(" vendorMsgId=").append(x.vendorMsgId)
					.append(x.udhRef >= 0 ? " udh=" + x.udhRef + ":" + x.udhSeq + "/" + x.udhTotal : "").append(" dc=").append(x.dc)
					.append(" sm=").append(x.sm == null ? "" : toHex(x.sm, 0, x.sm.length)).append('\n');
		}
		for (String[] d : drPdus(c)) ev.append("DR to the customer after ").append(Long.parseLong(d[2]) - c.sentAt).append("ms: ").append(d[0]).append('\n');
		ev.append("first DR to the customer: ").append(c.drText == null ? "none" : c.drText).append(c.drTlv == null ? "" : " network_error_code=" + c.drTlv).append('\n');
		if (c.dbSeen != null) ev.append("DB row seen: ").append(c.dbSeen).append('\n');
		if (c.vendorGot != null) ev.append("vendor got (last attempt): ").append(c.vendorGot).append('\n');
		ev.append("customer sent: enc=").append(s.enc).append(" bytes=").append(toHex(encode(c), 0, encode(c).length)).append('\n');
		ev.append("result: ").append(c.fails.isEmpty() ? "PASS" : "FAIL " + String.join("; ", c.fails)).append("\n\n--- module log lines ---\n");
		for (String l : c.evidence) ev.append(l).append('\n');
		ev.append("\n--- CDR lines ---\n");
		for (String l : c.cdr) ev.append(l).append('\n');
		Files.write(out.resolve("evidence").resolve(c.label + "_" + (c.msgId() == null ? "nomsgid" : c.msgId()) + ".log"), ev.toString().getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * check.metric.N=modules|name|field|min|max: a [METRICS-*] line since the start has the metric whose name ends with
	 * name (timer: field count / avg / p50 / p95 / p99 / max; counter, gauge: field value) in [min, max]
	 */
	String metricCheck(String[] f, String sinceTs, String what) throws Exception {
		List<String> files = new ArrayList<>();
		for (String m : f[0].split(",")) files.addAll(logFiles(m.trim()));
		Pattern key = Pattern.compile("\"([^\"]*[.]" + Pattern.quote(f[1].trim()) + ")\":(\\{[^}]*\\}|[-0-9.]+)");
		String field = f[2].trim();
		double min = Double.parseDouble(f[3].trim()), max = Double.parseDouble(f[4].trim());
		List<String> seen = new ArrayList<>();
		for (String l : grep(Collections.singletonList(f[1].trim()), files)) {
			if (!l.contains("[METRICS-") || ts(l).compareTo(sinceTs) < 0) continue;
			Matcher m = key.matcher(l);
			while (m.find()) {
				String v = m.group(2);
				Double d = null;
				if (v.startsWith("{")) {
					Matcher x = Pattern.compile("\"" + Pattern.quote(field) + "\":([-0-9.]+)").matcher(v);
					if (x.find()) d = Double.parseDouble(x.group(1));
				} else if (field.equals("value")) d = Double.parseDouble(v);
				if (d == null) continue;
				seen.add(m.group(1) + "=" + d);
				if (d >= min && d <= max) return "PASS " + what + "\n      " + ts(l) + " " + m.group(1) + " " + field + "=" + d;
			}
		}
		return "FAIL " + what + ": " + (seen.isEmpty() ? "no such metric since the start" : "values " + String.join(", ", seen.subList(Math.max(0, seen.size() - 6), seen.size())));
	}

	/** check.log.N, check.redis.N, check.redisAbsent.N, check.metric.N, check.redisCmd.N, check.cmd.N */
	List<String> caseChecks(Properties p) throws Exception {
		TreeMap<String, String> checks = new TreeMap<>(Comparator.comparing((String k) -> k.replaceAll("\\d+$", ""))
				.thenComparingInt(k -> Integer.parseInt(k.replaceAll("^.*\\.", ""))));
		for (String k : p.stringPropertyNames()) if (k.matches("check\\.(log|redis|redisAbsent|metric|redisCmd|cmd)\\.\\d+")) checks.put(k, p.getProperty(k));
		String sinceTs = new SimpleDateFormat("yyMMdd HH:mm:ss,SSS").format(new Date(since));
		List<String> out = new ArrayList<>();
		for (Map.Entry<String, String> e : checks.entrySet()) {
			String[] f = e.getValue().split("\\|");
			String what = e.getKey() + "=" + e.getValue(), r;
			if (e.getKey().startsWith("check.cmd.") || e.getKey().startsWith("check.redisCmd.")) {
				String[] cw = e.getValue().split(" => ", 2);
				List<String> x = e.getKey().startsWith("check.cmd.") ? exec(Arrays.asList("bash", "-c", cw[0]), cmdEnv())
						: redis(cw[0].trim().split("\\s+"));
				String v = String.join("\n", x).trim();
				String want = cw.length > 1 ? cw[1].trim() : "";
				boolean ok = want.startsWith("=") ? v.equals(want.substring(1)) : v.contains(want);
				r = (ok ? "PASS " : "FAIL ") + what + "\n      -> " + (v.length() > 600 ? v.substring(0, 600) + " ..." : v).replace("\n", "\n         ");
			} else if (e.getKey().startsWith("check.metric.")) {
				r = metricCheck(f, sinceTs, what);
			} else if (e.getKey().startsWith("check.log.")) {
				List<String> hits = new ArrayList<>();
				List<String> files = new ArrayList<>();
				for (String m : f[0].split(",")) files.addAll(logFiles(m.trim()));
				for (String l : grep(Collections.singletonList(f[1]), files)) {
					boolean all = ts(l).compareTo(sinceTs) >= 0;
					for (int i = 2; i < f.length; i++) all &= l.contains(f[i]);
					if (all) hits.add(l);
				}
				r = hits.isEmpty() ? "FAIL " + what + ": no such line" : "PASS " + what + "\n      " + hits.get(0).trim();
			} else if (e.getKey().startsWith("check.redisAbsent.")) {
				List<String> x = redis("EXISTS", f[0].trim());
				r = x.size() == 1 && x.get(0).trim().equals("0") ? "PASS " + what : "FAIL " + what + ": EXISTS " + x;
			} else {
				List<String> x = redis("HGET", f[0].trim(), f[1].trim());
				String v = String.join("\n", x);
				r = (v.contains(f[2]) ? "PASS " : "FAIL ") + what + "\n      HGET -> " + v;
			}
			out.add(r);
		}
		return out;
	}
}
