package rt;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The high-TPS consistency test (docs/高TPS一致性和内存测试设计.md): follows the pcap files tcpdump rolls over
 * (cap_{side}.pcap, cap_{side}.pcap1, ...: one sequence per tcpdump), puts the TCP streams of the Server ports (customer side) and the vendor port (Client side)
 * back together, decodes the SMPP PDUs and counts them per MT (the marker "{case}-S{n}" in the text); a finished file is
 * deleted (-Dflowcheck.keep=true: kept). After the stop file: reads the CDRs of the run and compares them with the capture, MT by MT and ID by ID.
 *
 *   FlowCheck pcapDir stopFile markerPrefix maxMTs serverPorts vendorPorts cdrDirs sinceMs outDir [events]
 *
 * events: also write every decoded PDU as a line (for a short trial run).
 */
public final class FlowCheck {
	// ---------------------------------------------------------------- counters per MT
	static final int P_CS = 0, P_CSR_OK = 1, P_CSR_ERR = 2, P_VS = 3, P_VSR_OK = 4, P_VSR_ERR = 5, P_VDR = 6, P_CDR = 7, P_CDRR = 8, P_VDRR = 9,
			C_IS = 10, C_ISP = 11, C_OS_OK = 12, C_OS_ERR = 13, C_IDR = 14, C_ODR = 15, NCOUNT = 16;
	static final String[] CNAME = { "cust submit_sm", "cust submit_sm_resp ok", "cust submit_sm_resp err", "vendor submit_sm", "vendor submit_sm_resp ok",
			"vendor submit_sm_resp err", "vendor DR", "DR to cust", "cust deliver_sm_resp", "client deliver_sm_resp",
			"CDR IN_SUBMIT", "CDR IN_SUBMIT_PARTIAL", "CDR OUT_SUBMIT ok", "CDR OUT_SUBMIT failed", "CDR IN_DELI_REP", "CDR OUT_DELI_REP" };
	final byte[][] cnt = new byte[NCOUNT][];
	final byte[] tmpl;
	final int max;
	final byte[] prefix;
	final Set<Integer> serverPorts = new HashSet<>(), vendorPorts = new HashSet<>();

	/** ID lists, compared as multisets: capture vs CDR */
	static final class LongList {
		long[] a = new long[1 << 16]; int n;
		void add(long v) { if (n == a.length) a = Arrays.copyOf(a, a.length * 2); a[n++] = v; }
		long[] sorted() { long[] s = Arrays.copyOf(a, n); Arrays.sort(s); return s; }
	}
	final LongList capInId = new LongList(), capDrToCustId = new LongList(), capVendorId = new LongList(), capVendorDrId = new LongList();
	final LongList cdrInId = new LongList(), cdrInDrId = new LongList(), cdrOutId = new LongList(), cdrOutDrId = new LongList();

	/** vendor message id "RTVS1-123": the prefix gets an index, the number an array slot -> MT */
	final Map<String, Integer> vendorPrefix = new HashMap<>();
	final List<int[]> vendorIdToMt = new ArrayList<>();

	/** InMsgID (submit_sm_resp to the customer) -> MT: a DR to the customer whose text holds no whole marker */
	static final class LongIntMap {
		long[] k = new long[1 << 22]; int[] v = new int[1 << 22]; int n;
		void put(long key, int val) {
			if (key == 0) return;
			if (n * 2 > k.length) grow();
			int i = slot(key);
			while (k[i] != 0 && k[i] != key) i = (i + 1) & (k.length - 1);
			if (k[i] == 0) n++;
			k[i] = key; v[i] = val;
		}
		int get(long key) {
			int i = slot(key);
			while (k[i] != 0) { if (k[i] == key) return v[i]; i = (i + 1) & (k.length - 1); }
			return 0;
		}
		int slot(long key) { long h = key * 0x9E3779B97F4A7C15L; return (int) (h >>> 40) & (k.length - 1); }
		void grow() {
			long[] ok = k; int[] ov = v;
			k = new long[ok.length * 2]; v = new int[ok.length * 2]; n = 0;
			for (int i = 0; i < ok.length; i++) if (ok[i] != 0) put(ok[i], ov[i]);
		}
	}
	final LongIntMap inIdToMt = new LongIntMap();

	final Map<String, Long> stats = new TreeMap<>();
	void stat(String k) { stats.merge(k, 1L, Long::sum); }
	void stat(String k, long v) { stats.merge(k, v, Long::sum); }

	BufferedWriter events;
	static final boolean KEEP = Boolean.getBoolean("flowcheck.keep");

	FlowCheck(String markerPrefix, int max) {
		this.max = max;
		prefix = markerPrefix.getBytes(StandardCharsets.US_ASCII);
		for (int i = 0; i < NCOUNT; i++) cnt[i] = new byte[max + 1];
		tmpl = new byte[max + 1];
	}

	void inc(int c, int mt) { if (mt > 0 && mt <= max && cnt[c][mt] < 127) cnt[c][mt]++; }

	// ---------------------------------------------------------------- pcap and TCP

	static final class Flow {
		long next = -1; byte[] buf = new byte[1 << 16]; int len; boolean resync; int dir; String conn;
	}
	static final class Conn {
		final Map<Integer, Integer> submits = new HashMap<>(), delivers = new HashMap<>();
	}
	final Map<String, Flow> flows = new HashMap<>();
	final Map<String, Conn> conns = new HashMap<>();
	long pktTime;

	void readPcap(File f) throws IOException {
		try (InputStream fin = new FileInputStream(f); DataInputStream in = new DataInputStream(new BufferedInputStream(fin, 1 << 20))) {
			byte[] gh = new byte[24];
			try { in.readFully(gh); } catch (EOFException e) { stat("pcap empty file"); return; }
			int magic = ((gh[0] & 0xff) << 24) | ((gh[1] & 0xff) << 16) | ((gh[2] & 0xff) << 8) | (gh[3] & 0xff);
			boolean le = magic == 0xd4c3b2a1 || magic == 0x4d3cb2a1;
			if (!le && magic != 0xa1b2c3d4 && magic != 0xa1b23c4d) throw new IOException("not a pcap file: " + f);
			int link = le ? u32le(gh, 20) : u32be(gh, 20);
			byte[] rh = new byte[16];
			byte[] pkt = new byte[1 << 17];
			while (true) {
				try { in.readFully(rh); } catch (EOFException e) { break; }
				long sec = le ? u32le(rh, 0) & 0xffffffffL : u32be(rh, 0) & 0xffffffffL;
				int incl = le ? u32le(rh, 8) : u32be(rh, 8), orig = le ? u32le(rh, 12) : u32be(rh, 12);
				if (incl < 0 || incl > 1 << 20) throw new IOException("bad pcap record in " + f);
				if (pkt.length < incl) pkt = new byte[incl];
				try { in.readFully(pkt, 0, incl); } catch (EOFException e) { stat("pcap cut record (file end)"); break; }
				pktTime = sec;
				stat("packets");
				if (incl < orig) stat("packets cut by snaplen");
				packet(pkt, incl, link);
			}
		}
	}

	static int u32le(byte[] b, int o) { return (b[o] & 0xff) | ((b[o + 1] & 0xff) << 8) | ((b[o + 2] & 0xff) << 16) | ((b[o + 3] & 0xff) << 24); }
	static int u32be(byte[] b, int o) { return ((b[o] & 0xff) << 24) | ((b[o + 1] & 0xff) << 16) | ((b[o + 2] & 0xff) << 8) | (b[o + 3] & 0xff); }
	static int u16(byte[] b, int o) { return ((b[o] & 0xff) << 8) | (b[o + 1] & 0xff); }

	void packet(byte[] p, int n, int link) {
		int o;
		if (link == 1) {                                                  // Ethernet (lo is Ethernet on Linux)
			int et = u16(p, 12); o = 14;
			if (et == 0x8100) { et = u16(p, 16); o = 18; }
			if (et != 0x0800) { stat("packets not IPv4"); return; }
		} else if (link == 113) {                                         // Linux cooked (-i any)
			if (u16(p, 14) != 0x0800) { stat("packets not IPv4"); return; }
			o = 16;
		} else if (link == 0) {                                           // BSD loopback
			o = 4;
		} else { stat("packets of link type " + link); return; }
		int ihl = (p[o] & 0x0f) * 4, total = u16(p, o + 2);
		if ((p[o + 9] & 0xff) != 6) return;
		int src = u32be(p, o + 12), dst = u32be(p, o + 16);
		int t = o + ihl;
		int sp = u16(p, t), dp = u16(p, t + 2);
		long seq = u32be(p, t + 4) & 0xffffffffL;
		int flags = p[t + 13] & 0xff, thl = ((p[t + 12] & 0xff) >> 4) * 4;
		int data = t + thl, plen = Math.min(o + total, n) - data;
		int dir;
		if (serverPorts.contains(dp)) dir = 0;                            // customer -> Server
		else if (serverPorts.contains(sp)) dir = 1;                       // Server -> customer
		else if (vendorPorts.contains(dp)) dir = 2;                       // Client -> vendor
		else if (vendorPorts.contains(sp)) dir = 3;                       // vendor -> Client
		else return;
		String key = ip(src) + ":" + sp + ">" + ip(dst) + ":" + dp;
		Flow f = flows.get(key);
		if (f == null) {
			f = new Flow(); f.dir = dir;
			f.conn = dir == 0 || dir == 2 ? ip(src) + ":" + sp + "-" + ip(dst) + ":" + dp : ip(dst) + ":" + dp + "-" + ip(src) + ":" + sp;
			flows.put(key, f);
		}
		if ((flags & 0x02) != 0) { f.next = (seq + 1) & 0xffffffffL; f.len = 0; f.resync = false; stat("tcp SYN"); return; }
		if ((flags & 0x04) != 0) stat("tcp RST");
		if (plen <= 0) return;
		if (f.next < 0) { f.next = seq; f.resync = true; stat("tcp streams joined in the middle"); }
		int d = (int) (seq - f.next);
		int from = data, len = plen;
		if (d < 0) {                                                      // retransmission (all or part seen already)
			if (-d >= plen) { stat("tcp retransmitted segments"); return; }
			from += -d; len -= -d;
		} else if (d > 0) {                                               // bytes missing: the capture lost them
			stat("tcp gaps (capture lost bytes)"); stat("tcp gap bytes", d);
			f.len = 0; f.resync = true;
		}
		if (f.buf.length < f.len + len) f.buf = Arrays.copyOf(f.buf, Math.max(f.buf.length * 2, f.len + len));
		System.arraycopy(p, from, f.buf, f.len, len);
		f.len += len;
		f.next = (seq + plen) & 0xffffffffL;
		pdus(f);
	}

	static String ip(int a) { return ((a >>> 24) & 0xff) + "." + ((a >>> 16) & 0xff) + "." + ((a >>> 8) & 0xff) + "." + (a & 0xff); }

	static boolean knownCmd(int c) {
		switch (c) {
		case 1: case 2: case 4: case 5: case 6: case 9: case 0x15: case 0x103:
		case 0x80000000: case 0x80000001: case 0x80000002: case 0x80000004: case 0x80000005: case 0x80000006: case 0x80000009: case 0x80000015: case 0x80000103:
			return true;
		default: return false;
		}
	}

	void pdus(Flow f) {
		int i = 0;
		while (f.len - i >= 16) {
			int len = u32be(f.buf, i), cmd = u32be(f.buf, i + 4), st = u32be(f.buf, i + 8);
			boolean ok = len >= 16 && len <= 70000 && knownCmd(cmd) && st >= 0 && st < 0x10000;
			if (!ok || (f.resync && len > 4096)) {
				if (!f.resync) { stat("smpp stream out of step"); f.resync = true; }
				i++;
				continue;
			}
			if (f.len - i < len) break;
			f.resync = false;
			try { pdu(f, cmd, st, u32be(f.buf, i + 12), f.buf, i + 16, i + len); } catch (RuntimeException e) { stat("smpp PDUs not decoded"); }
			i += len;
		}
		if (i > 0) { System.arraycopy(f.buf, i, f.buf, 0, f.len - i); f.len -= i; }
	}

	// ---------------------------------------------------------------- SMPP

	static int cstrEnd(byte[] b, int o, int end) { while (o < end && b[o] != 0) o++; return o; }

	/** marker number in b[o, end) or -1; tmplOut[0] gets the template number after " M" */
	int marker(byte[] b, int o, int end, int[] tmplOut) {
		outer:
		for (int i = o; i + prefix.length < end; i++) {
			for (int j = 0; j < prefix.length; j++) if (b[i + j] != prefix[j]) continue outer;
			int k = i + prefix.length; long v = 0; int digits = 0;
			while (k < end && b[k] >= '0' && b[k] <= '9' && digits < 10) { v = v * 10 + (b[k] - '0'); k++; digits++; }
			if (digits == 0) continue;
			if (tmplOut != null && k + 2 < end && b[k] == ' ' && b[k + 1] == 'M') {
				int t = 0, m = k + 2;
				while (m < end && b[m] >= '0' && b[m] <= '9') { t = t * 10 + (b[m] - '0'); m++; }
				tmplOut[0] = t;
			}
			return v > max ? -2 : (int) v;
		}
		return -1;
	}

	/** short_message (or message_payload) of a submit_sm / deliver_sm body: {from, to} into b; ucs2 -> the bytes are UTF-16 */
	static int[] shortMessage(byte[] b, int o, int end) {
		int i = cstrEnd(b, o, end) + 1 + 2;                                // service_type, source ton npi
		i = cstrEnd(b, i, end) + 1 + 2;                                    // source, dest ton npi
		i = cstrEnd(b, i, end) + 1;                                        // dest
		int esm = b[i] & 0xff; i += 3;
		i = cstrEnd(b, i, end) + 1; i = cstrEnd(b, i, end) + 1;            // schedule, validity
		i += 2; int dc = b[i] & 0xff; i += 2;
		int n = b[i++] & 0xff;
		int from = i, to = i + n;
		for (int j = to; j + 4 <= end; ) {
			int tag = u16(b, j), l = u16(b, j + 2);
			if (tag == 0x0424 && n == 0) { from = j + 4; to = j + 4 + l; }
			j += 4 + l;
		}
		if ((esm & 0x40) != 0 && to > from) from += 1 + (b[from] & 0xff); // UDH
		return new int[] { from, Math.min(to, end), esm, dc };
	}

	/** "id:XXX ... stat:YYY" of a receipt */
	static String field(byte[] b, int from, int to, String name) {
		byte[] k = name.getBytes(StandardCharsets.US_ASCII);
		outer:
		for (int i = from; i + k.length <= to; i++) {
			for (int j = 0; j < k.length; j++) if (b[i + j] != k[j]) continue outer;
			int s = i + k.length, e = s;
			while (e < to && b[e] != ' ') e++;
			return new String(b, s, e - s, StandardCharsets.ISO_8859_1);
		}
		return null;
	}

	static long numId(String id) {
		if (id == null) return 0;
		boolean digits = !id.isEmpty() && id.length() <= 18;
		for (int i = 0; digits && i < id.length(); i++) digits = Character.isDigit(id.charAt(i));
		if (digits) return Long.parseLong(id);
		long h = 0xcbf29ce484222325L;
		for (int i = 0; i < id.length(); i++) { h ^= id.charAt(i); h *= 0x100000001b3L; }
		return h | Long.MIN_VALUE;
	}

	/** vendor id "RTVS1-123" -> (prefix index << 40) | 123 */
	long vendorId(String id, int mtToRemember) {
		int dash = id == null ? -1 : id.lastIndexOf('-');
		if (dash <= 0) return numId(id);
		int num;
		try { num = Integer.parseInt(id.substring(dash + 1)); } catch (NumberFormatException e) { return numId(id); }
		String pre = id.substring(0, dash).toUpperCase();
		Integer x = vendorPrefix.get(pre);
		if (x == null) { x = vendorPrefix.size(); vendorPrefix.put(pre, x); vendorIdToMt.add(null); }
		if (mtToRemember != 0) {
			int[] a = vendorIdToMt.get(x);
			if (a == null) { a = new int[1 << 20]; vendorIdToMt.set(x, a); }
			if (num >= a.length) { a = Arrays.copyOf(a, Math.max(a.length * 2, num + 1)); vendorIdToMt.set(x, a); }
			a[num] = mtToRemember;
		}
		return ((long) x << 40) | num;
	}

	int mtOfVendorId(String id) {
		int dash = id == null ? -1 : id.lastIndexOf('-');
		if (dash <= 0) return 0;
		Integer x = vendorPrefix.get(id.substring(0, dash).toUpperCase());
		if (x == null) return 0;
		int[] a = vendorIdToMt.get(x);
		try { int num = Integer.parseInt(id.substring(dash + 1)); return a != null && num < a.length ? a[num] : 0; } catch (NumberFormatException e) { return 0; }
	}

	static final String[] DIR = { "cust>srv", "srv>cust", "cli>vend", "vend>cli" };

	void pdu(Flow f, int cmd, int st, int seq, byte[] b, int o, int end) throws RuntimeException {
		Conn c = conns.computeIfAbsent(f.conn, k -> new Conn());
		int[] tp = { 0 };
		int mt = 0; String id = null, extra = null;
		stat("pdu " + DIR[f.dir] + " " + Integer.toHexString(cmd));
		if (cmd == 4 && (f.dir == 0 || f.dir == 2)) {                      // submit_sm
			int[] sm = shortMessage(b, o, end);
			mt = marker(b, sm[0], sm[1], tp);
			if (mt <= 0) { stat(mt == -2 ? "submit_sm marker over maxMTs" : "submit_sm without a marker"); mt = 0; }
			if (f.dir == 0) { inc(P_CS, mt); if (mt > 0 && tp[0] > 0) tmpl[mt] = (byte) tp[0]; } else inc(P_VS, mt);
			c.submits.put(seq, mt);
		} else if (cmd == 0x80000004 && (f.dir == 1 || f.dir == 3)) {     // submit_sm_resp
			Integer m = c.submits.remove(seq);
			if (m == null) { stat("submit_sm_resp without its submit_sm " + DIR[f.dir]); m = 0; }
			mt = m;
			id = o < end ? new String(b, o, cstrEnd(b, o, end) - o, StandardCharsets.ISO_8859_1) : "";
			if (f.dir == 1) {
				inc(st == 0 ? P_CSR_OK : P_CSR_ERR, mt);
				if (st == 0) { capInId.add(numId(id)); inIdToMt.put(numId(id), mt); }
			} else {
				inc(st == 0 ? P_VSR_OK : P_VSR_ERR, mt);
				if (st == 0) capVendorId.add(vendorId(id, mt));
			}
		} else if (cmd == 5 && (f.dir == 1 || f.dir == 3)) {              // deliver_sm: the DRs
			int[] sm = shortMessage(b, o, end);
			id = field(b, sm[0], sm[1], "id:");
			extra = field(b, sm[0], sm[1], "stat:");
			if (f.dir == 1) {
				mt = inIdToMt.get(numId(id));                                 // by its id: the text may hold only part of a marker
				if (mt == 0) {
					mt = marker(b, sm[0], sm[1], null);
					if (mt <= 0) { stat("DR to cust of an unknown id, without a marker"); mt = 0; }
					else stat("DR to cust found by the marker (id unknown)");
				}
				inc(P_CDR, mt);
				capDrToCustId.add(numId(id));
			} else {
				mt = mtOfVendorId(id);
				if (mt == 0) stat("vendor DR for an unknown vendor id");
				inc(P_VDR, mt);
				capVendorDrId.add(vendorId(id, 0));
			}
			c.delivers.put(seq, mt);
		} else if (cmd == 0x80000005 && (f.dir == 0 || f.dir == 2)) {     // deliver_sm_resp
			Integer m = c.delivers.remove(seq);
			if (m == null) { stat("deliver_sm_resp without its deliver_sm " + DIR[f.dir]); m = 0; }
			mt = m;
			if (st != 0) stat("deliver_sm_resp not ok " + DIR[f.dir]);
			inc(f.dir == 0 ? P_CDRR : P_VDRR, mt);
		}
		if (events != null && (cmd == 4 || cmd == 5 || cmd == 0x80000004 || cmd == 0x80000005)) {
			try {
				events.write(pktTime + "\t" + DIR[f.dir] + "\t" + f.conn + "\t" + Integer.toHexString(cmd) + "\t" + seq + "\t" + st + "\t" + mt + "\t" + tp[0]
						+ "\t" + (id == null ? "" : id) + "\t" + (extra == null ? "" : extra) + "\n");
			} catch (IOException e) { throw new RuntimeException(e); }
		}
	}

	// ---------------------------------------------------------------- following tcpdump's files

	static int fileNo(String name) {
		String s = name.substring(name.indexOf(".pcap") + 5);
		return s.isEmpty() ? 0 : Integer.parseInt(s);
	}

	static final java.util.regex.Pattern PCAP = java.util.regex.Pattern.compile("(cap[_a-z0-9]*)\\.pcap([0-9]*)");

	void follow(Path dir, Path stop) throws Exception {
		Map<String, Integer> done = new TreeMap<>();                        // per tcpdump: the last file read
		long t0 = System.currentTimeMillis(), lastNote = 0, files = 0;
		while (true) {
			boolean stopped = Files.exists(stop);
			File[] all = dir.toFile().listFiles();
			Map<String, List<File>> seqs = new TreeMap<>();
			for (File f : all == null ? new File[0] : all) {
				java.util.regex.Matcher m = PCAP.matcher(f.getName());
				if (m.matches()) seqs.computeIfAbsent(m.group(1), k -> new ArrayList<>()).add(f);
			}
			boolean any = false;
			for (Map.Entry<String, List<File>> e : seqs.entrySet()) {
				List<File> fs = e.getValue();
				fs.sort((a, b2) -> Integer.compare(fileNo(a.getName()), fileNo(b2.getName())));
				int top = fileNo(fs.get(fs.size() - 1).getName()), last = done.getOrDefault(e.getKey(), -1);
				for (File f : fs) {
					int no = fileNo(f.getName());
					if (no <= last) continue;
					if (no == top && !stopped) break;                           // tcpdump still writes the newest one
					readPcap(f);
					stat("pcap files", 1); stat("pcap bytes", f.length());
					if (!KEEP) { if (!f.delete()) stat("pcap files not deleted"); }
					last = no; any = true; files++;
				}
				done.put(e.getKey(), last);
			}
			if (stopped && !any) break;
			if (System.currentTimeMillis() - lastNote > 60000) {
				lastNote = System.currentTimeMillis();
				System.out.println(new java.util.Date() + " pcap files read " + files + ", packets " + stats.getOrDefault("packets", 0L));
			}
			if (!any) Thread.sleep(1000);
		}
		System.out.println("capture read in " + (System.currentTimeMillis() - t0) / 1000 + " s");
	}

	public static void main(String[] a) throws Exception {
		FlowCheck fc = new FlowCheck(a[2], Integer.parseInt(a[3]));
		for (String s : a[4].split(",")) fc.serverPorts.add(Integer.parseInt(s.trim()));
		for (String s : a[5].split(",")) fc.vendorPorts.add(Integer.parseInt(s.trim()));
		Path out = Paths.get(a[8]);
		Files.createDirectories(out);
		if (a.length > 9) fc.events = Files.newBufferedWriter(Paths.get(a[9]), StandardCharsets.ISO_8859_1);
		fc.follow(Paths.get(a[0]), Paths.get(a[1]));
		if (fc.events != null) fc.events.close();
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Long> e : fc.stats.entrySet()) sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
		Files.write(out.resolve("capture_stats.txt"), sb.toString().getBytes(StandardCharsets.UTF_8));
		boolean ok = fc.compare(Arrays.asList(a[6].split(",")), Long.parseLong(a[7]), out);
		System.exit(ok ? 0 : 1);
	}

	// ---------------------------------------------------------------- CDRs

	/** fields of a CDR line: comma separated, "..." may hold commas */
	static List<String> csv(String l) {
		List<String> f = new ArrayList<>(48);
		StringBuilder b = new StringBuilder();
		boolean q = false;
		for (int i = 0; i < l.length(); i++) {
			char c = l.charAt(i);
			if (c == '"') q = !q;
			else if (c == ',' && !q) { f.add(b.toString()); b.setLength(0); }
			else b.append(c);
		}
		f.add(b.toString());
		return f;
	}

	final LongList cdrInIdAll = new LongList();
	long cdrFiles, cdrLines, cdrLinesOfTest;
	final Map<String, Long> cdrTypes = new TreeMap<>();

	void readCdrs(List<String> dirs, long since) throws IOException {
		Set<String> seen = new HashSet<>();
		byte[] pre = prefix;
		for (String d : dirs) {
			File[] fs = new File(d.trim()).listFiles();
			if (fs == null) continue;
			Arrays.sort(fs);
			for (File f : fs) {
				String n = f.getName();
				if (!f.isFile() || !n.contains(".cdr.") || f.lastModified() < since || !seen.add(n.replace(".gz", ""))) continue;
				cdrFiles++;
				InputStream in = new FileInputStream(f);
				if (n.endsWith(".gz")) in = new java.util.zip.GZIPInputStream(in, 1 << 16);
				try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(in, StandardCharsets.ISO_8859_1), 1 << 20)) {
					for (String l; (l = r.readLine()) != null; ) {
						cdrLines++;
						byte[] lb = l.getBytes(StandardCharsets.ISO_8859_1);
						int mt = marker(lb, 0, lb.length, null);
						if (mt == -1) continue;
						cdrLinesOfTest++;
						List<String> x = csv(l);
						if (x.size() < 23) { stat("CDR lines with too few fields"); continue; }
						String type = x.get(1), in0 = x.get(4), out0 = x.get(5), st = x.get(22).trim();
						cdrTypes.merge(type, 1L, Long::sum);
						if (mt == -2) { stat("CDR marker over maxMTs"); continue; }
						for (String id : in0.split(",")) if (!id.isEmpty() && !"NULL".equals(id)) cdrInIdAll.add(numId(id.trim()));
						switch (type) {
						case "IN_SUBMIT": inc(C_IS, mt); break;
						case "IN_SUBMIT_PARTIAL": inc(C_ISP, mt); break;
						case "OUT_SUBMIT":
							if ("0".equals(st)) { inc(C_OS_OK, mt); if (!out0.isEmpty() && !"NULL".equals(out0)) cdrOutId.add(vendorId(out0, 0)); }
							else inc(C_OS_ERR, mt);
							break;
						case "IN_DELI_REP": inc(C_IDR, mt); cdrInDrId.add(numId(in0.trim())); break;
						case "OUT_DELI_REP": inc(C_ODR, mt); if (!out0.isEmpty() && !"NULL".equals(out0)) cdrOutDrId.add(vendorId(out0, 0)); break;
						default: stat("CDR type " + type);
						}
					}
				}
			}
		}
	}

	// ---------------------------------------------------------------- the comparison

	/** a rule broken by some MTs: how many, the first few markers */
	final Map<String, long[]> broken = new LinkedHashMap<>();
	final Map<String, StringBuilder> examples = new HashMap<>();
	String markerName;

	void fail(String rule, int mt) {
		broken.computeIfAbsent(rule, k -> new long[1])[0]++;
		StringBuilder b = examples.computeIfAbsent(rule, k -> new StringBuilder());
		if (b.length() < 300) b.append(b.length() == 0 ? "" : " ").append(markerName).append(mt);
	}

	/** a vs b as multisets: {only in a, only in b, duplicates in a, duplicates in b} */
	static long[] diff(long[] a, long[] b) {
		long onlyA = 0, onlyB = 0, dupA = 0, dupB = 0;
		for (int i = 1; i < a.length; i++) if (a[i] == a[i - 1]) dupA++;
		for (int i = 1; i < b.length; i++) if (b[i] == b[i - 1]) dupB++;
		int i = 0, j = 0;
		while (i < a.length || j < b.length) {
			if (j >= b.length || (i < a.length && a[i] < b[j])) { onlyA++; i++; }
			else if (i >= a.length || b[j] < a[i]) { onlyB++; j++; }
			else { i++; j++; }
		}
		return new long[] { onlyA, onlyB, dupA, dupB };
	}

	static long[] distinct(long[] s) {
		int n = 0;
		for (int i = 0; i < s.length; i++) if (n == 0 || s[i] != s[n - 1]) s[n++] = s[i];
		return Arrays.copyOf(s, n);
	}

	boolean compare(List<String> cdrDirs, long since, Path out) throws IOException {
		long t0 = System.currentTimeMillis();
		markerName = new String(prefix, StandardCharsets.US_ASCII);
		readCdrs(cdrDirs, since);
		StringBuilder r = new StringBuilder();
		r.append("high-TPS consistency check: capture (customer side ").append(serverPorts).append(", Client side ").append(vendorPorts)
				.append(") against the CDRs, MT by MT (marker ").append(markerName).append("{n})\n\n");
		long gaps = stats.getOrDefault("tcp gaps (capture lost bytes)", 0L), outOfStep = stats.getOrDefault("smpp stream out of step", 0L);
		long midstream = stats.getOrDefault("tcp streams joined in the middle", 0L), undecoded = stats.getOrDefault("smpp PDUs not decoded", 0L);
		r.append(String.format("capture: %d pcap files, %.1f GB, %d packets; TCP gaps %d, streams joined in the middle %d, SMPP out of step %d, PDUs not decoded %d%n",
				stats.getOrDefault("pcap files", 0L), stats.getOrDefault("pcap bytes", 0L) / 1e9, stats.getOrDefault("packets", 0L), gaps, midstream, outOfStep, undecoded));
		boolean captureOk = gaps == 0 && outOfStep == 0 && undecoded == 0;
		r.append(String.format("CDR: %d files, %d lines, %d of this test (%s)%n%n", cdrFiles, cdrLines, cdrLinesOfTest, cdrTypes));

		// per MT
		long mts = 0, cdrOnly = 0;
		long[] sum = new long[NCOUNT];
		Map<String, Long> patterns = new TreeMap<>();
		for (int mt = 1; mt <= max; mt++) {
			int cs = cnt[P_CS][mt];
			boolean inCdr = false;
			for (int c = C_IS; c < NCOUNT; c++) if (cnt[c][mt] != 0) inCdr = true;
			if (cs == 0 && !inCdr) continue;
			for (int c = 0; c < NCOUNT; c++) sum[c] += cnt[c][mt];
			if (cs == 0) { cdrOnly++; fail("in the CDRs, not in the capture (no customer submit_sm)", mt); continue; }
			mts++;
			int csOk = cnt[P_CSR_OK][mt], vs = cnt[P_VS][mt], vOk = cnt[P_VSR_OK][mt], vErr = cnt[P_VSR_ERR][mt], vdr = cnt[P_VDR][mt];
			int drC = cnt[P_CDR][mt], is = cnt[C_IS][mt], osOk = cnt[C_OS_OK][mt], osErr = cnt[C_OS_ERR][mt], idr = cnt[C_IDR][mt], odr = cnt[C_ODR][mt];
			// the customer side
			if (csOk + cnt[P_CSR_ERR][mt] != cs) fail("customer submit_sm without exactly one submit_sm_resp", mt);
			if (cnt[P_CSR_ERR][mt] > 0) fail("customer submit_sm rejected", mt);
			if (is != 1) fail("CDR IN_SUBMIT not exactly 1", mt);
			if (drC != cs) fail("DRs to the customer != its segments (lost or sent twice)", mt);
			if (idr != drC) fail("DRs to the customer (capture) != CDR IN_DELI_REP", mt);
			if (cnt[P_CDRR][mt] != drC) fail("DR to the customer without its deliver_sm_resp", mt);
			// the vendor side
			if (vOk + vErr != vs) fail("vendor submit_sm without exactly one submit_sm_resp", mt);
			if (vs > 0 && vOk != cs) fail("vendor accepted segments != customer segments (lost or sent twice)", mt);
			if ((vOk > 0 ? 1 : 0) != osOk) fail("vendor accepted (capture) but CDR OUT_SUBMIT ok not exactly 1, or the other way", mt);
			if (osErr != vErr + (vs == 0 ? 1 : 0)) fail("vendor rejections (capture) != CDR OUT_SUBMIT failed", mt);
			if (vdr > vOk) fail("more vendor DRs than accepted segments", mt);
			if ((vdr > 0 ? 1 : 0) != odr) fail("vendor DR (capture) but CDR OUT_DELI_REP not exactly 1, or the other way", mt);
			if (cnt[P_VDRR][mt] != vdr) fail("vendor DR without the Client's deliver_sm_resp", mt);
			StringBuilder k = new StringBuilder("M").append(tmpl[mt]).append("  capture ");
			k.append("cust ").append(cs).append('/').append(csOk).append(" DR ").append(drC).append(" | vendor ").append(vs).append('/').append(vOk).append('/').append(vErr).append(" DR ").append(vdr);
			k.append("  CDR IN_SUBMIT ").append(is).append('+').append(cnt[C_ISP][mt]).append(" OUT_SUBMIT ").append(osOk).append('/').append(osErr)
					.append(" OUT_DELI_REP ").append(odr).append(" IN_DELI_REP ").append(idr);
			patterns.merge(k.toString(), 1L, Long::sum);
		}
		r.append("MTs in the capture: ").append(mts).append(", only in the CDRs: ").append(cdrOnly).append("\n");
		r.append("totals:\n");
		for (int c = 0; c < NCOUNT; c++) r.append(String.format("  %-28s %d%n", CNAME[c], sum[c]));
		r.append("\nflows (template; capture: cust submit_sm/accepted DR | vendor submit_sm/accepted/rejected DR; CDR counts) -> MTs:\n");
		for (Map.Entry<String, Long> e : patterns.entrySet()) r.append(String.format("  %-120s %d%n", e.getKey(), e.getValue()));

		// the IDs
		r.append("\nIDs (capture vs CDR, as multisets):\n");
		long[] capIn = capInId.sorted(), cdrIn = distinct(cdrInIdAll.sorted());
		long[] d1 = diff(capIn, cdrIn);
		r.append(String.format("  customer submit_sm_resp message_id vs CDR InMsgID: %d / %d, only in the capture %d, only in the CDRs %d, the same id twice in the capture %d%n",
				capIn.length, cdrIn.length, d1[0], d1[1], d1[2]));
		long[] d2 = diff(capDrToCustId.sorted(), cdrInDrId.sorted());
		r.append(String.format("  DR to the customer id vs CDR IN_DELI_REP InMsgID: %d / %d, only in the capture %d, only in the CDRs %d, twice: capture %d, CDR %d%n",
				capDrToCustId.n, cdrInDrId.n, d2[0], d2[1], d2[2], d2[3]));
		long[] d3 = diff(capVendorId.sorted(), cdrOutId.sorted());
		r.append(String.format("  vendor submit_sm_resp message_id vs CDR OUT_SUBMIT OutMsgID: %d / %d, only in the CDRs %d (must be 0), twice in the capture %d%n",
				capVendorId.n, cdrOutId.n, d3[1], d3[2]));
		long[] d4 = diff(capVendorDrId.sorted(), cdrOutDrId.sorted());
		r.append(String.format("  vendor DR id vs CDR OUT_DELI_REP OutMsgID: %d / %d, only in the CDRs %d (must be 0), twice: capture %d, CDR %d%n",
				capVendorDrId.n, cdrOutDrId.n, d4[1], d4[2], d4[3]));
		boolean idsOk = d1[0] == 0 && d1[1] == 0 && d1[2] == 0 && d2[0] == 0 && d2[1] == 0 && d2[2] == 0 && d2[3] == 0 && d3[1] == 0 && d3[2] == 0
				&& d4[1] == 0 && d4[2] == 0 && d4[3] == 0;
		// a long MT: one OUT_SUBMIT / OUT_DELI_REP for its segments, so the vendor ids of the other segments are not in the CDRs
		r.append("  (vendor ids of the 2nd, 3rd ... segment of a long MT are not in the CDRs: Core writes one OUT_SUBMIT and one OUT_DELI_REP per MT)\n");

		r.append("\nrules broken:\n");
		if (broken.isEmpty()) r.append("  none\n");
		for (Map.Entry<String, long[]> e : broken.entrySet())
			r.append(String.format("  %-90s %d MTs, e.g. %s%n", e.getKey(), e.getValue()[0], examples.get(e.getKey())));
		r.append("\nother counts of the capture:\n");
		for (Map.Entry<String, Long> e : stats.entrySet()) if (!e.getKey().startsWith("pdu ")) r.append("  ").append(e.getKey()).append('=').append(e.getValue()).append('\n');
		boolean pass = captureOk && idsOk && broken.isEmpty() && mts > 0;
		r.append("\nRESULT: ").append(pass ? "PASS" : "FAIL").append(" (")
				.append(captureOk ? "capture complete" : "capture NOT complete").append(", ")
				.append(broken.isEmpty() ? "every MT the same in the capture and the CDRs, no MT lost or sent twice" : broken.size() + " rule(s) broken").append(", ")
				.append(idsOk ? "IDs the same" : "IDs differ").append(")\n");
		r.append("compared in ").append((System.currentTimeMillis() - t0) / 1000).append(" s\n");
		Files.write(out.resolve("report.txt"), r.toString().getBytes(StandardCharsets.UTF_8));
		System.out.print(r);
		return pass;
	}
}
