package rt;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * An SMPP long message to an HTTP vendor and its DR back (docs/HTTP服务器全流程测试.md, HT002), JDK only:
 * the SMPP customer (env.properties customer.user) sends a long MT in parts (UDH) to MultiSmppServer; Core routes it to
 * the Yulore vendor (CommonHttpClient, YuloreRESTJsonHttpHandler: JSON Sender/Recipient/Charset/Content/Token), played
 * here: it answers {"code":"200","message":id} and sends the DR (REFERENCE, STATUS=3) to CommonHttpDRServlet
 * /Gmms/Yulore/DRReply; Core sends the DR to the customer as deliver_sm.
 *   java -cp routetest.jar rt.SmppHttpFlow CASE_DIR GATEWAY_PORT ENV_PROPERTIES
 */
public class SmppHttpFlow {
	static final List<String> lines = Collections.synchronizedList(new ArrayList<String>());
	static int pass = 0, fail = 0;
	static final List<Map<String, String>> vendorMts = Collections.synchronizedList(new ArrayList<Map<String, String>>());
	static final List<String> drReplies = Collections.synchronizedList(new ArrayList<String>());
	/** submit_sm sequence -> {status, message_id} */
	static final Map<Integer, String[]> submitResps = new ConcurrentHashMap<Integer, String[]>();
	/** the DR receipts (deliver_sm, esm_class 0x04) the customer got */
	static final List<String> receipts = Collections.synchronizedList(new ArrayList<String>());
	static volatile int bindStatus = -1;
	static final AtomicInteger seq = new AtomicInteger(1);
	static OutputStream smppOut;

	public static void main(String[] args) throws Exception {
		File caseDir = new File(args[0]);
		Properties p = load(new File(caseDir, "case.properties"));
		Properties env = load(new File(args[2]));
		final String gateway = "http://127.0.0.1:" + args[1] + "/Gmms";
		int vendorPort = Integer.parseInt(p.getProperty("vendor.port", "18603"));
		long drDelayMs = Long.parseLong(p.getProperty("vendor.drDelayMs", "500"));
		String run = new SimpleDateFormat("HHmmss").format(new Date());
		ScheduledExecutorService drSender = Executors.newScheduledThreadPool(4);
		final AtomicInteger vendorIds = new AtomicInteger();

		HttpServer vendor = HttpServer.create(new InetSocketAddress(vendorPort), 64);
		vendor.createContext("/yulore", ex -> {
			String body = read(ex.getRequestBody());
			Map<String, String> mt = new LinkedHashMap<String, String>();
			for (String f : new String[] {"Token", "Sender", "Recipient", "Charset", "Content"}) {
				mt.put(f, jsonField(body, f));
			}
			mt.put("contentType", ex.getRequestHeaders().getFirst("Content-Type"));
			final String id = "YL" + run + "-" + vendorIds.incrementAndGet();
			mt.put("id", id);
			vendorMts.add(mt);
			log("Yulore vendor got " + mt);
			answer(ex, 200, "{\"code\":\"200\",\"message\":\"" + id + "\"}");
			drSender.schedule(() -> {
				String[] r = post(gateway + "/Yulore/DRReply", "REFERENCE=" + enc(id) + "&STATUS=3", "application/x-www-form-urlencoded");
				drReplies.add(id + " -> " + r[0] + " " + r[1]);
				log("vendor DR " + id + " to /Yulore/DRReply: " + r[0] + " " + r[1]);
			}, drDelayMs, TimeUnit.MILLISECONDS);
		});
		vendor.setExecutor(Executors.newFixedThreadPool(8));
		vendor.start();

		// ---- the SMPP customer ----
		Socket s = new Socket();
		s.connect(new InetSocketAddress(env.getProperty("server.host"), Integer.parseInt(env.getProperty("server.port"))), 5000);
		smppOut = s.getOutputStream();
		final DataInputStream in = new DataInputStream(s.getInputStream());
		Thread reader = new Thread(() -> readLoop(in), "smpp-reader");
		reader.setDaemon(true);
		reader.start();
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		cstr(b, env.getProperty("customer.user"));
		cstr(b, env.getProperty("customer.password"));
		cstr(b, "");
		b.write(0x34); b.write(0); b.write(0);
		cstr(b, "");
		pdu(0x00000009, 0, seq.getAndIncrement(), b.toByteArray());
		waitFor(() -> bindStatus >= 0, 10000);
		check(bindStatus == 0, "LY-01 bind_transceiver as " + env.getProperty("customer.user") + ": status " + bindStatus);

		String to = p.getProperty("mt.recipient", "8613900000021"), from = p.getProperty("mt.sender", "LONGT");
		StringBuilder t = new StringBuilder("long " + run + " ");
		while (t.length() < 200) t.append("abcdefghij");
		String text = t.substring(0, 200);
		String[] parts = {text.substring(0, 153), text.substring(153)};
		int ref = Integer.parseInt(run.substring(run.length() - 2)) + 1;
		List<Integer> seqs = new ArrayList<Integer>();
		for (int i = 0; i < parts.length; i++) {
			byte[] udh = {0x05, 0x00, 0x03, (byte) ref, (byte) parts.length, (byte) (i + 1)};
			seqs.add(submit(from, to, udh, parts[i].getBytes(StandardCharsets.US_ASCII)));
		}
		waitFor(() -> submitResps.size() >= parts.length, 10000);
		List<String> ids = new ArrayList<String>();
		boolean allOk = true;
		for (int sq : seqs) {
			String[] r = submitResps.get(sq);
			allOk &= r != null && "0".equals(r[0]) && !r[1].isEmpty();
			if (r != null) ids.add(r[1]);
		}
		check(allOk, "LY-02 both parts (UDH 05 00 03, 153 + 47 characters) accepted: " + ids);

		// ---- the vendor, the DRs ----
		long until = System.currentTimeMillis() + Long.parseLong(p.getProperty("wait.sec", "90")) * 1000L;
		while (System.currentTimeMillis() < until && (vendorMts.isEmpty() || drReplies.size() < vendorMts.size() || !allDrs(ids))) {
			Thread.sleep(500);
		}
		Thread.sleep(3000);
		StringBuilder got = new StringBuilder();
		boolean fields = !vendorMts.isEmpty();
		synchronized (vendorMts) {
			for (Map<String, String> m : vendorMts) {
				got.append(m.get("Content"));
				fields &= "ytoken9962".equals(m.get("Token")) && from.equals(m.get("Sender")) && to.equals(m.get("Recipient"))
						&& m.get("contentType") != null && m.get("contentType").startsWith("application/json");
			}
		}
		check(fields, "LY-03 the MT reached the Yulore vendor as JSON (Token from ChlAcctr, Sender, Recipient): " + vendorMts.size() + " request(s) " + vendorMts);
		check(text.equals(got.toString()), "LY-04 the vendor got the whole text (" + vendorMts.size() + " request(s), " + got.length() + " of " + text.length() + " characters)");
		int drOk = 0;
		synchronized (drReplies) {
			for (String d : drReplies) if (d.contains(" -> 200")) drOk++;
		}
		check(drOk == vendorMts.size() && drOk > 0, "LY-05 CommonHttpDRServlet /Yulore/DRReply answers the vendor's DRs with 200: " + drReplies);
		for (int i = 0; i < ids.size(); i++) {
			String rc = receiptOf(ids.get(i));
			check(rc != null && rc.contains("stat:DELIVRD"), "LY-06." + (i + 1) + " the customer got a DR (deliver_sm) for part " + (i + 1) + " (" + ids.get(i) + "): " + rc);
		}
		log("receipts: " + receipts);

		vendor.stop(0);
		drSender.shutdownNow();
		try {
			pdu(0x00000006, 0, seq.getAndIncrement(), new byte[0]);   // unbind
			Thread.sleep(300);
			s.close();
		} catch (Exception e) {
		}
		File out = new File(caseDir, "result/" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
		out.mkdirs();
		try (PrintWriter w = new PrintWriter(new File(out, "report.txt"), "UTF-8")) {
			synchronized (lines) {
				for (String l : lines) w.println(l);
			}
			w.println("TOTAL pass=" + pass + " fail=" + fail);
		}
		System.out.println("TOTAL pass=" + pass + " fail=" + fail + "  (" + out + "/report.txt)");
		System.exit(fail == 0 ? 0 : 1);
	}

	static boolean allDrs(List<String> ids) {
		for (String id : ids) if (receiptOf(id) == null) return false;
		return !ids.isEmpty();
	}

	static String receiptOf(String id) {
		synchronized (receipts) {
			for (String r : receipts) if (r.startsWith("id:" + id + " ")) return r;
		}
		return null;
	}

	static int submit(String from, String to, byte[] udh, byte[] text) throws Exception {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		cstr(b, "");
		b.write(5); b.write(0); cstr(b, from);            // alphanumeric sender
		b.write(1); b.write(1); cstr(b, to);
		b.write(0x40); b.write(0); b.write(0);            // esm_class: UDH; protocol_id, priority
		cstr(b, ""); cstr(b, "");                         // schedule, validity
		b.write(1); b.write(0); b.write(0); b.write(0);   // registered_delivery, replace, data_coding, default_msg_id
		b.write(udh.length + text.length);
		b.write(udh);
		b.write(text);
		int sq = seq.getAndIncrement();
		pdu(0x00000004, 0, sq, b.toByteArray());
		log("submit_sm seq=" + sq + " udh=" + hex(udh) + " " + text.length + " characters");
		return sq;
	}

	static void readLoop(DataInputStream in) {
		try {
			while (true) {
				int len = in.readInt(), cmd = in.readInt(), status = in.readInt(), sq = in.readInt();
				byte[] body = new byte[len - 16];
				in.readFully(body);
				if (cmd == 0x80000009) {
					bindStatus = status;
				} else if (cmd == 0x80000004) {
					submitResps.put(sq, new String[] {String.valueOf(status), cstrAt(body, 0)});
					log("submit_sm_resp seq=" + sq + " status=" + status + " id=" + cstrAt(body, 0));
				} else if (cmd == 0x00000005) {
					int[] pos = {0};
					skip(body, pos);                                  // service_type
					pos[0] += 2; skip(body, pos);                     // source
					pos[0] += 2; skip(body, pos);                     // destination
					int esm = body[pos[0]] & 0xff;
					pos[0] += 3; skip(body, pos); skip(body, pos);    // esm, protocol, priority; schedule, validity
					pos[0] += 4;                                      // registered, replace, dcs, default
					int smLen = body[pos[0]++] & 0xff;
					String sm = new String(body, pos[0], smLen, StandardCharsets.ISO_8859_1);
					if ((esm & 0x04) != 0) receipts.add(sm);
					log("deliver_sm seq=" + sq + " esm=0x" + Integer.toHexString(esm) + " " + sm);
					pdu(0x80000005, 0, sq, new byte[] {0});
				} else if (cmd == 0x00000015) {
					pdu(0x80000015, 0, sq, new byte[0]);
				} else if (cmd == 0x00000006) {
					pdu(0x80000006, 0, sq, new byte[0]);
				}
			}
		} catch (Exception e) {
			log("SMPP connection ended: " + e);
		}
	}

	static synchronized void pdu(int cmd, int status, int sq, byte[] body) throws java.io.IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		int len = 16 + body.length;
		for (int v : new int[] {len, cmd, status, sq}) {
			b.write(v >>> 24); b.write(v >>> 16); b.write(v >>> 8); b.write(v);
		}
		b.write(body);
		smppOut.write(b.toByteArray());
		smppOut.flush();
	}

	static void cstr(ByteArrayOutputStream b, String s) throws java.io.IOException {
		b.write(s.getBytes(StandardCharsets.US_ASCII));
		b.write(0);
	}

	static String cstrAt(byte[] body, int at) {
		int e = at;
		while (e < body.length && body[e] != 0) e++;
		return new String(body, at, e - at, StandardCharsets.US_ASCII);
	}

	static void skip(byte[] body, int[] pos) {
		while (pos[0] < body.length && body[pos[0]] != 0) pos[0]++;
		pos[0]++;
	}

	static String hex(byte[] b) {
		StringBuilder s = new StringBuilder();
		for (byte x : b) s.append(String.format("%02x", x & 0xff));
		return s.toString();
	}

	static String jsonField(String json, String name) {
		Matcher m = Pattern.compile("\"" + Pattern.quote(name) + "\"\\s*:\\s*(\"((?:[^\"\\\\]|\\\\.)*)\"|[^,}\\s]+)").matcher(json);
		if (!m.find()) return null;
		return m.group(2) != null ? m.group(2).replace("\\\"", "\"").replace("\\\\", "\\") : m.group(1);
	}

	static String[] post(String url, String body, String contentType) {
		try {
			HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
			c.setConnectTimeout(3000);
			c.setReadTimeout(15000);
			c.setDoOutput(true);
			c.setRequestMethod("POST");
			c.setRequestProperty("Content-Type", contentType + ";charset=UTF-8");
			try (OutputStream o = c.getOutputStream()) {
				o.write(body.getBytes(StandardCharsets.UTF_8));
			}
			int code = c.getResponseCode();
			InputStream is = code < 400 ? c.getInputStream() : c.getErrorStream();
			return new String[] {String.valueOf(code), is == null ? "" : read(is).trim()};
		} catch (Exception e) {
			return new String[] {"ERR", e.toString()};
		}
	}

	static String read(InputStream in) throws java.io.IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
		for (int n; (n = in.read(buf)) > 0;) b.write(buf, 0, n);
		return new String(b.toByteArray(), StandardCharsets.UTF_8);
	}

	static void answer(HttpExchange ex, int code, String text) throws java.io.IOException {
		byte[] b = text.getBytes(StandardCharsets.UTF_8);
		ex.getResponseHeaders().set("Content-Type", "application/json");
		ex.sendResponseHeaders(code, b.length);
		try (OutputStream o = ex.getResponseBody()) {
			o.write(b);
		}
		ex.close();
	}

	static String enc(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (Exception e) {
			return s;
		}
	}

	static Properties load(File f) throws java.io.IOException {
		Properties p = new Properties();
		try (InputStream in = new FileInputStream(f)) {
			p.load(in);
		}
		return p;
	}

	interface Cond {
		boolean ok();
	}

	static void waitFor(Cond c, long ms) throws InterruptedException {
		long until = System.currentTimeMillis() + ms;
		while (!c.ok() && System.currentTimeMillis() < until) Thread.sleep(100);
	}

	static void check(boolean ok, String what) {
		if (ok) pass++; else fail++;
		log((ok ? "PASS " : "FAIL ") + what);
	}

	static void log(String s) {
		String l = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " " + s;
		lines.add(l);
		System.out.println(l);
	}
}
