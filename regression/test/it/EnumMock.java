package it;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * A stand-in for the number lookups the Core makes before routing (IOSMSDispatcher, DNSClient):
 * <ul>
 * <li>ENUM / MNP over HTTP (SMSOptionEnumRoutingURL, GmmsConfig mnpqueryurl): POST {"number":"..."}, answered as the
 * real service does: {"errmsg":"SUCCESS","data":[{"number":{"country_code":..,"operator_id":..,"mcc":..,"mnc":..,
 * "operator":..,"type":..}}]};</li>
 * <li>IR21 over DNS (SMSOptionEnumIR21RoutingURL, GmmsConfig DNSAddress): the Core runs
 * {@code dig @server -t NAPTR 0.0.0.8.3.1.6.8.e164.arpa.}; answered with
 * {@code NAPTR 100 100 "u" "E2U+a2p" "!^.*$!a2p:number@cc86.opid1.mnc00.mcc460.gprs!" .}</li>
 * </ul>
 * The answers come from a rule file, one rule a line, the longest prefix of the number wins:
 * <pre>
 * # prefix   answer                          [delay=ms]
 * 8613800    86,1,460,00,ChinaMobile
 * 8613902    fail                            # errmsg FAIL, no data
 * 8613903    86,1,460,00,CM  delay=35000     # answers after 35 s (the Core reads for 30 s)
 * 8613904    http500 | badjson | empty | notfound
 * </pre>
 * notfound is also the answer for a number no rule matches. The file is read again when it changes.
 * GET /stats: queries by number; POST /reset clears them; GET /health.
 *
 * Standalone: java -cp a2p-test-classes it.EnumMock --http=127.0.0.1:15200 --dns=127.0.0.1:15353 --config=enum-mock.cfg
 */
public class EnumMock {
	static final class Rule {
		String prefix, action; int cc, opid; String mcc, mnc, name; long delayMs;
	}

	final Map<String, AtomicLong> httpCount = new ConcurrentHashMap<String, AtomicLong>();
	final Map<String, AtomicLong> dnsCount = new ConcurrentHashMap<String, AtomicLong>();
	volatile List<Rule> rules = new ArrayList<Rule>();
	Path configFile;
	long configMtime = -1;
	HttpServer http;
	DatagramSocket dns;
	volatile boolean running = true;

	// ---- rules ----

	static List<Rule> parse(List<String> lines) {
		List<Rule> out = new ArrayList<Rule>();
		for (String raw : lines) {
			String line = raw.contains("#") ? raw.substring(0, raw.indexOf('#')) : raw;
			line = line.trim();
			if (line.isEmpty()) continue;
			String[] f = line.split("\\s+");
			if (f.length < 2) continue;
			Rule r = new Rule();
			r.prefix = f[0];
			for (int i = 2; i < f.length; i++) if (f[i].startsWith("delay=")) r.delayMs = Long.parseLong(f[i].substring(6));
			String[] a = f[1].split(",");
			if (a.length >= 4) {
				r.action = "ok";
				r.cc = Integer.parseInt(a[0]);
				r.opid = Integer.parseInt(a[1]);
				r.mcc = a[2];
				r.mnc = a[3];
				r.name = a.length > 4 ? a[4] : "OP" + r.opid;
			} else {
				r.action = f[1].toLowerCase(Locale.ROOT);
			}
			out.add(r);
		}
		out.sort((x, y) -> y.prefix.length() - x.prefix.length());
		return out;
	}

	public void setRules(List<String> lines) { rules = parse(lines); }

	synchronized void reloadIfChanged() {
		if (configFile == null) return;
		try {
			long m = Files.getLastModifiedTime(configFile).toMillis();
			if (m != configMtime) {
				rules = parse(Files.readAllLines(configFile, StandardCharsets.UTF_8));
				configMtime = m;
				log("rules loaded from " + configFile + ": " + rules.size());
			}
		} catch (IOException e) {
			log("rules not read: " + e);
		}
	}

	Rule match(String number) {
		reloadIfChanged();
		String n = number.startsWith("+") ? number.substring(1) : number;
		for (Rule r : rules) if (n.startsWith(r.prefix)) return r;
		return null;
	}

	static void log(String s) { System.out.println(new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + " [ENUM-MOCK] " + s); }

	static void sleep(long ms) { if (ms > 0) try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }

	public long httpQueries(String number) { AtomicLong c = httpCount.get(number); return c == null ? 0 : c.get(); }
	public long dnsQueries(String number) { AtomicLong c = dnsCount.get(number); return c == null ? 0 : c.get(); }

	// ---- HTTP ----

	void handle(HttpExchange ex) throws IOException {
		String path = ex.getRequestURI().getPath();
		if ("GET".equals(ex.getRequestMethod()) && path.endsWith("/health")) { reply(ex, 200, "ok"); return; }
		if ("GET".equals(ex.getRequestMethod()) && path.endsWith("/stats")) {
			StringBuilder b = new StringBuilder("{\"http\":").append(json(httpCount)).append(",\"dns\":").append(json(dnsCount)).append('}');
			reply(ex, 200, b.toString());
			return;
		}
		if (path.endsWith("/reset")) { httpCount.clear(); dnsCount.clear(); reply(ex, 200, "ok"); return; }
		String body = new String(readAll(ex.getRequestBody()), StandardCharsets.UTF_8);
		String number = field(body, "number");
		if (number == null) { reply(ex, 400, "{\"errmsg\":\"no number\"}"); return; }
		httpCount.computeIfAbsent(number, k -> new AtomicLong()).incrementAndGet();
		Rule r = match(number);
		String action = r == null ? "notfound" : r.action;
		if (r != null) sleep(r.delayMs);
		log("HTTP " + number + " -> " + action + (r != null && r.delayMs > 0 ? " after " + r.delayMs + " ms" : ""));
		switch (action) {
		case "ok":
			reply(ex, 200, "{\"errmsg\":\"SUCCESS\",\"data\":[{\"" + number + "\":{\"country_code\":" + r.cc + ",\"operator_id\":" + r.opid
					+ ",\"mcc\":\"" + r.mcc + "\",\"mnc\":\"" + r.mnc + "\",\"operator\":\"" + r.name + "\",\"type\":\"mobile\"}}]}");
			break;
		case "fail": reply(ex, 200, "{\"errmsg\":\"FAIL\",\"data\":null}"); break;
		case "empty": reply(ex, 200, "{\"errmsg\":\"SUCCESS\",\"data\":[]}"); break;
		case "http500": reply(ex, 500, "{\"errmsg\":\"internal error\"}"); break;
		case "badjson": reply(ex, 200, "<html>not json</html>"); break;
		default: reply(ex, 200, "{\"errmsg\":\"NOT FOUND\",\"data\":null}");
		}
	}

	static String json(Map<String, AtomicLong> m) {
		StringBuilder b = new StringBuilder("{");
		for (Map.Entry<String, AtomicLong> e : new TreeMap<String, AtomicLong>(m).entrySet()) {
			if (b.length() > 1) b.append(',');
			b.append('"').append(e.getKey()).append("\":").append(e.getValue().get());
		}
		return b.append('}').toString();
	}

	/** the string or number value of a top level field of a small JSON object */
	static String field(String json, String name) {
		int i = json.indexOf("\"" + name + "\"");
		if (i < 0) return null;
		int c = json.indexOf(':', i);
		int s = c + 1;
		while (s < json.length() && (json.charAt(s) == ' ' || json.charAt(s) == '"')) s++;
		int e = s;
		while (e < json.length() && "\",} \r\n".indexOf(json.charAt(e)) < 0) e++;
		return json.substring(s, e);
	}

	static byte[] readAll(InputStream in) throws IOException {
		ByteArrayOutputStream o = new ByteArrayOutputStream();
		byte[] b = new byte[4096];
		for (int n; (n = in.read(b)) > 0;) o.write(b, 0, n);
		return o.toByteArray();
	}

	static void reply(HttpExchange ex, int code, String body) throws IOException {
		byte[] b = body.getBytes(StandardCharsets.UTF_8);
		ex.getResponseHeaders().set("Content-Type", "application/json");
		ex.sendResponseHeaders(code, b.length);
		try (OutputStream o = ex.getResponseBody()) { o.write(b); }
	}

	// ---- DNS (NAPTR for e164.arpa) ----

	void serveDns() {
		byte[] buf = new byte[512];
		while (running) {
			try {
				DatagramPacket p = new DatagramPacket(buf, buf.length);
				dns.receive(p);
				byte[] q = Arrays.copyOf(p.getData(), p.getLength());
				byte[] a = answer(q);
				if (a != null) dns.send(new DatagramPacket(a, a.length, p.getSocketAddress()));
			} catch (IOException e) {
				if (running) log("DNS error: " + e);
			}
		}
	}

	/** the reply to one query: NAPTR for a number of the rules, NXDOMAIN otherwise */
	byte[] answer(byte[] q) {
		if (q.length < 12) return null;
		int i = 12;
		List<String> labels = new ArrayList<String>();
		while (i < q.length && q[i] != 0) {
			int len = q[i] & 0xff;
			labels.add(new String(q, i + 1, len, StandardCharsets.US_ASCII));
			i += len + 1;
		}
		int qEnd = i + 5; // the 0, QTYPE, QCLASS
		if (qEnd > q.length) return null;
		int qtype = ((q[i + 1] & 0xff) << 8) | (q[i + 2] & 0xff);
		StringBuilder digits = new StringBuilder();
		int e164 = labels.indexOf("e164");
		for (int k = (e164 < 0 ? labels.size() : e164) - 1; k >= 0; k--) digits.append(labels.get(k));
		String number = digits.toString();
		dnsCount.computeIfAbsent(number, k -> new AtomicLong()).incrementAndGet();
		Rule r = e164 < 0 ? null : match(number);
		if (r != null) sleep(r.delayMs);
		boolean ok = r != null && "ok".equals(r.action) && qtype == 35;
		log("DNS " + String.join(".", labels) + " type " + qtype + " -> " + (ok ? "NAPTR cc" + r.cc + " opid" + r.opid : "NXDOMAIN"));
		ByteArrayOutputStream o = new ByteArrayOutputStream();
		o.write(q[0]); o.write(q[1]);                        // id
		o.write(0x84 | (q[2] & 0x01)); o.write(ok ? 0x00 : 0x03); // QR AA RD; NOERROR or NXDOMAIN
		o.write(0); o.write(1); o.write(0); o.write(ok ? 1 : 0); o.write(0); o.write(0); o.write(0); o.write(0);
		o.write(q, 12, qEnd - 12);                           // the question
		if (ok) {
			ByteArrayOutputStream rd = new ByteArrayOutputStream();
			rd.write(0); rd.write(100); rd.write(0); rd.write(100);   // order, preference
			str(rd, "u"); str(rd, "E2U+a2p");
			str(rd, "!^.*$!a2p:" + number + "@cc" + r.cc + ".opid" + r.opid + ".mnc" + r.mnc + ".mcc" + r.mcc + ".gprs!");
			rd.write(0);                                         // replacement: root
			byte[] d = rd.toByteArray();
			o.write(0xc0); o.write(12);                          // name: the question's
			o.write(0); o.write(35); o.write(0); o.write(1);     // NAPTR, IN
			o.write(0); o.write(0); o.write(0x0e); o.write(0x10); // TTL 3600
			o.write(d.length >> 8); o.write(d.length & 0xff);
			o.write(d, 0, d.length);
		}
		return o.toByteArray();
	}

	static void str(ByteArrayOutputStream o, String s) {
		byte[] b = s.getBytes(StandardCharsets.US_ASCII);
		o.write(b.length);
		o.write(b, 0, b.length);
	}

	// ---- start, stop ----

	public static EnumMock start(String httpHostPort, String dnsHostPort, Path config) throws IOException {
		EnumMock m = new EnumMock();
		m.configFile = config;
		if (config != null) m.reloadIfChanged();
		String[] h = httpHostPort.split(":");
		m.http = HttpServer.create(new InetSocketAddress(h[0], Integer.parseInt(h[1])), 50);
		m.http.createContext("/", ex -> { try { m.handle(ex); } catch (Exception e) { log("HTTP error: " + e); ex.close(); } });
		m.http.setExecutor(Executors.newFixedThreadPool(16));
		m.http.start();
		if (dnsHostPort != null && !dnsHostPort.isEmpty()) {
			String[] d = dnsHostPort.split(":");
			m.dns = new DatagramSocket(new InetSocketAddress(d[0], Integer.parseInt(d[1])));
			Thread t = new Thread(m::serveDns, "enum-mock-dns");
			t.setDaemon(true);
			t.start();
		}
		log("HTTP on " + httpHostPort + (m.dns != null ? ", DNS on " + dnsHostPort : "") + (config != null ? ", rules " + config : ""));
		return m;
	}

	public void stop() {
		running = false;
		if (http != null) http.stop(0);
		if (dns != null) dns.close();
	}

	public static void main(String[] args) throws Exception {
		String httpAddr = "127.0.0.1:15200", dnsAddr = "127.0.0.1:15353", cfg = "enum-mock.cfg";
		for (String a : args) {
			if (a.startsWith("--http=")) httpAddr = a.substring(7);
			else if (a.startsWith("--dns=")) dnsAddr = a.substring(6);
			else if (a.startsWith("--config=")) cfg = a.substring(9);
		}
		start(httpAddr, dnsAddr, Paths.get(cfg));
		Thread.currentThread().join();
	}
}
