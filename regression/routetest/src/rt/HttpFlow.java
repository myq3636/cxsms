package rt;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * The four HTTP servlets of CommonHttpServer through the whole flow (docs/HTTP服务器全流程测试.md), JDK only:
 * the customers send to CommonHttpServlet (XML), CommonJsonHttpServlet (JSON) and AliCainiaoJsonHttpServlet (Cainiao
 * JSON, signed); Core routes them to the HTTP vendor (CommonHttpClient, interface Common), played here: it answers the
 * submit and sends the DR to CommonHttpDRServlet (/Common/DRReply); Core pushes the DR to each customer (its
 * ChlDRURL, here too), in the format of the customer's interface.
 *   java -cp routetest.jar rt.HttpFlow CASE_DIR GATEWAY_PORT
 */
public class HttpFlow {
	static final List<String> lines = Collections.synchronizedList(new ArrayList<String>());
	static int pass = 0, fail = 0;
	/** what the vendor got: REFERENCE, TO, FROM, BODY, PRODUCTTOKEN; and the answer of /Common/DRReply */
	static final List<Map<String, String>> vendorMts = Collections.synchronizedList(new ArrayList<Map<String, String>>());
	static final List<String> drReplies = Collections.synchronizedList(new ArrayList<String>());
	/** the DRs pushed to the customers: path, body */
	static final List<String[]> customerDrs = Collections.synchronizedList(new ArrayList<String[]>());
	static String gateway;

	public static void main(String[] args) throws Exception {
		File caseDir = new File(args[0]);
		Properties p = new Properties();
		try (InputStream in = new FileInputStream(new File(caseDir, "case.properties"))) {
			p.load(in);
		}
		gateway = "http://127.0.0.1:" + args[1] + "/Gmms";
		int vendorPort = Integer.parseInt(p.getProperty("vendor.port", "18602"));
		int drPort = Integer.parseInt(p.getProperty("customer.dr.port", "18601"));
		long drDelayMs = Long.parseLong(p.getProperty("vendor.drDelayMs", "500"));
		String run = new SimpleDateFormat("HHmmss").format(new Date());
		ScheduledExecutorService drSender = Executors.newScheduledThreadPool(4);

		HttpServer vendor = HttpServer.create(new InetSocketAddress(vendorPort), 64);
		vendor.createContext("/vendor", ex -> {
			String body = body(ex);
			Map<String, String> mt = new LinkedHashMap<String, String>();
			for (String tag : new String[] {"PRODUCTTOKEN", "FROM", "TO", "BODY", "REFERENCE", "DCS"}) {
				mt.put(tag, xmlTag(body, tag));
			}
			vendorMts.add(mt);
			log("vendor got " + mt);
			answer(ex, 200, "");   // Common: an empty answer is SUCCESS (CommonMessageHttpHandler.parseResponse)
			final String ref = mt.get("REFERENCE"), to = mt.get("TO");
			drSender.schedule(() -> {
				String form = "REFERENCE=" + enc(ref) + "&STATUS=2&STATUSDESCRIPTION=Delivered&GSM=" + enc(to);
				String[] r = post(gateway + "/Common/DRReply", form, "application/x-www-form-urlencoded");
				drReplies.add(ref + " -> " + r[0] + " " + r[1]);
				log("vendor DR " + ref + " to /Common/DRReply: " + r[0] + " " + r[1]);
			}, drDelayMs, TimeUnit.MILLISECONDS);
		});
		vendor.setExecutor(Executors.newFixedThreadPool(8));
		vendor.start();
		HttpServer customers = HttpServer.create(new InetSocketAddress(drPort), 64);
		customers.createContext("/dr", ex -> {
			String q = ex.getRequestURI().getRawQuery();
			String body = body(ex);
			customerDrs.add(new String[] {ex.getRequestURI().getPath(), body + (q == null ? "" : (body.isEmpty() ? "" : "&") + q)});
			log("customer DR " + ex.getRequestURI().getPath() + ": " + body + (q == null ? "" : " ?" + q));
			answer(ex, 200, "");
		});
		customers.setExecutor(Executors.newFixedThreadPool(8));
		customers.start();

		// ---- the submits ----
		String xmlRef = "hx" + run, jsonRef = "hj" + run, cnRef = "hc" + run;
		String xmlTo = "8613900000001", jsonTo = "8613900000002", cnTo = "8613900000003";
		String[] r;
		r = post(gateway + "/Common/MTRequest", xml("htxml", "HTX", xmlTo, "xml test " + run, xmlRef), "text/xml");
		check(r[0].equals("200") && r[1].equals(xmlRef + ":OK"), "HT-01 CommonHttpServlet (XML) accepts: " + r[0] + " " + r[1]);
		r = post(gateway + "/Common/MTRequest", xml("nobody", "HTX", "8613900000011", "bad token", "hxbad" + run), "text/xml");
		check(r[1].contains("Invalid product token"), "HT-02 CommonHttpServlet: an unknown PRODUCTTOKEN is refused: " + r[1]);

		r = post(gateway + "/CommonJson/MTRequest", json("htjson", "HTJ", jsonTo, "json test " + run, jsonRef), "application/json");
		check(r[0].equals("200") && r[1].replace(" ", "").equals("{\"msgid\":\"" + jsonRef + "\",\"status\":\"OK\"}"),
				"HT-03 CommonJsonHttpServlet accepts: " + r[0] + " " + r[1]);
		r = post(gateway + "/CommonJson/MTRequest", json("htjson", "", "8613900000012", "no sender", "hjbad" + run), "application/json");
		check(r[1].contains("Sender Address Error"), "HT-04 CommonJsonHttpServlet: a message without FROM is refused: " + r[1]);

		r = post(gateway + "/CommonCainiao/MTRequest", cainiao("htcainiao", "HTC", cnTo, "cainiao test " + run, cnRef, "ord" + run, "ckey9953"), "application/json");
		String bizId = jsonField(r[1], "bizId");
		check(r[0].equals("200") && "OK".equals(jsonField(r[1], "code")) && cnRef.equals(jsonField(r[1], "requestId"))
				&& "1".equals(jsonField(r[1], "smsSize")) && bizId != null && !bizId.isEmpty(),
				"HT-05 AliCainiaoJsonHttpServlet accepts (signed): " + r[0] + " " + r[1]);
		r = post(gateway + "/CommonCainiao/MTRequest", cainiao("htcainiao", "HTC", "8613900000013", "bad sign", "hcbad" + run, null, "wrongkey"), "application/json");
		check(!"OK".equals(jsonField(r[1], "code")), "HT-06 AliCainiaoJsonHttpServlet: a wrong signature is refused: " + r[1]);
		// Cainiao: Chinese (UCS2) and a long message (smsSize from MessageUtils.splitNum: GSM 160 a part, else 140 bytes)
		String cnZhRef = "hcz" + run, cnLongRef = "hcl" + run, cnZhTo = "8613900000004", cnLongTo = "8613900000005";
		String zh = "菜鸟中文测试" + run;
		StringBuilder longText = new StringBuilder("long " + run + " ");
		while (longText.length() < 200) longText.append("abcdefghij");
		String cnLong = longText.substring(0, 200);
		r = post(gateway + "/CommonCainiao/MTRequest", cainiao("htcainiao", "HTC", cnZhTo, zh, cnZhRef, null, "ckey9953"), "application/json");
		check("OK".equals(jsonField(r[1], "code")) && "1".equals(jsonField(r[1], "smsSize")), "HT-16 Cainiao: Chinese content accepted, smsSize 1: " + r[1]);
		r = post(gateway + "/CommonCainiao/MTRequest", cainiao("htcainiao", "HTC", cnLongTo, cnLong, cnLongRef, null, "ckey9953"), "application/json");
		check("OK".equals(jsonField(r[1], "code")) && "2".equals(jsonField(r[1], "smsSize")), "HT-17 Cainiao: 200 characters accepted, smsSize 2: " + r[1]);

		// ---- the vendor, the DRs ----
		long until = System.currentTimeMillis() + Long.parseLong(p.getProperty("wait.sec", "90")) * 1000L;
		while (System.currentTimeMillis() < until && (mtTo(cnLongTo) == null || vendorMts.size() < 5 || drReplies.size() < vendorMts.size()
				|| drsOf(cnRef, cnZhRef, cnLongRef, xmlRef, jsonRef) < 5)) {
			Thread.sleep(500);
		}
		Thread.sleep(3000);   // anything that should not come
		Map<String, String> mx = mtTo(xmlTo), mj = mtTo(jsonTo), mc = mtTo(cnTo);
		check(mx != null && ("xml test " + run).equals(mx.get("BODY")) && "HTX".equals(mx.get("FROM")) && "vtoken9961".equals(mx.get("PRODUCTTOKEN"))
				&& notEmpty(mx.get("REFERENCE")), "HT-07 the XML customer's MT reached the vendor (Common XML: token, from, body, reference): " + mx);
		check(mj != null && ("json test " + run).equals(mj.get("BODY")) && "HTJ".equals(mj.get("FROM")), "HT-08 the JSON customer's MT reached the vendor: " + mj);
		check(mc != null && ("cainiao test " + run).equals(mc.get("BODY")) && "HTC".equals(mc.get("FROM")), "HT-09 the Cainiao customer's MT reached the vendor: " + mc);
		check(mtTo("8613900000011") == null && mtTo("8613900000012") == null && mtTo("8613900000013") == null,
				"HT-10 none of the refused requests reached the vendor: " + vendorMts.size() + " MTs");
		Map<String, String> mz = mtTo(cnZhTo);
		check(mz != null && zh.equals(mz.get("BODY")) && "8".equals(mz.get("DCS")), "HT-18 the Chinese MT reached the vendor as UCS2 (DCS 8), text intact: " + mz);
		StringBuilder parts = new StringBuilder();
		int nParts = 0;
		synchronized (vendorMts) {
			for (Map<String, String> m : vendorMts) if (cnLongTo.equals(m.get("TO"))) { parts.append(m.get("BODY")); nParts++; }
		}
		check(cnLong.equals(parts.toString()), "HT-19 the long MT reached the vendor whole (" + nParts + " part(s)): " + parts.length() + " characters");
		int drOk = 0;
		synchronized (drReplies) {
			for (String d : drReplies) if (d.contains(" -> 200")) drOk++;
		}
		check(drReplies.size() == vendorMts.size() && drOk == drReplies.size(),
				"HT-11 CommonHttpDRServlet answers every DR of the vendor (one per MT) with 200: " + drOk + " of " + vendorMts.size() + " " + drReplies);

		String dx = drBody("/dr/xml"), dj = drBody("/dr/json"), dc = cainiaoDr(cnRef);
		Map<String, String> fx = form(dx), fj = form(dj);
		check(dx != null && xmlRef.equals(fx.get("reference")) && "2".equals(fx.get("statuscode")),
				"HT-12 the DR pushed to the XML customer: its REFERENCE, statuscode 2 (Delivered): " + dx);
		check(dj != null && jsonRef.equals(fj.get("reference")) && "2".equals(fj.get("statuscode")),
				"HT-13 the DR pushed to the JSON customer: its REFERENCE, statuscode 2: " + dj);
		check(dc != null && cnRef.equals(jsonField(dc, "requestId")) && "DELIVERED".equals(jsonField(dc, "errorCode"))
				&& "true".equals(jsonField(dc, "success")) && bizId != null && bizId.equals(jsonField(dc, "bizId"))
				&& ("ord" + run).equals(jsonField(dc, "extend")) && notEmpty(jsonField(dc, "signature")),
				"HT-14 the DR pushed to the Cainiao customer (Cainiao JSON: requestId, bizId of the submit, DELIVERED, extend, signed): " + dc);
		String dz = cainiaoDr(cnZhRef), dl = cainiaoDr(cnLongRef);
		check(dz != null && "DELIVERED".equals(jsonField(dz, "errorCode")) && "1".equals(jsonField(dz, "smsSize")), "HT-20 the Chinese message's DR pushed (Cainiao, smsSize 1): " + dz);
		check(dl != null && "DELIVERED".equals(jsonField(dl, "errorCode")) && "2".equals(jsonField(dl, "smsSize")), "HT-21 the long message's DR pushed (Cainiao, smsSize 2): " + dl);
		boolean refusedDr = false;
		synchronized (customerDrs) {
			for (String[] d : customerDrs) refusedDr |= d[1].contains("bad" + run);
		}
		check(drsOf(xmlRef, jsonRef, cnRef, cnZhRef, cnLongRef) == 5 && !refusedDr,
				"HT-15 a DR for each accepted message, none for the refused ones: " + customerDrs.size() + " DR(s) pushed");

		vendor.stop(0);
		customers.stop(0);
		drSender.shutdownNow();
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

	static String xml(String token, String from, String to, String body, String ref) {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MESSAGES><AUTHENTICATION><PRODUCTTOKEN>" + token
				+ "</PRODUCTTOKEN></AUTHENTICATION><MSG><FROM>" + from + "</FROM><TO>" + to + "</TO><BODY>" + body
				+ "</BODY><DCS>0</DCS><REFERENCE>" + ref + "</REFERENCE></MSG></MESSAGES>";
	}

	static String json(String token, String from, String to, String body, String ref) {
		return "{\"AUTHENTICATION\":{\"PRODUCTTOKEN\":\"" + token + "\"},\"MSG\":[{\"FROM\":\"" + from + "\",\"TO\":\"" + to
				+ "\",\"BODY\":\"" + body + "\",\"DCS\":\"0\",\"REFERENCE\":\"" + ref + "\"}]}";
	}

	/** the Cainiao submit, signed as SMSSignatureUtil.getCainiaoSignature: base64(md5(key + enc(sorted k=v, &))) */
	static String cainiao(String supplier, String sender, String phone, String content, String requestId, String extend, String key) {
		String timestamp = String.valueOf(System.currentTimeMillis());
		TreeMap<String, String> m = new TreeMap<String, String>();
		m.put("supplier", supplier);
		m.put("timestamp", timestamp);
		m.put("method", "MD5");
		m.put("requestId", requestId);
		m.put("phoneNumber", phone);
		m.put("senderId", sender);
		m.put("content", content);
		m.put("smsType", "1");
		if (extend != null) m.put("extend", extend);
		StringBuilder s = new StringBuilder();
		for (Map.Entry<String, String> e : m.entrySet()) s.append('&').append(enc(e.getKey())).append('=').append(enc(e.getValue()));
		String sign;
		try {
			byte[] d = MessageDigest.getInstance("MD5").digest((key + enc(s.substring(1))).getBytes(StandardCharsets.UTF_8));
			sign = Base64.getEncoder().encodeToString(d);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		StringBuilder j = new StringBuilder("{");
		for (Map.Entry<String, String> e : m.entrySet()) j.append('"').append(e.getKey()).append("\":\"").append(e.getValue()).append("\",");
		return j.append("\"signature\":\"").append(sign).append("\"}").toString();
	}

	static Map<String, String> mtTo(String to) {
		synchronized (vendorMts) {
			for (Map<String, String> m : vendorMts) if (to.equals(m.get("TO"))) return m;
		}
		return null;
	}

	static String drBody(String path) {
		synchronized (customerDrs) {
			for (String[] d : customerDrs) if (d[0].equals(path)) return d[1];
		}
		return null;
	}

	/** how many of these customer references got a DR pushed */
	static int drsOf(String... refs) {
		int n = 0;
		for (String ref : refs) {
			synchronized (customerDrs) {
				for (String[] d : customerDrs) if (d[1].contains("=" + ref + "&") || d[1].endsWith("=" + ref) || d[1].contains("\"" + ref + "\"")) { n++; break; }
			}
		}
		return n;
	}

	static String cainiaoDr(String requestId) {
		synchronized (customerDrs) {
			for (String[] d : customerDrs) if (d[0].equals("/dr/cainiao") && requestId.equals(jsonField(d[1], "requestId"))) return d[1];
		}
		return null;
	}

	static Map<String, String> form(String s) {
		Map<String, String> m = new LinkedHashMap<String, String>();
		if (s == null) return m;
		for (String kv : s.split("&")) {
			int i = kv.indexOf('=');
			if (i <= 0) continue;
			try {
				m.put(kv.substring(0, i), URLDecoder.decode(kv.substring(i + 1), "UTF-8"));
			} catch (Exception e) {
				m.put(kv.substring(0, i), kv.substring(i + 1));
			}
		}
		return m;
	}

	/** a top-level string, number or boolean field of a flat JSON object */
	static String jsonField(String json, String name) {
		if (json == null) return null;
		Matcher m = Pattern.compile("\"" + Pattern.quote(name) + "\"\\s*:\\s*(\"((?:[^\"\\\\]|\\\\.)*)\"|[^,}\\s]+)").matcher(json);
		if (!m.find()) return null;
		return m.group(2) != null ? m.group(2) : m.group(1);
	}

	static String xmlTag(String xml, String tag) {
		Matcher m = Pattern.compile("<" + tag + ">(.*?)</" + tag + ">", Pattern.DOTALL).matcher(xml);
		return m.find() ? m.group(1).replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&") : null;
	}

	/** POST, the status and the body ("ERR" and the exception if it failed) */
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
			InputStream in = code < 400 ? c.getInputStream() : c.getErrorStream();
			return new String[] {String.valueOf(code), in == null ? "" : read(in).trim()};
		} catch (Exception e) {
			return new String[] {"ERR", e.toString()};
		}
	}

	static String body(HttpExchange ex) throws java.io.IOException {
		return read(ex.getRequestBody());
	}

	static String read(InputStream in) throws java.io.IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		byte[] buf = new byte[4096];
		for (int n; (n = in.read(buf)) > 0;) b.write(buf, 0, n);
		return new String(b.toByteArray(), StandardCharsets.UTF_8);
	}

	static void answer(HttpExchange ex, int code, String text) throws java.io.IOException {
		byte[] b = text.getBytes(StandardCharsets.UTF_8);
		ex.sendResponseHeaders(code, b.length == 0 ? -1 : b.length);
		if (b.length > 0) try (OutputStream o = ex.getResponseBody()) {
			o.write(b);
		}
		ex.close();
	}

	static String enc(String s) {
		try {
			return URLEncoder.encode(s == null ? "" : s, "UTF-8");
		} catch (Exception e) {
			return s;
		}
	}

	static boolean notEmpty(String s) {
		return s != null && !s.isEmpty();
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
