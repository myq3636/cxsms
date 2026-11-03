package it;

import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.king.gmms.messagequeue.HttpClientStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.gmms.GmmsUtility;
import com.sun.net.httpserver.HttpServer;

/** Real HTTP requests through the real session and consumer, against a local vendor: submit answers, HTTP errors, DR push. */
public class ClientHttpIT extends CoreRetryIT {
	static final int PORT = 18091, DEAD_PORT = 18099;
	static int no = 0;
	static final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());
	static volatile int httpStatus = 200;
	static volatile String answer = "id=VH1&code=0&text=ok";
	static volatile int delayMs = 0;

	static final String IF = "<ITHTTP>\n"
			+ "<CharsetMapping><ascii value=\"ascii\">ASCII</ascii></CharsetMapping>\n"
			+ "<StatusMapping>\n"
			+ "<SUBMITRESP><ok value=\"0\"><code>0</code><text>ok</text></ok><badto value=\"2120\"><code>9</code><text>bad</text></badto></SUBMITRESP>\n"
			+ "<DR><done value=\"10000\"><code>D</code><text>DELIVRD</text></done><und value=\"10400\"><code>U</code><text>UNDELIV</text></und></DR>\n"
			+ "<DRRESP><ok value=\"10000\"><code>0</code><text>ok</text></ok></DRRESP>\n"
			+ "</StatusMapping>\n"
			+ "<MO><DRRequest><HandlerClass>com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler</HandlerClass><inMsgID>msgid</inMsgID><statusCode>stat</statusCode><statusText>txt</statusText></DRRequest></MO>\n"
			+ "<MT><SubmitRequest><HandlerClass>it.ItHttpHandler</HandlerClass></SubmitRequest><SubmitResponse><outMsgID>id</outMsgID><StatusCode>code</StatusCode><StatusText>text</StatusText></SubmitResponse></MT>\n"
			+ "</ITHTTP>\n";

	static String channel(String name, int ssid, int port) { return channel(name, ssid, port, "ITHTTP"); }
	static String channel(String name, int ssid, int port, String protocol) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: " + protocol + "\nChlQueue: CommonHttpClient\nChlInit: true\nChlURL: http://127.0.0.1:" + port + "/sms\nSMSOptionHttpReadTimeout: 2\nSMSOptionHttpConnectTimeout: 1\n";
	}
	static GmmsMessage result(String id, long ms) throws Exception {
		waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), ms);
		List<GmmsMessage> l = inStreams("stream:core:results*", id);
		return l.isEmpty() ? null : l.get(l.size() - 1);
	}
	static String show(GmmsMessage g) { return g == null ? "none" : "type=" + g.getMessageType() + " status=" + g.getStatusCode() + " out=" + g.getOutMsgID(); }
	static String submit(int rssid) {
		String id = "8888_CH" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		q.produceHttpSubmitMessage(m);
		return id;
	}
	static String dr(int ssid, GmmsStatus st) {
		String id = "CHD" + (++no);
		GmmsMessage m = mt(id, ssid, 8, 0);
		m.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		m.setStatus(st);
		q.produceHttpDeliveryReport(m);
		return id;
	}
	static boolean empty(String pattern, String group) throws Exception {
		return waitFor(() -> {
			for (String k : jedis.keys(pattern)) { if (jedis.xlen(k) > 0) return false; try { if (jedis.xpending(k, group).getTotal() > 0) return false; } catch (Exception e) { } }
			return true;
		}, 5000);
	}
	/** what getResendDR does with a row of RDQ */
	static GmmsMessage fromRdq(GmmsMessage failed) throws Exception {
		java.lang.reflect.Method m = MessageStoreManager.class.getDeclaredMethod("getCodeFromState", String.class);
		m.setAccessible(true);
		GmmsMessage b = new GmmsMessage(failed);
		b.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		b.setStatusCode((Integer) m.invoke(GmmsUtility.getInstance().getMessageStoreManager(), failed.getStatusText()));
		b.setOriginalQueue("RDQ");
		b.setRowId(5);
		return b;
	}
	static String last() { return bodies.isEmpty() ? "" : bodies.get(bodies.size() - 1); }

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Files.write(Paths.get(home + "conf/ITHTTP.xml"), IF.getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/ITPLAIN.xml"), IF.replace("<HandlerClass>it.ItHttpHandler</HandlerClass>", "").replace("ITHTTP", "ITPLAIN").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\nRedis_master_port=17796\n").getBytes("UTF-8"));
		RedisGate gate = new RedisGate(17796, 16379);
		msmFactory = RecordingStore::new;
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + channel("HTV51", 51, PORT) + channel("HTV52", 52, DEAD_PORT) + channel("HTV53", 53, PORT, "ITPLAIN")).getBytes("UTF-8"));
		HttpServer vendor = HttpServer.create(new InetSocketAddress("127.0.0.1", PORT), 0);
		vendor.createContext("/", ex -> {
			bodies.add(new String(ex.getRequestBody().readAllBytes(), "UTF-8"));
			try { Thread.sleep(delayMs); } catch (InterruptedException e) { }
			byte[] b = answer.getBytes("UTF-8");
			ex.sendResponseHeaders(httpStatus, b.length == 0 ? -1 : b.length);
			if (b.length > 0) ex.getResponseBody().write(b);
			ex.close();
		});
		vendor.setExecutor(Executors.newCachedThreadPool());
		vendor.start();
		try {
			boot();
			q = StreamQueueManager.getInstance();
			HttpClientStreamConsumer.getInstance().start();
			Thread.sleep(500);

			// ---- submit, vendor accepts ----
			String id = submit(51);
			GmmsMessage r = result(id, 8000);
			System.out.println("INFO body " + last());
			check(bodies.size() == 1, "HT-07a one real request: " + bodies.size());
			check(last().contains("8613800000000") && last().contains("18667991226"), "HT-07b recipient and sender in the request");
			check(r != null && r.getStatusCode() == 0 && "VH1".equals(r.getOutMsgID()), "HT-07c answer parsed: success, outMsgId from the response: " + show(r));
			check(r != null && GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(r.getMessageType()), "HT-07d written as a submit result");
			check(empty("stream:http:submit:51:*", "HttpClientSubmitGroup"), "HT-07e stream empty, nothing pending");
			check(jedis.keys("client:sent:" + id + "*").isEmpty(), "HT-07f no marker left");

			// ---- submit, vendor answers an error ----
			Object[][] answers = { { 200, "id=VH2&code=9&text=bad", GmmsStatus.RECIPIENT_ADDR_ERROR.getCode(), "mapped code" },
				{ 200, "id=VH3&code=77&text=what", GmmsStatus.UNKNOWN_ERROR.getCode(), "code not in the mapping" },
				{ 200, "garbage", GmmsStatus.INVALID_MSG_FORMAT.getCode(), "not key=value" },
				{ 200, "", GmmsStatus.INVALID_MSG_FORMAT.getCode(), "empty body" } };
			for (Object[] c : answers) {
				httpStatus = (Integer) c[0]; answer = (String) c[1];
				r = result(submit(51), 8000);
				System.out.println("INFO answer [" + c[1] + "] " + show(r));
				check(r != null && r.getStatusCode() == (Integer) c[2], "HT-08a " + c[3] + " -> " + c[2] + ": " + show(r));
			}
			Object[][] https = { { 400, 2300 }, { 401, 5000 }, { 403, 5000 }, { 408, 2500 }, { 500, 2300 }, { 503, 2600 }, { 504, 2300 } };
			for (Object[] c : https) {
				httpStatus = (Integer) c[0]; answer = "";
				r = result(submit(51), 8000);
				System.out.println("INFO submit http " + c[0] + " " + show(r));
				check(r != null && r.getStatusCode() == (Integer) c[1], "HT-08b HTTP " + c[0] + " -> " + c[1] + ": " + show(r));
			}
			httpStatus = 200; answer = "id=VH9&code=0&text=ok";

			// ---- submit, nobody there ----
			r = result(submit(52), 8000);
			System.out.println("INFO submit refused " + show(r));
			check(r != null && r.getStatusCode() == 9000, "HT-08c connection refused: 9000, not the 2500 of an SMPP channel: " + show(r));
			delayMs = 3000; httpStatus = 200;
			r = result(submit(51), 8000);
			delayMs = 0;
			System.out.println("INFO submit slow " + show(r));
			check(r != null && r.getStatusCode() == 9000, "HT-08d read timeout: 9000: " + show(r));
			r = result(submit(53), 8000);
			System.out.println("INFO plain " + show(r));
			check(r != null && r.getStatusCode() == 2500 && logLine(r.getMsgID(), "Can't send empty message") != null, "HT-07g channel without HandlerClass: the default handler builds nothing, no request, 2500: " + show(r));

			// ---- DR pushed to an HTTP customer ----
			bodies.clear();
			answer = "";
			id = dr(51, GmmsStatus.DELIVERED);
			r = result(id, 8000);
			System.out.println("INFO dr body " + last() + " -> " + show(r));
			check(bodies.size() == 1 && last().contains("stat=D") && last().contains("txt=DELIVRD"), "HT-09a DR pushed: status from the mapping: " + last());
			check(last().contains("msgid=IN-" + id) , "HT-09b it carries the customer's message id: " + last());
			check(r != null && GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(r.getMessageType()) && r.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "HT-09c DR result written: " + show(r));
			check(empty("stream:http:dr:51:*", "HttpClientDRGroup"), "HT-09d stream empty, nothing pending");
			id = dr(51, GmmsStatus.UNDELIVERABLE);
			r = result(id, 8000);
			check(last().contains("stat=U") && last().contains("txt=UNDELIV"), "HT-09e failure status mapped: " + last());
			System.out.println("INFO dr und -> " + show(r));

			// what Core retries is 11005 only (MessageStoreManager.handleInDeliveryReportRes)
			Object[][] drs = { { 401, 10500 }, { 403, 10500 }, { 511, 10500 }, { 504, 11005 }, { 400, 10900 }, { 404, 10900 } };
			String refusedByAuth = null;
			for (Object[] c : drs) {
				httpStatus = (Integer) c[0]; answer = "";
				String did = dr(51, GmmsStatus.DELIVERED);
				if ((Integer) c[0] == 401) refusedByAuth = did;
				r = result(did, 8000);
				check(r != null && r.getStatusCode() == (Integer) c[1], "HT-10a HTTP " + c[0] + " -> " + c[1] + ": " + show(r));
			}
			Object[][] temp = { { "HT-10c", 408 }, { "HT-10d", 500 }, { "HT-10e", 503 } };
			for (Object[] c : temp) {
				httpStatus = (Integer) c[1]; answer = "";
				r = result(dr(51, GmmsStatus.DELIVERED), 8000);
				check(r != null && r.getStatusCode() == 11005, c[0] + " customer's server: HTTP " + c[1] + ", temporary: DR retried (11005): " + show(r));
			}
			httpStatus = 200;
			String refused = dr(52, GmmsStatus.DELIVERED);
			r = result(refused, 8000);
			check(r != null && r.getStatusCode() == 11005, "HT-10f customer's server down, connection refused: DR retried (11005): " + show(r));
			delayMs = 3000;
			r = result(dr(51, GmmsStatus.DELIVERED), 8000);
			delayMs = 0;
			check(r != null && r.getStatusCode() == 11005, "HT-10g customer's server does not answer in time: DR retried (11005): " + show(r));
			check(empty("stream:http:dr:*", "HttpClientDRGroup"), "HT-10h every entry acked");

			// ---- the retry of a DR, as it really goes: the failure result keeps the real status in its text,
			// getResendDR restores the code from it when it reads the row from RDQ ----
			Object[][] again = { { GmmsStatus.DELIVERED, "stat=D", "txt=DELIVRD" }, { GmmsStatus.UNDELIVERABLE, "stat=U", "txt=UNDELIV" } };
			for (Object[] c : again) {
				httpStatus = 500; answer = "";
				String rid = dr(51, (GmmsStatus) c[0]);
				GmmsMessage failed = result(rid, 8000);
				check(failed != null && failed.getStatusCode() == 11005 && ((GmmsStatus) c[0]).getText().equals(failed.getStatusText()), "HT-14a failure result: 11005, the real status stays in the text: " + (failed == null ? "none" : failed.getStatusCode() + " " + failed.getStatusText()));
				httpStatus = 200; bodies.clear();
				GmmsMessage back0 = fromRdq(failed);
				back0.setMsgID(rid + "R");
				back0.setInMsgID("IN-" + rid + "R");
				q.produceHttpDeliveryReport(back0);
				r = result(rid + "R", 8000);
				System.out.println("INFO retry " + c[0] + " body=[" + last() + "] " + show(r));
				check(last().contains((String) c[1]) && last().contains((String) c[2]), "HT-14b the retry carries the real status: " + last());
			}

			// ---- no session ----
			String nosess = submit(16);
			r = result(nosess, 8000);
			check(r != null && r.getStatusCode() != 0, "HT-06 channel without a session: failure result: " + show(r));
			check(empty("stream:http:submit:16:*", "HttpClientSubmitGroup"), "HT-06b acked");

			// ---- what Core does with these results ----
			com.king.gmms.messagequeue.ResultStreamConsumer.getInstance().start();
			check(sqlSeen("INSERT", "RDQ", refused, 1), "HT-15a Core keeps a DR that could not be pushed in RDQ for a retry");
			Thread.sleep(500);
			check(sqlOf("INSERT", "RDQ", refusedByAuth).isEmpty(), "HT-15b HTTP 401: final, not kept");
			httpStatus = 500; answer = "";
			com.king.gmms.messagequeue.ResultStreamConsumer.getInstance().stop();
			Thread.sleep(500);
			String fid = dr(51, GmmsStatus.DELIVERED);
			GmmsMessage failed2 = result(fid, 8000);
			com.king.gmms.messagequeue.ResultStreamConsumer.getInstance().start();
			GmmsMessage again2 = fromRdq(failed2);
			again2.setMsgID(fid + "R");
			again2.setInMsgID("IN-" + fid + "R");
			again2.setRowId(77);
			httpStatus = 200; answer = "";
			bodies.clear();
			q.produceHttpDeliveryReport(again2);
			check(waitFor(() -> { synchronized (sql) { return sql.contains("DELETE FROM RDQ WHERE ID=77"); } }, 8000), "HT-15c the retry reached the customer: the RDQ row is removed");
			check(last().contains("txt=DELIVRD"), "HT-15d with the real status: " + last());

			// ---- two copies of one entry: the vendor gets one real request ----
			HttpClientStreamConsumer.getInstance().stop();
			Thread.sleep(500);
			java.lang.reflect.Field sf = HttpClientStreamConsumer.class.getDeclaredField("submitConsumer");
			sf.setAccessible(true);
			Object lane = sf.get(HttpClientStreamConsumer.getInstance());
			java.lang.reflect.Method pm = lane.getClass().getDeclaredMethod("processMessage", String.class, GmmsMessage.class, boolean.class);
			pm.setAccessible(true);
			answer = "id=VH7&code=0&text=ok";
			for (boolean workerFirst : new boolean[] { true, false }) {
				bodies.clear();
				String tag = submit(51);
				String stream = null;
				for (String k : jedis.keys("stream:http:submit:51:*")) if (jedis.xlen(k) > 0) stream = k;
				q.createGroup(stream, "HttpClientSubmitGroup", true);
				Map<String, redis.clients.jedis.StreamEntryID> query = new HashMap<>();
				query.put(stream, redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY);
				GmmsMessage read = q.consumeBatch("HttpClientSubmitGroup", "worker", 10, query, true).get(0);
				GmmsMessage claimed = q.recoverPending(stream, "HttpClientSubmitGroup", "claimer", 0L, 10, true).get(0);
				pm.invoke(lane, stream, workerFirst ? read : claimed, !workerFirst);
				pm.invoke(lane, stream, workerFirst ? claimed : read, true);
				check(bodies.size() == 1 && inStreams("stream:core:results*", tag).size() == 1, "HT-05 " + (workerFirst ? "claimed before the worker acked" : "the claim finished first") + ": one real request, one result: " + bodies.size());
			}

			// ---- Redis gone between reading an entry and sending it ----
			bodies.clear();
			String cutId = submit(51);
			String cutStream = null;
			for (String k : jedis.keys("stream:http:submit:51:*")) if (!inStreams(k, cutId).isEmpty()) cutStream = k;
			Map<String, redis.clients.jedis.StreamEntryID> cutQuery = new HashMap<>();
			cutQuery.put(cutStream, redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY);
			GmmsMessage cutRead = q.consumeBatch("HttpClientSubmitGroup", "worker", 10, cutQuery, true).get(0);
			gate.cut();
			try { pm.invoke(lane, cutStream, cutRead, false); } catch (java.lang.reflect.InvocationTargetException e) { System.out.println("INFO while cut: " + e.getCause()); }
			gate.restore();
			check(bodies.isEmpty(), "HT-13a the marker cannot be written: no request: " + bodies.size());
			check(jedis.xpending(cutStream, "HttpClientSubmitGroup").getTotal() >= 1, "HT-13b the entry stays pending");
			Thread.sleep(2000);
			GmmsMessage again3 = q.recoverPending(cutStream, "HttpClientSubmitGroup", "claimer", 0L, 10, true).get(0);
			pm.invoke(lane, cutStream, again3, true);
			check(bodies.size() == 1 && inStreams("stream:core:results*", cutId).size() == 1, "HT-13c Redis back: one request, one result: " + bodies.size() + "/" + inStreams("stream:core:results*", cutId).size());
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally { vendor.stop(0); }
		exit();
	}
}
