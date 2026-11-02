package it;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Core log chain: one INFO line per step of MT, submit result and vendor DR; not part of the repo. */
public class LogChainCoreIT extends ItSupport {

	/** it.log lines that mention one of ids, in order */
	static List<String> lines(String... ids) throws Exception {
		List<String> out = new ArrayList<String>();
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath())) {
			for (String id : ids) {
				if (l.contains(" " + id + " ")) {
					out.add(l);
					break;
				}
			}
		}
		return out;
	}

	/** the tags appear in this order among the lines (other lines may come between) */
	static boolean inOrder(List<String> ls, String... tags) {
		int i = 0;
		for (String l : ls) {
			while (i < tags.length && l.contains(tags[i])) i++;    // several tags may be on one line
		}
		return i == tags.length;
	}

	static long count(List<String> ls, String level) {
		long n = 0;
		for (String l : ls) if (l.startsWith(level)) n++;
		return n;
	}

	static void dump(List<String> ls) {
		for (String l : ls) System.out.println("    | " + (l.length() > 200 ? l.substring(0, 200) : l));
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		props.setProperty("RedisStreamTraceLogEnable", "true");
		StreamQueueManager q = StreamQueueManager.getInstance();
		ResultStreamConsumer.getInstance().start();
		MTStreamConsumer.getInstance().start();
		InboundDRStreamConsumer.getInstance().start();

		// ---- L-C1 MT: received, routed, to the client, IN_SUBMIT CDR, acked ----
		GmmsMessage m = mt("8888_LC1", 8888, -1, 0);
		q.produceSubmitMessage(m);
		check(waitFor(() -> inStreams("stream:mt:routed:8:*", "8888_LC1").size() == 1
				&& logLine("8888_LC1", "[STREAM-ACK] flow=server->core") != null, 8000), "L-C1a MT routed and acked");
		List<String> l1 = lines("8888_LC1");
		dump(l1);
		check(inOrder(l1, "[STREAM-GET] flow=server->core", "[CDR] type=IN_SUBMIT", "[MT-ROUTE] ossid=8888 rssid=8",
				"[STREAM-PUT] flow=core->client", "[MT-TO-CLIENT] rssid=8", "[STREAM-ACK] flow=server->core"),
				"L-C1b MT chain: STREAM-GET -> CDR IN_SUBMIT -> MT-ROUTE -> STREAM-PUT -> MT-TO-CLIENT -> STREAM-ACK");
		check(count(l1, "WARN") == 0 && count(l1, "ERROR") == 0, "L-C1c normal MT: no WARN/ERROR line");

		// ---- L-C2 submit_resp: result, OUT_SUBMIT CDR, acked ----
		GmmsMessage resp = inStreams("stream:mt:routed:8:*", "8888_LC1").get(0);
		resp.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		resp.setStatus(GmmsStatus.SUCCESS);
		resp.setOutMsgID("OUT-LC1");
		q.produceResult(resp);
		check(waitFor(() -> logLine("8888_LC1", "[STREAM-ACK] flow=result->core") != null && jedis.exists("OUT-LC1"), 8000),
				"L-C2a submit_resp processed and acked");
		List<String> l2 = lines("8888_LC1");
		check(inOrder(l2, "[STREAM-GET] flow=result->core", "[RESULT] type=Submit Resp status=0", "outMsgId=OUT-LC1 outbox=FIRST",
				"[CDR] type=OUT_SUBMIT status=0", "[STREAM-ACK] flow=result->core"),
				"L-C2b result chain: STREAM-GET -> RESULT (outbox=FIRST) -> CDR OUT_SUBMIT -> STREAM-ACK");

		// ---- L-C3 vendor DR: matched, OUT_DELI_REP CDR, to the server, acked ----
		GmmsMessage dr = new GmmsMessage();
		dr.setMsgID("8_LCDR1");
		dr.setOutMsgID("OUT-LC1");
		dr.setRSsID(8);
		dr.setOSsID(-1);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(GmmsStatus.DELIVERED);
		q.produceInboundDeliveryReport(dr);
		check(waitFor(() -> logLine("8_LCDR1", "[STREAM-ACK] flow=client->core") != null
				&& logLine("8888_LC1", "[DR-TO-SERVER]") != null, 8000), "L-C3a vendor DR processed and acked");
		List<String> l3 = lines("8888_LC1", "8_LCDR1");
		check(inOrder(l3, "[STREAM-GET] flow=client->core", "[VDR-MATCH] outMsgId=OUT-LC1", "-> msgId=8888_LC1 ossid=8888 (redis)",
				"[CDR] type=OUT_DELI_REP", "[DR-TO-SERVER] ossid=8888 status=10000", "[STREAM-ACK] flow=client->core"),
				"L-C3b DR chain: STREAM-GET -> VDR-MATCH (both ids on one line) -> CDR OUT_DELI_REP -> DR-TO-SERVER -> STREAM-ACK");
		check(count(lines("8888_LC1", "8_LCDR1"), "WARN") == 0,
				"L-C3c no WARN for the normal DR (bind registry fallback is INFO)");

		// ---- L-C4 an unknown outMsgId: DB lookup and stored for later matching, one line each ----
		GmmsMessage nomatch = new GmmsMessage();
		nomatch.setMsgID("8_LCDR2");
		nomatch.setOutMsgID("OUT-LC-NONE");
		nomatch.setRSsID(8);
		nomatch.setOSsID(-1);
		nomatch.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		nomatch.setStatus(GmmsStatus.DELIVERED);
		q.produceInboundDeliveryReport(nomatch);
		// no database in the local environment: the lookup fails and the entry stays pending (retried)
		check(waitFor(() -> logLine("8_LCDR2", "not in redis, looking up the DB") != null, 8000), "L-C4a unmatched DR: lookup logged");
		List<String> l4 = lines("8_LCDR2");
		dump(l4);
		String miss = logLine("8_LCDR2", "[VDR-MATCH] outMsgId=OUT-LC-NONE");
		check(miss != null && miss.startsWith("INFO") && inOrder(l4, "[STREAM-GET]", "[VDR-MATCH] outMsgId=OUT-LC-NONE")
				&& logLine("8_LCDR2", "TMP-DRLOOKUP") == null,
				"L-C4b unmatched DR: one INFO VDR-MATCH miss line (no TMP-DRLOOKUP WARN with sender/recipient/text)");

		// ---- L-C5 a late result: one INFO line, no WARN ----
		GmmsMessage late = inStreams("stream:mt:routed:8:*", "8888_LC1").get(0);
		late.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		late.setStatus(GmmsStatus.SUCCESS);
		late.setOutMsgID("OUT-LC1");
		q.produceResult(late);
		check(waitFor(() -> logLine("8888_LC1", "[RESULT-DROPPED]") != null, 8000), "L-C5a duplicate submit_resp: [RESULT-DROPPED]");
		String dropped = logLine("8888_LC1", "[RESULT-DROPPED]");
		check(dropped.startsWith("INFO") && cdrCount("8888_LC1", "OUT_SUBMIT") == 1,
				"L-C5b dropped result is INFO, no second OUT_SUBMIT CDR");
	}
}
