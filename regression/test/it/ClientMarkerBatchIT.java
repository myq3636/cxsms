package it;

import java.lang.reflect.Method;
import java.util.*;

import com.king.gmms.messagequeue.ClientSendMarker;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/**
 * Client send markers by batch (ClientSendMarkerBatch, docs/5000TPS多主测试慢的环节分析.md): the MTs of one stream read
 * together that have their window and permits are claimed in one Lua, sent, and acked in one Lua; the marker states
 * (sent before, another sender, a stopped sender) are those of one by one; the rate limit is the same and an MT
 * waiting for a permit has no marker; with the switch off the same MTs need more Lua calls. Vendors as ClientBatch1IT:
 * 12 unlimited, 13 SMSOptionOutgoingThrottlingNum 5.
 */
public class ClientMarkerBatchIT extends ItSupport {
	static final String GROUP = ClientBatch1IT.GROUP;

	static long lua() {
		long n = 0;
		for (String l : jedis.info("commandstats").split("\r?\n")) {
			if (l.startsWith("cmdstat_eval:") || l.startsWith("cmdstat_evalsha:")) {
				n += Long.parseLong(l.replaceAll("^[^:]+:calls=(\\d+),.*", "$1"));
			}
		}
		return n;
	}

	static List<GmmsMessage> make(String tag, int n, int rssid) {
		List<GmmsMessage> l = new ArrayList<>();
		for (int i = 0; i < n; i++) l.add(mt("8888_" + tag + i, 8888, rssid, 0));
		return l;
	}

	/** produce like Core (one sender and recipient: one stream) and read them back in one batch with the Client group */
	static List<GmmsMessage> readBatch(List<GmmsMessage> ms, String[] stream) {
		StreamQueueManager q = StreamQueueManager.getInstance();
		for (GmmsMessage m : ms) q.produceOutboundMessage(m);
		String key = q.outboundTargetKey(ms.get(0));
		stream[0] = key;
		q.createGroup(key, GROUP, true);
		Map<String, StreamEntryID> query = new HashMap<>();
		query.put(key, StreamEntryID.UNRECEIVED_ENTRY);
		return q.consumeBatch(GROUP, "client-outbound-submit-1", 100, query, true);
	}

	static boolean inBatches(String stream, List<GmmsMessage> l) throws Exception {
		Method pm = OutboundStreamConsumer.class.getDeclaredMethod("processInBatches", String.class, List.class, long.class);
		pm.setAccessible(true);
		return (Boolean) pm.invoke(OutboundStreamConsumer.getInstance(), stream, l, System.currentTimeMillis());
	}

	static int sentOnce(List<GmmsMessage> l) {
		int n = 0;
		for (GmmsMessage m : l) if (ClientBatch1IT.sent(m.getMsgID()) == 1) n++;
		return n;
	}

	static int markers(List<GmmsMessage> l) {
		int n = 0;
		for (GmmsMessage m : l) if (jedis.exists(ClientSendMarker.key(m))) n++;
		return n;
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		ClientBatch1IT.vendor(12);
		ClientBatch1IT.vendor(13);
		String[] stream = new String[1];

		// ---- MB-01 30 MTs of one stream: claimed in one Lua, sent, acked in one Lua ----
		List<GmmsMessage> a = readBatch(make("MBA", 30, 12), stream);
		check(a.size() == 30, "MB-01a 30 MTs read in one batch from " + stream[0]);
		jedis.configResetStat();
		boolean done = inBatches(stream[0], a);
		long luaA = lua();
		check(done && sentOnce(a) == 30, "MB-01b all sent once: " + sentOnce(a));
		check(jedis.xpending(stream[0], GROUP).getTotal() == 0 && jedis.xlen(stream[0]) == 0 && markers(a) == 0,
				"MB-01c all acked and deleted, no marker left");
		check(luaA <= 4, "MB-01d one Lua for the claims, one for the acks (60 one by one): " + luaA);

		// ---- MB-02 the marker states in one batch, as one by one ----
		List<GmmsMessage> b = readBatch(make("MBB", 5, 12), stream);
		long now = System.currentTimeMillis();
		jedis.set(ClientSendMarker.key(b.get(1)), "D");                                        // sent before
		jedis.setex(ClientSendMarker.key(b.get(2)), 600, "S|another:x:1|" + now);              // being sent by another
		jedis.setex(ClientSendMarker.key(b.get(3)), 600, "S|stopped:x:1|" + (now - 3600000L)); // its sender stopped
		inBatches(stream[0], b);
		int[] s = new int[5];
		for (int i = 0; i < 5; i++) s[i] = ClientBatch1IT.sent(b.get(i).getMsgID());
		check(s[0] == 1 && s[4] == 1 && s[1] == 0 && s[2] == 0 && s[3] == 0, "MB-02a sent: the two new ones; not: sent before, another sender, a stopped sender: " + Arrays.toString(s));
		check(jedis.xpending(stream[0], GROUP).getTotal() == 1 && jedis.exists(ClientSendMarker.key(b.get(2)))
				&& !jedis.exists(ClientSendMarker.key(b.get(1))) && !jedis.exists(ClientSendMarker.key(b.get(3))),
				"MB-02b only the one another consumer is sending stays pending, with its marker; the others acked, markers gone");
		jedis.del(ClientSendMarker.key(b.get(2)));
		jedis.xack(stream[0], GROUP, new StreamEntryID(b.get(2).getRedisStreamID()));

		// ---- MB-03 the rate limit is the same; an MT waiting for its permit has no marker ----
		OutboundStreamConsumer.getInstance().start();
		List<GmmsMessage> c = make("MBC", 20, 13);
		long t0 = System.currentTimeMillis();
		for (GmmsMessage m : c) StreamQueueManager.getInstance().produceOutboundMessage(m);
		Thread.sleep(1200);
		int early = 0, unsentWithMarker = 0;
		for (GmmsMessage m : c) {
			if (ClientBatch1IT.sent(m.getMsgID()) > 0) early++;
			else if (jedis.exists(ClientSendMarker.key(m))) unsentWithMarker++;
		}
		boolean all = waitFor(() -> sentOnce(c) == 20, 15000);
		long took = System.currentTimeMillis() - t0;
		check(early < 20 && unsentWithMarker == 0, "MB-03a after 1.2 s at 5/s: " + early + " sent, the " + (20 - early) + " waiting for a permit have no marker: " + unsentWithMarker);
		check(all && took >= 2500, "MB-03b 20 at 5/s: all sent once in " + took + " ms (not faster than the limit)");
		check(waitFor(() -> markers(c) == 0, 5000), "MB-03c no marker left");

		// ---- MB-04 the same with the switch off: sent once each, more Lua calls ----
		long[] luaPer = new long[2];
		for (int round = 0; round < 2; round++) {
			if (round == 1) props.setProperty("ClientSendMarkerBatch", "false");
			List<GmmsMessage> d = make(round == 0 ? "MBD" : "MBE", 30, 12);
			jedis.configResetStat();
			for (GmmsMessage m : d) StreamQueueManager.getInstance().produceOutboundMessage(m);
			boolean ok = waitFor(() -> sentOnce(d) == 30 && markers(d) == 0, 15000);
			luaPer[round] = lua();
			check(ok, "MB-04" + (round == 0 ? "a batches" : "b switch off") + ": 30 sent once each, no marker left");
		}
		props.remove("ClientSendMarkerBatch");
		check(luaPer[1] - luaPer[0] >= 30, "MB-04c the switch off needs more Lua calls for the same 30: " + luaPer[1] + " vs " + luaPer[0]);
		OutboundStreamConsumer.getInstance().stop();
	}
}
