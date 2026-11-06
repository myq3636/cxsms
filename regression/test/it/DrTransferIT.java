package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;

/**
 * A2pCtl drain-dr: a Server killed with -9, its DR streams moved with the real tool. No module registry: the usual case after a crash.
 * Y is bound to both Servers. X is bound to the dead one only and not restricted: as Core does, its DRs go to an online Server and wait
 * there for it. Z may use server 1 only: its DRs have nowhere to go and stay.
 */
public class DrTransferIT extends MultiServerIT {
	static long len(String module) {
		long n = 0;
		for (String k : jedis.keys("stream:dr:pending:*:" + module + ":*")) n += jedis.xlen(k);
		return n;
	}
	/** what a session gets within ms, answered */
	static List<String> received(R r, int ms) throws Exception {
		List<String> texts = new ArrayList<>();
		long end = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < end) {
			Pdu p = ServerFuncIT.nextDeliver(r, 500);
			if (p == null) continue;
			r.send(0x80000005, p.seq, new byte[] { 0 });
			texts.add(ServerFuncIT.deliver(p).get("text"));
		}
		return texts;
	}
	/** DR ids reported back to Core as not delivered (11005) */
	static List<String> backToCore() {
		List<String> ids = new ArrayList<>();
		for (String k : jedis.keys("stream:core:results*"))
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equals(m.getMessageType()) && m.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode()) ids.add(m.getInMsgID());
			}
		return ids;
	}
	static int times(List<String> texts, String id) { int n = 0; for (String t : texts) if (t.contains(id)) n++; return n; }
	static GmmsMessage dr(String id, int ossid, String module) {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID(id);
		d.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", module), "conn"));
		return d;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8") + cust("TX", 140, null) + cust("TY", 141, null) + cust("TZ", 142, S1)).getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleStatusTTLSeconds=5\nModuleStatusHeartbeatSeconds=1\n").getBytes("UTF-8"));
		Process n1 = null, n2 = null;
		R x1 = null, y1 = null, y2 = null, z1 = null, z2 = null;
		StreamQueueManager q = null;
		try {
			boot();
			q = StreamQueueManager.getInstance();
			jedis.set("thcon", "1");
			n1 = start(S1, 1, P1);
			n2 = start(S2, 2, P2);
			x1 = new R(P1); x1.bind("tx", "pw140");
			y1 = new R(P1); y1.bind("ty", "pw141");
			y2 = new R(P2); y2.bind("ty", "pw141");
			z1 = new R(P1); z1.bind("tz", "pw142");
			check(waitFor(() -> active(140, S1) && active(141, S1) && active(141, S2) && active(142, S1), 10000), "DT-00 X and Z on server 1, Y on both");

			// ---- read by server 1, not answered: pending on it ----
			List<String> pending = new ArrayList<>();
			for (int i = 0; i < 5; i++) { String id = "300000" + i; sendDr(id, 141, S1); pending.add(id); }
			int got = 0;
			for (int i = 0; i < 5; i++) if (ServerFuncIT.nextDeliver(y1, 4000) != null) got++;
			check(got == 5, "DT-01a 5 DRs of Y out on server 1, never answered");
			n1.destroyForcibly().waitFor();
			x1.close(); y1.close(); z1.close();
			// as with the default heartbeat TTL of 30 s: a JVM takes longer to start than the 5 s here
			jedis.expire("module:status:server:" + S1 + ":1", 30L);

			// ---- server 1 is dead, more come to its streams ----
			List<String> unread = new ArrayList<>();
			for (int i = 0; i < 5; i++) { String id = "300001" + i; q.produceDeliveryReportToQueue(dr(id, 141, S1), "stream:dr:pending:141:" + S1 + ":" + (i % 2)); unread.add(id); }
			List<String> forX = new ArrayList<>();
			for (int i = 0; i < 3; i++) { String id = "300002" + i; q.produceDeliveryReportToQueue(dr(id, 140, S1), "stream:dr:pending:140:" + S1 + ":0"); forX.add(id); }
			List<String> forZ = new ArrayList<>();
			for (int i = 0; i < 2; i++) { String id = "300004" + i; q.produceDeliveryReportToQueue(dr(id, 142, S1), "stream:dr:pending:142:" + S1 + ":0"); forZ.add(id); }
			GmmsMessage mo = mt("8888_MO1", 8888, 141, 0);
			mo.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY);
			mo.setTextContent("mo 3000031");
			mo.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", S1), "conn"));
			q.produceDeliveryReportToQueue(mo, "stream:dr:pending:141:" + S1 + ":1");
			long before = len(S1);
			check(before == 16, "DT-01b left in server 1's streams: 5 pending, 5 unread, 3 for X, 2 for Z, 1 MO: " + before);

			// ---- refused while its heartbeat is there ----
			String[] t = DrainCoreIT.tool("drain-dr", S1);
			check("1".equals(t[0]) && t[1].contains("is running"), "DT-02 its heartbeat is still there: refused, drain it instead");
			jedis.del("module:status:server:" + S1 + ":1");

			// ---- dry run ----
			t = DrainCoreIT.tool("drain-dr", S1, "--dry-run");
			System.out.println("INFO dry run: " + t[1].trim().replace("\n", " | ").replaceAll(".*(Would move)", "$1"));
			check("0".equals(t[0]) && t[1].contains("Would move: {" + S2 + "=14}") && t[1].contains("{142=2}") && len(S1) == before, "DT-03 dry run: 14 to server 2, 2 of Z with no Server, nothing changed");

			// ---- moved ----
			t = DrainCoreIT.tool("drain-dr", S1);
			System.out.println("INFO run: " + t[1].trim().replace("\n", " | ").replaceAll(".*(Moved)", "$1"));
			check("0".equals(t[0]) && t[1].contains("Moved: {" + S2 + "=14}") && t[1].contains("{142=2}"), "DT-04a 14 moved to server 2, Z's 2 reported as left");
			List<String> texts = received(y2, 8000);
			boolean once = true;
			for (String id : pending) once &= times(texts, "id:" + id + " ") == 1;
			for (String id : unread) once &= times(texts, "id:" + id + " ") == 1;
			check(once, "DT-04b Y gets on server 2 the 5 that were pending on the dead server and the 5 unread, each once: " + texts.size() + " deliveries");
			check(times(texts, "mo 3000031") == 1, "DT-04c and the MO");
			check(len(S1) == 2 && jedis.keys("stream:dr:pending:*:" + S1 + ":*").equals(Set.of("stream:dr:pending:142:" + S1 + ":0")), "DT-04d only Z's 2 are left in server 1's streams: " + len(S1));
			String bells = jedis.zrange("zset:dr:pending:active", 0, -1).toString();
			check(!bells.contains("141:" + S1) && !bells.contains("140:" + S1), "DT-04e the emptied streams removed, with their doorbells");

			// ---- not removed from the registry while DRs are left ----
			com.king.gmms.ha.ModuleRegistry.write(S1, "type", "MultiSmppServer", "role", "smpp-server", "nodeId", "1", "state", "DRAINED", "removedAt", "0");
			t = DrainCoreIT.tool("remove", S1);
			check("1".equals(t[0]) && t[1].contains("2 DR(s) left") && "DRAINED".equals(jedis.hget("system:module:" + S1, "state")), "DT-04f remove: refused while Z's 2 are left");
			t = DrainCoreIT.tool("remove", S1, "--force");
			check("1".equals(t[0]) && t[1].contains("also not with --force") && "DRAINED".equals(jedis.hget("system:module:" + S1, "state")), "DT-04g --force: refused all the same");

			// ---- X has no session on server 2: back to Core as 11005, which sends it again (no Core here) ----
			check(waitFor(() -> backToCore().containsAll(forX), 8000), "DT-05 X's 3 went to server 2 as Core sends them, and back to Core as 11005 for a retry: " + backToCore());

			// ---- Z may use server 2 too: run again ----
			for (String h : new String[] { home, home.replaceAll("/$", "") + "_" + S2 + "/" }) {
				Path c = Paths.get(h + "conf/cm.cfg");
				Files.write(c, new String(Files.readAllBytes(c), "UTF-8").replace("SmppServerModules: " + S1 + "\n", "SmppServerModules: " + S1 + "," + S2 + "\n").getBytes("UTF-8"));
			}
			jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
			Thread.sleep(2000);
			z2 = new R(P2); z2.bind("tz", "pw142");
			check(waitFor(() -> active(142, S2), 8000), "DT-06a Z assigned to server 2 as well and bound there");
			t = DrainCoreIT.tool("drain-dr", S1);
			check("0".equals(t[0]) && t[1].contains("Moved: {" + S2 + "=2}"), "DT-06b run again: its 2 moved");
			List<String> zt = received(z2, 8000);
			boolean zOnce = true;
			for (String id : forZ) zOnce &= times(zt, "id:" + id + " ") == 1;
			check(zOnce, "DT-06c Z gets them on server 2, each once: " + zt.size() + " deliveries");
			check(len(S1) == 0 && jedis.keys("stream:dr:pending:*:" + S1 + ":*").isEmpty() && !jedis.zrange("zset:dr:pending:active", 0, -1).toString().contains(S1), "DT-06d nothing of server 1 left: streams and doorbells gone");
			t = DrainCoreIT.tool("drain-dr", S1);
			check("0".equals(t[0]) && t[1].contains("Moved: {}") && !t[1].contains("No Server"), "DT-06e a third run finds nothing");
			t = DrainCoreIT.tool("remove", S1);
			check("0".equals(t[0]) && "REMOVED".equals(jedis.hget("system:module:" + S1, "state")), "DT-06f nothing left: removed");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			for (R r : new R[] { x1, y1, y2, z1, z2 }) if (r != null) r.close();
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
