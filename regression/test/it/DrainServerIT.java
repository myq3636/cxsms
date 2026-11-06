package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleRegistry;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import it.ServerFuncIT.R;

/**
 * Draining an SMPP Server: two real server processes, the real A2pCtl, the registry on. This process plays Core:
 * it resolves the DR targets with the product code. Customer X is bound to Server 1 only, Y to both.
 */
public class DrainServerIT extends MultiServerIT {
	static String stage(String module) { return jedis.hget("module:drain:" + module, "stage"); }
	static String state(String module) { return jedis.hget("system:module:" + module, "state"); }
	static GmmsMessage dr(String id, int ossid, String originalModule) {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID(id);
		d.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", originalModule), "conn"));
		return d;
	}
	static String target(String id, int ossid, String originalModule) {
		return StreamQueueManager.getInstance().getDrPendingQueue(dr(id, ossid, originalModule));
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		String base = new String(Files.readAllBytes(cm), "UTF-8") + cust("SX", 130, null) + cust("SY", 131, null);
		Files.write(cm, (base + cust("SP", 132, S1)).getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleStatusTTLSeconds=5\nModuleStatusHeartbeatSeconds=1\nModuleDrainSessionIntervalMs=5000\nModuleDrainMaxSeconds=30\n").getBytes("UTF-8"));
		Process n1 = null, n2 = null;
		R x1 = null, y1 = null, y2 = null, x2 = null;
		try {
			boot();
			jedis.set("thcon", "1");
			Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleRegistryEnable=true\n").getBytes("UTF-8"));
			check("0".equals(DrainCoreIT.tool("init")[0]), "DS-00a registry initialized");
			props.setProperty("ModuleRegistryEnable", "true");
			ModuleRegistry.startup(GmmsUtility.getInstance());
			n1 = start(S1, 1, P1);
			n2 = start(S2, 2, P2);
			x1 = new R(P1); x1.bind("sx", "pw130");
			y1 = new R(P1); y1.bind("sy", "pw131");
			y2 = new R(P2); y2.bind("sy", "pw131");
			check(waitFor(() -> active(130, S1) && active(131, S1) && active(131, S2), 10000), "DS-00b X on server 1, Y on both");
			check(target("DY0", 131, S1).contains(":" + S1 + ":"), "DS-00c control: a DR of Y that came in on server 1 goes back there: " + target("DY0", 131, S1));
			// a DR the customer never answers, out before the drain
			sendDr("2000077", 131, S1);
			ServerFuncIT.nextDeliver(y1, 6000);

			// ---- refused ----
			String[] t = DrainCoreIT.tool("drain", S1);
			check("1".equals(t[0]) && t[1].contains("SP(132)"), "DS-01a a customer that may only use server 1 (SmppServerModules): refused, named");
			Files.write(cm, base.getBytes("UTF-8"));
			t = DrainCoreIT.tool("drain", S1);
			check("0".equals(t[0]) && "DRAINING".equals(state(S1)) && t[1].contains("load balancer"), "DS-01b configuration changed: DRAINING, told about the load balancer");
			check(waitFor(() -> ModuleRegistry.isDraining(S1), 4000), "DS-02a Core knows it is draining");

			// ---- where DRs go now ----
			check(target("DY1", 131, S1).contains(":" + S2 + ":"), "DS-02b Y is bound to server 2 too: its DR goes there now: " + target("DY1", 131, S1));
			check(target("DX1", 130, S1).contains(":" + S1 + ":"), "DS-02c X is bound to server 1 only: its DR still goes there while it is bound: " + target("DX1", 130, S1));
			check(rejected(tryBind(P1, "sx", "pw130")), "DS-03 a new connection to the draining server: closed");

			// ---- sessions closed one by one ----
			check(waitFor(() -> "CLOSE_SESSIONS".equals(stage(S1)), 4000), "DS-04a it closes its sessions: " + jedis.hgetAll("module:drain:" + S1));
			final R fx = x1, fy = y1;
			long t0 = System.currentTimeMillis();
			boolean xClosed = fx.closedWithin(15000), yClosed = fy.closedWithin(15000);
			System.out.println("INFO both sessions closed after " + (System.currentTimeMillis() - t0) + " ms");
			check(xClosed && yClosed, "DS-04b both customer sessions of server 1 closed, one after the other");
			check(!y2.closedWithin(500), "DS-04c the session on server 2 stays");
			waitFor(() -> GmmsUtility.getInstance() != null && !active(130, S1) && !active(131, S1), 8000);
			List<GmmsMessage> fail77 = inStreams("stream:core:results*", "2000077");
			System.out.println("INFO the unanswered DR: " + (fail77.isEmpty() ? "no result" : fail77.get(0).getStatusCode()));
			check(!fail77.isEmpty() && fail77.get(0).getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode(), "DS-05a the DR sent but not answered when its session closed: back to Core as 11005, to be sent again");

			// ---- a DR left in the stream of server 1 ----
			GmmsMessage left = dr("2000088", 130, S1);
			StreamQueueManager.getInstance().produceDeliveryReportToQueue(left, "stream:dr:pending:130:" + S1 + ":0");
			check(waitFor(() -> !inStreams("stream:core:results*", "2000088").isEmpty(), 8000)
					&& inStreams("stream:core:results*", "2000088").get(0).getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode(), "DS-05b a DR left for server 1, no session: back to Core as 11005");
			check(waitFor(() -> "DONE".equals(stage(S1)), 10000), "DS-05c nothing left: DONE: " + jedis.hgetAll("module:drain:" + S1));
			// after the checks above, which must come soon after the drain started: a tool JVM takes seconds on a loaded machine
			t = DrainCoreIT.tool("drain", S2);
			check("1".equals(t[0]) && t[1].contains("last ACTIVE"), "DS-01c the last active server is not drained");

			// ---- X binds again, to server 2 ----
			x2 = new R(P2); x2.bind("sx", "pw130");
			check(waitFor(() -> active(130, S2), 8000), "DS-06a X bound to server 2");
			check(target("DX2", 130, S1).contains(":" + S2 + ":"), "DS-06b X's DR goes to server 2 now: " + target("DX2", 130, S1));
			sendDr("2000099", 130, S1);
			check(gets(x2, "2000099", 8000), "DS-06c and it gets there");

			// ---- cancelled ----
			t = DrainCoreIT.tool("cancel", S1);
			check("0".equals(t[0]) && "ACTIVE".equals(state(S1)), "DS-07a cancel: ACTIVE");
			check(waitFor(() -> bound(tryBind(P1, "sx", "pw130")), 6000), "DS-07b server 1 takes connections again");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			for (R r : new R[] { x1, y1, y2, x2 }) if (r != null) r.close();
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
