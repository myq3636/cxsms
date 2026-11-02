package it;

import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.SmppServerDrRouteResolver;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * DR target module when the original server was killed (not part of the repo): bind entries of
 * MultiSmppServer1 (node 1) and MultiSmppServer2 (node 2) written as the servers would.
 */
public class DrLivenessIT extends ItSupport {
	static final int OSSID = 9999;

	static void bind(String module, String node, String sessionKey) {
		long now = System.currentTimeMillis();
		jedis.setex("smpp:server:session:" + OSSID + ":" + sessionKey, 660,
				"role=smpp-server;module=" + module + ";nodeId=" + node + ";ossid=" + OSSID + ";expireAt=" + (now + 660000) + ";");
		jedis.zadd("smpp:server:active:" + OSSID, now, sessionKey);
		jedis.zadd("smpp:server:active:" + OSSID + ":" + module, now, sessionKey);
		jedis.zadd("smpp:server:module:" + OSSID, now, module);
	}

	static void heartbeat(String module, String node, boolean online) {
		String key = "module:status:server:" + module + ":" + node;
		if (online) jedis.setex(key, 30, "ONLINE"); else jedis.del(key);
		if (online) jedis.sadd("system:server:nodes", node);
	}

	static GmmsMessage dr(String id) {
		GmmsMessage d = mt(id, OSSID, 2322, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
		return d;
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
		SmppServerDrRouteResolver r = SmppServerDrRouteResolver.getInstance();
		bind("MultiSmppServer1", "1", "s1a");
		bind("MultiSmppServer2", "2", "s2a");
		heartbeat("MultiSmppServer1", "1", true);
		heartbeat("MultiSmppServer2", "2", true);

		check("MultiSmppServer1".equals(r.resolveTargetModule(dr("LV1"))),
				"LV-01 both servers alive: DR goes to the original module MultiSmppServer1");

		heartbeat("MultiSmppServer1", "1", false);       // killed: heartbeat gone, bind entries still there
		Thread.sleep(1100);                               // local cache of the heartbeat (1 s)
		String t = r.resolveTargetModule(dr("LV2"));
		check("MultiSmppServer2".equals(t) && jedis.exists("smpp:server:session:" + OSSID + ":s1a"),
				"LV-02 original module killed (bind entries not expired yet): DR goes to MultiSmppServer2 (got " + t + ")");
		check(logLine("", "[DR-ROUTE] server module=MultiSmppServer1 node=1 has no heartbeat") != null,
				"LV-03 one INFO line names the module and node without heartbeat");
		check(jedis.zscore("smpp:server:active:" + OSSID + ":MultiSmppServer1", "s1a") != null,
				"LV-04 the killed node's entries are skipped, not removed");

		heartbeat("MultiSmppServer1", "1", true);        // restarted
		Thread.sleep(1100);
		check("MultiSmppServer1".equals(r.resolveTargetModule(dr("LV5"))),
				"LV-05 heartbeat back: DR goes to the original module again");

		heartbeat("MultiSmppServer1", "1", false);
		props.setProperty("SmppServerDrLivenessCheck", "false");
		Thread.sleep(1100);
		check("MultiSmppServer1".equals(r.resolveTargetModule(dr("LV6"))),
				"LV-06 SmppServerDrLivenessCheck=false: old behaviour, DR to the original module");
		props.remove("SmppServerDrLivenessCheck");

		// customer bound nowhere alive, no bind entries at all: not to the killed original module
		jedis.del("smpp:server:session:" + OSSID + ":s1a", "smpp:server:session:" + OSSID + ":s2a",
				"smpp:server:active:" + OSSID, "smpp:server:active:" + OSSID + ":MultiSmppServer1",
				"smpp:server:active:" + OSSID + ":MultiSmppServer2", "smpp:server:module:" + OSSID);
		Thread.sleep(1100);
		String n = r.resolveTargetModule(dr("LV7"));
		check(!"MultiSmppServer1".equals(n),
				"LV-07 no bind entries, original module has no heartbeat: not sent to it (got " + n + ")");

		// a node whose status reporter never started (not in system:server:nodes): old behaviour
		bind("MultiSmppServer3", "3", "s3a");
		jedis.del("module:status:server:MultiSmppServer3:3");
		Thread.sleep(1100);
		GmmsMessage d9 = dr("LV9");
		d9.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer3"), "conn1"));
		check("MultiSmppServer3".equals(r.resolveTargetModule(d9)),
				"LV-09 node never registered its heartbeat (status reporter failed): still used, as before");

		// cost: heartbeat GETs per DR with the 1 s cache
		bind("MultiSmppServer1", "1", "s1a");
		heartbeat("MultiSmppServer1", "1", true);
		jedis.configResetStat();
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) r.resolveTargetModule(dr("LVP" + i));
		long ms = System.currentTimeMillis() - t0;
		String stats = jedis.info("commandstats");
		long gets = 0;
		for (String l : stats.split("\r?\n")) {
			if (l.startsWith("cmdstat_get:")) gets = Long.parseLong(l.substring(l.indexOf("calls=") + 6, l.indexOf(',')));
		}
		check(gets <= 1000 + 5, "LV-08 1000 DRs: " + gets + " GETs in all (bind detail 1 per DR + heartbeat at most 1 per second), "
				+ ms + " ms");
	}
}
