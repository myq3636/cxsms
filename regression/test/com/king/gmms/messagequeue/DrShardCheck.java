package com.king.gmms.messagequeue;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.king.gmms.GmmsUtility;

/** Scratch check of the DR stream ownership decision (not part of the repo). */
public class DrShardCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	static Object field(String name) throws Exception {
		Field f = GmmsUtility.class.getDeclaredField(name);
		f.setAccessible(true);
		return f.get(GmmsUtility.getInstance());
	}

	static void set(String name, Object v) throws Exception {
		Field f = GmmsUtility.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(GmmsUtility.getInstance(), v);
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("module", "MultiSmppServer1");
		Properties props = (Properties) field("properties");
		if (props == null) {
			props = new Properties();
			set("properties", props);
		}
		Set<Integer> mine = new HashSet<Integer>();
		mine.add(0);
		set("myShards", mine);
		set("totalShards", 4);
		StreamQueueManager q = StreamQueueManager.getInstance();

		String own1 = "stream:dr:pending:8888:MultiSmppServer1:1";
		String own0 = "stream:dr:pending:8888:MultiSmppServer1:0";
		String other = "stream:dr:pending:8888:MultiSmppServer2:1";
		check(q.isResponsibleForStream(own1), "default: own module shard 1 consumed (MyShards=0)");
		check(q.isResponsibleForStream(own0), "default: own module shard 0 consumed");
		check(!q.isResponsibleForStream(other), "default: other module not consumed");
		check(q.isResponsibleForStream("stream:dr:pending:8888"), "legacy key without module consumed");
		check(q.isResponsibleForStream("stream:dr:pending:8888:MultiSmppServer1"), "key without shard consumed");

		props.setProperty("ServerDrStreamShardFilter", "true");
		check(!q.isResponsibleForStream(own1), "filter on: shard 1 skipped");
		check(q.isResponsibleForStream(own0), "filter on: shard 0 consumed");
		check(!q.isResponsibleForStream(other), "filter on: other module not consumed");

		check(!q.isResponsibleForStream("stream:mt:pending:8888:1"), "MT shard filter unchanged (shard 1 not mine)");
		check(q.isResponsibleForStream("stream:mt:pending:8888:0"), "MT shard filter unchanged (shard 0 mine)");

		// vendor DRs to Core: stream:core:inbound-dr:{rssid}:{shard} by sender and recipient (CoreInboundDRShardEnable)
		check(q.isResponsibleForStream("stream:core:inbound-dr:9991:0"), "inbound DR shard 0 consumed (MyShards=0)");
		check(!q.isResponsibleForStream("stream:core:inbound-dr:9991:1"), "inbound DR shard 1 not consumed");
		check(q.isResponsibleForStream("stream:core:inbound-dr:9991"), "inbound DR key without a shard (before the switch) consumed by every Core");
		com.king.message.gmms.GmmsMessage d = new com.king.message.gmms.GmmsMessage();
		d.setRSsID(9991); d.setSenderAddress("OTP"); d.setRecipientAddress("8613800000001"); d.setOutMsgID("RTVS1-1");
		String k1 = q.getInboundDrQueue(d);
		d.setOutMsgID("RTVS1-2");
		check(k1.matches("stream:core:inbound-dr:9991:[0-3]") && k1.equals(q.getInboundDrQueue(d)),
				"inbound DR key has a shard, the same for the DRs of one sender and recipient (the segments of one MT): " + k1);
		java.util.Set<String> keys = new java.util.HashSet<String>();
		for (int i = 0; i < 200; i++) { d.setRecipientAddress("86138" + (10000000 + i * 7919)); keys.add(q.getInboundDrQueue(d)); }
		check(keys.size() == 4, "200 recipients spread over all 4 shards: " + keys.size());
		props.setProperty("CoreInboundDRShardEnable", "false");
		check(q.getInboundDrQueue(d).equals("stream:core:inbound-dr:9991"), "CoreInboundDRShardEnable=false: the key without a shard");
		props.remove("CoreInboundDRShardEnable");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
