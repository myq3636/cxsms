package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.king.framework.SystemLogger;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamGroupInfo;

/**
 * Moves what is left in the DR streams of an SMPP Server that is gone (stream:dr:pending:{ssid}:{module}:*,
 * DRs and MOs, read or not) to the Server each customer is bound to now, chosen as Core chooses it. While the
 * Server runs this is not needed: what it cannot deliver goes back to Core as 11005 and is sent again.
 * An entry written to its new stream and not yet deleted from the old one when this stops is there twice.
 */
public final class DrTransfer {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DrTransfer.class);
	private static final int PAGE = 500;

	public static final class Result {
		public final Map<String, Integer> movedTo = new TreeMap<String, Integer>();
		public final Map<String, Integer> noTarget = new TreeMap<String, Integer>();
		public int failed = 0;
		public int streamsRemoved = 0;
	}

	private DrTransfer() {
	}

	public static Result run(String module, boolean dryRun) {
		Result result = new Result();
		ModuleRegistry.excludeLocally(module);
		RedisClient redis = RedisClient.getInstance();
		StreamQueueManager queue = StreamQueueManager.getInstance();
		for (String key : redis.keysReportMq(StreamQueueManager.STR_DR_PENDING_PREFIX + "*:" + module + ":*")) {
			String[] parts = key.split(":");
			if (parts.length < 6) {
				continue;
			}
			int ssid;
			try {
				ssid = Integer.parseInt(parts[3]);
			} catch (NumberFormatException e) {
				continue;
			}
			String shard = parts[5];
			List<String> groups = groups(redis, key);
			StreamEntryID from = new StreamEntryID(0, 0);
			while (true) {
				List<StreamEntry> page = range(redis, key, from);
				if (page.isEmpty()) {
					break;
				}
				for (StreamEntry e : page) {
					GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
					if (msg == null) {
						result.failed++;
						continue;
					}
					// MOs carry the customer as rssid, DRs as ossid: the stream key has it either way
					GmmsMessage probe = new GmmsMessage(msg);
					probe.setOSsID(ssid);
					String target = SmppServerDrRouteResolver.getInstance().resolveTargetModule(probe);
					if (target == null || target.trim().length() == 0 || target.equalsIgnoreCase(module)) {
						count(result.noTarget, String.valueOf(ssid));
						continue;
					}
					if (!dryRun) {
						String newKey = StreamQueueManager.STR_DR_PENDING_PREFIX + ssid + ":" + target + ":" + shard;
						if (!queue.produceDeliveryReportToQueue(msg, newKey)) {
							result.failed++;
							continue;
						}
						for (String group : groups) {
							redis.xackReportMq(key, group, e.getID());
						}
						redis.xdelReportMq(key, e.getID());
					}
					count(result.movedTo, target);
				}
				StreamEntryID last = page.get(page.size() - 1).getID();
				from = new StreamEntryID(last.getTime(), last.getSequence() + 1);
			}
			if (!dryRun && redis.xlenReportMq(key) == 0) {
				try (Jedis j = redis.getReportMqRedis().getJedis()) {
					j.del(key);
				}
				redis.zremReportMq(StreamQueueManager.ZSET_DR_PENDING_ACTIVE, key);
				result.streamsRemoved++;
			}
		}
		log.info("[DR-TRANSFER] {}{}: moved {}, no target {}, failed {}", module, dryRun ? " (dry run)" : "",
				result.movedTo, result.noTarget, result.failed);
		return result;
	}

	private static List<String> groups(RedisClient redis, String key) {
		List<String> names = new ArrayList<String>();
		try (Jedis j = redis.getReportMqRedis().getJedis()) {
			for (StreamGroupInfo g : j.xinfoGroups(key)) {
				names.add(g.getName());
			}
		} catch (Exception e) {
			log.warn("[DR-TRANSFER] no group info for " + key, e);
		}
		return names;
	}

	private static List<StreamEntry> range(RedisClient redis, String key, StreamEntryID from) {
		try (Jedis j = redis.getReportMqRedis().getJedis()) {
			return j.xrange(key, from, (StreamEntryID) null, PAGE);
		}
	}

	private static void count(Map<String, Integer> map, String key) {
		Integer n = map.get(key);
		map.put(key, n == null ? 1 : n + 1);
	}
}
