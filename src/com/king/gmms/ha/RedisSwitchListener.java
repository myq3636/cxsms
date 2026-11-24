package com.king.gmms.ha;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

/**
 * SWITCH_REDIS:master|slave on the control channel (mgt switchredis, as the reload commands): every module moves its
 * state Redis and all its submit / report MQ instances to that side together (RedisClient.setRedisHaFlag), records
 * the side in ha/RedisStatus and subscribes the control channel again on the new state Redis
 * (docs/流模式下系统管理和模块间连接代码清理分析.md 4.1).
 *
 * <p>Only onto a side whose every Redis answers role:master: the operator promotes the slaves first (SLAVEOF NO ONE),
 * a module never moves onto a read-only replica. Registered when SystemManager is off, as {@link DbSwitchListener}.
 */
public class RedisSwitchListener implements LifecycleListener {
	private static final SystemLogger log = SystemLogger.getSystemLogger(RedisSwitchListener.class);

	@Override
	public int OnEvent(Event event) {
		if (event == null || event.getEventType() != Event.TYPE_SWITCHREDIS) {
			return 0;
		}
		boolean toMaster;
		if (event.getEventSubType() == Event.SUBTYPE_SWITCH_MASTER) {
			toMaster = true;
		} else if (event.getEventSubType() == Event.SUBTYPE_SWITCH_SLAVE) {
			toMaster = false;
		} else {
			log.warn("[REDIS-SWITCH] SWITCH_REDIS with unknown subtype: {}", event.getEventSubType());
			return 0;
		}
		String side = toMaster ? "M" : "S";
		GmmsUtility gu = GmmsUtility.getInstance();
		RedisClient rc = gu.getRedisClient();
		if (rc == null) {
			log.warn("[REDIS-SWITCH] no Redis client, SWITCH_REDIS ignored");
			return 0;
		}
		if (rc.isRedisHaFlag() == toMaster) {
			log.info("[REDIS-SWITCH] already on the {} side", toMaster ? "master" : "slave");
			gu.writeRedisStatus(side);
			return 1;
		}
		if (!rc.sideIsMaster(toMaster)) {
			log.error("[REDIS-SWITCH] not switched to the {} side: not every Redis of it answers role:master (promote it first: SLAVEOF NO ONE)",
					toMaster ? "master" : "slave");
			return 0;
		}
		log.warn("[REDIS-SWITCH] switching to the {} side", toMaster ? "master" : "slave");
		gu.setRedisClientFlag(side);
		gu.writeRedisStatus(side);
		gu.resubscribeControl();
		log.warn("[REDIS-SWITCH] on the {} side now", toMaster ? "master" : "slave");
		return 1;
	}
}
