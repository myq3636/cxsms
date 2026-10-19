package com.king.gmms.ha;

import com.king.db.DatabaseStatus;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;

/**
 * DB 主备切换监听器（DB HA Phase 1，不依赖 SystemManager）。
 *
 * <p>监听 Redis 控制命令 {@code SWITCH_DB master|slave}：
 * {@code RedisControlSubscriber} 收到后经 {@code LifecycleSupport.notify} 派发 {@link Event#TYPE_SWITCHDB}
 * 到本监听器；本监听器把切换应用到当前进程（{@code setHandover} 翻转 {@code dbStatus4Used}），
 * 并把状态**双写**到 Redis 真源键 {@code system:db:status} 与本节点文件 {@code ha/DBStatus}，
 * 以保证重启/新节点对齐（D2/D6）。
 *
 * <p>仅在 {@code SystemManager.enable=False}（默认）时注册——SystemManager 开启时由其自身处理同一事件，
 * 避免重复处理（见 {@code GmmsUtility.startControlSubscriber}）。写键为幂等同值，多节点重复写无害。
 */
public class DbSwitchListener implements LifecycleListener {

	private static final SystemLogger log = SystemLogger.getSystemLogger(DbSwitchListener.class);

	@Override
	public int OnEvent(Event event) {
		if (event == null || event.getEventType() != Event.TYPE_SWITCHDB) {
			return 0;
		}
		DatabaseStatus target;
		String statusStr;
		int sub = event.getEventSubType();
		if (sub == Event.SUBTYPE_SWITCH_MASTER) {
			target = DatabaseStatus.MASTER_USED;
			statusStr = "master";
		} else if (sub == Event.SUBTYPE_SWITCH_SLAVE) {
			target = DatabaseStatus.SLAVE_USED;
			statusStr = "slave";
		} else {
			log.warn("SWITCH_DB with unknown subtype: {}", sub);
			return 0;
		}
		try {
			GmmsUtility gu = GmmsUtility.getInstance();
			// 1) 应用到本进程：翻转 dbStatus4Used + 释放旧角色空闲连接
			gu.setHandover(target);
			// 2) 双写状态(幂等)：Redis 真源键(全集群) + 本节点文件(重启回退/兼容旧脚本)
			try {
				if (gu.getRedisClient() != null) {
					gu.getRedisClient().setDbStatus(statusStr);
				}
			} catch (Exception e) {
				log.warn("setDbStatus(redis) failed: {}", e.getMessage());
			}
			try {
				DatabaseStatus.updateDBStatus2File(target);
			} catch (Exception e) {
				log.warn("updateDBStatus2File failed: {}", e.getMessage());
			}
			log.warn("DB switched by SWITCH_DB control command. target={}", statusStr);
		} catch (Exception e) {
			log.error("DbSwitchListener handle SWITCH_DB failed", e);
		}
		return 0;
	}
}
