package com.king.framework.lifecycle;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;
import redis.clients.jedis.JedisPubSub;

/**
 * V4.0 Redis Pub/Sub 控制面订阅者
 * 监听控制指令并触发本地 LifecycleEvent 流程，实现配置分布式同步
 */
public class RedisControlSubscriber extends JedisPubSub implements Runnable {
	private static final SystemLogger log = SystemLogger.getSystemLogger(RedisControlSubscriber.class);
	private static final String CONTROL_CHANNEL = "channel:gmms:control";

	private final LifecycleSupport lifecycleSupport;
	private final EventFactory eventFactory;
	private volatile boolean running = false;

	public RedisControlSubscriber() {
		this.lifecycleSupport = GmmsUtility.getInstance().getLifecycleSupport();
		this.eventFactory = EventFactory.getInstance();
	}

	@Override
	public void onMessage(String channel, String message) {
		log.info("Received control message from channel [{}]: {}", channel, message);
		try {
			String cmd = message;
			String args = null;
			if (message.contains(":")) {
				String[] parts = message.split(":", 2);
				cmd = parts[0];
				args = parts[1];
			}

			int eventType = parseCommandToEventType(cmd);
			if (eventType != Event.TYPE_INVAILED) {
				Event event = eventFactory.newEvent(eventType);
				if (event != null) {
					if (args != null) {
						event.setArgs(new Object[] { args });
					}
					log.info("Triggering lifecycle event: {} with args: {}", event.getClass().getSimpleName(), args);
					lifecycleSupport.notify(event);
					
					// V4.1 分布式流控动态重载 Hook
					if (eventType == Event.TYPE_CUSTOMER_RELOAD && args != null) {
						try {
							int ssid = Integer.parseInt(args);
							com.king.gmms.domain.A2PCustomerInfo cst = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
							if (cst != null) {
								com.king.gmms.throttle.DistributedThrottlingManager.getInstance().updateRate(ssid, cst.getOutgoingThrottlingNum());
							}
						} catch (Exception e) {
							log.error("Failed to update dynamic throttling rate for args: " + args, e);
						}
					}
				}
			} else {
				log.warn("Unknown control command: {}", message);
			}
		} catch (Exception e) {
			log.error("Process control message error: " + message, e);
		}
	}

	private int parseCommandToEventType(String cmd) {
		if (cmd == null) return Event.TYPE_INVAILED;
		
		switch (cmd.toUpperCase()) {
			case "RELOAD_CUSTOMER": return Event.TYPE_CUSTOMER_RELOAD;
			case "RELOAD_ROUTING": return Event.TYPE_ROUTINFO_RELOAD;
			case "RELOAD_ANTISPAM": return Event.TYPE_ANTISPAM_RELOAD;
			case "RELOAD_CONTENT_TEMPLATE": return Event.TYPE_CONTENT_TEMPLATE_RELOAD;
			case "RELOAD_PHONE_PREFIX": return Event.TYPE_PHONEPREFIX_RELOAD;
			case "RELOAD_BLACKLIST": return Event.TYPE_BLACKLIST_RELOAD;
			case "RELOAD_WHITELIST": return Event.TYPE_WHITELIST_RELOAD;
			case "RELOAD_SENDER_BLACKLIST": return Event.TYPE_SENDER_BLACKLIST_RELOAD;
			case "RELOAD_SENDER_WHITELIST": return Event.TYPE_SENDER_WHITELIST_RELOAD;
			case "RELOAD_CONTENT_BLACKLIST": return Event.TYPE_CONTENT_BLACKLIST_RELOAD;
			case "RELOAD_CONTENT_WHITELIST": return Event.TYPE_CONTENT_WHITELIST_RELOAD;
			case "RELOAD_RECIPIENT_RULE": return Event.TYPE_RECEIPIENT_RULE_RELOAD;
			case "RELOAD_VENDOR_REPLACE": return Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD;
			case "RELOAD_SYSTEM_REPLACE": return Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD;
			case "RELOAD_RECIPIENT_BLACKLIST": return Event.TYPE_RECIPIENT_BLACKLIST_RELOAD;
			case "SWITCH_REDIS": return Event.TYPE_SWITCHREDIS;
			case "SHUTDOWN": return Event.TYPE_SHUTDOWN;
			default: return Event.TYPE_INVAILED;
		}
	}

	@Override
	public void run() {
		running = true;
		log.info("RedisControlSubscriber started, subscribing to {}", CONTROL_CHANNEL);
		while (running) {
			try {
				// 使用 StateRedis 连接池进行订阅
				RedisClient.getInstance().getStateRedis().subscribe(this, CONTROL_CHANNEL);
			} catch (Exception e) {
				if (running) {
					log.error("Redis subscription lost, retrying in 5 seconds...", e);
					try {
						Thread.sleep(5000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	public void stop() {
		running = false;
		this.unsubscribe();
	}
}
