package com.king.framework.lifecycle;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleStatusReporter;
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
			String targets = null;
			if (message.contains(":")) {
				String[] parts = message.split(":", 2);
				cmd = parts[0];
				args = parts[1];
			}
			if (cmd.contains("@")) {
				String[] parts = cmd.split("@", 2);
				cmd = parts[0];
				targets = parts[1];
			}

			if (!isTargetMatched(targets)) {
				log.info("Ignore control command: {} because target [{}] does not match local module.", message, targets);
				return;
			}

			if (ModuleStatusReporter.handleControlCommand(cmd, args)) {
				log.info("Handled module status control command: {} args: {}", cmd, args);
				return;
			}
			if ("RELOAD_CORE_SHARDS".equalsIgnoreCase(cmd)) {
				GmmsUtility.getInstance().initCoreShardAssignment();
				log.info("Reloaded core shard assignment by redis control command. args={}", args);
				return;
			}
			if ("RELOAD_CLIENT_SHARDS".equalsIgnoreCase(cmd)) {
				GmmsUtility.getInstance().initClientShardAssignment();
				log.info("Reloaded client shard assignment by redis control command. args={}", args);
				return;
			}

			int eventType = parseCommandToEventType(cmd);
			if (eventType != Event.TYPE_INVAILED) {
				Event event = eventFactory.newEvent(eventType);
				if (event != null) {
					String eventArgs = normalizeControlArgs(eventType, args);
					if (eventArgs != null) {
						event.setArgs(new Object[] { eventArgs });
					}
					applyControlArgs(event, eventType, eventArgs);
					log.info("Triggering lifecycle event: {} with args: {}", event.getClass().getSimpleName(), eventArgs);
					int result = lifecycleSupport.notify(event);
					if (result == 0) {
						log.info("Lifecycle event completed: {} args: {} result: {}", event.getClass().getSimpleName(), eventArgs, result);
					} else {
						log.warn("Lifecycle event completed with non-zero result: {} args: {} result: {}", event.getClass().getSimpleName(), eventArgs, result);
					}
					
					// V4.1 分布式流控动态重载 Hook
					if (eventType == Event.TYPE_CUSTOMER_RELOAD && isInteger(eventArgs)) {
						try {
							int ssid = Integer.parseInt(eventArgs);
							com.king.gmms.domain.A2PCustomerInfo cst = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
							if (cst != null) {
								com.king.gmms.throttle.DistributedThrottlingManager.getInstance().updateRate(ssid, cst.getOutgoingThrottlingNum());
							}
						} catch (Exception e) {
							log.error("Failed to update dynamic throttling rate for args: " + eventArgs, e);
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

	private String normalizeControlArgs(int eventType, String args) {
		if (isReloadEvent(eventType)) {
			if (args == null || args.trim().length() == 0) {
				return "-a";
			}
			return args.trim();
		}
		return args;
	}

	private boolean isReloadEvent(int eventType) {
		switch (eventType) {
			case Event.TYPE_CUSTOMER_RELOAD:
			case Event.TYPE_ROUTINFO_RELOAD:
			case Event.TYPE_ANTISPAM_RELOAD:
			case Event.TYPE_CONTENT_TEMPLATE_RELOAD:
			case Event.TYPE_PHONEPREFIX_RELOAD:
			case Event.TYPE_BLACKLIST_RELOAD:
			case Event.TYPE_WHITELIST_RELOAD:
			case Event.TYPE_SENDER_BLACKLIST_RELOAD:
			case Event.TYPE_SENDER_WHITELIST_RELOAD:
			case Event.TYPE_CONTENT_BLACKLIST_RELOAD:
			case Event.TYPE_CONTENT_WHITELIST_RELOAD:
			case Event.TYPE_RECEIPIENT_RULE_RELOAD:
			case Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD:
			case Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD:
			case Event.TYPE_RECIPIENT_BLACKLIST_RELOAD:
			case Event.TYPE_GMMS_CONFIG_RELOAD:
				return true;
			default:
				return false;
		}
	}

	private boolean isInteger(String value) {
		if (value == null || value.trim().length() == 0) {
			return false;
		}
		try {
			Integer.parseInt(value.trim());
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void applyControlArgs(Event event, int eventType, String args) {
		if (event == null || args == null) {
			return;
		}
		if (eventType == Event.TYPE_SWITCHDB || eventType == Event.TYPE_SWITCHREDIS) {
			if ("master".equalsIgnoreCase(args)) {
				event.setEventSubType(Event.SUBTYPE_SWITCH_MASTER);
			} else if ("slave".equalsIgnoreCase(args)) {
				event.setEventSubType(Event.SUBTYPE_SWITCH_SLAVE);
			}
			return;
		}
		if (eventType == Event.TYPE_SWITCHDNS) {
			if ("master".equalsIgnoreCase(args)) {
				event.setEventSubType(Event.SUBTYPE_SWITCHDNS_MASTER);
			} else if ("slave".equalsIgnoreCase(args)) {
				event.setEventSubType(Event.SUBTYPE_SWITCHDNS_SLAVE);
			}
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
			case "RELOAD_GMMS_CONFIG": return Event.TYPE_GMMS_CONFIG_RELOAD;
			case "RELOAD_GMMSCONFIG": return Event.TYPE_GMMS_CONFIG_RELOAD;
			case "RELOAD_CONFIG": return Event.TYPE_GMMS_CONFIG_RELOAD;
			case "SWITCH_DB": return Event.TYPE_SWITCHDB;
			case "SWITCH_REDIS": return Event.TYPE_SWITCHREDIS;
			case "SWITCH_DNS": return Event.TYPE_SWITCHDNS;
			case "SHUTDOWN": return Event.TYPE_SHUTDOWN;
			default: return Event.TYPE_INVAILED;
		}
	}

	private boolean isTargetMatched(String targets) {
		if (targets == null || targets.trim().length() == 0) {
			return true;
		}
		String module = System.getProperty("module");
		String nodeId = GmmsUtility.getInstance().getNodeId();
		String[] targetArray = targets.split(",");
		for (String target : targetArray) {
			String value = target == null ? "" : target.trim();
			if ("*".equals(value)) {
				return true;
			}
			if (module != null && module.equalsIgnoreCase(value)) {
				return true;
			}
			if (nodeId != null && nodeId.equalsIgnoreCase(value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void run() {
		running = true;
		log.info("RedisControlSubscriber started, channel={}", CONTROL_CHANNEL);
		while (running) {
			try {
				RedisClient redisClient = RedisClient.getInstance();
				if (redisClient.getStateRedis() == null) {
					log.warn("RedisControlSubscriber waiting for stateRedis initialization, channel={}", CONTROL_CHANNEL);
					sleepBeforeRetry();
					continue;
				}
				log.info("RedisControlSubscriber subscribing to {}", CONTROL_CHANNEL);
				redisClient.subscribeState(this, CONTROL_CHANNEL);
				if (running) {
					log.warn("RedisControlSubscriber subscribe returned, retrying in 5 seconds. channel={}", CONTROL_CHANNEL);
					sleepBeforeRetry();
				}
			} catch (Exception e) {
				if (running) {
					log.error("Redis subscription lost, retrying in 5 seconds...", e);
					sleepBeforeRetry();
				}
			}
		}
	}

	private void sleepBeforeRetry() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
	}

	public void stop() {
		running = false;
		this.unsubscribe();
	}
}
