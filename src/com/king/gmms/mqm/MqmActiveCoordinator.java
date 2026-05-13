package com.king.gmms.mqm;

import java.util.StringTokenizer;
import java.util.UUID;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

public class MqmActiveCoordinator implements Runnable {
	private static final SystemLogger log = SystemLogger.getSystemLogger(MqmActiveCoordinator.class);
	private static final String LEADER_KEY = "mqm:active:leader";
	private static final String INSTANCE_SET = "system:mqm:instances";

	private final GmmsUtility gmmsUtility;
	private final String module;
	private final String nodeId;
	private final String identity;
	private final String token;

	private volatile boolean running = false;
	private volatile boolean active = false;
	private volatile String leaderValue = null;
	private long noLeaderSince = 0L;
	private Thread thread;

	public MqmActiveCoordinator(GmmsUtility gmmsUtility, String module, String nodeId) {
		this.gmmsUtility = gmmsUtility;
		this.module = normalize(module, "MsgQueueMonitor");
		this.nodeId = normalize(nodeId, "0");
		this.identity = this.module + ":" + this.nodeId;
		this.token = UUID.randomUUID().toString();
	}

	public synchronized void start() {
		if (running) {
			return;
		}
		if (!isEnabled()) {
			MqmActiveState.setActive(true, "active-standby-disabled");
			active = true;
			log.info("MQM active-standby disabled. MQM runtime is active. identity={}", identity);
			return;
		}
		running = true;
		active = false;
		MqmActiveState.setActive(false, "standby-starting");
		thread = new Thread(A2PThreadGroup.getInstance(), this, "MQMActiveCoordinator");
		thread.start();
		log.info("MQMActiveCoordinator started. identity={}, token={}", identity, token);
	}

	public synchronized void stop() {
		running = false;
		releaseLeadership("stop");
		MqmActiveState.setActive(false, "stopped");
		if (thread != null) {
			thread.interrupt();
			thread = null;
		}
		log.info("MQMActiveCoordinator stopped. identity={}", identity);
	}

	public boolean isActive() {
		return active;
	}

	public void run() {
		while (running) {
			try {
				registerCandidate();
				if (!isEnabled()) {
					releaseLeadership("disabled");
					becomeActive("active-standby-disabled");
					sleepSeconds(getStandbyCheckSeconds());
					continue;
				}
				if (active) {
					renewLeadership();
					sleepSeconds(getRenewSeconds());
				} else {
					checkAndAcquire();
					sleepSeconds(getStandbyCheckSeconds());
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				log.warn("MQMActiveCoordinator loop failed. identity={}", identity, e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignored) {
					break;
				}
			}
		}
	}

	private void checkAndAcquire() {
		RedisClient redis = gmmsUtility.getRedisClient();
		if (redis == null) {
			becomeStandby("redis-client-null");
			return;
		}
		String current = redis.getString(LEADER_KEY);
		long now = System.currentTimeMillis();
		if (current == null || current.length() == 0) {
			if (noLeaderSince <= 0L) {
				noLeaderSince = now;
			}
			long delayMs = getAcquireDelayMs();
			if (now - noLeaderSince < delayMs) {
				becomeStandby("waiting-priority-delay");
				return;
			}
			String value = buildLeaderValue();
			boolean acquired = redis.setStringIfAbsent(LEADER_KEY, value, getLeaseTtlSeconds());
			if (acquired) {
				leaderValue = value;
				noLeaderSince = 0L;
				becomeActive("lease-acquired");
			} else {
				becomeStandby("lease-owned-by-other");
			}
			return;
		}
		noLeaderSince = 0L;
		if (isMine(current)) {
			leaderValue = current;
			renewLeadership();
			return;
		}
		becomeStandby("leader=" + extractModule(current));
	}

	private void renewLeadership() {
		RedisClient redis = gmmsUtility.getRedisClient();
		if (redis == null) {
			becomeStandby("redis-client-null");
			return;
		}
		if (leaderValue == null) {
			String current = redis.getString(LEADER_KEY);
			if (current == null || !isMine(current)) {
				becomeStandby("leader-lost");
				return;
			}
			leaderValue = current;
		}
		String newValue = buildLeaderValue();
		boolean renewed = redis.compareAndSetString(LEADER_KEY, leaderValue, newValue, getLeaseTtlSeconds());
		if (renewed) {
			leaderValue = newValue;
			becomeActive("lease-renewed");
		} else {
			log.warn("MQM active lease lost. identity={}, oldLeader={}", identity, leaderValue);
			leaderValue = null;
			becomeStandby("lease-lost");
		}
	}

	private void releaseLeadership(String reason) {
		if (leaderValue == null) {
			return;
		}
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			if (redis != null) {
				redis.deleteIfStringEquals(LEADER_KEY, leaderValue);
			}
		} catch (Exception e) {
			log.warn("Failed to release MQM active lease. identity={}, reason={}", identity, reason, e);
		} finally {
			leaderValue = null;
			active = false;
		}
	}

	private void becomeActive(String reason) {
		if (!active) {
			log.info("MQM role changed to ACTIVE. identity={}, reason={}", identity, reason);
		}
		active = true;
		MqmActiveState.setActive(true, reason);
	}

	private void becomeStandby(String reason) {
		if (active) {
			log.warn("MQM role changed to STANDBY. identity={}, reason={}", identity, reason);
		}
		active = false;
		MqmActiveState.setActive(false, reason);
	}

	private void registerCandidate() {
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			if (redis != null) {
				redis.sadd(INSTANCE_SET, identity);
			}
		} catch (Exception e) {
			log.warn("Failed to register MQM candidate. identity={}", identity, e);
		}
	}

	private String buildLeaderValue() {
		return "module=" + module
				+ ";nodeId=" + nodeId
				+ ";token=" + token
				+ ";updatedAt=" + System.currentTimeMillis();
	}

	private boolean isMine(String value) {
		return value != null && value.indexOf("module=" + module + ";") >= 0
				&& value.indexOf("nodeId=" + nodeId + ";") >= 0
				&& value.indexOf("token=" + token) >= 0;
	}

	private String extractModule(String value) {
		if (value == null) {
			return "";
		}
		String[] parts = value.split(";");
		for (int i = 0; i < parts.length; i++) {
			if (parts[i].startsWith("module=")) {
				return parts[i].substring("module=".length());
			}
		}
		return value;
	}

	private boolean isEnabled() {
		return Boolean.parseBoolean(gmmsUtility.getCommonProperty("MQM.ActiveStandbyEnable", "true"));
	}

	private int getLeaseTtlSeconds() {
		int ttl = intProperty("MQM.ActiveLeaseTtlSeconds", 30);
		return ttl <= 0 ? 30 : ttl;
	}

	private int getRenewSeconds() {
		int renew = intProperty("MQM.ActiveRenewSeconds", 10);
		return renew <= 0 ? 10 : renew;
	}

	private int getStandbyCheckSeconds() {
		int check = intProperty("MQM.StandbyCheckSeconds", 5);
		return check <= 0 ? 5 : check;
	}

	private long getAcquireDelayMs() {
		int index = priorityIndex();
		if (index <= 0) {
			return 0L;
		}
		long delay = longProperty("MQM.ActivePriorityDelayMs", 3000L);
		if (delay < 0L) {
			delay = 3000L;
		}
		return delay * index;
	}

	private int priorityIndex() {
		String priority = gmmsUtility.getCommonProperty("MQM.ActivePriority",
				"MsgQueueMonitor1,MsgQueueMonitor2");
		StringTokenizer tokenizer = new StringTokenizer(priority, ",");
		int index = 0;
		while (tokenizer.hasMoreTokens()) {
			String candidate = tokenizer.nextToken().trim();
			if (module.equalsIgnoreCase(candidate)) {
				return index;
			}
			index++;
		}
		return index;
	}

	private int intProperty(String key, int defaultValue) {
		try {
			return Integer.parseInt(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private void sleepSeconds(int seconds) throws InterruptedException {
		Thread.sleep(seconds * 1000L);
	}

	private static String normalize(String value, String defaultValue) {
		if (value == null || value.trim().length() == 0) {
			return defaultValue;
		}
		return value.trim();
	}
}
