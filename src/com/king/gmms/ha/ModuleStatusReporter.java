package com.king.gmms.ha;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

public class ModuleStatusReporter {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ModuleStatusReporter.class);
	public static final String CONTROL_CHANNEL = "channel:gmms:control";
	public static final String CMD_STATUS_REFRESH = "STATUS_REFRESH";
	public static final String CMD_STATUS_REREGISTER = "STATUS_REREGISTER";
	public static final String CMD_STATUS_OFFLINE = "STATUS_OFFLINE";

	private static final Map<String, ModuleStatusReporter> reporters =
			new ConcurrentHashMap<String, ModuleStatusReporter>();

	private final GmmsUtility gmmsUtility;
	private final String role;
	private final String module;
	private final String nodeId;
	private final String identity;
	private final String nodesKey;
	private final String statusKey;
	private final String detailKey;
	private final int ttlSeconds;
	private final int heartbeatSeconds;
	private final int selfCheckSeconds;
	private volatile boolean running = false;
	private ScheduledExecutorService executor;

	private ModuleStatusReporter(GmmsUtility gmmsUtility, String role, String module, String nodeId) {
		this.gmmsUtility = gmmsUtility;
		this.role = normalize(role, "unknown");
		this.module = normalize(module, "unknown");
		this.nodeId = normalize(nodeId, System.getProperty("NodeID", "0"));
		this.identity = this.role + ":" + this.module + ":" + this.nodeId;
		this.nodesKey = "system:nodes:" + this.role;
		this.statusKey = "module:status:" + this.identity;
		this.detailKey = "module:detail:" + this.identity;
		this.ttlSeconds = intProperty("ModuleStatusTTLSeconds", 30);
		this.heartbeatSeconds = intProperty("ModuleStatusHeartbeatSeconds", 10);
		this.selfCheckSeconds = intProperty("ModuleStatusSelfCheckSeconds", 15);
	}

	public static synchronized ModuleStatusReporter start(GmmsUtility gmmsUtility, String role, String module, String nodeId) {
		ModuleStatusReporter reporter = new ModuleStatusReporter(gmmsUtility, role, module, nodeId);
		ModuleStatusReporter old = reporters.put(reporter.identity, reporter);
		if (old != null) {
			old.stop();
		}
		reporter.start();
		return reporter;
	}

	private synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		register();
		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(new Runnable() {
			public void run() {
				heartbeat();
			}
		}, heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
		executor.scheduleAtFixedRate(new Runnable() {
			public void run() {
				selfCheck();
			}
		}, selfCheckSeconds, selfCheckSeconds, TimeUnit.SECONDS);
		log.info("ModuleStatusReporter started. identity={}, nodesKey={}", identity, nodesKey);
	}

	public synchronized void stop() {
		running = false;
		if (executor != null) {
			executor.shutdownNow();
			executor = null;
		}
		unregister();
		reporters.remove(identity);
		log.info("ModuleStatusReporter stopped. identity={}", identity);
	}

	public void register() {
		long start = System.currentTimeMillis();
		try {
			log.info("Registering module status start. identity={}, nodesKey={}, statusKey={}, detailKey={}",
					identity, nodesKey, statusKey, detailKey);
			RedisClient redis = gmmsUtility.getRedisClient();
			if (redis == null) {
				log.warn("Registering module status skipped. redisClient is null, identity={}", identity);
				return;
			}
			long stepStart = System.currentTimeMillis();
			redis.sadd(nodesKey, nodeId);
			log.info("Registered module node set. identity={}, key={}, nodeId={}, costMs={}",
					identity, nodesKey, nodeId, System.currentTimeMillis() - stepStart);
			if ("server".equalsIgnoreCase(role)) {
				stepStart = System.currentTimeMillis();
				redis.sadd("system:server:nodes", nodeId);
				log.info("Registered server node set. identity={}, key={}, nodeId={}, costMs={}",
						identity, "system:server:nodes", nodeId, System.currentTimeMillis() - stepStart);
			} else if ("httpserver".equalsIgnoreCase(role)) {
				stepStart = System.currentTimeMillis();
				redis.sadd("system:httpserver:nodes", nodeId);
				log.info("Registered httpserver node set. identity={}, key={}, nodeId={}, costMs={}",
						identity, "system:httpserver:nodes", nodeId, System.currentTimeMillis() - stepStart);
			}
			stepStart = System.currentTimeMillis();
			redis.setString(statusKey, "ONLINE", ttlSeconds);
			log.info("Registered module status key. identity={}, key={}, ttlSeconds={}, costMs={}",
					identity, statusKey, ttlSeconds, System.currentTimeMillis() - stepStart);
			stepStart = System.currentTimeMillis();
			redis.setString(detailKey, buildDetail(), ttlSeconds * 2);
			log.info("Registered module detail key. identity={}, key={}, ttlSeconds={}, costMs={}",
					identity, detailKey, ttlSeconds * 2, System.currentTimeMillis() - stepStart);
		} catch (Exception e) {
			log.warn("Failed to register module status for " + identity, e);
		} finally {
			log.info("Registering module status finished. identity={}, costMs={}",
					identity, System.currentTimeMillis() - start);
		}
	}

	private void unregister() {
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			redis.srem(nodesKey, nodeId);
			if ("server".equalsIgnoreCase(role)) {
				redis.srem("system:server:nodes", nodeId);
			} else if ("httpserver".equalsIgnoreCase(role)) {
				redis.srem("system:httpserver:nodes", nodeId);
			}
			redis.del(statusKey);
			redis.del(detailKey);
		} catch (Exception e) {
			log.warn("Failed to unregister module status for " + identity, e);
		}
	}

	private void heartbeat() {
		if (!running) {
			return;
		}
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			redis.setString(statusKey, "ONLINE", ttlSeconds);
			redis.setString(detailKey, buildDetail(), ttlSeconds * 2);
		} catch (Exception e) {
			log.warn("Failed to heartbeat module status for " + identity, e);
		}
	}

	private void selfCheck() {
		if (!running) {
			return;
		}
		try {
			String status = gmmsUtility.getRedisClient().getString(statusKey);
			if (status == null) {
				log.warn("Module status key missing, re-registering. identity={}", identity);
				register();
			}
		} catch (Exception e) {
			log.warn("Failed to self-check module status for " + identity, e);
		}
	}

	private String buildDetail() {
		return "role=" + role
				+ ";module=" + module
				+ ";nodeId=" + nodeId
				+ ";totalShards=" + gmmsUtility.getTotalShards()
				+ ";myShards=" + gmmsUtility.formatShardSet(gmmsUtility.getMyShards())
				+ ";status=ONLINE"
				+ ";updatedAt=" + System.currentTimeMillis();
	}

	private int intProperty(String key, int defaultValue) {
		try {
			return Integer.parseInt(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private static String normalize(String value, String defaultValue) {
		if (value == null || value.trim().length() == 0) {
			return defaultValue;
		}
		return value.trim();
	}

	public static boolean handleControlCommand(String cmd, String args) {
		if (cmd == null || !cmd.toUpperCase().startsWith("STATUS_")) {
			return false;
		}
		String command = cmd.toUpperCase();
		for (ModuleStatusReporter reporter : reporters.values()) {
			if (args == null || args.length() == 0 || reporter.matches(args)) {
				if (CMD_STATUS_REFRESH.equals(command) || CMD_STATUS_REREGISTER.equals(command)) {
					reporter.register();
				} else if (CMD_STATUS_OFFLINE.equals(command)) {
					reporter.unregister();
				}
			}
		}
		return true;
	}

	private boolean matches(String args) {
		String[] parts = args.split(",");
		if (parts.length > 0 && parts[0].length() > 0 && !role.equalsIgnoreCase(parts[0])) {
			return false;
		}
		if (parts.length > 1 && parts[1].length() > 0 && !module.equalsIgnoreCase(parts[1])) {
			return false;
		}
		if (parts.length > 2 && parts[2].length() > 0 && !nodeId.equalsIgnoreCase(parts[2])) {
			return false;
		}
		return true;
	}

	public static Long publishStatusCommand(String command, String args) {
		String message = command;
		if (args != null && args.length() > 0) {
			message = message + ":" + args;
		}
		return RedisClient.getInstance().publishState(CONTROL_CHANNEL, message);
	}

	public static void stopAll() {
		for (ModuleStatusReporter reporter : reporters.values()) {
			reporter.stop();
		}
	}
}
