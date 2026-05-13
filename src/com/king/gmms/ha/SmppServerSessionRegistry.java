package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.redis.RedisClient;

/**
 * Tracks active SMPP server bind sessions in state Redis.
 *
 * Core uses this registry only to choose the target server module for DR
 * delivery. The target server module still uses the existing customer
 * connection strategy to choose the concrete bind session.
 */
public class SmppServerSessionRegistry {
	private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerSessionRegistry.class);
	private static final SmppServerSessionRegistry INSTANCE = new SmppServerSessionRegistry();

	private static final String DETAIL_PREFIX = "smpp:server:session:";
	private static final String ACTIVE_PREFIX = "smpp:server:active:";
	private static final String CONN_PREFIX = "smpp:server:conn:";
	private static final String NODE_PREFIX = "smpp:server:node:";
	private static final String MODULE_PREFIX = "smpp:server:module:";

	private final GmmsUtility gmmsUtility = GmmsUtility.getInstance();

	private SmppServerSessionRegistry() {
	}

	public static SmppServerSessionRegistry getInstance() {
		return INSTANCE;
	}

	public boolean isEnabled() {
		return booleanProperty("SmppServerSessionRegistryEnable", true);
	}

	public int refreshIntervalMs() {
		return intProperty("SmppServerSessionRegistryRefreshIntervalMs", 30000);
	}

	public String register(MultiSmppSession session) {
		return register(session, true);
	}

	public String refresh(MultiSmppSession session) {
		return register(session, false);
	}

	public void unregister(MultiSmppSession session, String sessionKey) {
		if (!isEnabled() || session == null) {
			return;
		}
		try {
			SessionSnapshot snapshot = snapshot(session, sessionKey);
			if (snapshot == null || isBlank(snapshot.sessionKey)) {
				return;
			}
			RedisClient redis = redis();
			if (redis == null) {
				return;
			}
			String detailKey = detailKey(snapshot.ossid, snapshot.sessionKey);
			redis.del(detailKey);
			redis.zrem(activeKey(snapshot.ossid), new String[] { snapshot.sessionKey });
			redis.zrem(activeModuleKey(snapshot.ossid, snapshot.module), new String[] { snapshot.sessionKey });
			redis.zrem(connKey(snapshot.ossid, snapshot.module, snapshot.connectionName), new String[] { snapshot.sessionKey });
			redis.zrem(nodeKey(snapshot.ossid, snapshot.module, snapshot.nodeName), new String[] { snapshot.sessionKey });
			if (!hasActiveSessionInModule(snapshot.ossid, snapshot.module)) {
				redis.zrem(moduleKey(snapshot.ossid), new String[] { snapshot.module });
			}
			log.info("SMPP server bind registry unregistered. ossid={}, module={}, connection={}, sessionKey={}",
					snapshot.ossid, snapshot.module, snapshot.connectionName, snapshot.sessionKey);
		} catch (Exception e) {
			log.warn("Failed to unregister SMPP server bind registry.", e);
		}
	}

	public boolean hasActiveSessionInModule(int ossid, String module) {
		if (!isEnabled() || ossid < 0 || isBlank(module)) {
			return false;
		}
		RedisClient redis = redis();
		if (redis == null) {
			return false;
		}
		String key = activeModuleKey(ossid, module);
		Set<String> members = redis.zrange(key, 0, -1);
		if (members == null || members.isEmpty()) {
			return false;
		}
		boolean found = false;
		for (String member : members) {
			if (isActiveDetail(ossid, member, module)) {
				found = true;
			} else {
				redis.zrem(key, new String[] { member });
				redis.zrem(activeKey(ossid), new String[] { member });
			}
		}
		if (!found) {
			redis.zrem(moduleKey(ossid), new String[] { module });
		}
		return found;
	}

	public boolean hasKnownModuleData(int ossid) {
		if (!isEnabled() || ossid < 0) {
			return false;
		}
		RedisClient redis = redis();
		if (redis == null) {
			return false;
		}
		Set<String> modules = redis.zrange(moduleKey(ossid), 0, -1);
		return modules != null && !modules.isEmpty();
	}

	public String selectActiveModule(int ossid, String excludedModule) {
		if (!isEnabled() || ossid < 0) {
			return null;
		}
		RedisClient redis = redis();
		if (redis == null) {
			return null;
		}
		Set<String> moduleSet = redis.zrange(moduleKey(ossid), 0, -1);
		if (moduleSet == null || moduleSet.isEmpty()) {
			return null;
		}
		List<String> modules = new ArrayList<String>(moduleSet);
		Collections.reverse(modules);
		for (String module : modules) {
			if (isBlank(module) || module.equalsIgnoreCase(excludedModule)) {
				continue;
			}
			if (hasActiveSessionInModule(ossid, module)) {
				return module;
			}
		}
		return null;
	}

	private String register(MultiSmppSession session, boolean forceLog) {
		if (!isEnabled() || session == null || !session.isServer()) {
			return null;
		}
		try {
			SessionSnapshot snapshot = snapshot(session, null);
			if (snapshot == null || snapshot.ossid < 0 || isBlank(snapshot.module) || isBlank(snapshot.sessionKey)) {
				return null;
			}
			RedisClient redis = redis();
			if (redis == null) {
				return snapshot.sessionKey;
			}
			long now = System.currentTimeMillis();
			int ttlSeconds = ttlSeconds();
			String detailKey = detailKey(snapshot.ossid, snapshot.sessionKey);
			String detail = detail(snapshot, now, ttlSeconds);
			redis.setString(detailKey, detail, ttlSeconds);
			redis.zadd(activeKey(snapshot.ossid), now, snapshot.sessionKey);
			redis.zadd(activeModuleKey(snapshot.ossid, snapshot.module), now, snapshot.sessionKey);
			redis.zadd(connKey(snapshot.ossid, snapshot.module, snapshot.connectionName), now, snapshot.sessionKey);
			redis.zadd(nodeKey(snapshot.ossid, snapshot.module, snapshot.nodeName), now, snapshot.sessionKey);
			redis.zadd(moduleKey(snapshot.ossid), now, snapshot.module);
			if (forceLog) {
				log.info("SMPP server bind registry registered. ossid={}, module={}, node={}, connection={}, session={}, sessionKey={}, ttlSeconds={}",
						snapshot.ossid, snapshot.module, snapshot.nodeName, snapshot.connectionName,
						snapshot.sessionName, snapshot.sessionKey, ttlSeconds);
			}
			return snapshot.sessionKey;
		} catch (Exception e) {
			log.warn("Failed to register SMPP server bind registry.", e);
			return null;
		}
	}

	private boolean isActiveDetail(int ossid, String sessionKey, String expectedModule) {
		RedisClient redis = redis();
		if (redis == null || isBlank(sessionKey)) {
			return false;
		}
		String value = redis.getString(detailKey(ossid, sessionKey));
		if (isBlank(value)) {
			return false;
		}
		if (isBlank(expectedModule)) {
			return true;
		}
		Map<String, String> map = parse(value);
		String module = map.get("module");
		return expectedModule.equalsIgnoreCase(module);
	}

	private SessionSnapshot snapshot(MultiSmppSession session, String existingSessionKey) {
		A2PMultiConnectionInfo customerInfo = session.getCustomerInfo();
		ConnectionInfo connectionInfo = session.getConnectionInfo();
		if (customerInfo == null || connectionInfo == null) {
			return null;
		}
		SessionSnapshot snapshot = new SessionSnapshot();
		snapshot.ossid = customerInfo.getSSID();
		snapshot.module = safe(System.getProperty("module", ""));
		snapshot.nodeId = safe(gmmsUtility.getNodeId());
		snapshot.connectionName = safe(connectionInfo.getConnectionName());
		snapshot.sessionName = safe(session.getSessionName());
		snapshot.systemId = safe(session.getSourceSysID());
		snapshot.bindType = safe(session.getBindOptionName());
		snapshot.status = session.getStatus() == null ? "" : session.getStatus().toString();
		snapshot.remoteIp = safe(session.getRemoteIp());
		snapshot.localIp = safe(gmmsUtility.getServerIP());
		snapshot.transactionUri = session.getTransactionURI() == null ? "" : session.getTransactionURI().toString();
		snapshot.sessionUuid = session.getTransactionURI() == null || session.getTransactionURI().getId() == null
				? Integer.toString(session.getSessionNum())
				: session.getTransactionURI().getId().toString();
		snapshot.nodeName = resolveNodeName(customerInfo, connectionInfo);
		snapshot.sessionKey = isBlank(existingSessionKey)
				? buildSessionKey(snapshot)
				: existingSessionKey;
		return snapshot;
	}

	private String resolveNodeName(A2PMultiConnectionInfo customerInfo, ConnectionInfo connectionInfo) {
		if (customerInfo != null && customerInfo.getConnectionType() == 3 && connectionInfo != null) {
			try {
				String nodeName = gmmsUtility.getCustomerManager().getNodeIDByConnectionID(connectionInfo.getConnectionName());
				if (!isBlank(nodeName)) {
					return nodeName;
				}
			} catch (Exception e) {
				log.debug("Failed to resolve nodeName for connection {}", connectionInfo.getConnectionName(), e);
			}
		}
		return connectionInfo == null ? "" : safe(connectionInfo.getConnectionName());
	}

	private String buildSessionKey(SessionSnapshot snapshot) {
		return safePart(snapshot.module) + "|" + safePart(snapshot.nodeId) + "|" + safePart(snapshot.nodeName)
				+ "|" + safePart(snapshot.connectionName) + "|" + safePart(snapshot.sessionName)
				+ "|" + safePart(snapshot.sessionUuid);
	}

	private String detail(SessionSnapshot snapshot, long now, int ttlSeconds) {
		Map<String, String> values = new LinkedHashMap<String, String>();
		values.put("role", "smpp-server");
		values.put("module", snapshot.module);
		values.put("nodeId", snapshot.nodeId);
		values.put("nodeName", snapshot.nodeName);
		values.put("ossid", Integer.toString(snapshot.ossid));
		values.put("systemId", snapshot.systemId);
		values.put("connectionName", snapshot.connectionName);
		values.put("sessionName", snapshot.sessionName);
		values.put("sessionUuid", snapshot.sessionUuid);
		values.put("bindType", snapshot.bindType);
		values.put("status", snapshot.status);
		values.put("remoteIp", snapshot.remoteIp);
		values.put("localIp", snapshot.localIp);
		values.put("transactionUri", snapshot.transactionUri);
		values.put("lastSeen", Long.toString(now));
		values.put("expireAt", Long.toString(now + ttlSeconds * 1000L));
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : values.entrySet()) {
			sb.append(entry.getKey()).append("=").append(safe(entry.getValue())).append(";");
		}
		return sb.toString();
	}

	private Map<String, String> parse(String detail) {
		Map<String, String> map = new LinkedHashMap<String, String>();
		if (detail == null) {
			return map;
		}
		String[] parts = detail.split(";");
		for (String part : parts) {
			int idx = part.indexOf('=');
			if (idx <= 0) {
				continue;
			}
			map.put(part.substring(0, idx), part.substring(idx + 1));
		}
		return map;
	}

	private RedisClient redis() {
		try {
			return gmmsUtility.getRedisClient();
		} catch (Exception e) {
			log.warn("State redis is not available for SMPP server bind registry.", e);
			return null;
		}
	}

	private int ttlSeconds() {
		int configured = intProperty("SmppServerSessionRegistryTtlSeconds", -1);
		if (configured > 0) {
			return configured;
		}
		return intProperty("MaxSilentTime", 600) + 60;
	}

	private int intProperty(String key, int defaultValue) {
		try {
			String value = gmmsUtility.getCommonProperty(key, Integer.toString(defaultValue));
			return Integer.parseInt(value.trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private boolean booleanProperty(String key, boolean defaultValue) {
		try {
			String value = gmmsUtility.getCommonProperty(key, Boolean.toString(defaultValue));
			return Boolean.parseBoolean(value.trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private String detailKey(int ossid, String sessionKey) {
		return DETAIL_PREFIX + ossid + ":" + sessionKey;
	}

	private String activeKey(int ossid) {
		return ACTIVE_PREFIX + ossid;
	}

	private String activeModuleKey(int ossid, String module) {
		return ACTIVE_PREFIX + ossid + ":" + safePart(module);
	}

	private String connKey(int ossid, String module, String connectionName) {
		return CONN_PREFIX + ossid + ":" + safePart(module) + ":" + safePart(connectionName);
	}

	private String nodeKey(int ossid, String module, String nodeName) {
		return NODE_PREFIX + ossid + ":" + safePart(module) + ":" + safePart(nodeName);
	}

	private String moduleKey(int ossid) {
		return MODULE_PREFIX + ossid;
	}

	private String safePart(String value) {
		return safe(value).replace(':', '_').replace('|', '_').replace(';', '_');
	}

	private String safe(String value) {
		return value == null ? "" : value.trim();
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}

	private static class SessionSnapshot {
		private int ossid;
		private String module;
		private String nodeId;
		private String nodeName;
		private String connectionName;
		private String sessionName;
		private String sessionUuid;
		private String systemId;
		private String bindType;
		private String status;
		private String remoteIp;
		private String localIp;
		private String transactionUri;
		private String sessionKey;
	}
}
