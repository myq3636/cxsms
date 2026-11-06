package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import redis.clients.jedis.Response;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.redis.RedisClient;

import redis.clients.jedis.resps.ScanResult;

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
	private volatile String moduleCleanupCursor = "0";
	private volatile long lastLegacyCleanupMs = 0L;

	private static final long NODE_ONLINE_CACHE_MS = 1000L;
	/** module:status key -> {checkedAt, 1 online / 0 offline}: at most one GET per node and second */
	private final ConcurrentHashMap<String, long[]> nodeOnline = new ConcurrentHashMap<String, long[]>();

	private volatile long lastReconcileFinishedMs = 0L;
	private volatile int consecutiveFailedRounds = 0;
	private volatile long lastSelfRefreshLogMs = 0L;

	private SmppServerSessionRegistry() {
	}

	public static SmppServerSessionRegistry getInstance() {
		return INSTANCE;
	}

	public boolean isEnabled() {
		return booleanProperty("SmppServerSessionRegistryEnable", true);
	}

	/** a session refreshes itself on a PDU after this long without a successful write; below the TTL by a wide margin */
	public int refreshIntervalMs() {
		int configured = intProperty("SmppServerSessionRegistryRefreshIntervalMs", 180000);
		if (configured <= 0) {
			configured = 180000;
		}
		return (int) Math.min(configured, Math.max(1000L, ttlSeconds() * 1000L / 3));
	}

	public int batchSize() {
		return Math.max(1, intProperty("SmppServerSessionRegistryBatchSize", 200));
	}

	public int reconcileIntervalMs() {
		return intProperty("SmppServerSessionRegistryReconcileIntervalMs", 60000);
	}

	public int legacyCleanupIntervalMs() {
		return intProperty("SmppServerRegistryLegacyCleanupIntervalMs", 300000);
	}

	public String register(MultiSmppSession session) {
		RefreshResult result = write(Collections.singletonList(session), true);
		return result.liveKeys.isEmpty() ? null : result.liveKeys.iterator().next();
	}

	/** true if the session's entry was written */
	public boolean refresh(MultiSmppSession session) {
		return write(Collections.singletonList(session), false).ok == 1;
	}

	public RefreshResult refreshBatch(List<MultiSmppSession> sessions) {
		return write(sessions, false);
	}

	public static final class RefreshResult {
		public final Set<String> liveKeys = new HashSet<String>();
		public final List<MultiSmppSession> okSessions = new ArrayList<MultiSmppSession>();
		public int ok = 0;
		public int failed = 0;
	}

	/** One reconcile round is over: logs it, ERROR after failures in 3 rounds in a row */
	public void reconcileFinished(int live, int ok, int failed, int removed, boolean cleanupSkipped, long costMs) {
		lastReconcileFinishedMs = System.currentTimeMillis();
		consecutiveFailedRounds = failed > 0 ? consecutiveFailedRounds + 1 : 0;
		if (consecutiveFailedRounds >= 3) {
			log.error("SMPP server bind registry reconcile has failures in {} rounds in a row. live={}, ok={}, failed={}, removed={}, costMs={}",
					consecutiveFailedRounds, live, ok, failed, removed, costMs);
		} else if (failed > 0 || cleanupSkipped) {
			log.warn("SMPP server bind registry reconcile finished with problems. live={}, ok={}, failed={}, removed={}, cleanupSkipped={}, costMs={}",
					live, ok, failed, removed, cleanupSkipped, costMs);
		} else {
			log.info("SMPP server bind registry reconcile finished. live={}, ok={}, failed={}, removed={}, costMs={}",
					live, ok, failed, removed, costMs);
		}
	}

	/** A session had to refresh itself: reconcile did not reach it. Logged at most once a minute. */
	public void selfRefreshNeeded(int ossid) {
		long lastRound = lastReconcileFinishedMs;
		if (lastRound <= 0) {
			return; // no reconcile in this module (TLS server) or the first round is still to come
		}
		long now = System.currentTimeMillis();
		if (now - lastSelfRefreshLogMs < 60000L) {
			return;
		}
		lastSelfRefreshLogMs = now;
		if (now - lastRound > 5L * reconcileIntervalMs()) {
			log.error("SMPP server bind registry reconcile has not finished for {} ms, sessions refresh themselves on PDUs. Sessions without PDUs will expire.", now - lastRound);
		} else {
			log.warn("SMPP server session refreshed itself, reconcile did not reach it. ossid={}", ossid);
		}
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
			removeSessionKey(redis, snapshot.ossid, snapshot.sessionKey, snapshot);
			removeSlotIfMatches(redis, snapshot);
			if (!hasActiveSessionInModule(snapshot.ossid, snapshot.module)) {
				redis.zrem(moduleKey(snapshot.ossid), new String[] { snapshot.module });
			}
			log.info("SMPP server bind registry unregistered. ossid={}, module={}, connection={}, sessionKey={}",
					snapshot.ossid, snapshot.module, snapshot.connectionName, snapshot.sessionKey);
		} catch (Exception e) {
			log.warn("Failed to unregister SMPP server bind registry.", e);
		}
	}

	public String sessionKey(MultiSmppSession session) {
		SessionSnapshot snapshot = snapshot(session, null);
		return snapshot == null ? null : snapshot.sessionKey;
	}

	/**
	 * Removes this module's entries that are not live. An entry written since roundStartMs is kept: the
	 * session was bound after the live set was taken. Returns how many were removed.
	 */
	public int cleanupCurrentModule(Set<String> liveSessionKeys, long roundStartMs) {
		if (!isEnabled()) {
			return 0;
		}
		RedisClient redis = redis();
		if (redis == null || redis.getStateRedis() == null) {
			return 0;
		}
		String module = safe(System.getProperty("module", ""));
		if (isBlank(module)) {
			return 0;
		}
		int removed = 0;
		try {
			String pattern = DETAIL_PREFIX + "*:" + safePart(module) + "|*";
			int scanCount = Math.max(10, intProperty("SmppServerRegistryLegacyCleanupScanCount", 200));
			int maxKeys = Math.max(1, intProperty("SmppServerRegistryLegacyCleanupMaxKeysPerRun", 5000));
			boolean legacyCleanupEnabled = booleanProperty("SmppServerRegistryLegacyCleanupEnable", true);
			long now = System.currentTimeMillis();
			boolean allowLegacyCleanup = legacyCleanupEnabled && now - lastLegacyCleanupMs >= legacyCleanupIntervalMs();
			int scanned = 0;
			String cursor = isBlank(moduleCleanupCursor) ? "0" : moduleCleanupCursor;
			while (scanned < maxKeys) {
				ScanResult<String> result = redis.getStateRedis().scanKeys(cursor, pattern, scanCount);
				if (result == null) {
					break;
				}
				cursor = result.getCursor();
				List<String> keys = result.getResult();
				if (keys != null) {
					for (String key : keys) {
						if (scanned >= maxKeys) {
							break;
						}
						scanned++;
						ParsedDetailKey parsedKey = parseDetailKey(key);
						if (parsedKey == null) {
							continue;
						}
						boolean live = liveSessionKeys != null && liveSessionKeys.contains(parsedKey.sessionKey);
						if (live && !allowLegacyCleanup) {
							continue;
						}
						String detail = redis.getString(key);
						Map<String, String> detailMap = parse(detail);
						String detailModule = detailMap.get("module");
						if (!isBlank(detailModule) && !module.equalsIgnoreCase(detailModule)) {
							continue;
						}
						SessionSnapshot snapshot = snapshotFromDetail(parsedKey.ossid, parsedKey.sessionKey, detailMap);
						boolean stale = !live && !writtenSince(detailMap, roundStartMs);
						boolean legacy = allowLegacyCleanup && snapshot != null
								&& !parsedKey.sessionKey.equals(snapshot.slotKey);
						if (!stale && !legacy) {
							continue;
						}
						removeSessionKey(redis, parsedKey.ossid, parsedKey.sessionKey, snapshot);
						removed++;
					}
				}
				if ("0".equals(cursor)) {
					break;
				}
			}
			moduleCleanupCursor = cursor == null ? "0" : cursor;
			if (allowLegacyCleanup) {
				lastLegacyCleanupMs = now;
			}
			if (removed > 0) {
				log.info("SMPP server bind registry reconciled. module={}, live={}, scanned={}, removed={}, cursor={}, legacyCleanup={}",
						module, liveSessionKeys == null ? 0 : liveSessionKeys.size(), scanned, removed,
						moduleCleanupCursor, allowLegacyCleanup);
			}
		} catch (Exception e) {
			log.warn("Failed to reconcile SMPP server bind registry.", e);
		}
		return removed;
	}

	private static boolean writtenSince(Map<String, String> detailMap, long ms) {
		try {
			return Long.parseLong(detailMap.get("lastSeen")) >= ms;
		} catch (Exception e) {
			return false;
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
		boolean offline = false;
		for (String member : members) {
			Map<String, String> detail = activeDetail(ossid, member, module);
			if (detail == null) {
				redis.zrem(key, new String[] { member });
				redis.zrem(activeKey(ossid), new String[] { member });
			} else if (!isNodeOnline(module, detail.get("nodeId"))) {
				// killed node: its entries live until the TTL; skipped, not removed, so they count
				// again as soon as the heartbeat is back
				offline = true;
			} else {
				found = true;
			}
		}
		if (!found && !offline) {
			redis.zrem(moduleKey(ossid), new String[] { module });
		}
		return found;
	}

	/**
	 * A bind entry is kept for SmppServerSessionRegistryTtlSeconds (default MaxSilentTime + 60 s)
	 * when a server is killed. The module heartbeat (module:status:server:{module}:{node}, TTL
	 * ModuleStatusTTLSeconds, default 30 s) tells sooner that the node is gone.
	 * Unknown node, a node that never registered, or Redis error: online (as before).
	 */
	public boolean isNodeOnline(String module, String nodeId) {
		if (isBlank(module) || isBlank(nodeId) || !booleanProperty("SmppServerDrLivenessCheck", true)) {
			return true;
		}
		String statusKey = "module:status:server:" + module + ":" + nodeId;
		long now = System.currentTimeMillis();
		long[] cached = nodeOnline.get(statusKey);
		if (cached != null && now - cached[0] < NODE_ONLINE_CACHE_MS) {
			return cached[1] != 0L;
		}
		boolean online = true;
		try {
			RedisClient redis = redis();
			// offline only for a node that registered (system:server:nodes, left there when it is
			// killed) and has no heartbeat now; a node whose status reporter never started keeps
			// the old behaviour
			if (redis != null && !"ONLINE".equalsIgnoreCase(redis.getString(statusKey))) {
				Set<String> nodes = redis.smembers("system:server:nodes");
				online = nodes == null || !nodes.contains(nodeId);
			}
		} catch (Exception e) {
			online = true;
		}
		if (nodeOnline.size() > 1000) {
			nodeOnline.clear();
		}
		nodeOnline.put(statusKey, new long[] { now, online ? 1L : 0L });
		if (!online) {
			long n = com.king.gmms.util.LogRateLimiter.allow("dr-route-offline:" + statusKey, 600000L);
			if (n > 0) {
				log.info("[DR-ROUTE] server module={} node={} has no heartbeat ({} missing), its bind sessions are not used for DRs"
						+ " ({} time(s) since the last line, next in 10 min at most)", module, nodeId, statusKey, n);
			}
		}
		return online;
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
			if (isBlank(module) || module.equalsIgnoreCase(excludedModule) || ModuleRegistry.isDraining(module)) {
				continue;
			}
			if (hasActiveSessionInModule(ossid, module)) {
				return module;
			}
		}
		return null;
	}

	/**
	 * Registers the sessions in two round trips: MGET of the slots, then one pipeline with all writes,
	 * the detail before the indexes. A failed session stays connected and stays live; the next round writes it again.
	 */
	private RefreshResult write(List<MultiSmppSession> sessions, boolean forceLog) {
		RefreshResult result = new RefreshResult();
		if (!isEnabled() || sessions == null) {
			return result;
		}
		final List<MultiSmppSession> targets = new ArrayList<MultiSmppSession>();
		final List<SessionSnapshot> snapshots = new ArrayList<SessionSnapshot>();
		for (MultiSmppSession session : sessions) {
			if (session == null || !session.isServer()) {
				continue;
			}
			try {
				SessionSnapshot snapshot = snapshot(session, null);
				if (snapshot == null || snapshot.ossid < 0 || isBlank(snapshot.module) || isBlank(snapshot.sessionKey)) {
					continue;
				}
				targets.add(session);
				snapshots.add(snapshot);
				result.liveKeys.add(snapshot.sessionKey);
			} catch (Exception e) {
				result.failed++;
				log.warn("Failed to read SMPP server session for the bind registry.", e);
			}
		}
		if (targets.isEmpty()) {
			return result;
		}
		RedisClient redis = redis();
		if (redis == null || redis.getStateRedis() == null) {
			result.failed += targets.size();
			return result;
		}
		try {
			final long now = System.currentTimeMillis();
			final int ttlSeconds = ttlSeconds();
			List<String> slotKeys = new ArrayList<String>(snapshots.size());
			for (SessionSnapshot snapshot : snapshots) {
				slotKeys.add(slotKey(snapshot));
			}
			List<String> previous = redis.getStateRedis().mgetStrict(slotKeys);
			for (int i = 0; i < snapshots.size(); i++) {
				SessionSnapshot snapshot = snapshots.get(i);
				String previousSessionKey = previous.get(i);
				if (!isBlank(previousSessionKey) && !previousSessionKey.equals(snapshot.sessionKey)) {
					removeSessionKey(redis, snapshot.ossid, previousSessionKey, snapshot);
				}
			}
			final List<List<Response<?>>> responses = new ArrayList<List<Response<?>>>(snapshots.size());
			redis.getStateRedis().runPipeline(p -> {
				Set<Integer> moduleWritten = new HashSet<Integer>();
				for (SessionSnapshot snapshot : snapshots) {
					List<Response<?>> r = new ArrayList<Response<?>>(7);
					r.add(p.setex(detailKey(snapshot.ossid, snapshot.sessionKey), (long) ttlSeconds, detail(snapshot, now, ttlSeconds)));
					r.add(p.setex(slotKey(snapshot), (long) ttlSeconds, snapshot.sessionKey));
					r.add(p.zadd(activeKey(snapshot.ossid), now, snapshot.sessionKey));
					r.add(p.zadd(activeModuleKey(snapshot.ossid, snapshot.module), now, snapshot.sessionKey));
					r.add(p.zadd(connKey(snapshot.ossid, snapshot.module, snapshot.connectionName), now, snapshot.sessionKey));
					r.add(p.zadd(nodeKey(snapshot.ossid, snapshot.module, snapshot.nodeName), now, snapshot.sessionKey));
					if (moduleWritten.add(snapshot.ossid)) {
						r.add(p.zadd(moduleKey(snapshot.ossid), now, snapshot.module));
					}
					responses.add(r);
				}
			});
			for (int i = 0; i < targets.size(); i++) {
				SessionSnapshot snapshot = snapshots.get(i);
				try {
					for (Response<?> response : responses.get(i)) {
						response.get();
					}
					targets.get(i).markServerRegistryRefreshed(now);
					result.ok++;
					result.okSessions.add(targets.get(i));
					if (forceLog) {
						log.info("SMPP server bind registry registered. ossid={}, module={}, node={}, connection={}, session={}, sessionKey={}, ttlSeconds={}",
								snapshot.ossid, snapshot.module, snapshot.nodeName, snapshot.connectionName,
								snapshot.sessionName, snapshot.sessionKey, ttlSeconds);
					}
				} catch (Exception e) {
					result.failed++;
					log.warn("Failed to write SMPP server bind registry. Local SMPP session remains connected and reconcile will retry. ossid={}, module={}, connection={}, sessionKey={}",
							snapshot.ossid, snapshot.module, snapshot.connectionName, snapshot.sessionKey, e);
				}
			}
		} catch (Exception e) {
			result.failed += targets.size();
			log.warn("Failed to write SMPP server bind registry for {} sessions. They remain connected and reconcile will retry.", targets.size(), e);
		}
		return result;
	}

	/** the entry's details, or null if it expired or belongs to another module */
	private Map<String, String> activeDetail(int ossid, String sessionKey, String expectedModule) {
		RedisClient redis = redis();
		if (redis == null || isBlank(sessionKey)) {
			return null;
		}
		String value = redis.getString(detailKey(ossid, sessionKey));
		if (isBlank(value)) {
			return null;
		}
		Map<String, String> map = parse(value);
		if (isBlank(expectedModule)) {
			return map;
		}
		String module = map.get("module");
		return expectedModule.equalsIgnoreCase(module) ? map : null;
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
		snapshot.slotKey = buildSlotKey(snapshot);
		snapshot.sessionKey = isBlank(existingSessionKey)
				? snapshot.slotKey
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
		return buildSlotKey(snapshot);
	}

	private String buildSlotKey(SessionSnapshot snapshot) {
		return safePart(snapshot.module) + "|" + safePart(snapshot.nodeId) + "|" + safePart(snapshot.nodeName)
				+ "|" + safePart(snapshot.connectionName) + "|" + safePart(snapshot.sessionName);
	}

	private void removeSessionKey(RedisClient redis, int ossid, String sessionKey, SessionSnapshot fallback) {
		if (redis == null || isBlank(sessionKey)) {
			return;
		}
		String detailKey = detailKey(ossid, sessionKey);
		SessionSnapshot snapshot = fallback;
		try {
			String detail = redis.getString(detailKey);
			Map<String, String> detailMap = parse(detail);
			if (!detailMap.isEmpty()) {
				snapshot = snapshotFromDetail(ossid, sessionKey, detailMap);
			}
		} catch (Exception ignored) {
		}
		redis.del(detailKey);
		redis.zrem(activeKey(ossid), new String[] { sessionKey });
		if (snapshot != null) {
			redis.zrem(activeModuleKey(ossid, snapshot.module), new String[] { sessionKey });
			redis.zrem(connKey(ossid, snapshot.module, snapshot.connectionName), new String[] { sessionKey });
			redis.zrem(nodeKey(ossid, snapshot.module, snapshot.nodeName), new String[] { sessionKey });
			removeSlotIfMatches(redis, snapshot);
		}
	}

	private void removeSlotIfMatches(RedisClient redis, SessionSnapshot snapshot) {
		if (redis == null || snapshot == null || isBlank(snapshot.slotKey)) {
			return;
		}
		String key = slotKey(snapshot);
		String value = redis.getString(key);
		if (snapshot.sessionKey.equals(value)) {
			redis.del(key);
		}
	}

	private SessionSnapshot snapshotFromDetail(int ossid, String sessionKey, Map<String, String> detailMap) {
		SessionSnapshot snapshot = new SessionSnapshot();
		snapshot.ossid = ossid;
		snapshot.sessionKey = sessionKey;
		snapshot.module = safe(detailMap.get("module"));
		snapshot.nodeId = safe(detailMap.get("nodeId"));
		snapshot.nodeName = safe(detailMap.get("nodeName"));
		snapshot.connectionName = safe(detailMap.get("connectionName"));
		snapshot.sessionName = safe(detailMap.get("sessionName"));
		snapshot.sessionUuid = safe(detailMap.get("sessionUuid"));
		snapshot.systemId = safe(detailMap.get("systemId"));
		snapshot.bindType = safe(detailMap.get("bindType"));
		snapshot.status = safe(detailMap.get("status"));
		snapshot.remoteIp = safe(detailMap.get("remoteIp"));
		snapshot.localIp = safe(detailMap.get("localIp"));
		snapshot.transactionUri = safe(detailMap.get("transactionUri"));
		snapshot.slotKey = buildSlotKey(snapshot);
		return snapshot;
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

	private String slotKey(SessionSnapshot snapshot) {
		return "smpp:server:slot:" + snapshot.ossid + ":" + snapshot.slotKey;
	}

	private ParsedDetailKey parseDetailKey(String key) {
		if (isBlank(key) || !key.startsWith(DETAIL_PREFIX)) {
			return null;
		}
		String rest = key.substring(DETAIL_PREFIX.length());
		int idx = rest.indexOf(':');
		if (idx <= 0 || idx >= rest.length() - 1) {
			return null;
		}
		try {
			ParsedDetailKey parsed = new ParsedDetailKey();
			parsed.ossid = Integer.parseInt(rest.substring(0, idx));
			parsed.sessionKey = rest.substring(idx + 1);
			return parsed;
		} catch (Exception e) {
			return null;
		}
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
		private String slotKey;
	}

	private static class ParsedDetailKey {
		private int ossid;
		private String sessionKey;
	}
}
