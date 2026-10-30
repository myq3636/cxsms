package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;

/**
 * Resolves the server module that should receive an SMPP DR stream message.
 * It does not select a concrete SMPP bind session; that remains owned by the
 * server-side customer connection strategy.
 */
public class SmppServerDrRouteResolver {
	private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerDrRouteResolver.class);
	private static final SmppServerDrRouteResolver INSTANCE = new SmppServerDrRouteResolver();

	private final SmppServerSessionRegistry registry = SmppServerSessionRegistry.getInstance();
	private final SmppServerAssignmentResolver assignmentResolver = SmppServerAssignmentResolver.getInstance();
	private final GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private final ModuleManager moduleManager = ModuleManager.getInstance();

	private SmppServerDrRouteResolver() {
	}

	public static SmppServerDrRouteResolver getInstance() {
		return INSTANCE;
	}

	public String resolveTargetModule(GmmsMessage msg) {
		if (msg == null) {
			return null;
		}
		int ossid = msg.getOSsID();
		String originalModule = originalModule(msg);
		if (!registry.isEnabled()) {
			A2PCustomerInfo customer = gmmsUtility.getCustomerManager().getCustomerBySSID(ossid);
			if (assignmentResolver.isRestricted(customer)) {
				String assignedModule = selectAssignedServerModule(customer, msg, originalModule);
				if (!isBlank(assignedModule)) {
					logLimited(msg, false, "registry-off-assigned", "SMPP DR registry disabled, fallback by customer assignment. ossid={}, originalModule={}, assignedModule={}, assigned={}",
					ossid, originalModule, assignedModule, assignmentResolver.assignedModules(customer));
					return assignedModule;
				}
				logLimited(msg, true, "registry-off-none", "SMPP DR registry disabled and no assigned online server module found. ossid={}, originalModule={}, assigned={}",
					ossid, originalModule, assignmentResolver.assignedModules(customer));
				return null;
			}
			return originalModule;
		}
		if (isBlank(originalModule)) {
			originalModule = activeDeliveryChannelModule(msg, ossid);
		}
		if (!isBlank(originalModule) && registry.hasActiveSessionInModule(ossid, originalModule)) {
			return originalModule;
		}
		String fallbackModule = registry.selectActiveModule(ossid, originalModule);
		if (!isBlank(fallbackModule)) {
			logLimited(msg, false, "fallback-active", "SMPP DR target server module fallback. ossid={}, originalModule={}, fallbackModule={}",
					ossid, originalModule, fallbackModule);
			return fallbackModule;
		}
		A2PCustomerInfo customer = gmmsUtility.getCustomerManager().getCustomerBySSID(ossid);
		if (assignmentResolver.isRestricted(customer)) {
			String assignedModule = selectAssignedServerModule(customer, msg, originalModule);
			if (!isBlank(assignedModule)) {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(MetricsNames.FLOW_CORE,
						msg, MetricsNames.ACTION_REGISTRY_FALLBACK_CONFIGURED_SERVER));
				logLimited(msg, true, "fallback-assigned", "SMPP DR registry has no active server session, fallback by customer assignment. ossid={}, originalModule={}, assignedModule={}, assigned={}",
					ossid, originalModule, assignedModule, assignmentResolver.assignedModules(customer));
				return assignedModule;
			}
			MetricsCollector.getInstance().incrementCounter(MetricsNames.business(MetricsNames.FLOW_CORE,
					msg, MetricsNames.ACTION_REGISTRY_NO_SERVER_MODULE));
			logLimited(msg, true, "none-assigned", "SMPP DR target server module not found by customer assignment. ossid={}, originalModule={}, assigned={}",
					ossid, originalModule, assignmentResolver.assignedModules(customer));
			return null;
		}
		if (!registry.hasKnownModuleData(ossid) && !isBlank(originalModule)) {
			logLimited(msg, false, "no-module-data", "SMPP DR bind registry has no module data, fallback to original module. ossid={}, originalModule={}",
					ossid, originalModule);
			return originalModule;
		}
		String configuredModule = selectConfiguredServerModule(originalModule);
		if (!isBlank(configuredModule)) {
			MetricsCollector.getInstance().incrementCounter(MetricsNames.business(MetricsNames.FLOW_CORE,
					msg, MetricsNames.ACTION_REGISTRY_FALLBACK_CONFIGURED_SERVER));
			logLimited(msg, true, "fallback-configured", "SMPP DR registry has no active server session, fallback to configured server module. ossid={}, originalModule={}, configuredModule={}",
					ossid, originalModule, configuredModule);
			return configuredModule;
		}
		MetricsCollector.getInstance().incrementCounter(MetricsNames.business(MetricsNames.FLOW_CORE,
				msg, MetricsNames.ACTION_REGISTRY_NO_SERVER_MODULE));
		logLimited(msg, true, "none", "SMPP DR target server module not found. ossid={}, originalModule={}",
					ossid, originalModule);
		return null;
	}

	/**
	 * DR routing notes repeat for every DR of a customer while its situation lasts: at most one line
	 * per customer and reason every 10 minutes, with the number of DRs since the last line.
	 */
	private void logLimited(GmmsMessage msg, boolean warn, String reason, String format, Object... args) {
		long n = com.king.gmms.util.LogRateLimiter.allow("dr-route:" + reason + ":" + msg.getOSsID(), 600000L);
		if (n <= 0) {
			return;
		}
		String line = format + " (" + n + " DR(s) since the last line, next in 10 min at most)";
		if (warn) {
			log.warn(msg, line, args);
		} else if (log.isInfoEnabled()) {
			log.info(msg, line, args);
		}
	}

	public String originalModule(GmmsMessage msg) {
		if (msg == null || msg.getInnerTransaction() == null
				|| msg.getInnerTransaction().getModule() == null) {
			return null;
		}
		return msg.getInnerTransaction().getModule().getModule();
	}

	private String activeDeliveryChannelModule(GmmsMessage msg, int ossid) {
		String module = deliveryChannelModule(msg);
		if (isBlank(module) || !registry.hasActiveSessionInModule(ossid, module)) {
			return null;
		}
		return module;
	}

	private String deliveryChannelModule(GmmsMessage msg) {
		if (msg == null || isBlank(msg.getDeliveryChannel())) {
			return null;
		}
		String deliveryChannel = msg.getDeliveryChannel();
		int idx = deliveryChannel.lastIndexOf(':');
		if (idx >= 0 && idx + 1 < deliveryChannel.length()) {
			return deliveryChannel.substring(idx + 1);
		}
		return deliveryChannel;
	}

	public boolean isOriginalModule(GmmsMessage msg, String moduleName) {
		String originalModule = originalModule(msg);
		return !isBlank(originalModule) && !isBlank(moduleName) && originalModule.equalsIgnoreCase(moduleName);
	}

	private String selectConfiguredServerModule(String originalModule) {
		List<String> modules = moduleManager.getServerModules();
		if (modules == null || modules.isEmpty()) {
			return null;
		}
		List<String> candidates = new ArrayList<String>();
		List<String> originalCandidates = new ArrayList<String>();
		for (String module : modules) {
			if (isBlank(module)) {
				continue;
			}
			if (!isOnlineServerModule(module)) {
				continue;
			}
			if (!isBlank(originalModule) && originalModule.equalsIgnoreCase(module)) {
				originalCandidates.add(module);
			} else {
				candidates.add(module);
			}
		}
		if (candidates.isEmpty()) {
			candidates = originalCandidates;
		}
		if (candidates.isEmpty()) {
			return null;
		}
		return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
	}

	private String selectAssignedServerModule(A2PCustomerInfo customer, GmmsMessage msg, String originalModule) {
		List<String> modules = assignmentResolver.assignedModules(customer);
		if (modules.isEmpty()) {
			return null;
		}
		List<String> candidates = new ArrayList<String>();
		for (String module : modules) {
			if (isBlank(module)) {
				continue;
			}
			if (!isOnlineServerModule(module)) {
				continue;
			}
			candidates.add(module);
		}
		if (candidates.isEmpty()) {
			return null;
		}
		if (!isBlank(originalModule)) {
			for (String module : candidates) {
				if (originalModule.equalsIgnoreCase(module)) {
					return module;
				}
			}
		}
		return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
	}

	private boolean isOnlineServerModule(String module) {
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			if (redis == null) {
				return false;
			}
			Set<String> nodes = redis.smembers("system:server:nodes");
			if (nodes == null || nodes.isEmpty()) {
				nodes = redis.smembers("system:nodes:server");
			}
			if (nodes == null || nodes.isEmpty()) {
				return false;
			}
			for (String nodeId : nodes) {
				if (isBlank(nodeId)) {
					continue;
				}
				String status = redis.getString("module:status:server:" + module + ":" + nodeId);
				if ("ONLINE".equalsIgnoreCase(status)) {
					return true;
				}
			}
		} catch (Exception e) {
			log.warn("Failed to check configured server module online status. module={}", module, e);
		}
		return false;
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}
}
