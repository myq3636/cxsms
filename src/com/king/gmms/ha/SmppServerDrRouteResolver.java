package com.king.gmms.ha;

import com.king.framework.SystemLogger;
import com.king.message.gmms.GmmsMessage;

/**
 * Resolves the server module that should receive an SMPP DR stream message.
 * It does not select a concrete SMPP bind session; that remains owned by the
 * server-side customer connection strategy.
 */
public class SmppServerDrRouteResolver {
	private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerDrRouteResolver.class);
	private static final SmppServerDrRouteResolver INSTANCE = new SmppServerDrRouteResolver();

	private final SmppServerSessionRegistry registry = SmppServerSessionRegistry.getInstance();

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
			return originalModule;
		}
		if (!isBlank(originalModule) && registry.hasActiveSessionInModule(ossid, originalModule)) {
			return originalModule;
		}
		String fallbackModule = registry.selectActiveModule(ossid, originalModule);
		if (!isBlank(fallbackModule)) {
			log.info(msg, "SMPP DR target server module fallback. ossid={}, originalModule={}, fallbackModule={}",
					ossid, originalModule, fallbackModule);
			return fallbackModule;
		}
		if (!registry.hasKnownModuleData(ossid) && !isBlank(originalModule)) {
			log.warn(msg, "SMPP DR bind registry has no module data, fallback to original module. ossid={}, originalModule={}",
					ossid, originalModule);
			return originalModule;
		}
		log.warn(msg, "SMPP DR target server module not found. ossid={}, originalModule={}", ossid, originalModule);
		return null;
	}

	public String originalModule(GmmsMessage msg) {
		if (msg == null || msg.getInnerTransaction() == null
				|| msg.getInnerTransaction().getModule() == null) {
			return null;
		}
		return msg.getInnerTransaction().getModule().getModule();
	}

	public boolean isOriginalModule(GmmsMessage msg, String moduleName) {
		String originalModule = originalModule(msg);
		return !isBlank(originalModule) && !isBlank(moduleName) && originalModule.equalsIgnoreCase(moduleName);
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}
}
