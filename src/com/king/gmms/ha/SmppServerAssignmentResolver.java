package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;

/**
 * Resolves optional customer-to-SMPP-server module assignment.
 *
 * Empty SmppServerModules means unrestricted and keeps legacy behavior.
 */
public class SmppServerAssignmentResolver {
	private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerAssignmentResolver.class);
	private static final SmppServerAssignmentResolver INSTANCE = new SmppServerAssignmentResolver();

	private SmppServerAssignmentResolver() {
	}

	public static SmppServerAssignmentResolver getInstance() {
		return INSTANCE;
	}

	public boolean shouldLoadOnCurrentModule(A2PCustomerInfo customer) {
		return shouldLoadOnModule(customer, System.getProperty("module"));
	}

	public boolean shouldLoadOnModule(A2PCustomerInfo customer, String moduleName) {
		List<String> modules = assignedModules(customer);
		if (modules.isEmpty()) {
			return true;
		}
		if (isBlank(moduleName)) {
			return false;
		}
		for (String module : modules) {
			if (moduleName.equalsIgnoreCase(module)) {
				return true;
			}
		}
		return false;
	}

	public boolean isRestricted(A2PCustomerInfo customer) {
		return !assignedModules(customer).isEmpty();
	}

	public List<String> assignedModules(A2PCustomerInfo customer) {
		if (customer == null) {
			return Collections.emptyList();
		}
		return parseModules(customer.getSmppServerModules());
	}

	public String selectAssignedModule(A2PCustomerInfo customer, GmmsMessage msg, String originalModule) {
		List<String> modules = assignedModules(customer);
		if (modules.isEmpty()) {
			return null;
		}
		if (!isBlank(originalModule)) {
			for (String module : modules) {
				if (originalModule.equalsIgnoreCase(module)) {
					return module;
				}
			}
		}
		String selected = modules.get(ThreadLocalRandom.current().nextInt(modules.size()));
		if (log.isDebugEnabled()) {
			log.debug(msg, "SMPP server assignment selected. ssid={}, modules={}, selected={}",
					customer.getSSID(), modules, selected);
		}
		return selected;
	}

	private List<String> parseModules(String value) {
		if (isBlank(value)) {
			return Collections.emptyList();
		}
		String trimmed = value.trim();
		if ("*".equals(trimmed) || "all".equalsIgnoreCase(trimmed)) {
			return Collections.emptyList();
		}
		String[] parts = trimmed.split(",");
		List<String> modules = new ArrayList<String>();
		for (String part : parts) {
			if (isBlank(part)) {
				continue;
			}
			String module = part.trim();
			if ("*".equals(module) || "all".equalsIgnoreCase(module)) {
				return Collections.emptyList();
			}
			if (!containsIgnoreCase(modules, module)) {
				modules.add(module);
			}
		}
		return modules;
	}

	private boolean containsIgnoreCase(List<String> values, String target) {
		for (String value : values) {
			if (value.equalsIgnoreCase(target)) {
				return true;
			}
		}
		return false;
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}
}
