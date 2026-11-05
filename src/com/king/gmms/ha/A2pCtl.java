package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.redis.RedisClient;

/**
 * Operations on the module registry: java -Da2p_home=... -cp ... com.king.gmms.ha.A2pCtl status
 * init [--from-file]   take the modules of a2p_modules.conf into the registry
 * status               modules, states, numbers, nodes, shards, and what differs from the file
 */
public final class A2pCtl {
	private A2pCtl() {
	}

	public static void main(String[] args) {
		int code;
		try {
			code = run(args);
		} catch (Exception e) {
			System.out.println("FAILED: " + e);
			code = 1;
		}
		System.exit(code);
	}

	private static int run(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Usage: A2pCtl init [--from-file] | status");
			return 1;
		}
		String home = System.getProperty("a2p_home", "/usr/local/a2p/");
		if (!home.endsWith("/")) {
			home = home + "/";
			System.setProperty("a2p_home", home);
		}
		System.setProperty("module", "A2pCtl");
		GmmsUtility.getInstance().initUtility(home + "Gmms/GmmsConfig.properties");
		RedisClient.getInstance().setRedisHaFlag(true);
		if ("init".equals(args[0])) {
			return init(args.length > 1 && "--from-file".equals(args[1]));
		}
		if ("status".equals(args[0])) {
			return status();
		}
		System.out.println("Unknown command: " + args[0]);
		return 1;
	}

	private static int init(boolean fromFile) {
		if (ModuleRegistry.initialized() && !fromFile) {
			System.out.println("The registry already exists, nothing changed. To add what is missing: init --from-file");
			return 1;
		}
		List<String[]> lines = ModuleRegistry.readModuleList();
		if (lines.isEmpty()) {
			System.out.println("No module found in a2p_modules.conf");
			return 1;
		}
		ModuleManager manager = ModuleManager.getInstance();
		List<String> errors = new ArrayList<String>();
		Map<String, String> numbers = new HashMap<String, String>();
		for (String[] line : lines) {
			String type = manager.getFullModuleType(line[0]);
			int number = ModuleManager.trailingNumber(line[0]);
			if (number == 0 || number > 9) {
				errors.add(line[0] + ": the number at the end of the name must be 1 to 9");
			} else if (number > 0) {
				String other = numbers.put(type + ":" + number, line[0]);
				if (other != null) {
					errors.add(line[0] + " and " + other + " have the same number " + number);
				}
			}
		}
		if (!errors.isEmpty()) {
			for (String error : errors) {
				System.out.println(error);
			}
			System.out.println("Nothing changed.");
			return 1;
		}
		Map<String, ModuleRegistry.Module> existing = ModuleRegistry.load();
		int added = 0;
		for (String[] line : lines) {
			if (existing.containsKey(line[0])) {
				continue;
			}
			ModuleRegistry.write(line[0], "type", manager.getFullModuleType(line[0]), "role", line[1],
					"nodeId", line[2].length() == 0 ? "0" : line[2], "state", ModuleRegistry.ACTIVE, "removedAt", "0");
			added++;
		}
		RedisClient.getInstance().publishState(ModuleStatusReporter.CONTROL_CHANNEL, "RELOAD_MODULES");
		System.out.println(added + " modules added, " + existing.size() + " were there");
		return 0;
	}

	private static int status() {
		RedisClient redis = RedisClient.getInstance();
		String version = redis.getString("system:modules:version");
		if (version == null) {
			System.out.println("The registry is empty. Run: init");
			return 1;
		}
		System.out.println("version " + version);
		Map<String, ModuleRegistry.Module> modules = new TreeMap<String, ModuleRegistry.Module>(ModuleRegistry.load());
		System.out.println(String.format("%-22s %-20s %-3s %-9s %-5s %-14s %s", "module", "type", "no", "state", "node", "running on", "shards"));
		for (ModuleRegistry.Module m : modules.values()) {
			List<String> running = new ArrayList<String>();
			for (String key : redis.getStateRedis().scanAll("module:status:*:" + m.name + ":*")) {
				running.add(key.substring(key.lastIndexOf(':') + 1));
			}
			String shards = "";
			for (String key : redis.getStateRedis().scanAll("system:shards:*:" + m.name + ":*")) {
				String value = redis.getString(key);
				if (value != null && value.indexOf('|') >= 0) {
					shards = value.substring(value.indexOf('|') + 1);
					shards = shards.length() == 0 ? "none" : shards;
				}
			}
			System.out.println(String.format("%-22s %-20s %-3d %-9s %-5s %-14s %s", m.name, m.type,
					ModuleManager.trailingNumber(m.name), m.state, m.nodeId, running.isEmpty() ? "-" : running.toString(), shards));
		}
		for (String[] line : ModuleRegistry.readModuleList()) {
			if (!modules.containsKey(line[0])) {
				System.out.println("in the file, not registered: " + line[0]);
			}
		}
		Map<String, String> inFile = new HashMap<String, String>();
		for (String[] line : ModuleRegistry.readModuleList()) {
			inFile.put(line[0], line[1]);
		}
		for (ModuleRegistry.Module m : modules.values()) {
			if (!ModuleRegistry.REMOVED.equals(m.state) && !inFile.containsKey(m.name)) {
				System.out.println("registered, not in this file: " + m.name);
			}
		}
		return 0;
	}
}
