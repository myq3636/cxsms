package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.redis.RedisClient;

/**
 * Operations on the module registry: java -Da2p_home=... -cp ... com.king.gmms.ha.A2pCtl status
 * init [--from-file]   take the modules of a2p_modules.conf into the registry
 * status               modules, states, numbers, nodes, shards, drain progress, and what differs from the file
 * drain NAME [--wait]  start draining a Core or a Client (move its shards away by hand), --wait until it is done
 * cancel NAME          back to normal service
 * remove NAME [--force] the module is drained and stopped: REMOVED
 * drain-dr NAME [--dry-run] [--force]  a Server that is gone: move the DRs left in its streams
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
			System.out.println("Usage: A2pCtl init [--from-file] | status | drain NAME [--wait] | cancel NAME | remove NAME [--force] | drain-dr NAME [--dry-run] [--force]");
			return 1;
		}
		String home = System.getProperty("a2p_home", "/usr/local/a2p/");
		if (!home.endsWith("/")) {
			home = home + "/";
			System.setProperty("a2p_home", home);
		}
		System.setProperty("module", "A2pCtl");
		GmmsUtility.getInstance().initUtility(home + "Gmms/GmmsConfig.properties");
		// also GmmsUtility.getRedisClient(): the SMPP bind registry reads through it (drain-dr)
		GmmsUtility.getInstance().setRedisClientFlag("M");
		if ("init".equals(args[0])) {
			return init(args.length > 1 && "--from-file".equals(args[1]));
		}
		if ("status".equals(args[0])) {
			return status();
		}
		if (args.length > 1 && "drain".equals(args[0])) {
			return drain(args[1], args.length > 2 && "--wait".equals(args[2]));
		}
		if (args.length > 1 && "cancel".equals(args[0])) {
			return cancel(args[1]);
		}
		if (args.length > 1 && "drain-dr".equals(args[0])) {
			List<String> flags = java.util.Arrays.asList(args).subList(2, args.length);
			return drainDr(args[1], flags.contains("--dry-run"), flags.contains("--force"));
		}
		if (args.length > 1 && "remove".equals(args[0])) {
			return remove(args[1], args.length > 2 && "--force".equals(args[2]));
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

	private static Map<String, String> progress(String module) {
		Map<String, String> p = RedisClient.getInstance().hgetAll("module:drain:" + module);
		return p == null ? new HashMap<String, String>() : p;
	}

	/** entries in stream:dr:pending:*:{module}:*, read or not */
	private static long drLeft(String module) {
		long n = 0;
		RedisClient redis = RedisClient.getInstance();
		for (String key : redis.keysReportMq(StreamQueueManager.STR_DR_PENDING_PREFIX + "*:" + module + ":*")) {
			n += redis.xlenReportMq(key);
		}
		return n;
	}

	private static boolean running(String module) {
		return !RedisClient.getInstance().getStateRedis().scanAll("module:status:*:" + module + ":*").isEmpty();
	}

	private static void changed(String module, String state) {
		ModuleRegistry.write(module, "state", state);
		RedisClient.getInstance().publishState(ModuleStatusReporter.CONTROL_CHANNEL, "RELOAD_MODULES");
		System.out.println(module + ": " + state);
	}

	private static int drain(String module, boolean wait) throws Exception {
		Map<String, ModuleRegistry.Module> modules = ModuleRegistry.load();
		ModuleRegistry.Module m = modules.get(module);
		if (m == null || !ModuleRegistry.ACTIVE.equals(m.state)) {
			System.out.println(module + " is not an ACTIVE module: " + (m == null ? "not registered" : m.state));
			return 1;
		}
		if (!"CoreEngine".equals(m.type) && !"MultiSmppClient".equals(m.type) && !"CommonHttpClient".equals(m.type)
				&& !"MultiSmppServer".equals(m.type)) {
			System.out.println("Draining is done for Core, Client and SMPP Server modules only so far; " + module + " is " + m.type);
			return 1;
		}
		boolean other = false;
		for (ModuleRegistry.Module o : modules.values()) {
			other |= !o.name.equals(module) && m.type.equals(o.type) && ModuleRegistry.ACTIVE.equals(o.state);
		}
		if (!other) {
			System.out.println(module + " is the last ACTIVE " + m.type + ": not drained");
			return 1;
		}
		if ("MultiSmppServer".equals(m.type)) {
			List<String> pinned = pinnedCustomers(module, modules);
			if (!pinned.isEmpty()) {
				System.out.println("Customers that may only use " + module + " (SmppServerModules): " + pinned + ". Change their configuration first");
				return 1;
			}
		}
		changed(module, ModuleRegistry.DRAINING);
		if ("MultiSmppServer".equals(m.type)) {
			System.out.println("It takes no new connection and closes its customer sessions one by one. Take it out of the load balancer");
		} else {
			String group = "CoreEngine".equals(m.type) ? "core" : "client";
			System.out.println("Now move its shards to other modules of its type: system:" + group + ":shards:" + module
					+ " = none, then RELOAD_" + group.toUpperCase() + "_SHARDS");
		}
		if (!wait) {
			return 0;
		}
		while (!"true".equals(progress(module).get("done"))) {
			Thread.sleep(1000L);
			System.out.println("  " + progress(module));
			if (!ModuleRegistry.DRAINING.equals(ModuleRegistry.load().get(module).state)) {
				System.out.println(module + " is not draining any more");
				return 1;
			}
		}
		changed(module, ModuleRegistry.DRAINED);
		System.out.println("Done: stop the process now");
		return 0;
	}

	/** customers whose SmppServerModules lists this module and no other ACTIVE one */
	private static List<String> pinnedCustomers(String module, Map<String, ModuleRegistry.Module> modules) {
		List<String> pinned = new ArrayList<String>();
		com.king.gmms.domain.A2PCustomerManager customers = GmmsUtility.getInstance().getCustomerManager();
		if (customers == null) {
			return pinned;
		}
		SmppServerAssignmentResolver assignments = SmppServerAssignmentResolver.getInstance();
		for (com.king.gmms.domain.A2PCustomerInfo c : customers.getAllCustomerInfos()) {
			if (c == null || !assignments.isRestricted(c)) {
				continue;
			}
			boolean here = false, elsewhere = false;
			for (String m : assignments.assignedModules(c)) {
				if (m.equalsIgnoreCase(module)) {
					here = true;
				} else if (modules.containsKey(m) && ModuleRegistry.ACTIVE.equals(modules.get(m).state)) {
					elsewhere = true;
				}
			}
			if (here && !elsewhere) {
				pinned.add(c.getShortName() + "(" + c.getSSID() + ")");
			}
		}
		return pinned;
	}

	private static int drainDr(String module, boolean dryRun, boolean force) {
		if (running(module) && !force) {
			System.out.println(module + " is running: drain it instead (drain " + module + "); its DRs go back to Core and are sent again");
			return 1;
		}
		if (ModuleRegistry.enabled()) {
			ModuleRegistry.reloadForTool();
		}
		DrTransfer.Result r = DrTransfer.run(module, dryRun);
		System.out.println((dryRun ? "Would move: " : "Moved: ") + r.movedTo);
		if (!r.noTarget.isEmpty()) {
			System.out.println("No Server to move to, left where they are (customer ssid: count): " + r.noTarget
					+ ". Run again once these customers can use another Server (bound there, SmppServerModules)");
		}
		if (r.failed > 0) {
			System.out.println("Failed: " + r.failed + ", left where they are");
		}
		if (!dryRun) {
			System.out.println("Streams emptied and removed: " + r.streamsRemoved);
		}
		return r.failed > 0 ? 1 : 0;
	}

	private static int cancel(String module) {
		ModuleRegistry.Module m = ModuleRegistry.load().get(module);
		if (m == null || !(ModuleRegistry.DRAINING.equals(m.state) || ModuleRegistry.DRAINED.equals(m.state))) {
			System.out.println(module + " is not draining: " + (m == null ? "not registered" : m.state));
			return 1;
		}
		if (ModuleRegistry.DRAINED.equals(m.state) && !running(module)) {
			System.out.println(module + " is stopped: start it again instead");
			return 1;
		}
		boolean closed = progress(module).containsKey("channelsClosed");
		changed(module, ModuleRegistry.ACTIVE);
		System.out.println("Give it its shards back if it should have some");
		if (closed) {
			System.out.println("Its vendor connections were closed already: restart it with the start script to connect again");
		}
		return 0;
	}

	private static int remove(String module, boolean force) {
		ModuleRegistry.Module m = ModuleRegistry.load().get(module);
		if (m == null || ModuleRegistry.REMOVED.equals(m.state)) {
			System.out.println(module + " is " + (m == null ? "not registered" : "removed already"));
			return 1;
		}
		boolean drained = ModuleRegistry.DRAINED.equals(m.state) || "true".equals(progress(module).get("done"));
		if (!force && (!drained || running(module))) {
			System.out.println(module + (running(module) ? " is still running" : " is not drained") + ": not removed (--force when it is gone for good)");
			return 1;
		}
		long drLeft = drLeft(module);
		if (!force && drLeft > 0) {
			System.out.println(module + ": " + drLeft + " DR(s) left in its streams, nobody reads them once it is removed: drain-dr " + module
					+ " first (--force to remove anyway)");
			return 1;
		}
		ModuleRegistry.write(module, "state", ModuleRegistry.REMOVED, "removedAt", String.valueOf(System.currentTimeMillis()));
		RedisClient.getInstance().publishState(ModuleStatusReporter.CONTROL_CHANNEL, "RELOAD_MODULES");
		System.out.println(module + ": REMOVED");
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
		System.out.println(String.format("%-22s %-20s %-3s %-9s %-5s %-14s %-12s %s", "module", "type", "no", "state", "node", "running on", "shards", "drain"));
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
			Map<String, String> p = progress(m.name);
			String drain = p.isEmpty() ? "" : p.get("stage") + (p.containsKey("inflight") ? " inflight=" + p.get("inflight") : "");
			System.out.println(String.format("%-22s %-20s %-3d %-9s %-5s %-14s %-12s %s", m.name, m.type,
					ModuleManager.trailingNumber(m.name), m.state, m.nodeId, running.isEmpty() ? "-" : running.toString(), shards, drain));
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
