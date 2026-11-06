package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * a2pctl add / remove / resume / cancel (docs/模块动态扩缩容设计.md 6, 8, 10, 12): one operation at a time
 * (system:modules:lock), its progress in system:modules:op so that it can go on after the tool was stopped, shards
 * moved one at a time: taken from the old owner, confirmed, then given to the new one. Nothing is rolled back on a
 * failure: the tool stops and tells what is where.
 */
final class ModuleOps {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ModuleOps.class);
	static final String LOCK_KEY = "system:modules:lock";
	static final String OP_KEY = "system:modules:op";
	static final String MAINTENANCE_KEY = "system:modules:maintenance";
	private static final int MAINTENANCE_SECONDS = 300;

	static final String ADD = "add";
	static final String REMOVE = "remove";
	// steps
	static final String REGISTERED = "REGISTERED";
	static final String MOVING = "MOVING";
	static final String WAIT_DRAIN = "WAIT_DRAIN";
	static final String WAIT_STOP = "WAIT_STOP";

	/** a step that did not finish: the tool stops, the operation stays for resume or cancel */
	static final class Stopped extends Exception {
		private static final long serialVersionUID = 1L;

		Stopped(String text) {
			super(text);
		}
	}

	private final RedisClient redis = RedisClient.getInstance();
	private String lockId = null;
	private Thread renewer = null;

	// ---------------------------------------------------------------- commands

	int add(String module, String nodeId, String shards, boolean dryRun, boolean force) throws Exception {
		Map<String, ModuleRegistry.Module> modules = ModuleRegistry.load();
		String type = typeOf(module, modules);
		if (type == null) {
			System.out.println(module + ": unknown module type (the name is the type and a number, as CoreEngine3)");
			return 1;
		}
		ModuleRegistry.Module old = modules.get(module);
		if (old != null && !ModuleRegistry.REMOVED.equals(old.state)) {
			System.out.println(module + " is registered already: " + old.state);
			return 1;
		}
		String clash = numberClash(modules, module, type);
		if (clash != null) {
			System.out.println(clash);
			return 1;
		}
		if (refusedByOp(null)) {
			return 1;
		}
		String group = group(type);
		Map<String, TreeSet<Integer>> target = null;
		System.out.println("Add " + module + " (" + type + ", number " + ModuleManager.trailingNumber(module) + ", node " + nodeId + ")");
		if (group != null) {
			Map<String, TreeSet<Integer>> current = currentShards(modules, type);
			int total = totalShards(type, modules);
			List<String> problems = ShardPlan.problems(current, total);
			if (hasOverlap(problems)) {
				System.out.println("The shards overlap now: " + problems + ". Fix that first");
				return 1;
			}
			current.put(module, new TreeSet<Integer>());
			if (shards != null) {
				target = ShardPlan.take(current, module, ShardPlan.parse(shards), total);
			} else {
				List<String> members = members(modules, type, null);
				members.add(module);
				target = ShardPlan.balance(current, members, total);
			}
			if (!printPlan(current, target, total)) {
				return 1;
			}
		}
		boolean over = capacity(modules, type, +1);
		if (over && !force) {
			System.out.println("More connections than SessionNumber: change SessionNumber first, or --force");
			return 1;
		}
		System.out.println("Steps: register as JOINING" + (group != null ? " with no shard" : "") + "; [you] deploy and start it; resume: wait for it"
				+ (group != null ? ", move the shards one at a time" : "") + ", ACTIVE");
		if (dryRun) {
			System.out.println("Nothing changed (--dry-run).");
			return 0;
		}
		if (!lock()) {
			return 1;
		}
		try {
			if (group != null) {
				redis.setString("system:" + keyGroup(group) + ":shards:" + module, "none");
			}
			ModuleRegistry.write(module, "type", type, "role", role(type, module), "nodeId", nodeId, "state", ModuleRegistry.JOINING, "removedAt", "0");
			saveOp(ADD, module, REGISTERED, target, force, null);
			System.out.println(module + ": JOINING");
			return proceed();
		} finally {
			unlock();
		}
	}

	/** an ACTIVE module, the whole way: DRAINING, its shards to the others, drained; REMOVED once it is stopped */
	int remove(String module, String shardsTo, boolean dryRun, boolean force) throws Exception {
		Map<String, ModuleRegistry.Module> modules = ModuleRegistry.load();
		ModuleRegistry.Module m = modules.get(module);
		if (m == null || !ModuleRegistry.ACTIVE.equals(m.state)) {
			System.out.println(module + " is not an ACTIVE module: " + (m == null ? "not registered" : m.state));
			return 1;
		}
		if (refusedByOp(null)) {
			return 1;
		}
		String group = group(m.type);
		boolean other = false;
		for (ModuleRegistry.Module o : modules.values()) {
			other |= !o.name.equals(module) && m.type.equals(o.type) && ModuleRegistry.ACTIVE.equals(o.state);
		}
		if (!other) {
			System.out.println(module + " is the last ACTIVE " + m.type + ": not removed");
			return 1;
		}
		if ("MultiSmppServer".equals(m.type)) {
			List<String> pinned = A2pCtl.pinnedCustomers(module, modules);
			if (!pinned.isEmpty()) {
				System.out.println("Customers that may only use " + module + " (SmppServerModules): " + pinned + ". Change their configuration first");
				return 1;
			}
		} else if (group == null) {
			System.out.println("Removing is done for Core, Client and SMPP Server modules only so far; " + module + " is " + m.type);
			return 1;
		}
		boolean running = A2pCtl.running(module);
		if (!running && !force) {
			System.out.println(module + " is not running: --force to remove it anyway (its shards are given to the others"
					+ ("MultiSmppServer".equals(m.type) ? "; then drain-dr " + module : "") + ")");
			return 1;
		}
		System.out.println("Remove " + module + " (" + m.type + ", " + m.state + (running ? "" : ", not running") + ")");
		Map<String, TreeSet<Integer>> target = null;
		if (group != null) {
			Map<String, TreeSet<Integer>> current = currentShards(modules, m.type);
			int total = totalShards(m.type, modules);
			List<String> problems = ShardPlan.problems(current, total);
			if (hasOverlap(problems)) {
				System.out.println("The shards overlap now: " + problems + ". Fix that first");
				return 1;
			}
			if (shardsTo != null) {
				ModuleRegistry.Module to = modules.get(shardsTo);
				if (to == null || !ModuleRegistry.ACTIVE.equals(to.state) || !m.type.equals(to.type)) {
					System.out.println(shardsTo + " is not an ACTIVE " + m.type);
					return 1;
				}
				target = ShardPlan.giveAll(current, module, shardsTo, total);
				for (String name : new ArrayList<String>(target.keySet())) {
					if (!members(modules, m.type, module).contains(name)) {
						target.remove(name);
					}
				}
			} else {
				target = ShardPlan.balance(current, members(modules, m.type, module), total);
			}
			if (!printPlan(current, target, total)) {
				return 1;
			}
		}
		capacity(modules, m.type, -1);
		System.out.println("Steps: DRAINING" + (group != null ? ", move its shards one at a time" : ", close its customer sessions one by one (take it out of the load balancer)")
				+ ", wait until it is drained; [you] stop it; resume: REMOVED");
		if (dryRun) {
			System.out.println("Nothing changed (--dry-run).");
			return 0;
		}
		if (!lock()) {
			return 1;
		}
		try {
			saveOp(REMOVE, module, MOVING, target, force, null);
			A2pCtl.changed(module, ModuleRegistry.DRAINING);
			if ("MultiSmppServer".equals(m.type)) {
				System.out.println("Take it out of the load balancer");
			}
			return proceed();
		} finally {
			unlock();
		}
	}

	int resume() throws Exception {
		Map<String, String> op = op();
		if (op.isEmpty()) {
			System.out.println("No operation to resume");
			return 1;
		}
		if (!lock()) {
			return 1;
		}
		try {
			System.out.println("Resume: " + op.get("action") + " " + op.get("module") + " at " + op.get("step"));
			return proceed();
		} finally {
			unlock();
		}
	}

	/** null: the current operation, if there is one */
	int cancel(String module) throws Exception {
		Map<String, String> op = op();
		if (op.isEmpty() || (module != null && !module.equals(op.get("module")))) {
			if (module == null) {
				System.out.println("No operation to cancel");
				return 1;
			}
			return A2pCtl.cancelDrain(module);
		}
		if (!lock()) {
			return 1;
		}
		try {
			String name = op.get("module");
			if (ADD.equals(op.get("action"))) {
				ModuleRegistry.Module m = ModuleRegistry.load().get(name);
				Set<Integer> owned = m == null || group(m.type) == null ? new TreeSet<Integer>() : shardsOf(name, group(m.type));
				if (!owned.isEmpty()) {
					System.out.println(name + " owns shards " + ShardPlan.format(owned) + " already: not cancelled. To take it out again: remove " + name);
					return 1;
				}
				// a number of a module that never took traffic is free at once (removedAt 0)
				ModuleRegistry.write(name, "state", ModuleRegistry.REMOVED, "removedAt", "0");
				if (m != null && group(m.type) != null) {
					redis.getStateRedis().delObject("system:" + keyGroup(group(m.type)) + ":shards:" + name);
				}
				reloadModules();
				clearOp();
				System.out.println("Cancelled: " + name + " REMOVED" + (A2pCtl.running(name) ? ". It runs: stop it" : ""));
				return 0;
			}
			int code = A2pCtl.cancelDrain(name);
			if (code == 0) {
				clearOp();
				ModuleRegistry.Module m = ModuleRegistry.load().get(name);
				if (m != null && group(m.type) != null) {
					System.out.println("Shards now: " + ShardPlan.format(currentShards(ModuleRegistry.load(), m.type)));
				}
			}
			return code;
		} finally {
			unlock();
		}
	}

	static Map<String, String> op() {
		Map<String, String> op = RedisClient.getInstance().hgetAll(OP_KEY);
		return op == null ? new HashMap<String, String>() : op;
	}

	/** an operation in progress refuses everything else that changes modules */
	static boolean refusedByOp(String allowedModule) {
		Map<String, String> op = op();
		if (op.isEmpty() || (allowedModule != null && allowedModule.equals(op.get("module")))) {
			return false;
		}
		System.out.println("Operation in progress: " + op.get("action") + " " + op.get("module") + " at " + op.get("step")
				+ " (started by " + op.get("operator") + "). resume or cancel it first");
		return true;
	}

	// ---------------------------------------------------------------- the steps

	private int proceed() throws Exception {
		Map<String, String> op = op();
		String module = op.get("module");
		boolean force = "true".equals(op.get("force"));
		ModuleRegistry.Module m = ModuleRegistry.load().get(module);
		if (m == null) {
			System.out.println(module + " is not registered any more");
			clearOp();
			return 1;
		}
		String group = group(m.type);
		try {
			if (ADD.equals(op.get("action"))) {
				if (!A2pCtl.running(module)) {
					System.out.println("Deploy " + module + " and start it with the start script (-DNodeID=" + m.nodeId + "), then: resume");
					return 0;
				}
				if (group != null) {
					step(MOVING);
					moveAll(group, parseTarget(op.get("target")), force);
				}
				long version = ModuleRegistry.write(module, "state", ModuleRegistry.ACTIVE);
				reloadModules();
				waitVersion(version);
				clearOp();
				System.out.println(module + ": ACTIVE" + ("MultiSmppServer".equals(m.type) ? ". Add it to the load balancer" : ""));
				printConfig(group, parseTarget(op.get("target")), null);
				return 0;
			}
			if (MOVING.equals(op.get("step"))) {
				if (group != null) {
					moveAll(group, parseTarget(op.get("target")), force);
				}
				step(WAIT_DRAIN);
			}
			if (WAIT_DRAIN.equals(op().get("step"))) {
				if (A2pCtl.running(module)) {
					waitDrained(module);
				}
				ModuleRegistry.write(module, "state", ModuleRegistry.DRAINED);
				reloadModules();
				step(WAIT_STOP);
			}
			if (A2pCtl.running(module)) {
				System.out.println(module + ": DRAINED. Stop it with the stop script, then: resume");
				return 0;
			}
			long left = A2pCtl.drLeft(module);
			if (left > 0) {
				System.out.println(module + ": " + left + " DR(s) left in its streams, nobody reads them once it is removed: drain-dr " + module
						+ (force ? " (removed anyway, --force)" : ", then resume (or cancel)"));
				if (!force) {
					return 1;
				}
			}
			ModuleRegistry.write(module, "state", ModuleRegistry.REMOVED, "removedAt", String.valueOf(System.currentTimeMillis()));
			if (group != null) {
				redis.getStateRedis().delObject("system:" + keyGroup(group) + ":shards:" + module);
			}
			reloadModules();
			clearOp();
			System.out.println(module + ": REMOVED");
			printConfig(group, parseTarget(op.get("target")), module);
			return 0;
		} catch (Stopped e) {
			System.out.println("STOPPED: " + e.getMessage());
			System.out.println("Nothing was undone. Look at it (status), then: resume, or cancel");
			log.warn("[MODULE-OP] {} {} stopped: {}", op.get("action"), module, e.getMessage());
			return 1;
		}
	}

	private void moveAll(String group, Map<String, TreeSet<Integer>> target, boolean force) throws Exception {
		Map<String, ModuleRegistry.Module> modules = ModuleRegistry.load();
		String type = null;
		for (String name : target.keySet()) {
			type = modules.containsKey(name) ? modules.get(name).type : type;
		}
		// what each owns now, read again: a resumed operation skips what is done
		List<ShardPlan.Move> moves = ShardPlan.moves(currentShards(modules, type), target);
		for (ShardPlan.Move move : moves) {
			if (!A2pCtl.running(move.to)) {
				throw new Stopped(move.to + " is not running: it cannot take shards");
			}
		}
		int n = 0;
		for (ShardPlan.Move move : moves) {
			if (n++ > 0) {
				sleepSeconds(seconds("ModuleShardIntervalSeconds", 10));
			}
			moveOne(group, move, force);
		}
		System.out.println("Shards: " + ShardPlan.format(currentShards(ModuleRegistry.load(), type)));
	}

	/** 6.1: off the old owner and confirmed, then to the new one and confirmed. Never two owners. */
	private void moveOne(String group, ShardPlan.Move move, boolean force) throws Exception {
		int s = move.shard;
		long started = System.currentTimeMillis();
		renewMaintenance();
		if (move.from != null) {
			TreeSet<Integer> left = shardsOf(move.from, group);
			left.remove(s);
			setShards(group, move.from, left);
			if (A2pCtl.running(move.from)) {
				if (!waitPublished(group, move.from, s, false)) {
					if (!force) {
						throw new Stopped(move.from + " did not confirm giving up shard " + s + " within ModuleShardConfirmSeconds."
								+ " It is not given to " + move.to + ": no two owners. If " + move.from + " is gone for good: resume with --force");
					}
					System.out.println("  " + move.from + " did not confirm giving up shard " + s + ": taken as gone (--force)");
				}
			} else if (!force) {
				throw new Stopped(move.from + " is not running: shard " + s + " taken from it in Redis, not given to " + move.to
						+ ". If it is gone for good: resume with --force");
			}
		}
		TreeSet<Integer> more = shardsOf(move.to, group);
		more.add(s);
		setShards(group, move.to, more);
		if (!waitPublished(group, move.to, s, true)) {
			throw new Stopped(move.to + " did not confirm taking shard " + s + ": nobody reads it now. Look at " + move.to
					+ ", then resume (or give the shard to another module by hand)");
		}
		System.out.println("  shard " + s + ": " + (move.from == null ? "(nobody)" : move.from) + " -> " + move.to + " ("
				+ (System.currentTimeMillis() - started) + " ms)");
		log.info("[SHARD-MOVE] {} {} -> {}", s, move.from, move.to);
	}

	private void waitDrained(String module) throws Exception {
		step(WAIT_DRAIN);
		long last = 0L;
		long missingSince = 0L;
		while (true) {
			Map<String, String> p = A2pCtl.progress(module);
			if ("true".equals(p.get("done"))) {
				System.out.println("  drained: " + p);
				return;
			}
			if (p.isEmpty()) {
				missingSince = missingSince == 0L ? System.currentTimeMillis() : missingSince;
				if (!A2pCtl.running(module) || System.currentTimeMillis() - missingSince > 30000L) {
					throw new Stopped(module + " reports no drain progress (stopped or crashed?)");
				}
			} else {
				missingSince = 0L;
			}
			if (System.currentTimeMillis() - last >= 5000L) {
				System.out.println("  " + (p.isEmpty() ? "waiting for its drain progress" : p.toString()));
				last = System.currentTimeMillis();
			}
			renewMaintenance();
			Thread.sleep(1000L);
		}
	}

	/** every running module has the registry version (it reports it with its detail): warned only */
	private void waitVersion(long version) throws Exception {
		long end = System.currentTimeMillis() + seconds("ModuleShardConfirmSeconds", 30) * 1000L;
		List<String> behind;
		do {
			behind = new ArrayList<String>();
			for (String key : redis.getStateRedis().scanAll("module:detail:*")) {
				String detail = redis.getString(key);
				if (detail == null) {
					continue;
				}
				int i = detail.indexOf("registryVersion=");
				if (i < 0) {
					continue;
				}
				String v = detail.substring(i + "registryVersion=".length()).split(";")[0].trim();
				try {
					if (Long.parseLong(v) < version) {
						behind.add(key.substring("module:detail:".length()));
					}
				} catch (NumberFormatException e) {
					behind.add(key.substring("module:detail:".length()));
				}
			}
			if (behind.isEmpty()) {
				return;
			}
			Thread.sleep(500L);
		} while (System.currentTimeMillis() < end);
		System.out.println("  not on registry version " + version + " yet (they take it with their next heartbeat): " + behind);
	}

	// ---------------------------------------------------------------- shards

	/** the published group: system:shards:{group}:{module}:{node} */
	static String group(String type) {
		if ("CoreEngine".equals(type)) {
			return "core";
		}
		if ("MultiSmppClient".equals(type)) {
			return "client";
		}
		if ("CommonHttpClient".equals(type)) {
			return "httpclient";
		}
		return null;
	}

	/** where nodes read their shards: system:core:shards:{module}, system:client:shards:{module} */
	private static String keyGroup(String group) {
		return "core".equals(group) ? "core" : "client";
	}

	/** what the module owns: the value in Redis it reads, else what it publishes, else its configuration file */
	private TreeSet<Integer> shardsOf(String module, String group) {
		String value = redis.getString("system:" + keyGroup(group) + ":shards:" + module);
		if (value != null && value.trim().length() > 0) {
			return ShardPlan.parse(value);
		}
		String published = published(group, module);
		if (published != null) {
			return ShardPlan.parse(published);
		}
		String property = GmmsUtility.getInstance().getCommonProperty(("core".equals(group) ? "CoreShard." : "ClientShard.") + module);
		return ShardPlan.parse(property);
	}

	/** the shards a running node reports, null when none reports */
	private String published(String group, String module) {
		for (String key : redis.getStateRedis().scanAll("system:shards:" + group + ":" + module + ":*")) {
			String value = redis.getString(key);
			if (value != null && value.indexOf('|') >= 0) {
				return value.substring(value.indexOf('|') + 1);
			}
		}
		return null;
	}

	private Map<String, TreeSet<Integer>> currentShards(Map<String, ModuleRegistry.Module> modules, String type) {
		Map<String, TreeSet<Integer>> current = new TreeMap<String, TreeSet<Integer>>();
		String group = group(type);
		for (ModuleRegistry.Module m : modules.values()) {
			if (m.type != null && m.type.equals(type) && !ModuleRegistry.REMOVED.equals(m.state)) {
				current.put(m.name, shardsOf(m.name, group));
			}
		}
		return current;
	}

	/** who owns shards afterwards: ACTIVE and JOINING modules of the type, but the one that goes */
	private static List<String> members(Map<String, ModuleRegistry.Module> modules, String type, String leaving) {
		List<String> members = new ArrayList<String>();
		for (ModuleRegistry.Module m : new TreeMap<String, ModuleRegistry.Module>(modules).values()) {
			if (type.equals(m.type) && !m.name.equals(leaving)
					&& (ModuleRegistry.ACTIVE.equals(m.state) || ModuleRegistry.JOINING.equals(m.state))) {
				members.add(m.name);
			}
		}
		return members;
	}

	/** TotalShards the running nodes report; the configuration file of the tool when none runs */
	private int totalShards(String type, Map<String, ModuleRegistry.Module> modules) throws Stopped {
		Set<Integer> totals = new TreeSet<Integer>();
		for (String key : redis.getStateRedis().scanAll("system:shards:" + group(type) + ":*")) {
			String value = redis.getString(key);
			if (value != null && value.indexOf('|') > 0) {
				totals.add(Integer.parseInt(value.substring(0, value.indexOf('|')).trim()));
			}
		}
		if (totals.size() > 1) {
			throw new Stopped("TotalShards differs between nodes: " + totals);
		}
		return totals.isEmpty() ? GmmsUtility.getInstance().getTotalShards() : totals.iterator().next();
	}

	private static boolean hasOverlap(List<String> problems) {
		for (String p : problems) {
			if (p.contains("owned by [")) {
				return true;
			}
		}
		return false;
	}

	private void setShards(String group, String module, Set<Integer> shards) {
		redis.setString("system:" + keyGroup(group) + ":shards:" + module, ShardPlan.format(shards));
		redis.publishState(ModuleStatusReporter.CONTROL_CHANNEL,
				("core".equals(group) ? "RELOAD_CORE_SHARDS@" : "RELOAD_CLIENT_SHARDS@") + module);
	}

	private boolean waitPublished(String group, String module, int shard, boolean owned) throws Exception {
		long end = System.currentTimeMillis() + seconds("ModuleShardConfirmSeconds", 30) * 1000L;
		while (System.currentTimeMillis() < end) {
			String published = published(group, module);
			if (published != null && ShardPlan.parse(published).contains(shard) == owned) {
				return true;
			}
			Thread.sleep(200L);
		}
		return false;
	}

	private static boolean printPlan(Map<String, TreeSet<Integer>> current, Map<String, TreeSet<Integer>> target, int total) {
		List<String> problems = ShardPlan.problems(target, total);
		if (!problems.isEmpty()) {
			System.out.println("Not a valid assignment: " + problems);
			return false;
		}
		List<ShardPlan.Move> moves = ShardPlan.moves(current, target);
		System.out.println("Shards (TotalShards=" + total + "):");
		System.out.println("  now   " + ShardPlan.format(current));
		System.out.println("  after " + ShardPlan.format(target));
		System.out.println("  " + moves.size() + " to move, one at a time: " + moves);
		return true;
	}

	/** 6.1: the files are read only when Redis has no value; keep them the same by hand */
	private static void printConfig(String group, Map<String, TreeSet<Integer>> target, String removed) {
		if (group == null || target == null) {
			return;
		}
		String prefix = "core".equals(group) ? "CoreShard." : "ClientShard.";
		System.out.println("Make Gmms/GmmsConfig.properties the same on every machine (read when Redis has no value):");
		for (Map.Entry<String, TreeSet<Integer>> e : target.entrySet()) {
			System.out.println("  " + prefix + e.getKey() + "=" + ShardPlan.format(e.getValue()));
		}
		if (removed != null) {
			System.out.println("  delete " + prefix + removed);
		}
	}

	// ---------------------------------------------------------------- capacity (10)

	/** connections to vendors and outgoing TPS before and after; true when connections would exceed SessionNumber */
	private static boolean capacity(Map<String, ModuleRegistry.Module> modules, String type, int change) {
		if (!"MultiSmppClient".equals(type) && !"CommonHttpClient".equals(type)) {
			return false;
		}
		int before = 0;
		for (ModuleRegistry.Module m : modules.values()) {
			before += type.equals(m.type) && ModuleRegistry.ACTIVE.equals(m.state) ? 1 : 0;
		}
		int after = before + change;
		if (before <= 0 || after <= 0) {
			return false;
		}
		boolean over = false;
		List<String> lines = new ArrayList<String>();
		com.king.gmms.domain.A2PCustomerManager customers = GmmsUtility.getInstance().getCustomerManager();
		if (customers == null) {
			return false;
		}
		for (A2PCustomerInfo c : customers.getAllCustomerInfos()) {
			if (c == null || !connects(c)) {
				continue;
			}
			String name = c.getShortName() + "(" + c.getSSID() + ")";
			if ("MultiSmppClient".equals(type) && "SMPP".equalsIgnoreCase(c.getProtocol()) && c instanceof SingleNodeCustomerInfo) {
				for (ConnectionInfo ci : ((SingleNodeCustomerInfo) c).getConnectionMap(false).values()) {
					int limit = ci.getSessionNum();
					if (limit <= 0) {
						continue;
					}
					int perOld = ceil(limit, before);
					int now = perOld * before;
					// the others keep what they opened at their start until they are restarted
					int next = change > 0 ? now + ceil(limit, after) : perOld * after;
					int restarted = ceil(limit, after) * after;
					String warn = next > limit ? "  MORE THAN SessionNumber" : next < Math.min(now, limit) ? "  fewer" : "";
					over |= next > limit && next > now;
					lines.add(String.format("  %s %s: SessionNumber=%d, connections %d -> %d (after restarting the others: %d)%s",
							name, ci.getConnectionName(), limit, now, next, restarted, warn));
				}
			}
			int tps = c.getOutgoingThrottlingNum();
			if (tps > 0) {
				lines.add(String.format("  %s: outgoing TPS (per Client) %d, total %d -> %d", name, tps, tps * before, tps * after));
			}
		}
		System.out.println("Capacity, " + type + " " + before + " -> " + after + (lines.isEmpty() ? ": no channel with a limit" : ":"));
		for (String line : lines) {
			System.out.println(line);
		}
		return over;
	}

	private static boolean connects(A2PCustomerInfo c) {
		if (c instanceof A2PMultiConnectionInfo) {
			return ((A2PMultiConnectionInfo) c).isInit();
		}
		return c instanceof A2PSingleConnectionInfo && ((A2PSingleConnectionInfo) c).isChlInit();
	}

	private static int ceil(int a, int b) {
		return (a + b - 1) / b;
	}

	// ---------------------------------------------------------------- lock and progress (12.3)

	private boolean lock() {
		int ttl = (int) seconds("ModuleOpLockSeconds", 60);
		String id = UUID.randomUUID().toString();
		String holder;
		try (Jedis j = redis.getStateRedis().getJedis()) {
			if ("OK".equals(j.set(LOCK_KEY, id + "|" + operator(), SetParams.setParams().nx().ex(ttl)))) {
				lockId = id;
			}
			holder = j.get(LOCK_KEY);
		}
		if (lockId == null) {
			System.out.println("Another a2pctl is at work (" + holder + "): try again when it is done"
					+ " (the lock ends ModuleOpLockSeconds after it stopped)");
			return false;
		}
		final long renewMs = Math.max(1000L, ttl * 1000L / 6);
		final int lockTtl = ttl;
		renewer = new Thread("a2pctl-lock") {
			public void run() {
				while (!isInterrupted()) {
					try {
						Thread.sleep(renewMs);
						renew(lockTtl);
					} catch (InterruptedException e) {
						return;
					} catch (Exception e) {
						log.warn("[MODULE-OP] lock not renewed", e);
					}
				}
			}
		};
		renewer.setDaemon(true);
		renewer.start();
		renewMaintenance();
		return true;
	}

	private void renew(int ttl) {
		RedisConnection state = redis.getStateRedis();
		state.evalScript("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) end return 0",
				Arrays.asList(LOCK_KEY), Arrays.asList(lockId + "|" + operator(), String.valueOf(ttl)));
	}

	private void unlock() {
		if (renewer != null) {
			renewer.interrupt();
			renewer = null;
		}
		if (lockId != null) {
			redis.getStateRedis().evalScript("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
					Arrays.asList(LOCK_KEY), Arrays.asList(lockId + "|" + operator()));
			lockId = null;
		}
		redis.getStateRedis().delObject(MAINTENANCE_KEY);
	}

	/** ShardAssignmentChecker logs a shard nobody owns as INFO while this is there */
	private void renewMaintenance() {
		Map<String, String> op = op();
		redis.setString(MAINTENANCE_KEY, op.isEmpty() ? "a2pctl" : op.get("action") + " " + op.get("module"), MAINTENANCE_SECONDS);
	}

	private void saveOp(String action, String module, String step, Map<String, TreeSet<Integer>> target, boolean force, String extra) {
		Map<String, String> fields = new HashMap<String, String>();
		fields.put("id", UUID.randomUUID().toString());
		fields.put("action", action);
		fields.put("module", module);
		fields.put("step", step);
		fields.put("target", target == null ? "" : formatTarget(target));
		fields.put("force", String.valueOf(force));
		fields.put("operator", operator());
		fields.put("startedAt", String.valueOf(System.currentTimeMillis()));
		fields.put("updatedAt", String.valueOf(System.currentTimeMillis()));
		try (Jedis j = redis.getStateRedis().getJedis()) {
			j.del(OP_KEY);
			j.hset(OP_KEY, fields);
		}
	}

	private void step(String step) {
		try (Jedis j = redis.getStateRedis().getJedis()) {
			j.hset(OP_KEY, "step", step);
			j.hset(OP_KEY, "updatedAt", String.valueOf(System.currentTimeMillis()));
		}
	}

	private void clearOp() {
		redis.getStateRedis().delObject(OP_KEY);
	}

	private static String formatTarget(Map<String, TreeSet<Integer>> target) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, TreeSet<Integer>> e : target.entrySet()) {
			sb.append(sb.length() == 0 ? "" : ";").append(e.getKey()).append('=').append(ShardPlan.format(e.getValue()));
		}
		return sb.toString();
	}

	private static Map<String, TreeSet<Integer>> parseTarget(String text) {
		Map<String, TreeSet<Integer>> target = new TreeMap<String, TreeSet<Integer>>();
		if (text == null || text.length() == 0) {
			return target;
		}
		for (String part : text.split(";")) {
			int eq = part.indexOf('=');
			target.put(part.substring(0, eq), ShardPlan.parse(part.substring(eq + 1)));
		}
		return target;
	}

	// ---------------------------------------------------------------- small things

	/** the number at the end of the name, as the module checks it when it starts (ModuleRegistry.conflict) */
	private static String numberClash(Map<String, ModuleRegistry.Module> modules, String module, String type) {
		int number = ModuleManager.trailingNumber(module);
		if (number < 1 || number > 9) {
			return module + ": the number at the end of the name must be 1 to 9";
		}
		long retainMs = seconds("ModuleIndexRetainHours", 24) * 3600000L;
		for (ModuleRegistry.Module m : modules.values()) {
			boolean retained = !ModuleRegistry.REMOVED.equals(m.state) || System.currentTimeMillis() - m.removedAt < retainMs;
			if (!m.name.equals(module) && type.equals(m.type) && retained && ModuleManager.trailingNumber(m.name) == number) {
				return module + ": number " + number + " is the number of " + m.name + " (" + m.state + ")";
			}
		}
		return null;
	}

	private static final List<String> TYPES = Arrays.asList("CoreEngine", "MultiSmppClient", "MultiSmppServer", "CommonHttpClient", "CommonHttpServer");

	/** a new module may not be in a2p_modules.conf yet: its name without the number, as a type the cluster knows */
	private static String typeOf(String module, Map<String, ModuleRegistry.Module> modules) {
		String type = ModuleManager.getInstance().getFullModuleType(module);
		if (type != null) {
			return type;
		}
		String base = module.replaceAll("[0-9]+$", "");
		for (ModuleRegistry.Module m : modules.values()) {
			if (base.equals(m.type)) {
				return base;
			}
		}
		return TYPES.contains(base) ? base : null;
	}

	/** the role column of a2p_modules.conf, by type */
	private static String role(String type, String module) {
		for (String[] line : ModuleRegistry.readModuleList()) {
			if (line[0].equals(module)) {
				return line[1];
			}
		}
		if ("CoreEngine".equals(type)) {
			return "core";
		}
		if ("MultiSmppClient".equals(type)) {
			return "smpp-client";
		}
		if ("CommonHttpClient".equals(type)) {
			return "http-client";
		}
		if ("MultiSmppServer".equals(type)) {
			return "smpp-server";
		}
		return type;
	}

	private void reloadModules() {
		redis.publishState(ModuleStatusReporter.CONTROL_CHANNEL, "RELOAD_MODULES");
	}

	private static String operator() {
		String host = "";
		try {
			host = java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			host = "?";
		}
		return System.getProperty("user.name") + "@" + host;
	}

	static long seconds(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private static void sleepSeconds(long seconds) throws InterruptedException {
		if (seconds > 0) {
			Thread.sleep(seconds * 1000L);
		}
	}
}
