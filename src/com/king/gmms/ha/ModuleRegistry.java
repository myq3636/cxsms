package com.king.gmms.ha;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

/**
 * Which modules the cluster has and in what state (ModuleRegistryEnable, see docs/模块动态扩缩容设计.md section 4).
 * Addresses and ports stay in the configuration files; this holds membership only. A module started by the start
 * script registers itself; only a real conflict stops it.
 */
public final class ModuleRegistry {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ModuleRegistry.class);
	private static final String VERSION_KEY = "system:modules:version";
	private static final String MODULES_KEY = "system:modules";
	private static final String MODULE_PREFIX = "system:module:";
	private static final int MAX_MODULE_NO = 9;
	private static final long DEFAULT_RETAIN_HOURS = 24L;

	public static final String JOINING = "JOINING";
	public static final String ACTIVE = "ACTIVE";
	public static final String DRAINING = "DRAINING";
	public static final String DRAINED = "DRAINED";
	public static final String REMOVED = "REMOVED";

	private static final String WRITE_LUA =
			"redis.call('HSET', KEYS[1], unpack(ARGV, 3)) "
			+ "redis.call('SADD', KEYS[2], ARGV[1]) "
			+ "local old = tonumber(redis.call('GET', KEYS[3]) or '0') "
			+ "local now = tonumber(ARGV[2]) "
			+ "if now <= old then now = old + 1 end "
			+ "local text = string.format('%.0f', now) "
			+ "redis.call('SET', KEYS[3], text) "
			+ "return text";

	public static final class Module {
		public final String name;
		public final String type;
		public final String role;
		public final String nodeId;
		public final String state;
		public final long removedAt;

		public Module(String name, String type, String role, String nodeId, String state, long removedAt) {
			this.name = name;
			this.type = type;
			this.role = role;
			this.nodeId = nodeId;
			this.state = state;
			this.removedAt = removedAt;
		}
	}

	private static volatile long version = -1L;
	private static volatile Map<String, Module> current = new HashMap<String, Module>();

	private ModuleRegistry() {
	}

	public static boolean enabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("ModuleRegistryEnable", "false"));
	}

	private static RedisConnection redis() {
		return RedisClient.getInstance().getStateRedis();
	}

	public static long version() {
		return version;
	}

	public static String stateOf(String module) {
		Module m = current.get(module);
		return m == null ? null : m.state;
	}

	/** When the Redis client is up, before the message id format is set up and before any traffic. */
	public static synchronized void startup(GmmsUtility gmmsUtility) {
		String module = System.getProperty("module");
		if (module == null || !enabled()) {
			return;
		}
		Map<String, Module> modules;
		Long stored;
		try {
			stored = readVersion();
			if (stored == null) {
				log.warn("[MODULE-REG] the registry is empty, using the module list. Run a2pctl init");
				return;
			}
			modules = load();
		} catch (Exception e) {
			// the module list is enough to run: no traffic is lost for the registry
			log.warn("[MODULE-REG] registry not readable, using the module list", e);
			return;
		}
		String conflict = conflict(modules, module, gmmsUtility.getNodeId());
		if (conflict != null) {
			String text = "[MODULE-REG] " + module + " must not start: " + conflict;
			log.fatal(text);
			System.out.println(text);
			System.exit(-1);
		}
		register(modules.get(module), module, gmmsUtility.getNodeId());
		warnWithoutShards(gmmsUtility, module);
		apply();
	}

	/** RELOAD_MODULES, and the check with each heartbeat for a command that got lost. */
	public static synchronized void reload() {
		if (!enabled() || System.getProperty("module") == null) {
			return;
		}
		try {
			Long stored = readVersion();
			if (stored == null) {
				if (version > 0) {
					log.warn("[MODULE-REG] the registry is gone from Redis, keeping version {}. Run a2pctl init --from-file", version);
				}
				return;
			}
			if (stored < version) {
				log.warn("[MODULE-REG] registry version {} is older than {}, not taken", stored, version);
				return;
			}
			if (stored > version) {
				apply();
				ModuleStatusReporter.publishDetailNow();
			}
		} catch (Exception e) {
			log.warn("[MODULE-REG] registry not readable, keeping version " + version, e);
		}
	}

	private static void apply() {
		Long stored = readVersion();
		Map<String, Module> modules = load();
		String self = System.getProperty("module");
		List<String[]> visible = new ArrayList<String[]>();
		List<String> active = new ArrayList<String>();
		for (Module m : modules.values()) {
			if (ACTIVE.equals(m.state)) {
				active.add(m.name);
			}
			// a module always sees itself: "am I a Core" must not change while it drains
			if (ACTIVE.equals(m.state) || m.name.equals(self)) {
				visible.add(new String[] { m.name, m.role });
			}
		}
		ModuleManager.getInstance().applyRegistry(visible);
		current = modules;
		version = stored == null ? version : stored;
		log.info("[MODULE-REG] version {} in use: {} modules, active {}", version, modules.size(), active);
		Module me = modules.get(self);
		DrainController.onState(me == null ? null : me.state);
	}

	private static String conflict(Map<String, Module> modules, String module, String nodeId) {
		int number = ModuleManager.trailingNumber(module);
		if (number > MAX_MODULE_NO || number == 0) {
			return "its number " + number + " is not between 1 and " + MAX_MODULE_NO;
		}
		Module self = modules.get(module);
		String type = self != null ? self.type : ModuleManager.getInstance().getFullModuleType(module);
		if (number > 0 && type != null) {
			long retainMs = retainHours() * 3600000L;
			long now = System.currentTimeMillis();
			for (Module m : modules.values()) {
				boolean retained = !REMOVED.equals(m.state) || now - m.removedAt < retainMs;
				if (!m.name.equals(module) && type.equals(m.type) && retained
						&& ModuleManager.trailingNumber(m.name) == number) {
					return "its number " + number + " is the number of " + m.name + " (" + m.state + ")";
				}
			}
		}
		for (String key : redis().scanAll("module:status:*:" + module + ":*")) {
			String node = key.substring(key.lastIndexOf(':') + 1);
			if (!node.equals(nodeId) && redis().getString(key) != null) {
				return "it is running on node " + node + ", this is node " + nodeId
						+ ". After kill -9 its heartbeat ends within ModuleStatusTTLSeconds";
			}
		}
		return null;
	}

	/** A new module registers itself; a removed one started again is back. A module being drained stays so. */
	private static void register(Module self, String module, String nodeId) {
		if (self == null) {
			ModuleManager manager = ModuleManager.getInstance();
			String type = manager.getFullModuleType(module);
			if (type == null) {
				log.warn("[MODULE-REG] {} is neither registered nor in the module list, not registered", module);
				return;
			}
			write(module, "type", type, "role", roleInList(module, type), "nodeId", nodeId, "state", ACTIVE, "removedAt", "0");
			log.info("[MODULE-REG] {} registered itself as ACTIVE, node {}", module, nodeId);
			return;
		}
		if (REMOVED.equals(self.state)) {
			write(module, "nodeId", nodeId, "state", ACTIVE, "removedAt", "0");
			log.warn("[MODULE-REG] {} was removed and is started again: ACTIVE", module);
		} else if (!nodeId.equals(self.nodeId)) {
			write(module, "nodeId", nodeId);
		}
	}

	private static String roleInList(String module, String type) {
		for (String[] line : readModuleList()) {
			if (line[0].equals(module)) {
				return line[1];
			}
		}
		return type;
	}

	/** Without a shard setting it falls back to MyShards and overlaps with the node that owns them. */
	private static void warnWithoutShards(GmmsUtility gmmsUtility, String module) {
		ModuleManager manager = ModuleManager.getInstance();
		String group = manager.isCoreEngine(module) ? "core" : manager.isClientType(module) ? "client" : null;
		if (group == null) {
			return;
		}
		String property = ("core".equals(group) ? "CoreShard." : "ClientShard.") + module;
		if (gmmsUtility.getCommonProperty(property) == null
				&& redis().getString("system:" + group + ":shards:" + module) == null) {
			log.warn("[MODULE-REG] {} has no {} and no value in Redis: it falls back to MyShards", module, property);
		}
	}

	private static long retainHours() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ModuleIndexRetainHours",
					String.valueOf(DEFAULT_RETAIN_HOURS)).trim());
		} catch (Exception e) {
			return DEFAULT_RETAIN_HOURS;
		}
	}

	private static Long readVersion() {
		String text = redis().getString(VERSION_KEY);
		return text == null ? null : Long.valueOf(text.trim());
	}

	public static Map<String, Module> load() {
		Map<String, Module> modules = new HashMap<String, Module>();
		Set<String> names = redis().smembers(MODULES_KEY);
		if (names == null) {
			return modules;
		}
		for (String name : names) {
			Map<String, String> f = redis().hgetAll(MODULE_PREFIX + name);
			if (f == null || f.isEmpty()) {
				continue;
			}
			long removedAt = 0L;
			try {
				removedAt = Long.parseLong(f.get("removedAt"));
			} catch (Exception e) {
				log.debug("no removedAt for {}", name);
			}
			modules.put(name, new Module(name, f.get("type"), f.get("role"), f.get("nodeId"), f.get("state"), removedAt));
		}
		return modules;
	}

	/** Fields of one module and the version, in one step: nobody reads half a change. */
	public static long write(String module, String... fields) {
		List<String> args = new ArrayList<String>(Arrays.asList(module, String.valueOf(System.currentTimeMillis())));
		args.addAll(Arrays.asList(fields));
		args.add("updatedAt");
		args.add(String.valueOf(System.currentTimeMillis()));
		Object text = redis().evalScript(WRITE_LUA, Arrays.asList(MODULE_PREFIX + module, MODULES_KEY, VERSION_KEY), args);
		return Long.parseLong(String.valueOf(text));
	}

	public static boolean initialized() {
		return redis().getString(VERSION_KEY) != null;
	}

	/** a2p_modules.conf: lines switched on, as {module, role, nodeId}. */
	public static List<String[]> readModuleList() {
		List<String[]> lines = new ArrayList<String[]>();
		File file = new File(System.getProperty("a2p_home", "/usr/local/a2p/") + "/conf/a2p_modules.conf");
		if (!file.exists()) {
			return lines;
		}
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String trim = line.trim();
				if (trim.length() == 0 || trim.startsWith("#")) {
					continue;
				}
				String[] parts = trim.split("\\|", -1);
				if (parts.length < 4 || !"1".equals(parts[0].trim()) || parts[1].trim().length() == 0) {
					continue;
				}
				lines.add(new String[] { parts[1].trim(), parts[2].trim(), parts.length > 4 ? parts[4].trim() : "" });
			}
		} catch (Exception e) {
			log.error("[MODULE-REG] cannot read " + file, e);
		}
		return lines;
	}
}
