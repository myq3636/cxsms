package com.king.gmms;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerConfig;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

/**
 * Writes the content of a configuration file as this module runs it to the state Redis, for the
 * portal: config:file:{name}:{version} (one copy per content version, shared by all modules) and
 * config:files:{module}:{node} (which version each module uses). See 《配置文件生效内容写入Redis设计》.
 * Writing runs on one background thread; per file only the latest pending content is kept and a
 * write that fails (Redis not up yet) is retried every 5 seconds.
 */
public final class ConfigFilePublisher {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ConfigFilePublisher.class);
	private static final ConfigFilePublisher INSTANCE = new ConfigFilePublisher();

	static final String FILE_PREFIX = "config:file:";
	static final String MODULE_PREFIX = "config:files:";
	static final String MODULES = "config:effective:modules";
	private static final int TTL_SECONDS = 7 * 24 * 3600;
	private static final int BATCH = 500;
	private static final long RETRY_MS = 5000L;
	private static final Pattern INLINE_PASSWORD = Pattern.compile("(?i)\\b(password|passwd|pwd)\\s*=\\s*[^,;)\\s]*");

	private static final String HSET_LUA =
			"for i = 1, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end "
			+ "redis.call('EXPIRE', KEYS[1], " + TTL_SECONDS + ") return 1";
	/** KEYS: meta, content, extra index (may be absent); refresh the TTL of an existing version */
	private static final String TOUCH_LUA =
			"if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end "
			+ "for i = 1, #KEYS do redis.call('EXPIRE', KEYS[i], " + TTL_SECONDS + ") end return 1";

	public enum Format { BLOCKS, PROPERTIES, LINES }

	/** one file as this module runs it */
	private static final class Content {
		final String name;
		final String file;
		final Format format;
		final TreeMap<String, String> entries;
		final Map<String, String> ssidIndex;
		final Map<String, String> channelIndex;
		final Map<String, String> channelRoutes;
		final Map<String, String> moduleFields;
		final String result;

		Content(String name, String file, Format format, TreeMap<String, String> entries,
				Map<String, String> ssidIndex, Map<String, String> channelIndex, Map<String, String> channelRoutes,
				Map<String, String> moduleFields, String result) {
			this.name = name;
			this.file = file;
			this.format = format;
			this.entries = entries;
			this.ssidIndex = ssidIndex;
			this.channelIndex = channelIndex;
			this.channelRoutes = channelRoutes;
			this.moduleFields = moduleFields;
			this.result = result;
		}
	}

	private final ConcurrentHashMap<String, Content> pending = new ConcurrentHashMap<String, Content>();
	/** name -> version this module runs, for the hourly TTL refresh */
	private final ConcurrentHashMap<String, String> inUse = new ConcurrentHashMap<String, String>();
	private final ScheduledExecutorService executor;
	private final java.util.concurrent.ConcurrentHashMap<String, Runnable> latestPublish = new java.util.concurrent.ConcurrentHashMap<String, Runnable>();

	private ConfigFilePublisher() {
		executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "ConfigFilePublisher");
				t.setDaemon(true);
				return t;
			}
		});
		executor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				refreshInUse();
			}
		}, 1, 1, TimeUnit.HOURS);
	}

	public static ConfigFilePublisher getInstance() {
		return INSTANCE;
	}

	// ---- what the loaders call ----

	/** blocks of a cm.cfg-style file (block name -> its key/values) */
	public void publishBlocks(String name, String file, Map<String, A2PCustomerConfig> blocks, String result,
			Map<String, String> ssidIndex, Map<String, String> moduleFields) {
		publishBlocks(name, file, blocks, result, ssidIndex, moduleFields, null, null);
	}

	/** channelIndex, channelRoutes: written to config:file:{name}:{version}:channel and :channelRoutes */
	public void publishBlocks(String name, String file, Map<String, A2PCustomerConfig> blocks, String result,
			Map<String, String> ssidIndex, Map<String, String> moduleFields, Map<String, String> channelIndex,
			Map<String, String> channelRoutes) {
		if (blocks == null) {
			return;
		}
		TreeMap<String, String> entries = new TreeMap<String, String>();
		for (Map.Entry<String, A2PCustomerConfig> e : blocks.entrySet()) {
			A2PCustomerConfig block = e.getValue();
			Map<String, String> lines = block == null ? null : block.getLinesWithOriginalNames();
			entries.put(e.getKey(), JSON.toJSONString(mask(lines)));
		}
		submit(new Content(name, file, Format.BLOCKS, entries, ssidIndex, channelIndex, channelRoutes, moduleFields, result));
	}

	/** an entry per map key, its value written as JSON */
	public void publishMap(String name, String file, Map<String, ?> map) {
		if (map == null) {
			return;
		}
		TreeMap<String, String> entries = new TreeMap<String, String>();
		for (Map.Entry<String, ?> e : map.entrySet()) {
			Object v = e.getValue();
			entries.put(String.valueOf(e.getKey()), v instanceof String ? maskValue(e.getKey(), (String) v) : JSON.toJSONString(v));
		}
		submit(new Content(name, file, Format.BLOCKS, entries, null, null, null, null, "ok"));
	}

	public void publishProperties(String name, String file, Properties props) {
		if (props == null) {
			return;
		}
		TreeMap<String, String> entries = new TreeMap<String, String>();
		for (String key : props.stringPropertyNames()) {
			String v = props.getProperty(key);
			entries.put(key, maskValue(key, v == null ? "" : v.trim()));
		}
		submit(new Content(name, file, Format.PROPERTIES, entries, null, null, null, null, "ok"));
	}

	/** the text of a file just loaded, one entry per line (000001, 000002, ...) */
	public void publishFileLines(String name, File file) {
		if (file == null || !file.isFile()) {
			return;
		}
		try {
			List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
			TreeMap<String, String> entries = new TreeMap<String, String>();
			for (int i = 0; i < lines.size(); i++) {
				entries.put(String.format("%06d", i + 1), maskInline(lines.get(i)));
			}
			submit(new Content(name, file.getPath(), Format.LINES, entries, null, null, null, null, "ok"));
		} catch (Exception e) {
			log.warn("Config file not published, cannot read it. name={}, file={}", name, file, e);
		}
	}

	/** a reload failed: the module keeps the version it runs, only the result is recorded */
	public void publishFailure(String name) {
		publishFailure(name, "");
	}

	public void publishFailure(String name, String reason) {
		record(name, "failed", reason);
	}

	/** a reload found nothing to do: the file is what the module runs */
	/**
	 * Runs a costly publish on the publisher's thread, not the reload thread; when several are waiting
	 * for the same file only the newest runs.
	 */
	public void publishLater(final String name, final Runnable publish) {
		if (!enabled()) {
			return;
		}
		latestPublish.put(name, publish);
		executor.execute(new Runnable() {
			public void run() {
				if (latestPublish.remove(name, publish)) {
					publish.run();
				}
			}
		});
	}

	public void publishResultOk(String name) {
		record(name, "ok", "");
	}

	private void record(final String name, final String result, final String reason) {
		if (!enabled()) {
			return;
		}
		executor.execute(new Runnable() {
			public void run() {
				try {
					RedisConnection redis = redis();
					if (redis == null) {
						return;
					}
					redis.evalScript(HSET_NO_TTL_LUA, Arrays.asList(moduleKey()),
							Arrays.asList(name + ".result", result, name + ".failReason", reason, name + ".failedAt",
									"failed".equals(result) ? now() : ""));
				} catch (Exception e) {
					log.warn("Failed to record a config reload failure. name={}", name, e);
				}
			}
		});
	}

	private static final String HSET_NO_TTL_LUA =
			"for i = 1, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end return 1";

	// ---- writing ----

	private void submit(Content c) {
		if (!enabled()) {
			return;
		}
		boolean first = pending.put(c.name, c) == null;
		if (first) {
			schedule(c.name, 0L);
		}
	}

	private void schedule(final String name, long delayMs) {
		executor.schedule(new Runnable() {
			public void run() {
				Content c = pending.get(name);
				if (c == null) {
					return;
				}
				try {
					if (write(c)) {
						pending.remove(name, c);
						if (pending.containsKey(name)) {       // newer content arrived meanwhile
							schedule(name, 0L);
						}
						return;
					}
				} catch (Exception e) {
					log.warn("Config file not published yet, retry in {} ms. name={}", RETRY_MS, name, e);
				}
				schedule(name, RETRY_MS);
			}
		}, delayMs, TimeUnit.MILLISECONDS);
	}

	/** true when written (or nothing to do), false to retry later */
	private boolean write(Content c) throws Exception {
		if (!enabled()) {
			return true;
		}
		RedisConnection redis = redis();
		if (redis == null) {
			return false;
		}
		String version = version(c.entries);
		String base = FILE_PREFIX + c.name + ":" + version;
		String meta = base + ":meta";
		List<String> touchKeys = new ArrayList<String>(Arrays.asList(meta, base));
		if (c.ssidIndex != null) {
			touchKeys.add(base + ":ssid");
		}
		if (c.channelIndex != null) {
			touchKeys.add(base + ":channel");
			touchKeys.add(base + ":channelRoutes");
		}
		Object exists = redis.evalScript(TOUCH_LUA, touchKeys, Collections.<String>emptyList());
		if (!(exists instanceof Long) || (Long) exists == 0L) {
			writeHash(redis, base, c.entries);
			if (c.ssidIndex != null) {
				writeHash(redis, base + ":ssid", c.ssidIndex);
			}
			writeChannelIndex(redis, base, c);
			writeHash(redis, meta, metaFields(c, version));
			log.info("Config file content written to Redis. key={}, entries={}", base, c.entries.size());
		} else if (c.channelIndex != null && !c.channelIndex.isEmpty() && Long.valueOf(0L).equals(
				redis.evalScript("return redis.call('EXISTS', KEYS[1])", Arrays.asList(base + ":channel"), Collections.<String>emptyList()))) {
			// this version was written before the channel index existed
			writeChannelIndex(redis, base, c);
		}
		String previous = inUse.put(c.name, version);
		List<String> fields = new ArrayList<String>(Arrays.asList(
				c.name + ".version", version,
				c.name + ".loadedAt", now(),
				c.name + ".result", c.result,
				c.name + ".failReason", "",
				c.name + ".file", c.file == null ? "" : c.file,
				c.name + ".entries", String.valueOf(c.entries.size())));
		if (c.moduleFields != null) {
			for (Map.Entry<String, String> e : c.moduleFields.entrySet()) {
				fields.add(e.getKey());
				fields.add(e.getValue() == null ? "" : e.getValue());
			}
		}
		redis.evalScript(HSET_NO_TTL_LUA, Arrays.asList(moduleKey()), fields);
		redis.evalScript("redis.call('SADD', KEYS[1], ARGV[1]) return 1", Arrays.asList(MODULES),
				Arrays.asList(moduleId()));
		if (previous != null && !previous.equals(version)) {
			deleteIfUnused(redis, c.name, previous);
		}
		return true;
	}

	private void writeChannelIndex(RedisConnection redis, String base, Content c) {
		if (c.channelIndex != null) {
			writeHash(redis, base + ":channel", c.channelIndex);
			writeHash(redis, base + ":channelRoutes", c.channelRoutes);
		}
	}

	private void writeHash(RedisConnection redis, String key, Map<String, String> map) {
		List<String> args = new ArrayList<String>();
		for (Map.Entry<String, String> e : map.entrySet()) {
			args.add(e.getKey());
			args.add(e.getValue() == null ? "" : e.getValue());
			if (args.size() >= BATCH * 2) {
				redis.evalScript(HSET_LUA, Arrays.asList(key), args);
				args = new ArrayList<String>();
			}
		}
		if (!args.isEmpty() || map.isEmpty()) {
			redis.evalScript(HSET_LUA, Arrays.asList(key), args);
		}
	}

	private Map<String, String> metaFields(Content c, String version) {
		Map<String, String> m = new TreeMap<String, String>();
		m.put("name", c.name);
		m.put("version", version);
		m.put("file", c.file == null ? "" : c.file);
		m.put("format", c.format.name().toLowerCase());
		m.put("entries", String.valueOf(c.entries.size()));
		m.put("writtenAt", now());
		m.put("writer", moduleId());
		return m;
	}

	/** the module moved from an old version: delete it when no module uses it any more */
	private void deleteIfUnused(RedisConnection redis, String name, String old) {
		Set<String> modules = redis.smembers(MODULES);
		if (modules != null) {
			for (String m : modules) {
				Object v = redis.evalScript("return redis.call('HGET', KEYS[1], ARGV[1])",
						Arrays.asList(MODULE_PREFIX + m), Arrays.asList(name + ".version"));
				if (old.equals(v)) {
					return;
				}
			}
		}
		String base = FILE_PREFIX + name + ":" + old;
		redis.evalScript("for i = 1, #KEYS do redis.call('DEL', KEYS[i]) end return 1",
				Arrays.asList(base + ":meta", base, base + ":ssid", base + ":channel", base + ":channelRoutes"), Collections.<String>emptyList());
		log.info("Config file version no longer used by any module, deleted. key={}", base);
	}

	private void refreshInUse() {
		if (!enabled()) {
			return;
		}
		try {
			RedisConnection redis = redis();
			if (redis == null) {
				return;
			}
			for (Map.Entry<String, String> e : inUse.entrySet()) {
				String base = FILE_PREFIX + e.getKey() + ":" + e.getValue();
				redis.evalScript(TOUCH_LUA, Arrays.asList(base + ":meta", base, base + ":ssid", base + ":channel", base + ":channelRoutes"),
						Collections.<String>emptyList());
			}
		} catch (Exception e) {
			log.warn("Failed to refresh the TTL of the config files in use", e);
		}
	}

	// ---- helpers ----

	static String version(TreeMap<String, String> entries) throws Exception {
		MessageDigest md = MessageDigest.getInstance("MD5");
		for (Map.Entry<String, String> e : entries.entrySet()) {
			md.update(e.getKey().getBytes("UTF-8"));
			md.update((byte) 0);
			md.update((e.getValue() == null ? "" : e.getValue()).getBytes("UTF-8"));
			md.update((byte) 1);
		}
		return String.format("%032x", new BigInteger(1, md.digest()));
	}

	static TreeMap<String, String> mask(Map<String, String> lines) {
		TreeMap<String, String> out = new TreeMap<String, String>();
		if (lines != null) {
			for (Map.Entry<String, String> e : lines.entrySet()) {
				out.put(e.getKey(), maskValue(e.getKey(), e.getValue()));
			}
		}
		return out;
	}

	static String maskValue(String key, String value) {
		if (value == null) {
			return null;
		}
		String k = key == null ? "" : key.toLowerCase();
		if (k.contains("password") || k.contains("passwd") || k.contains("pwd") || k.contains("authkey")
				|| k.contains("secret")) {
			return value.length() == 0 ? value : "******";
		}
		return maskInline(value);
	}

	/** connection definitions carry password=... inside the value */
	static String maskInline(String value) {
		return value == null ? null : INLINE_PASSWORD.matcher(value).replaceAll("$1=******");
	}

	private static boolean enabled() {
		try {
			return !"false".equalsIgnoreCase(
					GmmsUtility.getInstance().getCommonProperty("ConfigFilePublishEnable", "true").trim());
		} catch (Exception e) {
			return true;
		}
	}

	private static RedisConnection redis() {
		try {
			RedisClient client = GmmsUtility.getInstance().getRedisClient();
			return client == null ? null : client.getStateRedis();
		} catch (Exception e) {
			return null;
		}
	}

	private static String moduleId() {
		return System.getProperty("module", "") + ":" + GmmsUtility.getInstance().getNodeId();
	}

	private static String moduleKey() {
		return MODULE_PREFIX + moduleId();
	}

	private static String now() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ").format(new Date());
	}
}
