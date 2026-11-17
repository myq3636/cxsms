package rt;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * The verdict of the memory leak test (docs/内存泄漏测试设计.md 2.4) from the samples of bin/memwatch.sh: per module
 * the heap after a full GC (trend in the measured phase, retained after the traffic stopped), threads, file
 * descriptors, Metaspace, restarts; per Redis the keys without a TTL. Writes mem_report.txt in the result directory
 * and adds the verdict to its summary.txt.
 *
 * java -cp routetest.jar rt.MemReport RESULT_DIR CASE_DIR
 */
public final class MemReport {
	static final class Row { long ts; String module, pid; Double heap, afterGc, old, meta, rss, threads, fds; }

	public static void main(String[] a) throws Exception {
		Path res = Paths.get(a[0]), mem = res.resolve("mem");
		Properties p = new Properties();
		try (Reader r = Files.newBufferedReader(Paths.get(a[1]).resolve("case.properties"), StandardCharsets.UTF_8)) { p.load(r); }
		Properties ph = new Properties();
		try (Reader r = Files.newBufferedReader(mem.resolve("phases.txt"))) { ph.load(r); }
		long warmupEnd = Long.parseLong(ph.getProperty("warmupEnd")), loadEnd = Long.parseLong(ph.getProperty("loadEnd"));
		long idleEnd = Long.parseLong(ph.getProperty("idleEnd", String.valueOf(Long.MAX_VALUE)));
		double maxSlope = d(p, "mem.maxSlopeMBperHour", 30), growMB = d(p, "mem.growMB", 20), growPct = d(p, "mem.growPct", 10);
		double retainMB = d(p, "mem.retainMB", 30), retainPct = d(p, "mem.retainPct", 15), thrMax = d(p, "mem.threadGrowth", 20);
		double fdMax = d(p, "mem.fdGrowth", 50), metaMax = d(p, "mem.metaGrowthMB", 10);

		Map<String, List<Row>> byModule = new TreeMap<>();
		List<String> lines = Files.readAllLines(mem.resolve("jvm.csv"));
		for (String l : lines.subList(1, lines.size())) {
			String[] f = l.split(",", -1);
			if (f.length < 10) continue;
			Row r = new Row();
			r.ts = Long.parseLong(f[0]); r.module = f[1]; r.pid = f[2];
			r.heap = num(f[3]); r.afterGc = num(f[4]); r.old = num(f[5]); r.meta = num(f[6]); r.rss = num(f[7]); r.threads = num(f[8]); r.fds = num(f[9]);
			byModule.computeIfAbsent(r.module, k -> new ArrayList<>()).add(r);
		}
		StringBuilder out = new StringBuilder("memory leak test: ").append(res).append('\n');
		out.append(String.format("phases: warm-up ends %s, traffic ends %s, idle ends %s%n", time(warmupEnd), time(loadEnd), idleEnd == Long.MAX_VALUE ? "-" : time(idleEnd)));
		out.append(String.format("thresholds: heap slope > %.0f MB/h and growth > max(%.0f MB, %.0f%%); retained after idle > max(%.0f MB, %.0f%%); threads +%.0f; fds +%.0f; Metaspace +%.0f MB%n%n",
				maxSlope, growMB, growPct, retainMB, retainPct, thrMax, fdMax, metaMax));
		out.append(String.format("%-18s %8s %8s %8s %9s %8s %7s %7s %7s %7s %8s  %s%n", "module", "base MB", "end MB", "idle MB", "MB/hour", "meta+", "thr", "thr end",
				"fds", "fds end", "rss MB", "verdict"));
		List<String> suspects = new ArrayList<>();
		for (Map.Entry<String, List<Row>> e : byModule.entrySet()) {
			String m = e.getKey();
			List<Row> rows = e.getValue();
			List<String> why = new ArrayList<>();
			Set<String> pids = new TreeSet<>();
			// after the warm-up only: a2pStart.sh runs a launcher JVM with the same -Dmodule that starts the module and ends
			for (Row r : rows) if (!r.pid.isEmpty() && r.ts >= warmupEnd) pids.add(r.pid);
			if (pids.size() > 1) why.add("restarted after the warm-up (pids " + pids + ")");
			if (rows.stream().anyMatch(r -> r.pid.isEmpty() && r.ts > warmupEnd && r.ts < idleEnd)) why.add("not running at some samples");
			List<Row> meas = new ArrayList<>(), measGc = new ArrayList<>(), idleGc = new ArrayList<>(), idleAll = new ArrayList<>();
			for (Row r : rows) {
				if (r.heap == null) continue;
				if (r.ts >= warmupEnd && r.ts <= loadEnd) { meas.add(r); if (r.afterGc != null) measGc.add(r); }
				if (r.ts > loadEnd) { idleAll.add(r); if (r.afterGc != null) idleGc.add(r); }
			}
			if (measGc.size() < 6) {
				why.add("too few samples after GC in the measured phase (" + measGc.size() + ")");
				out.append(String.format("%-18s %s%n", m, String.join("; ", why)));
				suspects.add(m + ": " + String.join("; ", why));
				continue;
			}
			double base = median(measGc.subList(0, 3), r -> r.afterGc), end = median(measGc.subList(measGc.size() - 3, measGc.size()), r -> r.afterGc);
			double slope = slopePerHour(measGc);
			Double idle = idleGc.isEmpty() ? null : idleGc.get(idleGc.size() - 1).afterGc;
			if (slope > maxSlope && end - base > Math.max(growMB, base * growPct / 100))
				why.add(String.format("heap after GC grows %.1f MB/h (%.0f -> %.0f MB)", slope, base, end));
			if (idle != null && idle - base > Math.max(retainMB, base * retainPct / 100))
				why.add(String.format("heap after GC stays at %.0f MB after the traffic stopped (base %.0f MB)", idle, base));
			double thrBase = median(meas.subList(0, Math.min(3, meas.size())), r -> r.threads), thrMaxSeen = max(meas, r -> r.threads);
			Double thrIdle = idleAll.isEmpty() ? null : idleAll.get(idleAll.size() - 1).threads;
			if (thrMaxSeen - thrBase > thrMax && (thrIdle == null || thrIdle - thrBase > thrMax))
				why.add(String.format("threads %.0f -> %.0f, %s at the end", thrBase, thrMaxSeen, thrIdle == null ? "?" : String.format("%.0f", thrIdle)));
			double fdBase = median(meas.subList(0, Math.min(3, meas.size())), r -> r.fds), fdMaxSeen = max(meas, r -> r.fds);
			Double fdIdle = idleAll.isEmpty() ? null : idleAll.get(idleAll.size() - 1).fds;
			if (fdMaxSeen - fdBase > fdMax && (fdIdle == null || fdIdle - fdBase > fdMax))
				why.add(String.format("file descriptors %.0f -> %.0f, %s at the end", fdBase, fdMaxSeen, fdIdle == null ? "?" : String.format("%.0f", fdIdle)));
			double metaGrowth = meas.get(meas.size() - 1).meta - meas.get(0).meta;
			if (metaGrowth > metaMax) why.add(String.format("Metaspace +%.1f MB", metaGrowth));
			Row last = rows.get(rows.size() - 1);
			out.append(String.format("%-18s %8.1f %8.1f %8s %9.1f %8.1f %7.0f %7s %7.0f %7s %8s  %s%n", m, base, end, idle == null ? "-" : String.format("%.1f", idle), slope,
					metaGrowth, thrBase, thrIdle == null ? "-" : String.format("%.0f", thrIdle), fdBase, fdIdle == null ? "-" : String.format("%.0f", fdIdle),
					last.rss == null ? "-" : String.format("%.0f", last.rss), why.isEmpty() ? "OK" : "SUSPECT: " + String.join("; ", why)));
			if (!why.isEmpty()) suspects.add(m + ": " + String.join("; ", why));
		}

		// Redis: the keys without a TTL (estimated from random samples)
		long sent = 0;
		Path soak = res.resolve("soak.txt");
		if (Files.exists(soak)) {
			Properties sp = new Properties();
			try (Reader r = Files.newBufferedReader(soak)) { sp.load(r); }
			sent = Long.parseLong(sp.getProperty("sent", "0"));
		}
		double keyLimit = Math.max(d(p, "mem.redisNoTtlKeys", 1000), sent * d(p, "mem.redisNoTtlPctOfSent", 1) / 100);
		out.append(String.format("%nRedis (keys without a TTL estimated from %s random keys; limit +%.0f = max(1000, 1%% of %d MTs))%n", "500", keyLimit, sent));
		out.append(String.format("%-6s %10s %10s %10s %10s %12s %12s  %s%n", "port", "base MB", "end MB", "base keys", "end keys", "base no-TTL", "end no-TTL", "verdict"));
		Map<String, List<String[]>> byPort = new TreeMap<>();
		List<String> rl = Files.readAllLines(mem.resolve("redis.csv"));
		for (String l : rl.subList(1, rl.size())) {
			String[] f = l.split(",", -1);
			if (f.length >= 6 && !f[3].isEmpty()) byPort.computeIfAbsent(f[1], k -> new ArrayList<>()).add(f);
		}
		for (Map.Entry<String, List<String[]>> e : byPort.entrySet()) {
			String[] b = null, en = e.getValue().get(e.getValue().size() - 1);
			for (String[] f : e.getValue()) if (Long.parseLong(f[0]) >= warmupEnd) { b = f; break; }
			if (b == null) b = e.getValue().get(0);
			double bn = noTtl(b), enn = noTtl(en);
			String v = enn - bn > keyLimit ? String.format("SUSPECT: +%.0f keys without a TTL", enn - bn) : "OK";
			out.append(String.format("%-6s %10s %10s %10s %10s %12.0f %12.0f  %s%n", e.getKey(), b[2], en[2], b[3], en[3], bn, enn, v));
			if (!v.equals("OK")) suspects.add("redis " + e.getKey() + ": " + v.substring(9));
		}
		boolean pass = suspects.isEmpty();
		out.append('\n').append("MEMORY: ").append(pass ? "PASS" : "FAIL").append('\n');
		for (String s : suspects) out.append("  ").append(s).append('\n');
		Files.write(res.resolve("mem_report.txt"), out.toString().getBytes(StandardCharsets.UTF_8));
		System.out.print(out);

		String sum = new String(Files.readAllBytes(res.resolve("summary.txt")), StandardCharsets.UTF_8);
		int again = sum.indexOf("\nmemory leak test: ");                   // run again on the same result: replace the verdict
		if (again >= 0) sum = sum.substring(0, again);
		boolean traffic = sum.contains("TRAFFIC: PASS");
		sum += "\n" + out + "RESULT: " + (pass && traffic ? "PASS" : "FAIL") + " (traffic " + (traffic ? "PASS" : "FAIL") + ", memory " + (pass ? "PASS" : "FAIL") + ")\n";
		Files.write(res.resolve("summary.txt"), sum.getBytes(StandardCharsets.UTF_8));
		System.exit(pass && traffic ? 0 : 1);
	}

	static double d(Properties p, String k, double def) { return Double.parseDouble(p.getProperty(k, String.valueOf(def)).trim()); }
	static Double num(String s) { return s == null || s.trim().isEmpty() ? null : Double.valueOf(s.trim()); }
	static String time(long ms) { return new java.text.SimpleDateFormat("HH:mm:ss").format(new Date(ms)); }

	static double noTtl(String[] f) {
		double keys = Double.parseDouble(f[3]), n = f[4].isEmpty() ? 0 : Double.parseDouble(f[4]), s = Double.parseDouble(f[5]);
		return s == 0 ? 0 : keys * n / s;
	}

	interface Get { Double of(Row r); }

	static double median(List<Row> rows, Get g) {
		List<Double> v = new ArrayList<>();
		for (Row r : rows) if (g.of(r) != null) v.add(g.of(r));
		Collections.sort(v);
		return v.isEmpty() ? 0 : v.get(v.size() / 2);
	}

	static double max(List<Row> rows, Get g) {
		double m = 0;
		for (Row r : rows) if (g.of(r) != null) m = Math.max(m, g.of(r));
		return m;
	}

	/** least squares slope of afterGc over time, MB per hour */
	static double slopePerHour(List<Row> rows) {
		double n = rows.size(), sx = 0, sy = 0, sxx = 0, sxy = 0;
		long t0 = rows.get(0).ts;
		for (Row r : rows) {
			double x = (r.ts - t0) / 3600000.0, y = r.afterGc;
			sx += x; sy += y; sxx += x * x; sxy += x * y;
		}
		double den = n * sxx - sx * sx;
		return den == 0 ? 0 : (n * sxy - sx * sy) / den;
	}
}
