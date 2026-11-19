package rt;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Performance test report (docs/性能测试部署与执行手册.md): one row per run, runs of several directories side by side.
 *   java -cp lib/routetest.jar rt.PerfReport DIR [DIR ...]
 * A run is DIR/run_{X}.out (or run.out, the output of bin/run_case.sh / bin/run_hightps.sh) with DIR/watch_{X}.* (or
 * watch.*, bin/perf_watch.sh). Only the steady part counts: from the end of the warm-up to the end of the load (the
 * phases.txt of the run's result directory). The backlog grows when its least-squares slope is above max(20, 0.5 % of
 * the TPS) entries a second.
 */
public class PerfReport {
	static final class Run {
		String dir, label, tps = "?", sent = "?", segs = "?", respOk = "?", respErr = "?", drs = "?", noDr = "?", latAll = "";
		final Map<String, long[]> latBy = new TreeMap<>();   // template group -> {count, p50 of the templates, p95 max, p99 max}
		final Map<String, List<Long>> p50s = new TreeMap<>();
		long warmupEnd, loadEnd;
		double slope = Double.NaN;
		long maxTotal;
		final Map<String, Long> maxBy = new HashMap<>();
		double idleMed = Double.NaN, idleMin = Double.NaN;
		final Map<String, Double> cpuBy = new TreeMap<>();
		final Map<String, Long> redisMax = new TreeMap<>();
		String guard = "";
	}

	static final Pattern TPL = Pattern.compile("^\\s+M\\d+\\(([a-z]+)\\d*\\) submit -> vendor: count=(\\d+) .*?p50=(\\d+) p95=(\\d+) p99=(\\d+)");

	public static void main(String[] a) throws Exception {
		List<Run> runs = new ArrayList<>();
		for (String d : a) runs.addAll(read(Paths.get(d)));
		StringBuilder o = new StringBuilder();
		o.append("| 目录 | 档 | 计划 TPS | 发出 MT / 段 | submit 被拒 | DR / 预热后没 DR 的 | 稳定期积压变化 (/s) | 积压最多 | 结论 | CPU 空闲 中位 / 最低 | 提交 → 通道（全部） |\n");
		o.append("|---|---|---|---|---|---|---|---|---|---|---|\n");
		for (Run r : runs) {
			double tps = num(r.tps);
			boolean growing = !Double.isNaN(r.slope) && r.slope > Math.max(20, tps * 0.005);
			o.append("| ").append(Paths.get(r.dir).getFileName()).append(" | ").append(r.label).append(" | ").append(r.tps)
					.append(" | ").append(r.sent).append(" / ").append(r.segs).append(" | ").append(r.respErr)
					.append(" | ").append(r.drs).append(" / ").append(r.noDr)
					.append(" | ").append(Double.isNaN(r.slope) ? "-" : String.format("%+.0f", r.slope))
					.append(" | ").append(r.maxTotal).append(" (").append(top(r.maxBy, 3)).append(")")
					.append(" | ").append(r.guard.isEmpty() ? (growing ? "**积压增长**" : "稳定") : "**保护停止**")
					.append(" | ").append(Double.isNaN(r.idleMed) ? "-" : String.format("%.0f%% / %.0f%%", r.idleMed, r.idleMin))
					.append(" | ").append(r.latAll).append(" |\n");
		}
		o.append("\n按客户分组的提交 → 通道延时（ms，组内各模板 p50 的中位、p95 和 p99 的最大）：\n\n| 目录 | 档 | 客户组 | 条数 | p50 | p95 | p99 |\n|---|---|---|---|---|---|---|\n");
		for (Run r : runs) {
			for (Map.Entry<String, long[]> e : r.latBy.entrySet()) {
				List<Long> l = r.p50s.get(e.getKey());
				Collections.sort(l);
				long[] v = e.getValue();
				o.append("| ").append(Paths.get(r.dir).getFileName()).append(" | ").append(r.label).append(" | ").append(e.getKey())
						.append(" | ").append(v[0]).append(" | ").append(l.get(l.size() / 2)).append(" | ").append(v[2]).append(" | ").append(v[3]).append(" |\n");
			}
		}
		o.append("\n稳定期平均 CPU（%，100 = 一个核）和 Redis 最高内存（MB）：\n\n| 目录 | 档 | CPU | Redis |\n|---|---|---|---|\n");
		for (Run r : runs) {
			StringBuilder c = new StringBuilder(), m = new StringBuilder();
			for (Map.Entry<String, Double> e : r.cpuBy.entrySet()) c.append(e.getKey()).append(' ').append(String.format("%.0f", e.getValue())).append("; ");
			for (Map.Entry<String, Long> e : r.redisMax.entrySet()) m.append(e.getKey()).append(' ').append(e.getValue()).append("; ");
			o.append("| ").append(Paths.get(r.dir).getFileName()).append(" | ").append(r.label).append(" | ").append(c).append(" | ").append(m)
					.append(r.guard.isEmpty() ? "" : " " + r.guard).append(" |\n");
		}
		System.out.print(o);
	}

	static List<Run> read(Path dir) throws IOException {
		List<Run> out = new ArrayList<>();
		List<Path> outs = new ArrayList<>();
		try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "run*.out")) { for (Path p : ds) outs.add(p); }
		outs.sort(Comparator.comparingDouble(p -> num(label(p))));
		for (Path p : outs) {
			Run r = new Run();
			r.dir = dir.toString();
			r.label = label(p);
			String watch = r.label.isEmpty() ? "watch" : "watch_" + r.label;
			parseRun(r, Files.readAllLines(p, StandardCharsets.ISO_8859_1));
			parseWatch(r, dir, watch);
			out.add(r);
		}
		return out;
	}

	static String label(Path p) {
		String n = p.getFileName().toString();
		return n.equals("run.out") ? "" : n.substring(4, n.length() - 4);
	}

	static void parseRun(Run r, List<String> lines) throws IOException {
		for (String l : lines) {
			Matcher m;
			if ((m = Pattern.compile("^soak: ([0-9.]+) TPS").matcher(l)).find()) r.tps = m.group(1);
			if ((m = Pattern.compile("^MTs sent (\\d+) of \\d+ planned, segments (\\d+)").matcher(l)).find()) { r.sent = m.group(1); r.segs = m.group(2); }
			if ((m = Pattern.compile("^submit_sm_resp ok (\\d+), failed (\\d+)").matcher(l)).find()) { r.respOk = m.group(1); r.respErr = m.group(2); }
			if ((m = Pattern.compile("^DRs to the customers \\d+ after the drain, (\\d+) at the end").matcher(l)).find()) r.drs = m.group(1);
			if ((m = Pattern.compile("^MTs after the warm-up without a DR at the end: (\\d+)").matcher(l)).find()) r.noDr = m.group(1);
			if ((m = Pattern.compile("customer submit -> vendor: count=\\d+ avg=\\d+ (p50=\\d+ p95=\\d+ p99=\\d+)").matcher(l)).find()) r.latAll = m.group(1);
			if ((m = TPL.matcher(l)).find()) {
				long[] v = r.latBy.computeIfAbsent(m.group(1), k -> new long[4]);
				v[0] += Long.parseLong(m.group(2));
				v[2] = Math.max(v[2], Long.parseLong(m.group(4)));
				v[3] = Math.max(v[3], Long.parseLong(m.group(5)));
				r.p50s.computeIfAbsent(m.group(1), k -> new ArrayList<>()).add(Long.parseLong(m.group(3)));
			}
			if ((m = Pattern.compile("^result: (\\S+)").matcher(l)).find()) {
				Path ph = Paths.get(m.group(1), "phases.txt");
				if (!Files.exists(ph)) ph = Paths.get(m.group(1), "mem", "phases.txt");
				if (Files.exists(ph)) {
					for (String x : Files.readAllLines(ph)) {
						if (x.startsWith("warmupEnd=")) r.warmupEnd = Long.parseLong(x.substring(10).trim()) / 1000;
						if (x.startsWith("loadEnd=")) r.loadEnd = Long.parseLong(x.substring(8).trim()) / 1000;
					}
				}
			}
		}
	}

	static boolean steady(Run r, long t) {
		return (r.warmupEnd == 0 || t >= r.warmupEnd) && (r.loadEnd == 0 || t <= r.loadEnd);
	}

	static void parseWatch(Run r, Path dir, String prefix) throws IOException {
		Path s = dir.resolve(prefix + ".streams");
		if (Files.exists(s)) {
			List<double[]> pts = new ArrayList<>();
			for (String l : Files.readAllLines(s, StandardCharsets.ISO_8859_1)) {
				String[] f = l.trim().split("\\s+");
				if (f.length == 0 || !f[0].matches("\\d+")) continue;
				long t = Long.parseLong(f[0]);
				if (!steady(r, t)) continue;
				Map<String, Long> by = new HashMap<>();
				long tot = 0;
				for (int i = 1; i < f.length; i++) {
					int eq = f[i].lastIndexOf('=');
					if (eq < 0) continue;
					String fam = f[i].substring(0, eq).replaceAll(":\\d+$", "");
					long n = Long.parseLong(f[i].substring(eq + 1));
					by.merge(fam, n, Long::sum);
					tot += n;
				}
				for (Map.Entry<String, Long> e : by.entrySet()) r.maxBy.merge(e.getKey(), e.getValue(), Math::max);
				r.maxTotal = Math.max(r.maxTotal, tot);
				pts.add(new double[] { t, tot });
			}
			if (pts.size() >= 3) {
				double mt = 0, mv = 0;
				for (double[] p : pts) { mt += p[0]; mv += p[1]; }
				mt /= pts.size(); mv /= pts.size();
				double num = 0, den = 0;
				for (double[] p : pts) { num += (p[0] - mt) * (p[1] - mv); den += (p[0] - mt) * (p[0] - mt); }
				r.slope = den == 0 ? 0 : num / den;
			}
		}
		Path c = dir.resolve(prefix + ".cpu");
		if (Files.exists(c)) {
			List<Double> idles = new ArrayList<>();
			Map<String, Double> sum = new HashMap<>();
			int n = 0;
			for (String l : Files.readAllLines(c, StandardCharsets.ISO_8859_1)) {
				String[] f = l.trim().split("\\s+");
				if (f.length == 0 || !f[0].matches("\\d+") || !steady(r, Long.parseLong(f[0]))) continue;
				n++;
				for (int i = 1; i < f.length; i++) {
					int eq = f[i].indexOf('=');
					if (eq < 0) continue;
					String k = f[i].substring(0, eq);
					double v = num(f[i].substring(eq + 1));
					if (Double.isNaN(v)) continue;
					if (k.equals("idle")) { idles.add(v); continue; }
					String role = k.startsWith("CoreEngine") ? "Core" : k.startsWith("MultiSmppServer") ? "Server" : k.startsWith("MultiSmppClient") ? "Client"
							: k.startsWith("MsgQueueMonitor") ? "MQM" : k.startsWith("redis") ? "Redis" : k.startsWith("rt.") ? "压测工具" : k;
					sum.merge(role, v, Double::sum);
				}
			}
			if (!idles.isEmpty()) {
				Collections.sort(idles);
				r.idleMed = idles.get(idles.size() / 2);
				r.idleMin = idles.get(0);
			}
			for (Map.Entry<String, Double> e : sum.entrySet()) r.cpuBy.put(e.getKey(), e.getValue() / Math.max(1, n));
		}
		Path m = dir.resolve(prefix + ".redis");
		if (Files.exists(m)) {
			for (String l : Files.readAllLines(m, StandardCharsets.ISO_8859_1)) {
				String[] f = l.trim().split("\\s+");
				if (f.length < 3 || !f[0].matches("\\d+") || !steady(r, Long.parseLong(f[0]))) continue;
				double v = num(f[2].replace("usedMB=", ""));
				if (!Double.isNaN(v)) r.redisMax.merge(f[1], (long) v, Math::max);
			}
		}
		Path g = dir.resolve(prefix + ".guard");
		if (Files.exists(g)) r.guard = new String(Files.readAllBytes(g), StandardCharsets.UTF_8).trim();
	}

	static String top(Map<String, Long> m, int n) {
		List<Map.Entry<String, Long>> l = new ArrayList<>(m.entrySet());
		l.sort((x, y) -> Long.compare(y.getValue(), x.getValue()));
		StringBuilder b = new StringBuilder();
		for (int i = 0; i < Math.min(n, l.size()); i++) b.append(i == 0 ? "" : ", ").append(l.get(i).getKey()).append(' ').append(l.get(i).getValue());
		return b.toString();
	}

	static double num(String s) {
		try {
			return Double.parseDouble(s.trim());
		} catch (Exception e) {
			return Double.NaN;
		}
	}
}
