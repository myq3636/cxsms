package it;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;

/** MessageIdFormat=v2 on two real server processes: ids in the real submit_sm_resp and DR, a kill -9, a module too far in the list. */
public class MessageIdIT extends MultiServerIT {
	static final int P = 17783;

	static List<String> submits(int port, String user, String pw, int n, String text) throws Exception {
		List<String> ids = new ArrayList<String>();
		try (R r = new R(port)) {
			r.bind(user, pw);
			Thread.sleep(300);
			for (int i = 0; i < n; i++) {
				Pdu p = r.submit(ServerFuncIT.sub(text + i));
				if (p != null && p.cmd == 0x80000004 && p.status == 0) ids.add(ServerFuncIT.msgId(p));
			}
		}
		return ids;
	}
	static int common(Collection<String> a, Collection<String> b) { Set<String> s = new HashSet<String>(a); s.retainAll(b); return s.size(); }
	static void kill(Process p) throws Exception { p.destroyForcibly().waitFor(); }
	static void addConfig(String h, String text) throws Exception {
		Path c = Paths.get(h + "Gmms/GmmsConfig.properties");
		Files.write(c, (new String(Files.readAllBytes(c), "UTF-8") + "\n" + text + "\n").getBytes("UTF-8"));
	}
	/** real launcher, real module list */
	static String launcher(String module) throws Exception {
		String h = home.replaceAll("/$", "") + "_L" + module + "/";
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		StringBuilder list = new StringBuilder("1|CoreEngine1|CoreEngine|x\n");
		for (int i = 1; i <= 10; i++) list.append("1|MultiSmppServer" + i + "|MultiSmppServer|x\n");
		Files.write(Paths.get(h + "conf/a2p_modules.conf"), list.toString().getBytes("UTF-8"));
		addConfig(h, module + ".Port=17790");
		File out = new File(h + "launch.out");
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-Da2p_home=" + h, "-Dmodule=" + module, "-DNodeID=9",
				"-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.MessageIdLauncherNode", "start", "com.king.gmms.listener.smppserver.MultiSmppServer");
		b.redirectErrorStream(true); b.redirectOutput(out);
		Process p = b.start();
		p.waitFor(8, TimeUnit.SECONDS);
		String text = new String(Files.readAllBytes(out.toPath()), "UTF-8");
		boolean exited = !p.isAlive();
		int code = exited ? p.exitValue() : 0;
		p.destroyForcibly().waitFor();
		return (exited ? "exit=" + code : "running") + "\n" + text;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8") + (cust("IA", 130, null) + cust("IB", 131, null) + cust("IH", 132, null).replace("WindowSize: 20\n", "WindowSize: 20\nSMSOptionSMPPIsGenHexMsgId: true\n")).replace("WindowSize: 20\n", "WindowSize: 20\nSMSOptionIncomingThrottlingNum: 20000\n")).getBytes("UTF-8"));
		addConfig(home, "ModuleStatusTTLSeconds=5\nModuleStatusHeartbeatSeconds=1");
		Process s1 = null, s2 = null;
		try {
			boot();
			jedis.set("thcon", "1");
			// the list position gives the number
			check(com.king.gmms.domain.ModuleManager.getInstance().getModuleNo(S2) == 2, "ID-02d MultiSmppServer2 is module number 2");
			s1 = start(S1, 1, P1);
			s2 = start(S2, 2, P2);
			check(logHas(S1, "[MSGID] v2 format on. module=" + S1 + ", moduleNo=1"), "ID-02e no MessageIdFormat configured: v2 is the default, on before traffic: module 1");

			// ---- two real servers under load ----
			ExecutorService ex = Executors.newFixedThreadPool(2);
			Future<List<String>> f1 = ex.submit(() -> submits(P1, "ia", "pw130", 1500, "a"));
			Future<List<String>> f2 = ex.submit(() -> submits(P2, "ib", "pw131", 1500, "b"));
			List<String> i1 = f1.get(), i2 = f2.get();
			ex.shutdown();
			check(i1.size() == 1500 && i2.size() == 1500, "ID-04a every submit_sm answered: " + i1.size() + " + " + i2.size());
			boolean shape = true;
			for (String s : i1) shape &= s.matches("1\\d{9}");
			for (String s : i2) shape &= s.matches("2\\d{9}");
			check(shape, "ID-04b ten digits, first digit the module: " + i1.get(0) + " " + i2.get(0));
			check(new HashSet<String>(i1).size() == 1500 && new HashSet<String>(i2).size() == 1500 && common(i1, i2) == 0, "ID-04c no message_id twice");
			check(new TreeSet<String>(i1).first().equals(i1.get(0)) || i1.get(0).compareTo(i1.get(1)) < 0, "ID-04d rising within a module");

			// ---- hex ids: customer with SMSOptionSMPPIsGenHexMsgId ----
			List<String> h1 = submits(P1, "ih", "pw132", 300, "h"), h2 = submits(P2, "ih", "pw132", 300, "i");
			boolean hexShape = h1.size() == 300 && h2.size() == 300;
			for (String s : h1) hexShape &= s.length() == 8 && Long.parseLong(s, 16) >>> 28 == 1;
			for (String s : h2) hexShape &= s.length() == 8 && Long.parseLong(s, 16) >>> 28 == 2;
			check(hexShape, "ID-14j hex customer on both servers: 8 hex digits, module number on top: " + h1.get(0) + " " + h2.get(0));
			check(new HashSet<String>(h1).size() == 300 && new HashSet<String>(h2).size() == 300 && common(h1, h2) == 0, "ID-14k no hex id twice");
			try (R r = new R(P2)) {
				r.bind("ih", "pw132");
				Thread.sleep(800);
				String hid = ServerFuncIT.msgId(r.submit(ServerFuncIT.sub("hex dr")));
				sendDr(hid, 132, S2);
				Pdu d = ServerFuncIT.nextDeliver(r, 8000);
				String text = d == null ? "none" : ServerFuncIT.deliver(d).get("text");
				if (d != null) r.send(0x80000005, d.seq, new byte[] { 0 });
				String dec = String.valueOf(Long.parseLong(hid, 16));
				System.out.println("INFO hex DR " + hid + " -> " + text);
				check(text.contains("id:" + dec + " ") || text.contains("id:" + String.format("%010d", Long.parseLong(dec)) + " "), "ID-14l the DR carries the hex id as a decimal number, as before: " + hid + " = " + dec + " in " + text);
			}
			check(waitFor(() -> jedis.get("system:msgid:hexmark:" + S1) != null, 4000), "ID-14m hex position saved in Redis");
			kill(s1);
			s1 = launch(S1, 1, P1, h(S1));
			List<String> h3 = submits(P1, "ih", "pw132", 300, "j");
			check(h3.size() == 300 && common(h1, h3) == 0, "ID-14n kill -9 and restart: no hex id used again: " + common(h1, h3));

			// ---- the same id in the DR ----
			try (R r = new R(P1)) {
				r.bind("ia", "pw130");
				Thread.sleep(800);
				Pdu resp = r.submit(ServerFuncIT.sub("for the dr"));
				String id = ServerFuncIT.msgId(resp);
				sendDr(id, 130, S1);
				Pdu d = ServerFuncIT.nextDeliver(r, 8000);
				String text = d == null ? "none" : ServerFuncIT.deliver(d).get("text");
				if (d != null) r.send(0x80000005, d.seq, new byte[] { 0 });
				check(text.contains("id:" + id + " "), "ID-11 the real deliver_sm carries the id of the real submit_sm_resp: " + id + " in " + text);
			}

			// ---- kill -9 ahead of the clock (K=1: every id runs ahead) ----
			kill(s1);
			addConfig(h(S1), "MessageIdCounterPerSecond=1");
			jedis.del("system:msgid:mark:" + S1);      // the mark of K=5000 is far ahead of K=1
			s1 = launch(S1, 1, P1, h(S1));
			List<String> first = submits(P1, "ia", "pw130", 300, "c");
			check(first.size() == 300, "ID-06b K=1: 300 ids");
			final String lastId = first.get(first.size() - 1);
			check(waitFor(() -> { String m = jedis.get("system:msgid:mark:" + S1); return m != null && String.valueOf(Long.parseLong(m) % 1000000000L).equals(String.valueOf(Long.parseLong(lastId.substring(1)))); }, 4000), "ID-06c the position is saved within seconds: " + jedis.get("system:msgid:mark:" + S1));
			kill(s1);
			s1 = launch(S1, 1, P1, h(S1));
			List<String> next = submits(P1, "ia", "pw130", 300, "d");
			check(next.size() == 300 && common(first, next) == 0, "ID-06d kill -9 and restart, with the mark: " + common(first, next) + " used again");
			// control: without it the same restart does reuse them
			final String lastNext = next.get(next.size() - 1);
			waitFor(() -> { String m = jedis.get("system:msgid:mark:" + S1); return m != null && Long.parseLong(m) % 1000000000L == Long.parseLong(lastNext.substring(1)); }, 4000);
			kill(s1);
			jedis.del("system:msgid:mark:" + S1);
			s1 = launch(S1, 1, P1, h(S1));
			check(logHas(S1, "[MSGID] no mark"), "ID-08 no saved position: starts and warns");
			List<String> noMark = submits(P1, "ia", "pw130", 300, "e");
			System.out.println("INFO without the mark, used again: " + common(first, noMark) + "/" + common(next, noMark));
			check(noMark.size() == 300 && common(first, noMark) + common(next, noMark) > 0, "ID-06z control: without the mark ids are used again");

			// ---- per module setting ----
			kill(s2);
			addConfig(h(S2), S2 + ".MessageIdFormat=v1");
			s2 = launch(S2, 2, P2, h(S2));
			List<String> old = submits(P2, "ib", "pw131", 3, "f");
			Calendar c = new GregorianCalendar();
			int expect = ((c.get(Calendar.DAY_OF_MONTH) * 24 + c.get(Calendar.HOUR_OF_DAY)) * 10000 + c.get(Calendar.MINUTE) * 60 + c.get(Calendar.SECOND) + 5000) % 1000000;
			check(old.size() == 3 && Math.abs(Integer.parseInt(old.get(0).substring(0, 6)) - expect) < 30, "ID-01e {module}.MessageIdFormat=v1: back to the old format on that module: " + old.get(0));
			kill(s1); kill(s2);
			s1 = null; s2 = null;

			// ---- module number ----
			String tenth = launcher("MultiSmppServer10");
			check(tenth.startsWith("exit=255") && tenth.contains("[MSGID] MessageIdFormat=v2 needs a module number 1 to 9"), "ID-09a tenth module of its type: refuses to start: " + tenth.split("\n")[0]);
			String ninth = launcher("MultiSmppServer9");
			check(ninth.contains("Starting service") && !ninth.contains("[MSGID] MessageIdFormat=v2 needs") && !ninth.startsWith("exit=255"), "ID-09b ninth: passes this check: " + ninth.split("\n")[0]);
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally {
			if (s1 != null) s1.destroyForcibly();
			if (s2 != null) s2.destroyForcibly();
		}
		exit();
	}
	static boolean logHas(String module, String text) throws Exception {
		Path p = Paths.get(h(module) + "log/it.log");
		return Files.exists(p) && new String(Files.readAllBytes(p), "UTF-8").contains(text);
	}
}
