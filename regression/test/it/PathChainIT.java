package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;
import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;

/**
 * GmmsMessage.modulePath over the whole chain, each module its own process: a customer binds to the real
 * Server, the real Core routes, the real Client sends to the FakeSmsc, the vendor DR comes back the same way.
 * The path in the log lines of each module and in the DeliveryChannel column of the Core CDRs.
 */
public class PathChainIT extends ItSupport {
	static final int VPORT = 17805, SPORT = 17806;
	static final String S = "MultiSmppServer1", C = "CoreEngine1", K = "MultiSmppClient1";
	static final List<Process> nodes = new ArrayList<>();

	static String h(String module) { return home.replaceAll("/$", "") + "_" + module + "/"; }

	static void launch(String module, String... args) throws Exception {
		String hm = h(module);
		new ProcessBuilder("rm", "-rf", hm).start().waitFor();
		new ProcessBuilder("cp", "-r", home, hm).start().waitFor();
		new File(hm + "log/it.log").delete();
		List<String> cmd = new ArrayList<>(Arrays.asList(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + hm,
				"-Dmodule=" + module, "-DNodeID=1", "-Dlog4j.configurationFile=" + hm + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.PathChainNode"));
		cmd.addAll(Arrays.asList(args));
		ProcessBuilder b = new ProcessBuilder(cmd);
		b.redirectErrorStream(true); b.redirectOutput(new File(hm + "node.out"));
		nodes.add(b.start());
		final String out = hm + "node.out";
		if (!waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000))
			throw new IllegalStateException(module + " did not start");
	}

	/** log lines of a module with this tag */
	static List<String> logs(String module, String tag) throws Exception {
		List<String> l = new ArrayList<>();
		Path p = Paths.get(h(module) + "log/it.log");
		if (Files.exists(p)) for (String s : Files.readAllLines(p)) if (s.contains(tag)) l.add(s);
		return l;
	}
	static boolean any(List<String> lines, String part) { for (String s : lines) if (s.contains(part)) return true; return false; }

	/** CDR lines of the Core, by type, that contain id */
	static Map<String, List<String>> cdrs(String id) throws Exception {
		Map<String, List<String>> m = new TreeMap<>();
		File[] fs = new File(h(C) + "cdr").listFiles();
		if (fs != null) for (File f : fs) {
			if (f.isDirectory()) continue;
			for (String l : Files.readAllLines(f.toPath())) {
				if (!l.contains(id)) continue;
				String[] c = l.split(",", 3);
				m.computeIfAbsent(c.length > 1 ? c[1] : "?", k -> new ArrayList<>()).add(l);
			}
		}
		return m;
	}
	static boolean cdrHas(Map<String, List<String>> m, String type, String path) {
		// the CDR column has ':' between the modules
		for (String l : m.getOrDefault(type, Collections.<String>emptyList())) if (l.contains(",\"" + path.replace(">", ":") + "\",")) return true;
		return false;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8")
				+ "\n[CUST8890]\nCCBSSID: 8890\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n"
				+ "IncomingConnection: (ConnName=CUST8890_c1,userName=pc8890,password=pw8890,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n"
				+ "\n[VENDOR80]\nCCBSSID: 80\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\n"
				+ "OutgoingConnection: (ConnName=VENDOR80_c1,userName=u80,password=p,URL=127.0.0.1,Port=" + VPORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/routing/CUST8890_routingInfo.cfg"), "CCBSSID: 8890\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR80)\n".getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(VPORT);
		FakeSmsc.Account v = smsc.account("u80");
		v.submitMessageId = "PCV1";
		try {
			boot();
			// ---- PC-00 the stream format: the path travels as field 57, this module added ----
			GmmsMessage m = mt("8888_PC00", 8888, 8, 0);
			m.setModulePath(S);
			String body = SerializableHandler.convertGmmsMessage2RedisMessage(m);
			GmmsMessage back = SerializableHandler.convertRedisMssage2GmmsMessage(body);
			check(back != null && (S + ">" + C).equals(back.getModulePath()), "PC-00a written with this module added, read back: " + (back == null ? "" : back.getModulePath()));
			GmmsMessage old = SerializableHandler.convertRedisMssage2GmmsMessage(String.join("`", java.util.Arrays.copyOf(body.split("`", -1), 57)));   // fields 0..56: the format before modulePath
			check(old != null && old.getModulePath() == null && "8888_PC00".equals(old.getMsgID()), "PC-00b an entry written before (no field 57): read, no path");

			jedis.set("thcon", "1");   // the Server takes submits only with this key (throttling control on)
			launch(C, "core");
			launch(K, "client");
			launch(S, "server", String.valueOf(SPORT));
			check(waitFor(() -> v.binds.get() == 1, 20000), "PC-01 the Client bound to the vendor");

			try (R r = new R(SPORT)) {
				Pdu b = r.bind("pc8890", "pw8890");
				check(b != null && b.status == 0, "PC-02 the customer bound to the Server");
				Pdu resp = r.submit(ServerFuncIT.sub("SENDER", "8613800000000", 0, 1, 0, "path test".getBytes(), null));
				String id = ServerFuncIT.msgId(resp);
				check(resp != null && resp.status == 0 && id != null && !id.isEmpty(), "PC-03 submit accepted: " + id);
				check(waitFor(() -> v.submits.size() == 1, 20000), "PC-04 the vendor got the MT");
				Thread.sleep(1500);

				String mtPath = S + ">" + C + ">" + K;
				List<String> out = logs(K, "[MT-OUT]");
				System.out.println("INFO Client " + out);
				check(any(out, "path=" + mtPath), "PC-05 Client [MT-OUT] path=" + mtPath);
				List<String> toClient = logs(C, "[MT-TO-CLIENT]");
				System.out.println("INFO Core " + toClient);
				check(any(toClient, "path=" + S + ">" + C), "PC-06 Core [MT-TO-CLIENT] path=" + S + ">" + C);

				// ---- the vendor DR back to the customer ----
				int seq = v.deliver("8613800000000", "SENDER", 4, ClientResultIT.receipt("PCV1", "DELIVRD", "000"), null);
				Pdu d = ServerFuncIT.nextDeliver(r, 20000);
				check(d != null, "PC-07 the customer got the DR");
				if (d != null) r.send(0x80000005, d.seq, new byte[] { 0 });
				Thread.sleep(2500);
				String drPath = K + ">" + C;
				List<String> toServer = logs(C, "[DR-TO-SERVER]");
				System.out.println("INFO Core " + toServer);
				check(any(toServer, "path=" + drPath), "PC-08 Core [DR-TO-SERVER] path=" + drPath + " (its own path, from the Client that got it)");
				List<String> drOut = logs(S, "[DR-OUT]");
				System.out.println("INFO Server " + drOut);
				check(any(drOut, "path=" + drPath + ">" + S), "PC-09 Server [DR-OUT] path=" + drPath + ">" + S);

				// ---- CDRs of the Core: DeliveryChannel column ----
				Thread.sleep(3000);   // CDR files are written asynchronously
				Map<String, List<String>> c = cdrs("8613800000000");   // the only MT of this run
				for (Map.Entry<String, List<String>> e : c.entrySet())
					for (String l : e.getValue()) System.out.println("INFO CDR " + (l.length() > 260 ? l.substring(0, 260) + "..." : l));
				check(cdrHas(c, "IN_SUBMIT", S + ">" + C), "PC-10 IN_SUBMIT: " + S + ">" + C);
				check(cdrHas(c, "OUT_SUBMIT", mtPath + ">" + C), "PC-11 OUT_SUBMIT (written when the result is back): " + mtPath + ">" + C);
				check(cdrHas(c, "OUT_DELI_REP", drPath), "PC-12 OUT_DELI_REP: " + drPath);
			}
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		for (Process p : nodes) p.destroyForcibly();
		smsc.close();
		exit();
	}
}
