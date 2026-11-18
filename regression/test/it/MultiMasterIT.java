package it;

import java.nio.file.*;
import java.util.*;

import com.king.redis.RedisClient;
import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Two submit MQ and two report MQ masters (Redis_submit_masters / Redis_report_masters, docs/Redis多主分片设计.md):
 * a customer binds to the real Server, the real Core routes, the real Client sends to the FakeSmsc and the vendor DRs
 * come back, each module its own process. Every MT goes out once and every DR comes back once; the streams of both
 * shards are on their own master; at the end nothing is left on any master (backlog, attempt markers, send markers,
 * outbox entries). Masters: submit 16379 + 16382, report 16379 + 16383 (run.sh starts them).
 */
public class MultiMasterIT extends ItSupport {
	static final int VPORT = 17815, SPORT = 17816, N = 40;
	static final String S = "MultiSmppServer1", C = "CoreEngine1", K = "MultiSmppClient1";

	/** keys matching pattern on one Redis */
	static Set<String> keys(Jedis j, String pattern) {
		Set<String> out = new TreeSet<>();
		String cursor = ScanParams.SCAN_POINTER_START;
		do {
			ScanResult<String> r = j.scan(cursor, new ScanParams().match(pattern).count(1000));
			out.addAll(r.getResult());
			cursor = r.getCursor();
		} while (!"0".equals(cursor));
		return out;
	}

	static long backlog(Jedis j) {
		long n = 0;
		for (String k : keys(j, "stream:*")) if ("stream".equals(j.type(k))) n += j.xlen(k);
		return n;
	}

	/** attempt markers, send markers and outbox entries still there (the outbox :done markers expire by their TTL) */
	static Set<String> leftovers(Jedis j) {
		Set<String> out = new TreeSet<>();
		out.addAll(keys(j, "core:mt:*"));
		out.addAll(keys(j, "core:att:*"));
		out.addAll(keys(j, "client:sent:*"));
		for (String k : keys(j, "core:outbox:submit:*")) if (!k.endsWith(":done")) out.add(k);
		return out;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path gc = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(gc, (new String(Files.readAllBytes(gc), "UTF-8")
				+ "\nRedis_submit_masters=127.0.0.1:16379,127.0.0.1:16382\nRedis_submit_slaves=127.0.0.1:16379,127.0.0.1:16382\n"
				+ "Redis_report_masters=127.0.0.1:16379,127.0.0.1:16383\nRedis_report_slaves=127.0.0.1:16379,127.0.0.1:16383\n").getBytes("UTF-8"));
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8")
				+ "\n[CUST8891]\nCCBSSID: 8891\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 100\n"
				+ "IncomingConnection: (ConnName=CUST8891_c1,userName=mm8891,password=pw8891,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n"
				+ "\n[VENDOR81]\nCCBSSID: 81\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\n"
				+ "OutgoingConnection: (ConnName=VENDOR81_c1,userName=u81,password=p,URL=127.0.0.1,Port=" + VPORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/routing/CUST8891_routingInfo.cfg"), "CCBSSID: 8891\nA2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR81)\n".getBytes("UTF-8"));
		Jedis s2 = new Jedis("127.0.0.1", 16382), r2 = new Jedis("127.0.0.1", 16383);
		s2.flushAll();
		r2.flushAll();
		FakeSmsc smsc = new FakeSmsc(VPORT);
		FakeSmsc.Account v = smsc.account("u81");
		try {
			boot();
			RedisClient rc = RedisClient.getInstance();
			check(rc.submitMqCount() == 2 && rc.reportMqCount() == 2, "MM-00a 2 submit and 2 report MQ masters: " + rc.submitMqCount() + "/" + rc.reportMqCount());
			check(RedisClient.instanceOf("stream:mt:pending:8891:0", 2) == 0 && RedisClient.instanceOf("stream:mt:pending:8891:1", 2) == 1
					&& RedisClient.instanceOf("stream:dr:pending:8891:" + S + ":1", 2) == 1 && RedisClient.instanceOf("stream:core:hold:81", 2) == 0
					&& RedisClient.instanceOf("stream:mt:routed:81:1", 1) == 0,
					"MM-00b a sharded stream on master shard % masters, a hold stream on master 0, one master: always 0");

			PathChainIT.nodes.clear();
			PathChainIT.launch(C, "core");
			PathChainIT.launch(K, "client");
			PathChainIT.launch(S, "server", String.valueOf(SPORT));
			check(waitFor(() -> v.binds.get() == 1, 20000), "MM-01 the Client bound to the vendor");

			try (R r = new R(SPORT)) {
				Pdu b = r.bind("mm8891", "pw8891");
				check(b != null && b.status == 0, "MM-02 the customer bound to the Server");
				int ok = 0;
				for (int i = 0; i < N; i++) {
					Pdu resp = r.submit(ServerFuncIT.sub("SENDER", "861380000" + String.format("%04d", i), 0, 1, 0, ("mm " + i).getBytes(), null));
					if (resp != null && resp.status == 0) ok++;
				}
				final int accepted = ok;
				check(accepted == N, "MM-03 " + N + " submits accepted: " + accepted);
				check(waitFor(() -> v.submits.size() >= N, 30000), "MM-04 the vendor got them: " + v.submits.size());
				Thread.sleep(1500);
				Set<String> dests = new HashSet<>();
				synchronized (v.submits) { for (FakeSmsc.Submit x : v.submits) dests.add(x.dest); }
				check(v.submits.size() == N && dests.size() == N, "MM-05 each MT sent to the vendor once: " + v.submits.size() + " submits, " + dests.size() + " recipients");

				List<String> pendingStreams0 = new ArrayList<>(), pendingStreams1 = new ArrayList<>();
				for (String k : jedis.smembers("idx:streams")) if (k.startsWith("stream:mt:")) pendingStreams0.add(k);
				for (String k : s2.smembers("idx:streams")) if (k.startsWith("stream:mt:")) pendingStreams1.add(k);
				System.out.println("INFO submit master 0: " + pendingStreams0 + ", master 1: " + pendingStreams1);
				boolean spread = !pendingStreams0.isEmpty() && !pendingStreams1.isEmpty();
				for (String k : pendingStreams0) spread &= k.endsWith(":0");
				for (String k : pendingStreams1) spread &= k.endsWith(":1");
				check(spread, "MM-06 the MT streams of shard 0 on submit master 0, of shard 1 on master 1");

				// the vendor DRs, one per MT; the customer gets each once
				List<FakeSmsc.Submit> sent;
				synchronized (v.submits) { sent = new ArrayList<>(v.submits); }
				for (int i = 0; i < sent.size(); i++) {
					v.deliver(sent.get(i).dest, "SENDER", 4, ClientResultIT.receipt("V" + (i + 1), "DELIVRD", "000"), null);
				}
				Set<String> drFor = new HashSet<>();
				int drs = 0;
				long end = System.currentTimeMillis() + 40000;
				while (drs < N && System.currentTimeMillis() < end) {
					Pdu d = ServerFuncIT.nextDeliver(r, (int) Math.max(100, end - System.currentTimeMillis()));
					if (d == null) break;
					drs++;
					r.send(0x80000005, d.seq, new byte[] { 0 });
					drFor.add(new String(d.body, "ISO-8859-1").replaceAll("(?s).*?(861380000\\d{4}).*", "$1"));
				}
				final int drCount = drs;
				Thread.sleep(2000);
				Pdu extra = ServerFuncIT.nextDeliver(r, 1500);
				check(drCount == N && drFor.size() == N && extra == null, "MM-07 the customer got " + drCount + " DRs, for " + drFor.size() + " MTs, none twice");

				List<String> report0 = new ArrayList<>(), report1 = new ArrayList<>();
				for (String k : jedis.smembers("idx:streams")) if (k.startsWith("stream:core:results") || k.startsWith("stream:dr:pending")) report0.add(k);
				for (String k : r2.smembers("idx:streams")) if (k.startsWith("stream:core:results") || k.startsWith("stream:dr:pending")) report1.add(k);
				System.out.println("INFO report master 0: " + report0 + ", master 1: " + report1);
				check(!report0.isEmpty() && !report1.isEmpty(), "MM-08 results and DR streams on both report masters");

				boolean drained = waitFor(() -> backlog(jedis) == 0 && backlog(s2) == 0 && backlog(r2) == 0
						&& leftovers(jedis).isEmpty() && leftovers(s2).isEmpty() && leftovers(r2).isEmpty(), 20000);
				check(drained, "MM-09 nothing left: backlog " + backlog(jedis) + "/" + backlog(s2) + "/" + backlog(r2) + ", markers and outbox "
						+ leftovers(jedis) + " " + leftovers(s2) + " " + leftovers(r2));
			}
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		for (Process p : PathChainIT.nodes) p.destroyForcibly();
		smsc.close();
		exit();
	}
}
