package it;

import java.io.FileWriter;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.Statement;

import com.king.framework.GracefulStop;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.CDRAsynWriter;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageIdGenerator;
import com.king.message.gmms.MessageStoreManager;
import com.king.message.gmms.SdqMessageManager;

/** One process of GracefulStopIT: fills a memory queue, then signals itself. The database is a proxy writing to a file. */
public class StopNode extends ItSupport {
	static volatile long statementMs = 0;

	static class FileStore extends MessageStoreManager {
		@Override public Connection currentConnection() {
			return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class }, (p, m, a) -> {
				switch (m.getName()) {
					case "createStatement": return statement();
					case "getAutoCommit": case "isClosed": return Boolean.FALSE;
					default: return null;
				}
			});
		}
		@Override protected int doUpdate(String s) { record(s); return 1; }
		@Override public void closeSession() { }
	}
	static synchronized void record(String sql) {
		try (FileWriter w = new FileWriter(home + "sql.out", true)) { w.write(sql.replace('\n', ' ') + "\n"); } catch (Exception e) { }
	}
	static Statement statement() {
		java.util.List<String> batch = new java.util.ArrayList<>();
		return (Statement) Proxy.newProxyInstance(StopNode.class.getClassLoader(), new Class<?>[] { Statement.class }, (p, m, a) -> {
			switch (m.getName()) {
				case "addBatch": batch.add((String) a[0]); return null;
				case "executeBatch":
					if (statementMs > 0) Thread.sleep(statementMs);
					for (String s : batch) record(s);
					return new int[batch.size()];
				default: return null;
			}
		});
	}
	static void signal(String sig) throws Exception {
		System.out.println("SIGNAL " + System.currentTimeMillis());
		System.out.flush();
		new ProcessBuilder("kill", sig, String.valueOf(ProcessHandle.current().pid())).inheritIO().start().waitFor();
		Thread.sleep(60000);
	}

	public static void main(String[] a) throws Exception {
		String what = a[0], sig = a[1];
		int n = Integer.parseInt(a[2]);
		msmFactory = FileStore::new;
		boot();
		System.out.println("BOOTED");
		switch (what) {
			case "sql":
			case "stuck":
				if ("stuck".equals(what)) statementMs = 600000;
				MessageStoreManager msm = GmmsUtility.getInstance().getMessageStoreManager();
				for (int i = 0; i < n; i++) {
					GmmsMessage m = mt("S" + i, 8888, 8, 0);
					msm.updateByInMsgId(m, "SMQ");
				}
				break;
			case "cdr":
				CDRAsynWriter.getInstance().initialize();
				for (int i = 0; i < n; i++) CDRAsynWriter.getInstance().recoreCDR("cdr-" + i + "\n");
				break;
			case "sdq":
				for (int i = 0; i < n; i++) SdqMessageManager.getInstance().insertSDQMessage(mt("Q" + i, 8888, 8, 0));
				break;
			case "id":
				for (int i = 0; i < n; i++) MessageIdGenerator.getInstance().generateDecID(0);
				System.out.println("COUNTER " + MessageIdGenerator.v2Counter());
				break;
			case "shutdown":
				CDRAsynWriter.getInstance().initialize();
				for (int i = 0; i < n; i++) CDRAsynWriter.getInstance().recoreCDR("cdr-" + i + "\n");
				jedis.publish("channel:gmms:control", "SHUTDOWN@" + System.getProperty("module"));
				System.out.println("PUBLISHED");
				waitFor(() -> false, 3000);
				System.out.println("STILL ALIVE");
				for (int i = 0; i < 5; i++) CDRAsynWriter.getInstance().recoreCDR("after-" + i + "\n");
				break;
			default:
				throw new IllegalArgumentException(what);
		}
		signal(sig);
	}
}
