package com.king.gmms.connectionpool.connection;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.strategy.IndexBalanceStrategy;
import com.king.message.gmms.GmmsMessage;

import sun.reflect.ReflectionFactory;

/**
 * A customer's old session closes while its new session binds (ServerBatch3IT S3-03, docs/服务器全量回归失败排查.md 2.4):
 * the connection must stay CONNECT with the new session in it. The race is forced: the closing thread is held
 * between "no sessions left" and setting DISCONNECT, while the binding thread adds the new session.
 */
public class ServerConnectionRaceCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	@SuppressWarnings("unchecked")
	static <T> T alloc(Class<T> type) throws Exception {
		// skips the constructors of the class (GmmsUtility, ModuleManager), not the one of Observable (its observer list)
		Constructor<?> base = java.util.Observable.class.isAssignableFrom(type)
				? java.util.Observable.class.getDeclaredConstructor() : Object.class.getDeclaredConstructor();
		Constructor<?> c = ReflectionFactory.getReflectionFactory().newConstructorForSerialization(type, base);
		return (T) c.newInstance();
	}

	/** The sessions map: size() on the closing thread, once armed, waits for the binding thread to finish. */
	@SuppressWarnings("rawtypes")
	static class HoldingMap extends HashMap {
		volatile Thread holdOn;
		final CountDownLatch held = new CountDownLatch(1);
		final CountDownLatch addDone = new CountDownLatch(1);

		@Override
		public int size() {
			int n = super.size();
			if (Thread.currentThread() == holdOn) {
				holdOn = null;
				held.countDown();
				try {
					addDone.await(500, TimeUnit.MILLISECONDS);   // with the fix the add waits for the lock: times out
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return n;
		}
	}

	static class StubSession extends Session {
		final String name;
		volatile ConnectionStatus status = ConnectionStatus.CONNECT;

		StubSession(String name) {
			this.name = name;
		}

		public String getSessionName() { return name; }
		public ConnectionStatus getStatus() { return status; }
		public void setStatus(ConnectionStatus s) { status = s; }
		public boolean isServer() { return true; }
		public boolean isFakeSession() { return false; }
		public TransactionURI getTransactionURI() { return null; }
		public boolean equals(Object o) { return this == o; }
		public int hashCode() { return System.identityHashCode(this); }
		public void setConnectionManager(ConnectionManager connManager) {}
		public void stop() {}
		public void initSenders() {}
		public void initReceivers() {}
		public void updateReceivers() {}
		public int enquireLink() { return 0; }
		public boolean connect() { return true; }
		public boolean createConnection() { return true; }
		public boolean submit(GmmsMessage msg) { return true; }
		public ByteBuffer submitAndRec(GmmsMessage msg) { return null; }
		public ConnectionInfo getConnectionInfo() { return null; }
		public long getLastActivity() { return 0; }
		public void setKeepRunning(boolean keepRunning) {}
		public boolean isKeepRunning() { return true; }
		public void setSessionName(String connectionName) {}
		public int getSessionNum() { return 0; }
		public OperatorMessageQueue getOperatorMessageQueue() { return null; }
		public void parse(ByteBuffer buffer) {}
		public boolean receive(Object obj) { return true; }
		public void connectionUnavailable() {}
	}

	/** A connection as the server has it after one session: CONNECT with s2 in it. */
	static <T extends AbstractMultiConnection> T connection(Class<T> type, HoldingMap sessions, Session s2) throws Exception {
		T c = alloc(type);
		c.mutex = new Object();
		c.strategy = new IndexBalanceStrategy();
		c.info = alloc(ConnectionInfo.class);
		c.connectedSessions = sessions;
		c.status = ConnectionStatus.CONNECT;
		if (type == NodeServerConnection.class) {
			Field f = NodeServerConnection.class.getDeclaredField("recoverSessions");
			f.setAccessible(true);
			f.set(c, new HashMap<Object, Object>());
		}
		c.addSession(s2);
		c.status = ConnectionStatus.CONNECT;
		return c;
	}

	static void race(Class<? extends AbstractMultiConnection> type) throws Exception {
		String n = type.getSimpleName();
		HoldingMap sessions = new HoldingMap();
		StubSession s2 = new StubSession("C_2");
		StubSession s3 = new StubSession("C_3");
		final AbstractMultiConnection c = connection(type, sessions, s2);

		Thread closing = new Thread(() -> {
			s2.setStatus(ConnectionStatus.DISCONNECT);
			((java.util.Observer) c).update(s2, ConnectionStatus.DISCONNECT);   // what the session's notify does
		}, "closing");
		sessions.holdOn = closing;
		closing.start();
		check(sessions.held.await(5, TimeUnit.SECONDS), n + " closing thread removed the old session and checks the count");
		Thread binding = new Thread(() -> {
			c.addSession(s3);
			sessions.addDone.countDown();
		}, "binding");
		binding.start();
		closing.join(5000);
		binding.join(5000);

		check(c.getConnectionStatus() == ConnectionStatus.CONNECT,
				n + " old session closes while the new one binds: connection CONNECT (was DISCONNECT with a live session): "
						+ c.getConnectionStatus());
		check(c.getSession() == s3, n + " the new session is in the connection (a DR reaches it through the pool only when the connection is CONNECT): " + (c.getSession() == null ? "none" : c.getSession().getSessionName()));
	}

	static void selfHeal() throws Exception {
		// a connection left DISCONNECT with a live session (before the fix) is set right by the next session
		HoldingMap sessions = new HoldingMap();
		MultiServerConnectionImpl c = connection(MultiServerConnectionImpl.class, sessions, new StubSession("C_3"));
		c.status = ConnectionStatus.DISCONNECT;
		c.addSession(new StubSession("C_4"));
		check(c.getConnectionStatus() == ConnectionStatus.CONNECT,
				"MultiServerConnectionImpl DISCONNECT with a live session, a second session binds: CONNECT again: " + c.getConnectionStatus());
	}

	public static void main(String[] args) throws Exception {
		race(MultiServerConnectionImpl.class);
		race(NodeServerConnection.class);
		selfHeal();

		// the normal paths are unchanged
		HoldingMap sessions = new HoldingMap();
		StubSession a = new StubSession("C_a");
		MultiServerConnectionImpl c = connection(MultiServerConnectionImpl.class, sessions, a);
		a.setStatus(ConnectionStatus.DISCONNECT);
		c.update(a, ConnectionStatus.DISCONNECT);
		check(c.getConnectionStatus() == ConnectionStatus.DISCONNECT && c.getSession() == null,
				"last session closes alone: DISCONNECT, no session");
		StubSession b = new StubSession("C_b");
		c.addSession(b);
		check(c.getConnectionStatus() == ConnectionStatus.CONNECT && c.getSession() == b, "a new bind after that: CONNECT");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
