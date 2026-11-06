package com.king.db;

import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.zaxxer.hikari.HikariDataSource;

/**
 * The JDBC connection a thread holds goes away (closed by the pool, or isClosed() itself fails): it is given back
 * and dropped from the thread's list before another is opened. A pool of fake connections stands in for HikariCP.
 */
public class DataManagerCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	/** a connection whose state the check sets */
	static final class Fake implements InvocationHandler {
		final String name;
		volatile boolean closed = false, isClosedFails = false;
		int closeCalls = 0;
		Connection proxy;

		Fake(String name) { this.name = name; }

		public Object invoke(Object p, Method m, Object[] a) throws Throwable {
			switch (m.getName()) {
				case "isClosed":
					if (isClosedFails) throw new SQLException("Communications link failure");
					return closed;
				case "close": closeCalls++; closed = true; return null;
				case "toString": return name;
				case "hashCode": return System.identityHashCode(p);
				case "equals": return p == a[0];
				default: return null;
			}
		}
	}

	static final List<Fake> made = new ArrayList<>();
	static volatile boolean poolDown = false;

	static final class FakePool extends HikariDataSource {
		@Override
		public Connection getConnection() throws SQLException {
			if (poolDown) throw new SQLException("Connection refused");
			Fake f = new Fake("c" + (made.size() + 1));
			f.proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] { Connection.class }, f);
			made.add(f);
			return f.proxy;
		}
	}

	static final class Dm extends DataManager {
		public void add(Data data) {
		}
	}

	@SuppressWarnings("unchecked")
	static List<Connection> threadList() throws Exception {
		Field f = DataManager.class.getDeclaredField("allConnectionsForThread");
		f.setAccessible(true);
		return ((ThreadLocal<List<Connection>>) f.get(null)).get();
	}

	static void setStatic(String name, Object v) throws Exception {
		Field f = DataControl.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(null, v);
	}

	public static void main(String[] args) throws Exception {
		Hashtable<String, Object> configs = new Hashtable<>();
		configs.put("ds", new DbObject());
		setStatic("dsConfigs", configs);
		Hashtable<String, Object> pools = new Hashtable<>();
		pools.put("ds", new FakePool());
		setStatic("dataSources", pools);
		Dm dm = new Dm();
		dm.setDsName("ds");

		Connection c1 = dm.currentConnection();
		check(c1 == made.get(0).proxy && threadList().equals(List.of(c1)), "DM-01 first use: a connection, held by the thread");
		check(dm.currentConnection() == c1 && made.size() == 1, "DM-02 still open: the same one, nothing opened");

		// ---- the pool closed it (database restarted) ----
		made.get(0).closed = true;
		Connection c2 = dm.currentConnection();
		check(c2 == made.get(1).proxy && threadList().equals(List.of(c2)), "DM-03a closed: a new one, the dead one is gone from the thread's list: " + threadList());

		// ---- isClosed() itself fails ----
		made.get(1).isClosedFails = true;
		Connection c3 = dm.currentConnection();
		check(c3 == made.get(2).proxy && threadList().equals(List.of(c3)), "DM-04a isClosed() fails: a new one, the old one dropped from the list: " + threadList());
		check(made.get(1).closeCalls == 1, "DM-04b and given back to its pool once (close): " + made.get(1).closeCalls);

		// ---- many outages: the list does not grow ----
		for (int i = 0; i < 100; i++) {
			made.get(made.size() - 1).closed = true;
			dm.currentConnection();
		}
		check(threadList().size() == 1, "DM-05 100 outages: the thread holds one connection: " + threadList().size());

		// ---- no database to open another: the dead one is still given back ----
		Fake last = made.get(made.size() - 1);
		last.isClosedFails = true;
		poolDown = true;
		boolean thrown = false;
		try { dm.currentConnection(); } catch (DataControlException e) { thrown = true; }
		check(thrown && threadList().isEmpty() && last.closeCalls == 1, "DM-06 the database is gone: an error, but the dead connection is given back and not kept: "
				+ threadList() + " / close " + last.closeCalls);
		poolDown = false;
		Connection back = dm.currentConnection();
		check(back == made.get(made.size() - 1).proxy && threadList().equals(List.of(back)), "DM-07 the database is back: a new one");

		// ---- control: closeSession as before ----
		dm.closeSession();
		check(threadList().isEmpty() && made.get(made.size() - 1).closeCalls == 1, "DM-08 closeSession gives it back and empties the list, as before");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
