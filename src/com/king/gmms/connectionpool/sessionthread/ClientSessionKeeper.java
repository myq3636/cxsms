package com.king.gmms.connectionpool.sessionthread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.AbstractSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;

/**
 * Connection upkeep of an outgoing SMPP session (a channel bind of the Client) on a shared scheduler instead of a thread
 * per session (same rules as {@link ClientSessionThread}; docs/会话线程优化设计.md 3.3):
 * <ul>
 * <li>connect and bind on ClientConnectThreads (default 32): they block (connect timeout, bind_resp); after a failure or
 * a disconnect again after ReconnectInterval, one attempt at a time per session;</li>
 * <li>bound, with EnquireLink: enquire_link when nothing was received for EnquireLinkTime, or with KeepEnquireLink also
 * when the last enquire_link_resp is EnquireLinkTime old; the session is stopped after EnquireLinkFailureNum unanswered
 * ones or ConnectionSilentTime without anything received. Each check is planned for the time the next enquire_link is
 * due (the thread per session slept EnquireLinkTime + 100 ms between checks: an idle link could wait almost twice
 * EnquireLinkTime); the first one of a session at a random point of its first EnquireLinkTime;</li>
 * <li>without EnquireLink only the silence check.</li>
 * </ul>
 * The checks run on ClientSessionKeeperThreads (default 4) threads, both read at the first session.
 */
public class ClientSessionKeeper implements SessionThread {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ClientSessionKeeper.class);
	private static volatile ScheduledExecutorService scheduler;
	private static volatile ExecutorService connector;

	private final AbstractSession session;
	private final int connectionInterval;
	private final int enquireLinkFailCount;
	private final A2PMultiConnectionInfo customerInfo;
	/** a connect (or the wait for it) is planned: one at a time */
	private final AtomicBoolean connecting = new AtomicBoolean(false);
	private volatile boolean started = false;
	private volatile boolean firstCheck = true;
	/** when this keeper last sent an enquire_link: the next one not before EnquireLinkTime after it */
	private volatile long lastSentAt = 0L;
	private volatile ScheduledFuture<?> next;

	/** ClientSessionKeeperShared (default true): the shared upkeep, else a thread per session as before */
	public static SessionThread create(AbstractSession session, int connectionInterval, int enquireLinkFailCount) {
		if ("false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("ClientSessionKeeperShared", "true").trim())) {
			return new ClientSessionThread(session, connectionInterval, enquireLinkFailCount);
		}
		return new ClientSessionKeeper(session, connectionInterval, enquireLinkFailCount);
	}

	ClientSessionKeeper(AbstractSession session, int connectionInterval, int enquireLinkFailCount) {
		this.session = session;
		this.connectionInterval = connectionInterval;
		this.enquireLinkFailCount = enquireLinkFailCount;
		this.customerInfo = session.getCustomerInfo();
	}

	private static int intProperty(String key, int def) {
		try {
			return Math.max(1, Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(def)).trim()));
		} catch (Exception e) {
			return def;
		}
	}

	private static ThreadFactory named(final String prefix) {
		final AtomicInteger no = new AtomicInteger();
		return new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, prefix + no.incrementAndGet());
				t.setDaemon(true);
				return t;
			}
		};
	}

	private static ScheduledExecutorService scheduler() {
		if (scheduler == null) {
			synchronized (ClientSessionKeeper.class) {
				if (scheduler == null) {
					ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(intProperty("ClientSessionKeeperThreads", 4),
							named("ClientSessionKeeper-"));
					s.setRemoveOnCancelPolicy(true);
					scheduler = s;
				}
			}
		}
		return scheduler;
	}

	private static ExecutorService connector() {
		if (connector == null) {
			synchronized (ClientSessionKeeper.class) {
				if (connector == null) {
					int n = intProperty("ClientConnectThreads", 32);
					connector = new ThreadPoolExecutor(n, n, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
							named("ClientConnect-"));
				}
			}
		}
		return connector;
	}

	/** the session starts (AbstractSession.start): connect now */
	public void start() {
		started = true;
		planConnect(0L);
	}

	public void run() {
		check();
	}

	/** connect after delayMs, unless one is already planned or running */
	private void planConnect(long delayMs) {
		if (!session.isKeepRunning() || !connecting.compareAndSet(false, true)) {
			return;
		}
		cancelCheck();
		Runnable submit = new Runnable() {
			public void run() {
				try {
					connector().execute(new Runnable() {
						public void run() {
							connect();
						}
					});
				} catch (Exception e) {
					connecting.set(false);
					log.warn("Client session {} connect not started", session.getSessionName(), e);
				}
			}
		};
		if (delayMs <= 0) {
			submit.run();
		} else {
			scheduler().schedule(submit, delayMs, TimeUnit.MILLISECONDS);
		}
	}

	private void connect() {
		boolean bound = false;
		try {
			if (!session.isKeepRunning()) {
				return;
			}
			ConnectionStatus before = session.getStatus();
			if (ConnectionStatus.CONNECT.equals(before) || ConnectionStatus.RECOVER.equals(before)) {
				bound = true;
				return;
			}
			if (session.createConnection() && session.connect()) {
				log.warn("The session({}) status is set:  CONNECT and the original status is {}", session.getSessionName(), before);
				session.setStatus(ConnectionStatus.CONNECT);
				bound = true;
			}
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			connecting.set(false);
			if (bound) {
				firstCheck = true;
				lastSentAt = 0L;
				planCheck(0L);
			} else if (session.isKeepRunning()) {
				// as the thread per session: stop (DISCONNECT), then try again after ReconnectInterval
				session.stop();
				planConnect(connectionInterval);
			}
		}
	}

	/** a check of the bound session after delayMs (0: when the next enquire_link is due) */
	private void planCheck(long delayMs) {
		if (!session.isKeepRunning() || connecting.get()) {
			return;
		}
		cancelCheck();
		next = scheduler().schedule(new Runnable() {
			public void run() {
				check();
			}
		}, Math.max(1L, delayMs <= 0 ? dueInMs() : delayMs), TimeUnit.MILLISECONDS);
	}

	private void check() {
		try {
			if (!session.isKeepRunning()) {
				return;
			}
			ConnectionStatus status = session.getStatus();
			if (!ConnectionStatus.CONNECT.equals(status) && !ConnectionStatus.RECOVER.equals(status)) {
				planConnect(connectionInterval);
				return;
			}
			long now = System.currentTimeMillis();
			if (now - session.getLastActivity() > customerInfo.getConnectionSilentTime()) {
				session.stop();
				return;
			}
			if (!customerInfo.isEnquireLink()) {
				planCheck(Math.max(100L, customerInfo.getConnectionSilentTime() - 100L));
				return;
			}
			int before = session instanceof com.king.gmms.connectionpool.session.MultiSmppSession
					? ((com.king.gmms.connectionpool.session.MultiSmppSession) session).unansweredEnquireLinks() : -1;
			int count = session.enquireLink();
			if (before >= 0 ? count > before : count > 0) {
				lastSentAt = System.currentTimeMillis();
			}
			if (count >= enquireLinkFailCount) {
				log.warn("{} alive requests didn't receive response, so close the connection.", count);
				session.stop();
				return;
			}
			planCheck(0L);
		} catch (Exception e) {
			log.error(e, e);
			session.stop();
		}
	}

	/**
	 * When the next enquire_link is due: nothing received for EnquireLinkTime (lastActivity), with KeepEnquireLink also
	 * the last enquire_link_resp EnquireLinkTime old (eqLastActivity); at least 100 ms, at most EnquireLinkTime. The
	 * first check of a session after a bind comes at a random point of its EnquireLinkTime.
	 */
	private long dueInMs() {
		long t = Math.max(100L, customerInfo.getEnquireLinkTime());
		if (firstCheck) {
			firstCheck = false;
			return 1L + ThreadLocalRandom.current().nextLong(t);
		}
		long now = System.currentTimeMillis();
		long due = session.getLastActivity() + t;
		if (customerInfo.isKeepEnquireLink()) {
			due = Math.min(due, session.getEqLastActivity() + t);
		}
		// one sent and not answered yet: lastActivity did not move, the next one EnquireLinkTime after it
		due = Math.max(due, lastSentAt + t);
		return Math.max(100L, Math.min(t, due - now));
	}

	private void cancelCheck() {
		ScheduledFuture<?> f = next;
		if (f != null) {
			f.cancel(false);
		}
	}

	/** the session stopped (connection lost): reconnect after ReconnectInterval, as the thread per session */
	public void stopThread() {
		switch (session.getStatus()) {
			case CONNECT:
			case INITIAL:
				log.warn("The session({}) status is set:  DISCONNECT and the original status is {}", session.getSessionName(),
						session.getStatus());
				session.setStatus(ConnectionStatus.DISCONNECT);
				break;
			default:
				break;
		}
		cancelCheck();
		if (started) {
			planConnect(connectionInterval);
		}
	}

	/** the session is destroyed (keepRunning false): nothing more */
	public void interrupt() {
		cancelCheck();
	}
}
