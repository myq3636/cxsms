package com.king.gmms.connectionpool.sessionthread;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.AbstractSession;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.MultiNodeCustomerInfo;

/**
 * Keep-alive of an SMPP server session on a shared scheduler instead of a thread per connection
 * (same checks as {@link ServerSessionThread}):
 * <ul>
 * <li>before bind: stop when disconnected, after SmppServerBindTimeoutMs from accept, or after
 * MaxSilentTime without activity; checked every second;</li>
 * <li>bound: with ServerEnquireLink, send enquire_link every EnquireLinkTime (FailEnquireLinkTime
 * while an earlier one is still unanswered) and stop after EnquireLinkFailureNum missing responses or
 * ConnectionSilentTime without activity; without it, only the silence check;</li>
 * <li>RECOVER (multi-node customers): back to CONNECT after the node recovery time.</li>
 * </ul>
 * The scheduler has SmppServerSessionKeeperThreads threads (default 4, read at the first session).
 */
public class ServerSessionKeeper implements SessionThread {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ServerSessionKeeper.class);
	private static volatile ScheduledExecutorService scheduler;

	private final AbstractSession session;
	private final int connectionSilentTime;
	private long acceptedAt;
	private long bindTimeoutMs;
	private A2PMultiConnectionInfo customerInfo;
	private boolean bound = false;
	private volatile boolean stopped = false;
	private volatile ScheduledFuture<?> next;
	private volatile ScheduledFuture<?> recovery;

	public ServerSessionKeeper(AbstractSession session, int connectionSilentTime) {
		this.session = session;
		this.connectionSilentTime = connectionSilentTime;
	}

	private static ScheduledExecutorService scheduler() {
		if (scheduler == null) {
			synchronized (ServerSessionKeeper.class) {
				if (scheduler == null) {
					int threads = 4;
					try {
						threads = Math.max(1, Integer.parseInt(GmmsUtility.getInstance()
								.getCommonProperty("SmppServerSessionKeeperThreads", "4").trim()));
					} catch (Exception e) {
						threads = 4;
					}
					final AtomicInteger no = new AtomicInteger();
					ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(threads, new ThreadFactory() {
						public Thread newThread(Runnable r) {
							Thread t = new Thread(r, "SmppSessionKeeper-" + no.incrementAndGet());
							t.setDaemon(true);
							return t;
						}
					});
					s.setRemoveOnCancelPolicy(true);
					scheduler = s;
				}
			}
		}
		return scheduler;
	}

	public void start() {
		acceptedAt = System.currentTimeMillis();
		bindTimeoutMs = bindTimeoutMs();
		schedule(session.getWaitingTime());
	}

	public void run() {
		tick();
	}

	private void schedule(long delayMs) {
		if (stopped) {
			return;
		}
		next = scheduler().schedule(new Runnable() {
			public void run() {
				tick();
			}
		}, Math.max(1L, delayMs), TimeUnit.MILLISECONDS);
	}

	private void tick() {
		if (stopped) {
			return;
		}
		try {
			if (!session.isKeepRunning()) {
				finish();
				return;
			}
			long now = System.currentTimeMillis();
			ConnectionStatus status = session.getStatus();
			if (!bound) {
				if (status.equals(ConnectionStatus.DISCONNECT)) {
					log.warn("Session from initial dump to disconnect, direct stop session!");
					stopSession();
					return;
				}
				if (status.equals(ConnectionStatus.INITIAL)) {
					if (bindTimeoutMs > 0 && now - acceptedAt > bindTimeoutMs) {
						log.warn("Session is stopped since it did not bind within SmppServerBindTimeoutMs={}", bindTimeoutMs);
						stopSession();
						return;
					}
					if (now - session.getLastActivity() > connectionSilentTime) {
						log.warn("Session is stopped since reach to Max Silent time! And last time is {}", session.getLastActivity());
						stopSession();
						return;
					}
					schedule(session.getWaitingTime());
					return;
				}
				if ((status.equals(ConnectionStatus.CONNECT) || status.equals(ConnectionStatus.RECOVER))
						&& session.getConnectionInfo() != null) {
					bound = true;
					customerInfo = session.getCustomerInfo();
					if (status.equals(ConnectionStatus.RECOVER)) {
						startRecovery();
					}
				} else {
					log.error("The server session is initialized unsuccessfully");
					stopSession();
					return;
				}
			}
			if (customerInfo.isServerEnquireLink()) {
				if (now - session.getLastActivity() > customerInfo.getConnectionSilentTime()) {
					stopSession();
					return;
				}
				// ping just sent does not count as missing: only one sent earlier and still unanswered
				// switches to FailEnquireLinkTime (the thread per connection used the count after
				// sending, so it pinged every FailEnquireLinkTime instead of every EnquireLinkTime)
				int missing = session instanceof MultiSmppSession
						? ((MultiSmppSession) session).unansweredEnquireLinks() : -1;
				int count = session.enquireLink();
				if (count >= customerInfo.getEnquireLinkFailureNum()) {
					stopSession();
					return;
				}
				if (missing < 0 ? count >= 1 : missing >= 1) {
					if (session.getStatus().equals(ConnectionStatus.RECOVER)) {
						startRecovery();
					}
					schedule(customerInfo.getFailEnquireLinkTime());
				} else if (count > missing) {
					schedule(customerInfo.getEnquireLinkTime());      // just sent one
				} else {
					// nothing sent (traffic or a recent answer): check again when the link has been
					// idle for EnquireLinkTime, not a whole EnquireLinkTime after this check
					long idle = System.currentTimeMillis() - session.getLastActivity();
					schedule(Math.max(100L, Math.min(customerInfo.getEnquireLinkTime(),
							customerInfo.getEnquireLinkTime() - idle)));
				}
			} else if (now - session.getLastActivity() > customerInfo.getConnectionSilentTime()) {
				stopSession();
			} else {
				schedule(Math.max(100L, customerInfo.getConnectionSilentTime() - 100L));
			}
		} catch (Exception e) {
			log.error(e, e);
			stopSession();
		}
	}

	private void stopSession() {
		session.stop();
		finish();
	}

	private void finish() {
		if (stopped) {
			return;
		}
		stopped = true;
		cancel();
		log.warn("ServerSessionKeeper stop, session number: {}", session.getSessionNum());
	}

	private void startRecovery() {
		if (!(customerInfo instanceof MultiNodeCustomerInfo)) {
			return;
		}
		ScheduledFuture<?> r = recovery;
		if (r != null) {
			r.cancel(false);
		}
		recovery = scheduler().schedule(new Runnable() {
			public void run() {
				if (session.getStatus().equals(ConnectionStatus.RECOVER)) {
					log.warn("The session({}) status is set: CONNECT and the original status is RECOVER",
							session.getSessionName());
					session.setStatus(ConnectionStatus.CONNECT);
				}
			}
		}, ((MultiNodeCustomerInfo) customerInfo).getNodeRecoveryTime(), TimeUnit.MILLISECONDS);
	}

	private void cancel() {
		ScheduledFuture<?> f = next;
		if (f != null) {
			f.cancel(false);
		}
		ScheduledFuture<?> r = recovery;
		if (r != null) {
			r.cancel(false);
		}
	}

	public void stopThread() {
		session.setKeepRunning(false);
		switch (session.getStatus()) {
			case CONNECT:
			case INITIAL:
			case RECOVER:
				log.warn("The session({}) status is set:  DISCONNECT and the original status is {}",
						session.getSessionName(), session.getStatus());
				session.setStatus(ConnectionStatus.DISCONNECT);
				break;
			default:
				break;
		}
		stopped = true;
		cancel();
	}

	public void interrupt() {
		stopped = true;
		cancel();
	}

	private static long bindTimeoutMs() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("SmppServerBindTimeoutMs", "30000").trim());
		} catch (Exception e) {
			return 30000L;
		}
	}
}
