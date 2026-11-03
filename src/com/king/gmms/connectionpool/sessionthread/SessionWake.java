package com.king.gmms.connectionpool.sessionthread;

import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.AbstractSession;

/**
 * The keep-alive wait of a client session thread: ends early when the connection is lost, so the
 * thread goes on to ReconnectInterval at once. The stop usually comes from the receiver thread;
 * it may also be this thread itself, so no Thread.interrupt: it would cut the reconnect wait short.
 */
final class SessionWake {
	private final AbstractSession session;
	private final Object lock = new Object();

	SessionWake(AbstractSession session) {
		this.session = session;
	}

	void sleepWhileConnected(long ms) throws InterruptedException {
		long end = System.currentTimeMillis() + ms;
		synchronized (lock) {
			while (connected() && session.isKeepRunning()) {
				long left = end - System.currentTimeMillis();
				if (left <= 0) {
					return;
				}
				lock.wait(left);
			}
		}
	}

	/** call after the status changed */
	void wake() {
		synchronized (lock) {
			lock.notifyAll();
		}
	}

	private boolean connected() {
		ConnectionStatus s = session.getStatus();
		return ConnectionStatus.CONNECT.equals(s) || ConnectionStatus.RECOVER.equals(s);
	}
}
