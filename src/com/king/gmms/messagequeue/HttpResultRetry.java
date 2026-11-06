package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.king.framework.GracefulStop;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;

/**
 * HTTP vendor submits whose request went out but whose result could not be written to Redis
 * (docs/HTTP外发结果写入失败重复发送修复设计.md): the vendor may have the message, so it is never sent again;
 * only the rest is retried here, every HttpResultRetryIntervalMs, at most HttpResultRetryMax times: the send
 * marker to "sent" (a redelivery then only acks), the result for Core, the ack of the routed entry. Given up,
 * the Core outbox reports 2700 after CoreOutboundRespTimeoutMs. Kept in memory, as the SMPP pending results.
 */
public final class HttpResultRetry {
	private static final SystemLogger log = SystemLogger.getSystemLogger(HttpResultRetry.class);
	private static final HttpResultRetry INSTANCE = new HttpResultRetry();
	private static final long DEFAULT_INTERVAL_MS = 5000L;
	private static final int DEFAULT_MAX = 50;

	private static final class Task {
		final GmmsMessage msg;
		final String streamKey;
		final String group;
		final String redisId;
		final boolean useMarker;
		volatile boolean marked;
		volatile boolean produced;
		volatile int tries = 0;

		Task(GmmsMessage msg, String streamKey, String group, String redisId, boolean useMarker, boolean marked, boolean produced) {
			this.msg = msg;
			this.streamKey = streamKey;
			this.group = group;
			this.redisId = redisId;
			this.useMarker = useMarker;
			this.marked = marked;
			this.produced = produced;
		}
	}

	/** by send marker key */
	private final Map<String, Task> tasks = new ConcurrentHashMap<String, Task>();
	private ScheduledExecutorService timer = null;

	private HttpResultRetry() {
	}

	public static HttpResultRetry getInstance() {
		return INSTANCE;
	}

	/** what is left to do for a sent submit; the caller tried each step once already */
	public void add(GmmsMessage msg, String streamKey, String group, String redisId, boolean useMarker, boolean marked, boolean produced) {
		Task task = new Task(msg, streamKey, group, redisId, useMarker, marked || !useMarker, produced);
		tasks.put(ClientSendMarker.key(msg), task);
		start();
		log.warn(msg, "[HTTP-RESULT-RETRY] sent, {} not written yet: retried every {} ms, not sent again. outMsgId={}, pending={}",
				task.marked ? "result" : "send marker and result", intervalMs(), msg.getOutMsgID(), tasks.size());
	}

	/** this process still writes the result of that send: a redelivered copy must not be sent */
	public boolean holds(GmmsMessage msg) {
		return !tasks.isEmpty() && tasks.containsKey(ClientSendMarker.key(msg));
	}

	public int size() {
		return tasks.size();
	}

	private synchronized void start() {
		if (timer != null) {
			return;
		}
		timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "HttpResultRetry");
				t.setDaemon(true);
				return t;
			}
		});
		schedule();
		GracefulStop.register("http result retry", new Runnable() {
			public void run() {
				flush();
			}
		});
	}

	private void schedule() {
		timer.schedule(new Runnable() {
			public void run() {
				try {
					round(false);
				} catch (Throwable e) {
					log.warn("[HTTP-RESULT-RETRY] round failed", e);
				} finally {
					schedule();
				}
			}
		}, intervalMs(), TimeUnit.MILLISECONDS);
	}

	/** the process stops: one more try for each, what is left is named */
	void flush() {
		if (tasks.isEmpty()) {
			return;
		}
		round(true);
		if (!tasks.isEmpty()) {
			List<String> ids = new ArrayList<String>();
			for (Task t : tasks.values()) {
				ids.add(t.msg.getMsgID());
			}
			log.error("[HTTP-RESULT-RETRY] stopping with {} result(s) not written, Core reports them 2700: {}", ids.size(), ids);
		}
	}

	private void round(boolean last) {
		int max = maxTries();
		for (Map.Entry<String, Task> e : tasks.entrySet()) {
			Task t = e.getValue();
			t.tries++;
			if (!t.marked) {
				t.marked = ClientSendMarker.markSent(t.msg);
			}
			if (!t.produced) {
				try {
					t.produced = StreamQueueManager.getInstance().produceResult(t.msg);
				} catch (Exception ex) {
					log.debug(t.msg, "result not written", ex);
				}
			}
			if (t.marked && t.produced) {
				ack(t);
				tasks.remove(e.getKey(), t);
				log.info(t.msg, "[HTTP-RESULT-RETRY] written after {} more tries. outMsgId={}", t.tries, t.msg.getOutMsgID());
			} else if (!last && t.tries >= max) {
				tasks.remove(e.getKey(), t);
				log.error(t.msg, "[HTTP-RESULT-RETRY] given up after {} tries ({}), Core reports 2700 after CoreOutboundRespTimeoutMs. outMsgId={}",
						t.tries, t.marked ? "result not written" : "send marker and result not written", t.msg.getOutMsgID());
			}
		}
	}

	/** an ack that fails is done by the redelivery: the marker says sent, it only acks */
	private void ack(Task t) {
		try {
			if (t.useMarker) {
				ClientSendMarker.ackSent(t.streamKey, t.group, t.redisId, t.msg);
			} else {
				StreamQueueManager.getInstance().ackById(t.streamKey, t.group, t.redisId, t.msg, true);
			}
		} catch (Exception e) {
			log.warn(t.msg, "[HTTP-RESULT-RETRY] ack failed, the redelivery acks it", e);
		}
	}

	private static long intervalMs() {
		try {
			return Math.max(100L, Long.parseLong(GmmsUtility.getInstance().getCommonProperty("HttpResultRetryIntervalMs",
					String.valueOf(DEFAULT_INTERVAL_MS)).trim()));
		} catch (Exception e) {
			return DEFAULT_INTERVAL_MS;
		}
	}

	private static int maxTries() {
		try {
			return Math.max(1, Integer.parseInt(GmmsUtility.getInstance().getCommonProperty("HttpResultRetryMax",
					String.valueOf(DEFAULT_MAX)).trim()));
		} catch (Exception e) {
			return DEFAULT_MAX;
		}
	}
}
