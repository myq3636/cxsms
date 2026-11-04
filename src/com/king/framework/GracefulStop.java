package com.king.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;

/**
 * What a module does when it stops (SIGTERM, or SHUTDOWN@module): stop reading, write what only memory
 * holds, and be gone within ModuleStopMaxWaitSeconds whatever hangs. Entries being worked on are not waited
 * for: they are not acked, another node takes them over. Draining a module for good is a separate, longer step.
 */
public final class GracefulStop implements LifecycleListener {
	private static final SystemLogger log = SystemLogger.getSystemLogger(GracefulStop.class);
	private static final int EXIT_CODE_SIGTERM = 143;
	private static final long DEFAULT_WAIT_SECONDS = 4L;
	/** keep this much of the time for the JVM to go down */
	private static final long EXIT_MARGIN_MS = 200L;

	private static final Map<String, Runnable> tasks = new ConcurrentHashMap<String, Runnable>();
	private static volatile A2PService service = null;
	private static volatile long deadline = 0L;
	private static boolean installed = false;

	private GracefulStop() {
	}

	/** A job that writes what memory holds. It may run more than once and must end by itself near remainingMs(). */
	public static void register(String name, Runnable task) {
		tasks.put(name, task);
	}

	public static void setService(A2PService s) {
		service = s;
	}

	/** Called once the module has a Redis client; every kind of module passes there. */
	public static synchronized void install(GmmsUtility gmmsUtility) {
		if (installed) {
			return;
		}
		installed = true;
		gmmsUtility.getLifecycleSupport().addListener(Event.TYPE_SHUTDOWN, new GracefulStop(), STOP_PRIORITY_0);
		Runtime.getRuntime().addShutdownHook(new Thread("GracefulStop") {
			public void run() {
				long waitMs = waitMs();
				startWatchdog(waitMs);
				runTasks("SIGTERM", waitMs - EXIT_MARGIN_MS);
			}
		});
	}

	public int OnEvent(Event event) {
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			runTasks("SHUTDOWN", waitMs() - EXIT_MARGIN_MS);
		}
		return 1;
	}

	private static long waitMs() {
		long seconds = DEFAULT_WAIT_SECONDS;
		try {
			seconds = Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ModuleStopMaxWaitSeconds",
					String.valueOf(DEFAULT_WAIT_SECONDS)).trim());
		} catch (Exception e) {
			log.warn("[STOP] invalid ModuleStopMaxWaitSeconds, using {}", DEFAULT_WAIT_SECONDS);
		}
		return Math.max(1L, seconds) * 1000L;
	}

	private static void startWatchdog(final long waitMs) {
		Thread watchdog = new Thread("GracefulStopWatchdog") {
			public void run() {
				try {
					Thread.sleep(waitMs);
				} catch (InterruptedException e) {
					return;
				}
				System.out.println("[STOP] still not finished after " + waitMs + " ms, exiting now");
				Runtime.getRuntime().halt(EXIT_CODE_SIGTERM);
			}
		};
		watchdog.setDaemon(true);
		watchdog.start();
	}

	/** Every job in its own thread: one that hangs does not hold up the others. */
	private static void runTasks(String why, long limitMs) {
		long start = System.currentTimeMillis();
		deadline = start + limitMs;
		Map<String, Runnable> all = new ConcurrentHashMap<String, Runnable>(tasks);
		final A2PService s = service;
		if (s != null) {
			all.put("stop reading", new Runnable() {
				public void run() {
					s.stopReading();
				}
			});
		}
		final CountDownLatch done = new CountDownLatch(all.size());
		final List<String> finished = java.util.Collections.synchronizedList(new ArrayList<String>());
		for (final Map.Entry<String, Runnable> task : all.entrySet()) {
			Thread t = new Thread("GracefulStop-" + task.getKey()) {
				public void run() {
					try {
						task.getValue().run();
						finished.add(task.getKey());
					} catch (Throwable e) {
						log.warn("[STOP] " + task.getKey() + " failed", e);
					} finally {
						done.countDown();
					}
				}
			};
			t.setDaemon(true);
			t.start();
		}
		try {
			done.await(limitMs, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		List<String> left = new ArrayList<String>(all.keySet());
		left.removeAll(finished);
		// also on stdout: the logging may be going down with the process
		String text = "[STOP] " + why + ": " + finished.size() + " of " + all.size() + " jobs finished in "
				+ (System.currentTimeMillis() - start) + " ms" + (left.isEmpty() ? "" : ", not finished: " + left);
		log.info(text);
		System.out.println(text);
	}

	/** How long a job may still take. */
	public static long remainingMs() {
		return deadline - System.currentTimeMillis();
	}
}
