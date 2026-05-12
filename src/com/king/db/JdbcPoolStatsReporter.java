package com.king.db;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;

public class JdbcPoolStatsReporter {
	private static final SystemLogger log = SystemLogger.getSystemLogger(JdbcPoolStatsReporter.class);
	private static final JdbcPoolStatsReporter INSTANCE = new JdbcPoolStatsReporter();

	private ScheduledExecutorService scheduler;
	private volatile boolean running = false;

	private JdbcPoolStatsReporter() {
	}

	public static JdbcPoolStatsReporter getInstance() {
		return INSTANCE;
	}

	public synchronized void start(int intervalSeconds) {
		if (running) {
			return;
		}
		if (intervalSeconds <= 0) {
			intervalSeconds = 60;
		}
		running = true;
		final int interval = intervalSeconds;
		scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "A2P-DBPoolStatsReporter");
				t.setDaemon(true);
				return t;
			}
		});
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					DataControl.logPoolStats();
				} catch (Exception e) {
					log.warn("JdbcPoolStatsReporter error", e);
				}
			}
		}, interval, interval, TimeUnit.SECONDS);
		log.info("JdbcPoolStatsReporter started, interval={}s", interval);
	}

	public synchronized void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (scheduler != null) {
			scheduler.shutdown();
			scheduler = null;
		}
		log.info("JdbcPoolStatsReporter stopped.");
	}

	public synchronized void restart(int intervalSeconds) {
		stop();
		start(intervalSeconds);
		log.info("JdbcPoolStatsReporter restarted, interval={}s", intervalSeconds);
	}
}
