package com.king.gmms.ha;

import java.sql.Connection;

import com.king.db.DataControl;
import com.king.db.DatabaseStatus;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.mqm.MqmActiveState;

/**
 * DB 健康单点探测器（DB HA Phase 2）。
 *
 * <p>在每个 MQM 实例上运行，但**仅在 active MQM 上实际探测**（运行时 {@link MqmActiveState#isActive()} 门控，
 * 复用 MQM 选主 → 全集群唯一探测决策者，避免各节点独立翻转导致状态分裂）。
 *
 * <p>探测"当前在用 DB"健康：仅当**当前在用 master** 时自动探测——master 连续探测失败 ≥ 阈值 且 slave 可达，
 * 则向控制通道发布 {@code SWITCH_DB:slave} 并写状态键，由各进程 {@link DbSwitchListener} 一致切换。
 * **切回 master 仅手动**（D4：避免主从未追平自动 failback 丢数据），故本探测器**不**自动 slave→master。
 *
 * <p>默认关闭（{@code DbHa.AutoDetect.Enable=false}）；自动 DB 故障转移影响面大，建议先在日志观察、确认后再开。
 */
public class DbHealthDetector implements Runnable {

	private static final SystemLogger log = SystemLogger.getSystemLogger(DbHealthDetector.class);
	private static final String CONTROL_CHANNEL = "channel:gmms:control";

	private final boolean enabled;
	private final long intervalMs;
	private final int failThreshold;
	private final long minSwitchIntervalMs;
	private final String dsName;

	private volatile boolean running = true;
	private int masterFailCount = 0;
	private long lastSwitchMs = 0L;

	public DbHealthDetector() {
		GmmsUtility gu = GmmsUtility.getInstance();
		this.enabled = "true".equalsIgnoreCase(gu.getCommonProperty("DbHa.AutoDetect.Enable", "false"));
		this.intervalMs = parseLong(gu.getCommonProperty("DbHa.AutoDetect.IntervalMs", "5000"), 5000L);
		this.failThreshold = (int) parseLong(gu.getCommonProperty("DbHa.AutoDetect.FailThreshold", "3"), 3L);
		this.minSwitchIntervalMs = parseLong(gu.getCommonProperty("DbHa.AutoDetect.MinSwitchIntervalMs", "60000"), 60000L);
		this.dsName = gu.getCommonProperty("DbHa.AutoDetect.DsName", "gmms");
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void stop() {
		running = false;
	}

	@Override
	public void run() {
		log.info("DbHealthDetector started. interval={}ms, failThreshold={}, minSwitchInterval={}ms, dsName={}",
				intervalMs, failThreshold, minSwitchIntervalMs, dsName);
		while (running) {
			try {
				Thread.sleep(intervalMs);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				break;
			}
			try {
				if (!MqmActiveState.isActive()) { // 仅 active MQM 探测
					masterFailCount = 0;
					continue;
				}
				detectOnce();
			} catch (Exception e) {
				log.error("DbHealthDetector tick failed", e);
			}
		}
		log.info("DbHealthDetector stopped.");
	}

	private void detectOnce() {
		// 仅自动 master->slave（D4：切回 master 手动）
		if (DataControl.getInstance().getUsedDatabaseStatus() != DatabaseStatus.MASTER_USED) {
			masterFailCount = 0;
			return;
		}
		if (probe(false)) { // master 可用
			masterFailCount = 0;
			return;
		}
		masterFailCount++;
		log.warn("DbHealthDetector: master probe failed ({}/{}).", masterFailCount, failThreshold);
		if (masterFailCount < failThreshold) {
			return;
		}
		if (!probe(true)) { // slave 不可达 → 切了也没用，避免抖动
			log.error("DbHealthDetector: master DOWN but slave also unreachable; NOT switching.");
			return;
		}
		long now = System.currentTimeMillis();
		if (now - lastSwitchMs < minSwitchIntervalMs) {
			log.warn("DbHealthDetector: switch suppressed by min-switch-interval.");
			return;
		}
		// D6: 一致流程「写真源 → 本地立即应用 → 广播」。setHandover 幂等(同值早返回)，
		// 本地直接应用确保决策进程绝不与刚写的真源不一致；publish 给其它进程。
		// 仅当 publish 成功才更新 lastSwitchMs/masterFailCount —— 失败则下一 tick 重试广播
		// (此时真源与本进程已是 slave，仅其它进程待对齐)，消除"真源已切、无人应用"的窗口。
		GmmsUtility gu = GmmsUtility.getInstance();
		try {
			gu.getRedisClient().setDbStatus("slave");
		} catch (Exception e) {
			log.error("DbHealthDetector: setDbStatus(slave) failed; will retry next tick", e);
			return;
		}
		try {
			gu.setHandover(DatabaseStatus.SLAVE_USED); // 本地立即应用(幂等)
		} catch (Exception e) {
			log.error("DbHealthDetector: local setHandover(slave) failed", e);
		}
		try {
			gu.getRedisClient().publishState(CONTROL_CHANNEL, "SWITCH_DB:slave");
			lastSwitchMs = now;
			masterFailCount = 0;
			log.error("DbHealthDetector: master DOWN, applied locally and published SWITCH_DB:slave to {}.", CONTROL_CHANNEL);
		} catch (Exception e) {
			log.error("DbHealthDetector: publish SWITCH_DB failed; real-source+local already slave, will retry broadcast next tick", e);
		}
	}

	/** 探测 slave(true)/master(false) 是否可用。 */
	private boolean probe(boolean slave) {
		Connection c = null;
		String ds = slave ? ("backup" + dsName) : dsName;
		try {
			c = slave ? DataControl.getSlaveConnection(dsName) : DataControl.getMasterConnection(dsName);
			return c != null && c.isValid(2);
		} catch (Exception e) {
			return false;
		} finally {
			DataControl.releaseConnection(ds, c);
		}
	}

	private static long parseLong(String s, long def) {
		try {
			return Long.parseLong(s.trim());
		} catch (Exception e) {
			return def;
		}
	}
}
