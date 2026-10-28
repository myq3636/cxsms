package com.king.gmms.mqm.task;

import java.util.ArrayList;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.gmms.processor.CsmExpirySweeper;
import com.king.gmms.processor.CsmUtility;
import com.king.message.gmms.GmmsMessage;

/**
 * CSM Redis Hash 合并缓冲的过期清理任务（取代旧 CSMQ 的 {@code ExpiredMessageTask}）。
 *
 * <p>跑在 active MQM；领取与收尾和 Core 的 {@link CsmExpirySweeper} 相同（领取后占用 LeaseMs，DR 交给
 * {@code messageSenders} 后才收尾），两边可以同时运行。对仍未集齐的残段生成 EXPIRED/UNDELIVERABLE
 * 过期 DR 回客户（{@link CsmUtility#splitCsmDr} 逐段拆分，经 MQM report 流 → core 路由回客户）。
 *
 * <p>与旧 CSMQ {@code ExpiredMessageTask} 并存注册即可：legacy 模式下 {@code csm:index} 为空 → 本任务空转；
 * redis 模式下 CSMQ DB 为空 → 旧任务空转。互不干扰，无需配置开关。
 */
public class CsmExpiryTask extends TaskTimer {

	private static SystemLogger log = SystemLogger.getSystemLogger(CsmExpiryTask.class);
	private int batch = 500;
	private long leaseMs = 300000L;

	public CsmExpiryTask() {
		timer = new Timer("CsmExpiryTask");
	}

	@Override
	public void init(String tableName) {
		taskTable = tableName;
		taskName = "CsmExpiryTask";
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(taskTable + "." + taskName + ".Interval", "60")) * 1000L;
		batch = Integer.parseInt(config.getProperty(taskTable + "." + taskName + ".Batch", "500"));
		leaseMs = Long.parseLong(config.getProperty(taskTable + "." + taskName + ".LeaseMs", "300000"));
		log.info("init Timer: CsmExpiryTask for table: {}, Interval:{}, Batch:{}", tableName, interval, batch);
	}

	@Override
	public void executeTask() {
		try {
			if (messageSenders == null || messageSenders.isEmpty()) {
				return;
			}
			int n = CsmExpirySweeper.sweepOnce(batch, leaseMs, new CsmExpirySweeper.DrSink() {
				public boolean send(GmmsMessage expiredDr) {
					ArrayList<GmmsMessage> drList = new ArrayList<GmmsMessage>();
					CsmUtility.splitCsmDr(expiredDr, drList);
					int idx = (int) (System.currentTimeMillis() % messageSenders.size());
					messageSenders.get(idx).putMsg(drList);
					return true;
				}
			});
			if (n > 0 && log.isInfoEnabled()) {
				log.info("CsmExpiryTask expired {} incomplete CSM group(s).", n);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}
}
