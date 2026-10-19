package com.king.gmms.mqm.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.gmms.processor.CsmUtility;
import com.king.gmms.processor.RedisCsmReassembler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.SerializableHandler;

/**
 * CSM Redis Hash 合并缓冲的过期清理任务（取代旧 CSMQ 的 {@code ExpiredMessageTask}）。
 *
 * <p>跑在 active MQM（单实例）→ 无多节点竞争；从 {@code csm:index} 原子认领到期项，对仍未集齐的残段生成
 * EXPIRED/UNDELIVERABLE 过期 DR 回客户（{@link CsmUtility#splitCsmDr} 逐段拆分，经
 * {@code messageSenders} → MQM report 流 → core 路由回客户），并删除残留 hash。
 *
 * <p>与旧 CSMQ {@code ExpiredMessageTask} 并存注册即可：legacy 模式下 {@code csm:index} 为空 → 本任务空转；
 * redis 模式下 CSMQ DB 为空 → 旧任务空转。互不干扰，无需配置开关。
 */
public class CsmExpiryTask extends TaskTimer {

	private static SystemLogger log = SystemLogger.getSystemLogger(CsmExpiryTask.class);
	private int batch = 500;

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
		log.info("init Timer: CsmExpiryTask for table: {}, Interval:{}, Batch:{}", tableName, interval, batch);
	}

	@Override
	public void executeTask() {
		try {
			long now = System.currentTimeMillis();
			List<String> dueKeys = GmmsUtility.getInstance().getRedisClient()
					.csmClaimExpired(RedisCsmReassembler.INDEX_ZSET, now, batch);
			if (dueKeys == null || dueKeys.isEmpty()) {
				return;
			}
			A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
			List<GmmsMessage> drList = new ArrayList<GmmsMessage>();
			int incomplete = 0;
			for (String hashKey : dueKeys) {
				Map<String, String> fields = GmmsUtility.getInstance().getRedisClient().hgetAll(hashKey);
				GmmsUtility.getInstance().getRedisClient().delKey(hashKey); // 已从 index 认领，删残留 hash
				if (fields == null || fields.isEmpty()) {
					continue; // 已完成/已被删
				}
				List<GmmsMessage> segs = new ArrayList<GmmsMessage>();
				int total = 0;
				for (String payload : fields.values()) {
					if (payload == null) {
						continue;
					}
					GmmsMessage m = SerializableHandler.convertRedisMssage2GmmsMessage(payload);
					if (m == null) {
						continue;
					}
					segs.add(m);
					if (m.getSarTotalSeqments() > total) {
						total = m.getSarTotalSeqments();
					}
				}
				if (segs.isEmpty()) {
					continue;
				}
				if (total > 0 && segs.size() >= total) {
					continue; // 实为集齐(completer 未删的孤儿) → 不发过期 DR
				}
				incomplete++;
				buildExpiredDr(segs, ctm, drList);
			}
			if (!drList.isEmpty() && messageSenders != null && !messageSenders.isEmpty()) {
				int idx = (int) (now % messageSenders.size());
				messageSenders.get(idx).putMsg(new ArrayList<GmmsMessage>(drList));
				if (log.isInfoEnabled()) {
					log.info("CsmExpiryTask sent {} expired CSM DR(s) for {} incomplete group(s).",
							drList.size(), incomplete);
				}
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	/** 用已收残段构造过期 DR：拼接各段 InMsgID → {@link CsmUtility#splitCsmDr} 逐段生成 DR。 */
	private void buildExpiredDr(List<GmmsMessage> segs, A2PCustomerManager ctm, List<GmmsMessage> out) {
		try {
			GmmsMessage rep = new GmmsMessage(segs.get(0));
			StringBuilder inIds = new StringBuilder();
			for (GmmsMessage s : segs) {
				inIds.append(s.getInMsgID()).append(CsmUtility.CSM_ID_SEPERATOR);
			}
			if (inIds.length() >= CsmUtility.CSM_ID_SEPERATOR.length()) {
				rep.setInMsgID(inIds.substring(0, inIds.length() - CsmUtility.CSM_ID_SEPERATOR.length()));
			}
			rep.setStatus(ctm.isSupportExpiredDR(rep.getOSsID()) ? GmmsStatus.EXPIRED : GmmsStatus.UNDELIVERABLE);
			rep.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			CsmUtility.splitCsmDr(rep, out);
		} catch (Exception e) {
			log.error("CsmExpiryTask buildExpiredDr failed", e);
		}
	}
}
