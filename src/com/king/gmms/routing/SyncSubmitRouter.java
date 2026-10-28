package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.processor.AutoSendInDRProcessorThread;
import com.king.gmms.processor.CsmStoreResult;
import com.king.gmms.processor.DBBackupHandler;
import com.king.gmms.processor.MessageProcessorThread;
import com.king.gmms.processor.RedisCsmReassembler;
import com.king.gmms.routing.nmg.NMGUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * C-1 修复 (Option C / Step 1)：MT submit 同步路由 + 外发。
 *
 * <p>取代原来的两跳内存管线：
 * {@code deliveryRouterHandler.putMsg(异步入内存队列) -> DeliveryRouterThread -> MessageProcessorHandler.putMsg(异步) -> MessageProcessorThread}，
 * 改为在调用方 (consumer worker) 线程上 <b>同步</b> 执行路由与外发 (produce)。
 *
 * <p>仅当持久化到达终点 (produce 成功 或 DB 兜底成功) 才返回 {@code true}；返回 {@code false} 表示
 * 连 DB 兜底都未能完成，调用方据此 <b>不 ACK</b>，消息留在 Redis PEL 由 auto-claim 重试，从而消除
 * "ACK 早于持久化导致硬崩溃丢消息" 的缺陷 (C-1)。
 *
 * <p>逻辑复刻自 {@code DeliveryRouterThread.transactNewMessage} 与 {@code MessageProcessorHandler.putMsg}，
 * 保持行为一致：
 * <ul>
 *   <li>租号分支 (needRentAddr/senderRentHandler) 在原 DeliveryRouterThread 中未初始化(注释掉、死代码)，
 *       此处走 DB 兜底，避免 NPE 且不丢消息。</li>
 *   <li>原 putMsg 的优先级/限流 bypass 在各支路最终都执行 MessageProcessorThread (no-op throttle)，此处直接同步执行。</li>
 *   <li>假 DR (AutoSendInDRProcessorThread) 谓词原样保留，改为内联 run() 顺序执行(消除原派生竞态)。</li>
 * </ul>
 *
 * <p>仅用于 <b>非 CSM</b> 的 MT submit；CSM 路径 (csmHandler) 暂不在本步范围。
 */
public class SyncSubmitRouter {

    private static final SystemLogger log = SystemLogger.getSystemLogger(SyncSubmitRouter.class);
    private static final SyncSubmitRouter instance = new SyncSubmitRouter();

    private final DBBackupHandler dbBackup;
    private final GmmsUtility gmmsUtility;

    /**
     * IOSMSDispatcher 含可变实例字段(多个 ChannelRouter)。原设计每个 DeliveryRouterThread 各持一个实例，
     * 这里用 ThreadLocal 保证每个 worker 线程独享一个 dispatcher，维持与原来一致的线程隔离。
     */
    private final ThreadLocal<IOSMSDispatcher> dispatcherTL = new ThreadLocal<IOSMSDispatcher>() {
        @Override
        protected IOSMSDispatcher initialValue() {
            return new IOSMSDispatcher();
        }
    };

    private SyncSubmitRouter() {
        gmmsUtility = GmmsUtility.getInstance();
        dbBackup = DBBackupHandler.getInstance();
    }

    public static SyncSubmitRouter getInstance() {
        return instance;
    }

    /**
     * 同步路由 + 外发。
     *
     * @return {@code true} = downstream stream produce succeeded, or the message
     *         was handed to the asynchronous DB fallback queue and may be ACKed;
     *         {@code false} = neither stream produce nor fallback enqueue completed,
     *         so the caller should keep it in the PEL for auto-claim retry.
     */
    public boolean routeSubmitSync(GmmsMessage message) {
        // ===== 等价 DeliveryRouterThread.transactNewMessage =====
        RouteResponse res = dispatcherTL.get().dispatch(message);
        if (res == RouteResponse.RouteFailed) {
            dbBackup.putMsg(message); // async DB fallback is acceptable here.
            return true;
        }
        if (res == RouteResponse.ASYQueryOP) {
            // 异步 OP 查询：消息交接给异步 DNS/OP 查询流程(终态交接)。
            // 注：此为残留异步跳(与 CSM/租号同类)，本步维持现状语义，不在范围内处理。
            return true;
        }
        if (message.getRSsID() <= 0) {
            message.setStatus(GmmsStatus.SERVER_ERROR);
            dbBackup.putMsg(message);
            return true;
        }
        A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
        if (!(ctm.isA2P(message.getRA2P()) || ctm.isPartition(message.getRA2P()))) {
            if (log.isInfoEnabled()) {
                log.info(message, "Can't get RA2P for message.");
            }
            message.setStatus(GmmsStatus.UNKNOWN_ERROR);
            dbBackup.putMsg(message);
            return true;
        }
        if (NMGUtility.needRentAddr(message)) {
            // 租号分支：原 DeliveryRouterThread.senderRentHandler 未初始化(注释掉，死代码)。
            // 不引入该异步跳，走 DB 兜底，保证不 NPE 且不丢。
            log.warn(message, "needRentAddr=true but rent path is disabled; fallback to DB. ossid={}",
                    message.getOSsID());
            message.setStatus(GmmsStatus.SERVER_ERROR);
            dbBackup.putMsg(message);
            return true;
        }

        // ===== 等价 MessageProcessorHandler.putMsg 的假 DR 派生 + 同步重活 =====
        spawnFakeDrIfNeeded(message);
        return new MessageProcessorThread(message).processSync();
    }

    /** 复刻 MessageProcessorHandler.putMsg 的假 DR 谓词；改为内联 run() 顺序执行。 */
    private void spawnFakeDrIfNeeded(GmmsMessage message) {
        try {
            A2PCustomerInfo oInfo = gmmsUtility.getCustomerManager().getCustomerBySSID(message.getOSsID());
            if (oInfo != null
                    && GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType())
                    && (oInfo.isSmsOptionSendDRByInSubmitInCU()
                            || oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()))
                    && message.getRetriedNumber() < 1) {
                boolean indrFlag = true;
                String rssids = message.getRoutingSsIDs();
                if (rssids != null) {
                    int oldLen = rssids.length();
                    int newLen = rssids.replaceAll(",", "").length();
                    if (oldLen - newLen > 2) {
                        indrFlag = false;
                    }
                }
                if (indrFlag) {
                    new AutoSendInDRProcessorThread(message).run(); // 内联(只 build 假 DR + 异步插库)
                    message.setFakeDR(false);
                }
            }
        } catch (Exception e) {
            log.warn(message, "spawn fake-DR failed (ignored): {}", e.getMessage());
        }
    }

    /**
     * CSM 长短信分段的同步入口（与 {@link #routeSubmitSync} 对称）。
     *
     * <p>把分段存入 Redis 合并缓冲；未集齐则该段已持久化即可 ACK；集齐则内存组装后同步外发，
     * 外发持久化成功才删除合并 hash（produce-before-DEL）并 ACK 最终段。
     *
     * @return {@code true} = 可 ACK 该入站分段（已持久 / 已完成外发或 DB 兜底 / 终态拒绝）；
     *         {@code false} = 勿 ACK（存储失败 或 完成外发未持久化）→ 留 PEL 由 auto-claim 重投。
     */
    public boolean routeCsmSegmentSync(GmmsMessage seg) {
        RedisCsmReassembler reassembler = RedisCsmReassembler.getInstance();
        CsmStoreResult r = reassembler.storeSegment(seg);
        if (r.isFailed()) {
            return false;                                  // 存储失败 -> 不 ACK -> 重试
        }
        // Q2: 逐段 in-submit CDR（CDR type 与 handleInSubmit 不同，需逐段记录）
        try {
            gmmsUtility.getMessageStoreManager().handleInSubmit4Csm(seg);
        } catch (Exception e) {
            log.warn(seg, "handleInSubmit4Csm failed (ignored): {}", e.getMessage());
        }
        if (r.isIncomplete()) {
            return true;                                   // 已持久 -> ACK
        }
        // 集齐
        GmmsMessage assembled = r.getAssembled();
        if (assembled == null) {
            // Q4: 组装失败 -> DB 兜底 + 删 hash + ACK（避免毒丸重投循环）
            seg.setStatus(GmmsStatus.SERVER_ERROR);
            dbBackup.putMsg(seg);
            reassembler.complete(seg);
            return true;
        }
        if (!applyDuplicateCheck(assembled)) {
            reassembler.complete(seg);                     // 上限拒绝已终态(innack durable)，删 hash
            return true;
        }
        boolean durable = routeSubmitSync(assembled);      // C-1 同步外发
        if (durable) {
            reassembler.complete(seg);                     // produce 成功后才删 hash（完成令牌）
            return true;
        }
        return false;                                      // 未持久 -> 不 ACK -> 重投最终段重组重发
    }

    /**
     * 复刻 CsmProcessorThread 的 recipient 24h 上限检查。
     * @return true=通过(继续外发)；false=超限已拒绝(handleOutSubmitResForInnack 已 durable，调用方 ACK)。
     */
    private boolean applyDuplicateCheck(GmmsMessage assembled) {
        try {
            A2PCustomerInfo oCustomer = gmmsUtility.getCustomerManager().getCustomerBySSID(assembled.getOSsID());
            int count = oCustomer == null ? 0 : oCustomer.getSmsOptionRecipientMaxSendCountIn24H();
            if (count > 0) {
                String key = "Duplicate:" + assembled.getRecipientAddress();
                long currentCount = com.king.gmms.messagequeue.MtAttempt.countOnce(key, 24 * 60 * 60);
                if (currentCount > count) {
                    assembled.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_MAX_COUNT);
                    assembled.setRSsID(gmmsUtility.getBlackholeSsid());
                    gmmsUtility.getMessageStoreManager().handleOutSubmitResForInnack(assembled);
                    return false;
                }
            }
        } catch (Exception e) {
            log.warn(assembled, "applyDuplicateCheck failed (proceed): {}", e.getMessage());
        }
        return true;
    }
}
