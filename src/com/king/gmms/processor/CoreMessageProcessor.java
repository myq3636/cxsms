package com.king.gmms.processor;

import com.king.db.DataManagerException;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.milter.ContentScanMilter;
import com.king.gmms.recipientlimit.RecipientSendCountLimitService;
import com.king.gmms.routing.DeliveryRouterHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.message.gmms.SdqMessageManager;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;
import java.util.Date;
import java.util.List;

/**
 * Core's processing of the stream entries (docs/流模式下系统管理和模块间连接代码清理分析.md 3.4): the customer's MT
 * (MTStreamConsumer), the channel's submit result (ResultStreamConsumer), the channel's DR (InboundDRStreamConsumer) and
 * the MQM's submits and reports (CoreMQMStreamConsumer). It was the processing of InternalCoreEngineSession, the TCP
 * session between the modules before the streams; one instance, no connection.
 */
public class CoreMessageProcessor {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CoreMessageProcessor.class);
	protected GmmsUtility gmmsUtility;
	private MessageStoreManager msgStoreManager = null;
	private DeliveryRouterHandler deliveryRouterHandler = null;
	private MessageProcessorHandler processorHandler = null;
	private A2PCustomerManager ctm;
	private ModuleManager moduleManager;
	private SdqMessageManager sdqManager = null;
	private ContentScanMilter csm;
	private boolean isRedisEnable = false;
	private boolean isStoreDRModeEnable = false;
	private RedisClient redis = null;

	// volatile double-checked: one volatile read per entry on the fast path, no lock
	private static volatile CoreMessageProcessor instance = null;
	public static CoreMessageProcessor getInstance() {
		CoreMessageProcessor local = instance;
		if (local == null) {
			synchronized (CoreMessageProcessor.class) {
				local = instance;
				if (local == null) {
					local = new CoreMessageProcessor();
					instance = local;
				}
			}
		}
		return local;
	}

	protected CoreMessageProcessor() {
		gmmsUtility = GmmsUtility.getInstance();
		moduleManager = ModuleManager.getInstance();
		msgStoreManager = gmmsUtility.getMessageStoreManager();
		ctm = gmmsUtility.getCustomerManager();
		csm = ContentScanMilter.getInstance();
		isStoreDRModeEnable = gmmsUtility.isStoreDRModeEnable();
		if ("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))) {
			isRedisEnable = true;
			redis = gmmsUtility.getRedisClient();
		}
		deliveryRouterHandler = DeliveryRouterHandler.getInstance();
		processorHandler = MessageProcessorHandler.getInstance();
		sdqManager = SdqMessageManager.getInstance();
	}

	public boolean handleOutboundResultInternal(GmmsMessage msg) {
		if (msg == null) {
			log.warn("Null outbound result from Redis Stream");
			return true;
		}
		boolean result = false;
		try {
			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())) {
				msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				msgStoreManager.handleInDeliveryReportRes(msg);
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
				msgStoreManager.handleInDeliveryReportRes(msg);
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
				msgStoreManager.handleOutDeliveryReportRes(msg);
			} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())) {
				A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getRSsID());
				if (cst != null) {
					msg.setOutClientPull("1".equals(cst.getOutClientPull()));
				}
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
				msgStoreManager.handleOutSubmitRes(msg, true);
			} else {
				log.warn(msg, "Unknown outbound result message type: {}", msg.getMessageType());
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				msgStoreManager.handleMessageError(msg);
			}
			result = true;
		} catch (Exception e) {
			log.error(msg, "Failed to handle outbound result from Redis Stream", e);
		}
		return result;
	}
    /**
     * V4.0 Stream Path Entry Point
     * 供 MTStreamConsumer 调用：从 Redis Stream 消费到消息后，执行与 TCP 路径
     * (processSuccessSubmit / processErrorSubmit) 完全相同的业务检查逻辑，
     * 但不使用 inBuffer + sendPdu(ack)，因为所有消息均走 Stream，Core 不再需要
     * 通过 TCP 向 Function 模块发送 CommandSubmitAck。
     */
    public boolean processSubmitFromStream(GmmsMessage msg) {
    	boolean result = false;
    	if (msg.getStatusCode() < 0) {
    		result = processSuccessSubmitFromStream(msg);
    	} else {
    		msg.setStatusText(GmmsStatus.getStatus(msg.getStatusCode()).getText());
    		// error path: the CDR, and the error DR for an HTTP (Common) customer through msgStoreManager
    		result = processErrorSubmitFromStream(msg);
    	}
    	return result;
    }
    /**
     * V4.0 内部辅助方法：Stream 路径成功提交处理
     * 与 processSuccessSubmit 逻辑完全一致，但最终将 inBuffer + sendPdu(ack) 替换为
     * 直接调用 processInnerAck，实现无 TCP 反馈的业务路由。
     */
    private boolean processSuccessSubmitFromStream(GmmsMessage msg) {
        msg.setOriginalSenderAddr(msg.getSenderAddress());
        A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());
        if (cst == null) {
        	log.warn(msg, "processSuccessSubmitFromStream: customer not found for OSsID={}", msg.getOSsID());
        	msg.setStatus(GmmsStatus.SERVER_ERROR);
        	gmmsUtility.getCdrManager().logInSubmit(msg);
        	return true;
        }
        String[] address = new String[] { msg.getSenderAddress(), msg.getRecipientAddress() };
        this.handleCommonPrefix4MO(address, cst);
        if (address[0] == null || "".equalsIgnoreCase(address[0])) {
        	msg.setSenderAddress("abc");
        } else {
        	msg.setSenderAddress(address[0]);
        }
        msg.setRecipientAddress(address[1]);
        msg.setInClientPull(cst.getInClientPull());

        // 1. 黑名单/白名单/内容检查（与 processSuccessSubmit 逻辑一致，错误时直接 return）
        if (!"VIP".equalsIgnoreCase(cst.getCustomerTypeBySender())) {
        	String recipientAddress = msg.getRecipientAddress();
        	if (recipientAddress.startsWith("+")) recipientAddress = recipientAddress.substring(1);
        	if (recipientAddress.startsWith("00")) recipientAddress = recipientAddress.substring(2);
        	if (recipientAddress.startsWith("0")) recipientAddress = recipientAddress.substring(1);

        	// NonPA for the recipient's country (SMSOptionCustomerTypeCountry, 65 by default): only the global blacklist
        	if (cst.isNonPaSenderCheck(recipientAddress)) {
        		if (ctm.doSenderBlacklistCheck(-1, msg)) {
        			gmmsUtility.getCdrManager().logInSubmit(msg);
        			if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_SDBL); msgStoreManager.sendDRMessageForServerReject(msg); }
        			return true;
        		}
        	} else {
        		if (!ctm.doSenderBlacklistCheck(msg.getOSsID(), msg)) {
        			if (ctm.doSenderBlacklistCheck(-1, msg)) {
        				gmmsUtility.getCdrManager().logInSubmit(msg);
        				if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_SDBL); msgStoreManager.sendDRMessageForServerReject(msg); }
        				return true;
        			}
        		} else {
        			gmmsUtility.getCdrManager().logInSubmit(msg);
        			if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_SDBL); msgStoreManager.sendDRMessageForServerReject(msg); }
        			return true;
        		}
        	}
        	if (!ctm.doSenderWhitelistCheck(msg.getOSsID(), msg)) {
        		gmmsUtility.getCdrManager().logInSubmit(msg);
        		if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_SDWL); msgStoreManager.sendDRMessageForServerReject(msg); }
        		return true;
        	}
        }
        if (!ctm.doContentBlacklistCheck(msg.getOSsID(), msg)) {
        	if (ctm.doContentBlacklistCheck(-1, msg)) {
        		gmmsUtility.getCdrManager().logInSubmit(msg);
        		if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_CTBL); msgStoreManager.sendDRMessageForServerReject(msg); }
        		return true;
        	}
        } else {
        	gmmsUtility.getCdrManager().logInSubmit(msg);
        	if (msg.getDeliveryReport()) { msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); msg.setRSsID(msg.getOSsID()); msg.setStatus(GmmsStatus.REJECTED_CTBL); msgStoreManager.sendDRMessageForServerReject(msg); }
        	return true;
        }

        // 2. 高级发件人替换
        try {
        	if (!cst.isNeedSupportBlackList() || (gmmsUtility.preBlackListCheck(msg) && cst.isNeedSupportBlackList())) {
        		if (cst.isSmsOptionAdvancedSenderReplacement()) ctm.doSenderAddressReplace(msg.getOSsID(), msg);
        	}
        } catch (Exception e) {
        	log.error("do sender replace in ocustomer failed", e);
        }
        // the recipient before the channel's steps: each send to a channel starts from it, the customer's DR gets it back
        if (msg.getOriginalRecipientAddr() == null || msg.getOriginalRecipientAddr().isEmpty()) {
        	msg.setOriginalRecipientAddr(msg.getRecipientAddress());
        }

        // 3. 过期时间设定
        if (msg.getExpiryDate() == null) {
        	java.util.TimeZone local = java.util.TimeZone.getDefault();
        	long now = new java.util.Date().getTime();
        	long diff = local.getRawOffset();
        	if (local.inDaylightTime(new java.util.Date(now))) diff += local.getDSTSavings();
        	long gmtNow = now - diff;
        	int expireTime = gmmsUtility.getExpireTimeInMinute();
        	if (cst.getExpireTime() > 0) expireTime = cst.getExpireTime();
        	else if (cst.getFinalExpireTime() > 0) expireTime = cst.getFinalExpireTime() * 3 / 4;
        	msg.setExpiryDate(new java.util.Date(gmtNow + expireTime * 60 * 1000));
        }

        // 4. CSM / prepareInSubmit
        if (gmmsUtility.preCheckMessage(msg)) {
        	csm.processGMMS(msg);
        	msgStoreManager.prepareInSubmit(msg);
        	gmmsUtility.afterCheckMessage(msg);
        }

        // 5. 业务路由 — 直接调 processInnerAck，不走 inBuffer + TCP sendPdu(ack)
        try {
        	if (msg.getStatus().getCode() == GmmsStatus.UNASSIGNED.getCode()) {
        		return processInnerAck(msg);
        	} else {
        		gmmsUtility.getCdrManager().logInSubmit(msg);
        		if (msg.getDeliveryReport()) {
        			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        			msg.setRSsID(msg.getOSsID());
        			GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(msg.getStatusText());
        			msg.setStatus(converDRStatus != null ? converDRStatus : GmmsStatus.REJECTED);
        			msgStoreManager.sendDRMessageForServerReject(msg);
        		}
        	}
        } catch (Exception e) {
        	log.warn(msg, "processSuccessSubmitFromStream error", e);
        	return false;
        }
        return true;
    }
    /**
     * V4.0 内部辅助方法：Stream 路径错误提交处理
     * 与 processErrorSubmit 逻辑一致，对 HTTP 协议正常走 sendDRMessage，
     * 对 SMPP（Stream）协议直接写 CDR+DR，忽略 TCP sendPdu(ack)。
     */
    private boolean processErrorSubmitFromStream(GmmsMessage msg) {
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
        	gmmsUtility.getCdrManager().logInSubmit(msg);
        } else {
        	gmmsUtility.getCdrManager().logInDelivery(msg);
        }
        A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());
        // 对 HTTP 协议保持发送错误 DR
        if (cst != null && !cst.getProtocol().startsWith("SMPP") && msg.getDeliveryReport()
                && GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
        	msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        	msg.setRSsID(msg.getOSsID());
        	msg.setOA2P(ctm.getCurrentA2P());
        	msg.setTransaction(null);   // no session in stream mode (it was the session's, never set)
        	msgStoreManager.sendDRMessage(msg);
        }
        // SMPP Stream 路径：不尝试 TCP sendPdu，直接返回 true
        return true;
    }
	public boolean processMqmSubmitFromStream(GmmsMessage msg) {
		try {
			if (msg == null) {
				log.warn("Null submit message when processing MQM stream.");
				return true;
			}
			// not routed yet (rssid; roperator keeps the operator of an operator route)
			if (CsmUtility.isConcatenatedMsg(msg) && msg.getRSsID() < 0) {
				// CSM：Redis Hash 同步合并(持久化后才 ACK)
				if (!com.king.gmms.routing.SyncSubmitRouter.getInstance().routeCsmSegmentSync(msg)) {
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					return false;   // 不 ACK -> MQM 流 PEL auto-claim 重试
				}
			} else {
				// C-1: downstream produce is synchronous; DB fallback is async and acceptable once enqueued.
				// routeSubmitSync returns false only when neither produce nor fallback enqueue completed.
				if (!com.king.gmms.routing.SyncSubmitRouter.getInstance().routeSubmitSync(msg)) {
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			log.warn(msg, "Failed to process MQM submit from Redis Stream", e);
			return false;
		}
	}
	public boolean processMqmReportFromStream(GmmsMessage msg) {
		try {
			if (msg == null) {
				log.warn("Null report message when processing MQM stream.");
				return true;
			}
			msg.setInnerTransaction(null);   // no session in stream mode
			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
				if (!processorHandler.putMsg(msg)) {
					msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
					msgStoreManager.handleInDeliveryReportRes(msg);
				}
				return true;
			}
			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
				if (!processorHandler.putMsg(msg)) {
					msg.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
					msgStoreManager.handleOutDeliveryReportRes(msg);
				}
				return true;
			}
			log.warn(msg, "Unsupported MQM report message type: {}", msg.getMessageType());
			msgStoreManager.handleMessageError(msg);
			return true;
		} catch (Exception e) {
			log.warn(msg, "Failed to process MQM report from Redis Stream", e);
			return false;
		}
	}
	/** the DR of a customer in DR store mode (or of the HTTP server): to SDQ, or (CommonHttpServer) routed at once */
	private boolean processStoredDeliveryReport(GmmsMessage msg) {
		boolean result = false;
		try {
			if (msg == null) {
				return true;
			}
			if (msg.getDeliveryChannel()!=null &&
					msg.getDeliveryChannel().startsWith("CommonHttpServer")) {
				//http protocal, didn't need send response to commonHttpServer.
				result = processInnerAck(msg);
			}else{
				result = sdqManager.insertSDQMessage(msg);
			}
		} catch (Exception e) {
			log.warn(msg, e, e);
			result = false;
		}
		return result;
	}

	/** a vendor DR matched with its original: routed to the customer */
	private boolean processDeliveryReport(GmmsMessage msg) {
		boolean result = false;
		try {
			if (msg != null) {
				result = processInnerAck(msg);
			} else {
				result = true;
			}
		} catch (Exception e) {
			log.warn(msg, e, e);
			result = false;
		}
		return result;
	}

	/**
	 * V4.5 Reactive DR Entry Point
	 * Shared logic between TCP path and Redis Stream path
	 */
	public boolean handleInboundDRInternal(GmmsMessage finalMsg) {
		return handleInboundDR(finalMsg, false);
	}
	/** matchedAgain: its lookup came while it was being offered to the early DR queue, this is the second match */
	private boolean handleInboundDR(GmmsMessage finalMsg, boolean matchedAgain) {
		GmmsMessage message = null;
		if (finalMsg == null) {
			log.warn("Null inbound DR from Redis Stream");
			return true;
		}
		try {
			if (ctm.isInDRStoreMode(finalMsg.getRSsID())
					|| (isStoreDRModeEnable && gmmsUtility.isRunningStoreDRMode())) {
				return processStoredDeliveryReport(finalMsg);
			}
			else{
				boolean deleteLookup = false;
				boolean fromDb = false;
				if(isRedisEnable){
					String object = com.king.gmms.messagequeue.DrLookup.get(finalMsg.getOutMsgID());
					if(object != null){
						log.debug(finalMsg,
								"TMP-DRLOOKUP inbound DR redis hit. msgId={}, in={}, out={}, rsid={}, status={}/{}, sender={}, recipient={}",
								finalMsg.getMsgID(), finalMsg.getInMsgID(), finalMsg.getOutMsgID(),
								finalMsg.getRSsID(), finalMsg.getStatusCode(), finalMsg.getStatusText(),
								finalMsg.getSenderAddress(), finalMsg.getRecipientAddress());
						message = SerializableHandler.convertRedisMssage2GmmsMessage(object);
						if(message!=null){
							log.debug(finalMsg,
									"TMP-DRLOOKUP inbound DR original decoded. originalMsgId={}, originalIn={}, originalOut={}, originalOs={}, originalRs={}, originalDateIn={}, originalExpire={}",
									message.getMsgID(), message.getInMsgID(), message.getOutMsgID(),
									message.getOSsID(), message.getRSsID(), message.getDateIn(),
									message.getExpiryDate());
							// A final DR removes the lookup entry, but only after it has been
							// processed: a failed attempt stays pending and the retry must find it again.
							if (GmmsStatus.ACCEPT.getCode() != finalMsg.getStatus().getCode()) {
								deleteLookup = true;
							}
							if (log.isInfoEnabled()) {
								log.info(finalMsg, "[VDR-MATCH] outMsgId={} status={} rssid={} -> msgId={} ossid={} (redis)",
										finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID(),
										message.getMsgID(), message.getOSsID());
							}
						}
					}else if (com.king.gmms.messagequeue.EarlyDrQueue.enabled()
							&& com.king.gmms.messagequeue.EarlyDrQueue.young(finalMsg)) {
						// not in Redis and received less than T ago: usually earlier than its submit result. Queued at once
						// without a WDQ query; the MQM job looks in WDQ for the batch (docs/DR早到重试与outbox应答超时改进设计.md 4)
						if (log.isDebugEnabled()) {
							log.debug(finalMsg, "[VDR-MATCH] outMsgId={} status={} rssid={} not in redis, early DR queued without a DB lookup",
									finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID());
						}
					}else{
						// a DR that waited longer than T: the DB is tried here (after the lookup was moved to WDQ, ...)
						if (log.isDebugEnabled()) {
							log.debug(finalMsg, "[VDR-MATCH] outMsgId={} status={} rssid={} not in redis, looking up the DB",
									finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID());
						}
						try {
							message = msgStoreManager.getGmmsMessageByOutMsgID(finalMsg.getOutMsgID());
							fromDb = true;
						} catch (DataManagerException e) {
							// the DB is not there: the entry stays pending and is retried
							log.error(finalMsg, "[VDR-MATCH] outMsgId={} status={} rssid={} not in redis, DB lookup failed: left pending",
									finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID());
							return false;
						}
					}
					
				}else{
					try {
						message = msgStoreManager.getGmmsMessageByOutMsgID(finalMsg.getOutMsgID());
						fromDb = true;
					} catch (DataManagerException e) {
						log.error(finalMsg, "[VDR-MATCH] outMsgId={} status={} rssid={} DB lookup failed: left pending",
								finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID());
						return false;
					}
				}
				if(message != null){
					if (fromDb && log.isInfoEnabled()) {
						log.info(finalMsg, "[VDR-MATCH] outMsgId={} -> msgId={} ossid={} (DB)", finalMsg.getOutMsgID(),
								message.getMsgID(), message.getOSsID());
					}
					mergeInboundDeliveryReport(message, finalMsg);
					// the channel's error code -> the internal DR error code (and its status)
					com.king.gmms.domain.DrErrorConfig.getInstance().applyVendorDr(message, finalMsg.getVendorErrorCode());
					// way 4: a final vendor DR ends the timing; after a timeout DR it is only recorded
					com.king.gmms.processor.FakeDr.onVendorDr(message);
					boolean processed = processDeliveryReport(message);
					if (processed && deleteLookup) {
						final GmmsMessage dr = finalMsg;
						// inside a DR attempt only once _done is written: a redelivery after a crash
						// must still find the original and take the same path
						com.king.gmms.messagequeue.StreamAttempt.afterDone(new Runnable() {
							public void run() {
								deleteDrLookup(dr);
							}
						});
					}
					return processed;
				} else {
					// usually the DR came before its submit result was processed (the lookup entry is written then):
					// the MQM job tries it again at t0+T/2 and t0+T, then it is dropped, not stored in SDQ
					// (docs/DR早到重试与outbox应答超时改进设计.md 4); the same entry again (a redelivery) is one queue entry
					if (isRedisEnable && com.king.gmms.messagequeue.EarlyDrQueue.enabled()) {
						// ERROR (Redis): not handled, the entry stays pending and is retried; FOUND: the lookup was
						// written meanwhile (the offer checks it in the same Lua), matched again now
						com.king.gmms.messagequeue.EarlyDrQueue.Offer o = com.king.gmms.messagequeue.EarlyDrQueue.offer(finalMsg);
						if (o == com.king.gmms.messagequeue.EarlyDrQueue.Offer.FOUND) {
							return !matchedAgain && handleInboundDR(finalMsg, true);
						}
						return o != com.king.gmms.messagequeue.EarlyDrQueue.Offer.ERROR;
					}
					if (log.isInfoEnabled()) {
						log.info(finalMsg, "[VDR-STORE] outMsgId={} status={} rssid={} has no original yet, stored for later matching",
								finalMsg.getOutMsgID(), finalMsg.getStatusCode(), finalMsg.getRSsID());
					}
					return processStoredDeliveryReport(finalMsg);
				}
			}
		} catch (Exception e) {
			log.warn(finalMsg, "Failed to handle inbound DR", e);
			return false;
		}
	}
	private void deleteDrLookup(GmmsMessage dr) {
		try {
			log.debug(dr, "TMP-DRLOOKUP inbound DR delete redis key. out={}", dr.getOutMsgID());
			com.king.gmms.messagequeue.DrLookup.delete(dr.getOutMsgID());
		} catch (Exception e) {
			// the entry expires by its TTL; the DR itself is already handled
			log.warn(dr, "TMP-DRLOOKUP inbound DR delete redis key failed. out={}", dr.getOutMsgID(), e);
		}
	}
	private void mergeInboundDeliveryReport(GmmsMessage original, GmmsMessage dr) {
		int code = dr.getStatusCode();
		if (code != GmmsStatus.ACCEPT.getCode() && code != GmmsStatus.ENROUTE.getCode()) {
			com.king.gmms.metrics.LatencyMetrics.vendorDr(original);
		}
		// from here on the DR's own times: e2e.dr counts from when the Client received it
		original.setInTimeMs(dr.getInTimeMs());
		original.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		// a DR has its own path from the Client that received it, not the one of its MT
		original.setModulePath(dr.getModulePath());
		original.setStatusCode(dr.getStatusCode());
		if (!GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(original.getStatusText())) {
			original.setStatusText(GmmsStatus.getStatus(dr.getStatusCode()).getText());
		}
		original.setOutTransID(dr.getOutTransID());
		if (dr.getDateIn() != null) {
			original.setDateIn(dr.getDateIn());
		}
	}
	/**
	 * NeedCheckMsgSize: longer than one SMS. ASCII is not checked (as before). GSM 7-bit (data_coding 0 of a customer
	 * with SMSOptionSupportIncomingGSM7bit): up to 160 GSM characters, although its contentType says ISO-8859-1 or
	 * UCS2 when it has characters such as é or Δ. Other (data_coding 3 ...): up to 140 bytes.
	 */
	static boolean overOneSms(GmmsMessage msg) {
		if ("ASCII".equalsIgnoreCase(msg.getContentType())) {
			return false;
		}
		if (msg.isGsm7bit()) {
			return com.king.gmms.util.charset.Convert.gsm7Septets(msg.getTextContent()) > 160;
		}
		return msg.getMessageSize() > 140;
	}

	public boolean processInnerAck(GmmsMessage msg) {		
		
		boolean result = false;
		try {
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
				if (msg.getStatus().getCode() == GmmsStatus.UNASSIGNED
						.getCode()) {
					Date scheduleDate = msg.getScheduleDeliveryTime();
					if (ctm.isInStoreMode(msg.getOSsID())) { //store mode
						if (log.isInfoEnabled()) {
							log.info(msg, "[MT-STORE] ossid={} store mode: kept in SMQ", msg.getOSsID());
						}
						msgStoreManager.handleInSubmit(msg, true);
					} else if ((scheduleDate != null && scheduleDate.after(gmmsUtility.getGMTTime()))) { //schedule delivery
						if (log.isInfoEnabled()) {
							log.info(msg, "[MT-STORE] ossid={} scheduled for {}: kept in SMQ", msg.getOSsID(), scheduleDate);
						}
						msgStoreManager.handleInSubmit(msg, true);
					} else {
						A2PCustomerInfo oCustomer = ctm.getCustomerBySSID(msg.getOSsID());
						//add check recipient length rule						
						if(ctm.doCheckRecipientAddressRule(msg)){
							//for record insubmit cdr
							msgStoreManager.handleInSubmit4Csm(msg);
							msg.setStatus(GmmsStatus.MSG_RECIPIET_RULE_ERROR);
							msg.setRSsID(gmmsUtility.getBlackholeSsid());
							msgStoreManager.handleOutSubmitResForInnack(msg);
							return true;
						}
						msgStoreManager.handleInSubmit(msg, false);
						if (CsmUtility.isConcatenatedMsg(msg)) {
							// CSM：Redis Hash 同步合并(持久化后才 ACK，不丢)
							if(!com.king.gmms.routing.SyncSubmitRouter.getInstance().routeCsmSegmentSync(msg)){
								msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
								return false;   // 不 ACK -> PEL auto-claim 重试
							}
						}else{
							//TODO							
							if("SMPP".equalsIgnoreCase(oCustomer.getProtocol())){
								//TODO
								if(oCustomer.isNeedCheckMsgSize()) {
									if(overOneSms(msg)){
										msg.setStatus(GmmsStatus.MSG_SIZE_OVERLENGTH);
										msg.setRSsID(gmmsUtility.getBlackholeSsid());
										msgStoreManager.handleOutSubmitResForInnack(msg);
										return true;
									}
								}
								//charge recipient 重复次数
								if (RecipientSendCountLimitService.isRejected(oCustomer,
										msg.getRecipientAddress(), redis)) {
									msg.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_MAX_COUNT);
									msg.setRSsID(gmmsUtility.getBlackholeSsid());
									msgStoreManager.handleOutSubmitResForInnack(msg);
									return true;
								}
								
								
							}
							// C-1: downstream produce is synchronous; DB fallback is async and acceptable once enqueued.
							// routeSubmitSync returns false only when neither produce nor fallback enqueue completed.
							if(!com.king.gmms.routing.SyncSubmitRouter.getInstance().routeSubmitSync(msg)){
								msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
								return false;
							}
						}
					}
					A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());
					if (msg.getInClientPull() == 2 && msg.getDeliveryReport()) {
						msgStoreManager.insertInMsgForQueryDR(msg);
					}
				}else{					
					gmmsUtility.getCdrManager().logInSubmit(msg);
				}
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
					.equalsIgnoreCase(msg.getMessageType())) {
				if(msg.getStatusCode() == GmmsStatus.ACCEPT
						.getCode()){
					gmmsUtility.getCdrManager().logOutDeliveryReportRes(msg);
				}else if (msg.getStatusCode() != GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					msgStoreManager.handleOutDeliveryReportResForInnerAck(msg);
				}else {
					gmmsUtility.getCdrManager().logOutDeliveryReportRes(msg);
				}
			}
			// add by kevin for REST query delivery report
			else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
						.equalsIgnoreCase(msg.getMessageType())) {
				if (msg.getStatus().getCode() == GmmsStatus.MSG_NOT_FOUND.getCode()) {
					// nothing to delete
				} else if (msg.getStatus().getCode() == GmmsStatus.ENROUTE.getCode()) {
					// don't delete, msg status may be updated later and customer may query again
				} else if (msg.getStatus().getCode() >= GmmsStatus.DELIVERED.getCode()) {
					msgStoreManager.deleteInMsgFromCache(msg);
				} else {
					boolean notSupportDR = gmmsUtility.getCustomerManager().isRssidNotSupportDR(msg.getRSsID());
					if(notSupportDR && GmmsStatus.SUCCESS.getText().equalsIgnoreCase(msg.getStatusText())) {
						msgStoreManager.deleteInMsgFromCache(msg);
					}
				}
				
				msg.setStatus(GmmsStatus.getStatus(msg.getStatusCode()));
				gmmsUtility.getCdrManager().logInDeliveryReportRes(msg);
			}
			result = true;
		} catch (Exception e) {
			if(log.isInfoEnabled()){
				log.info(msg, "Handle Command Inner ACK error:{}" , e.getMessage());
			}
			result = false;
		}
		return result;
	}
    /**
     * handle for common prefix for Mo
     * @param msg GmmsMessage
     * @param cst GmmsCustomer
     * add by brush
     */
    private void handleCommonPrefix4MO(String[] address,A2PCustomerInfo cst){
        if(cst.isHandlePrefix4NewMO()){
            cst.handlePrefix4NewMo(address);
        }
    }
}
