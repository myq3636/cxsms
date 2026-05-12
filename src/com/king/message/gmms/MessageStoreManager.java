package com.king.message.gmms;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import java_cup.internal_error;
import sun.applet.resources.MsgAppletViewer;

import com.king.db.Data;
import com.king.db.DataManager;
import com.king.db.DataManagerException;
import com.king.db.DbObject;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.RetryPolicyManager;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.gmms.processor.CsmKeyInfo;
import com.king.gmms.processor.CsmUtility;
import com.king.gmms.processor.CsmValueInfo;
import com.king.gmms.processor.CsmValueInfoMark;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.routing.IOSMSRerouteDispatcher;
import com.king.gmms.routing.RouteResponse;
import com.king.gmms.util.ExpiredMessageQueueWithSafeExit;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

public class MessageStoreManager extends DataManager {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageStoreManager.class);
	private static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private TimeZone local = TimeZone.getDefault();
	private A2PCustomerManager ctm = gmmsUtility.getCustomerManager();

	public static final String PMQ = "PMQ";
	public static final String RMQ = "RMQ";
	public static final String SMQ = "SMQ";
	public static final String WDQ = "WDQ";
	public static final String QDQ = "QDQ";
	public static final String RDQ = "RDQ";
	public static final String SDQ = "SDQ";

	public static final String IDDQ = "IQDQ";
	
	// add by kevin for InMsgID prefix
	public static final String INMSGIDPREFIXFORDRQUERY = "IQDR_";
	public static final String INMSGIDPREFIXFORDELAYDR = "DELAYDR_";

	/**
	 * Concatenated message queue
	 */
	public static final String CSMQ = "CSMQ";

	private ExpiredMessageQueueWithSafeExit messageQueuePmq = null;
	private boolean isPMQEnable = false;
	private MessageProcessorHandler processorHandler = null;
	private boolean isRedisEnable = false;
	private RedisClient redis = null;
	private String module = null;
	private IOSMSRerouteDispatcher rerouteDispatcher = null;
	private static final int SQL_BATCH_SIZE = 500;
	private static final long SQL_MAX_WAIT_MS = 200L;
	private final java.util.concurrent.BlockingQueue<String> asyncSqlQueue = new java.util.concurrent.LinkedBlockingQueue<String>(50000);
	private volatile boolean runAsyncWriter = false;

	/**
	 * Creates a new gmmsUtility of MessageQueueManager
	 * 
	 * @param dbo
	 *            DbObject
	 */
	public MessageStoreManager(DbObject dbo) {
		super(dbo);
	}

	public MessageStoreManager() {
	}

	public void init() {
		int queueTimeout = gmmsUtility.getCacheMsgTimeout();
		String pmqEnable = gmmsUtility.getFullModuleTypeProperty("PMQEnable",
				"False");
		if (pmqEnable.equalsIgnoreCase("true")) {
			isPMQEnable = true;
			messageQueuePmq = new ExpiredMessageQueueWithSafeExit(queueTimeout,
					false, "MsgQueueMonitorQueue_pmq");
			new PmqMessageThread().start();
		} else {
			isPMQEnable = false;
		}
		processorHandler = MessageProcessorHandler.getInstance();
		if ("True".equalsIgnoreCase(gmmsUtility.getCommonProperty(
				"RedisEnable", "True"))) {
			isRedisEnable = true;
			redis = gmmsUtility.getRedisClient();
		}
		module = System.getProperty("module");
		rerouteDispatcher = new IOSMSRerouteDispatcher();
		runAsyncWriter = true;
		new AsyncSqlWriterThread().start();
	}

	/**
	 * Provides a defualt implementation of enqueue function
	 * 
	 * @param message
	 *            GmmsMessage 0: normal 1: rejected by throttling control
	 * @throws DataManagerException
	 */

	public void prepareInSubmit(GmmsMessage message) {
		if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(message
				.getGmmsMsgType())) {
			String textContent = message.getTextContent();
			if (message.getUdh() == null
					&& (textContent == null || textContent.equals(""))) {
				message.setTextContent(" ");
				message.setContentType(GmmsMessage.AIC_CS_ASCII);
				message.setMessageSize(1);
			}
			String contentType = message.getContentType();
			if (contentType == null || contentType.equals("")
					|| contentType.equals(" ")) {
				message.setContentType(GmmsMessage.AIC_CS_ASCII);
			}
		}
		String inMsgID = message.getInMsgID();
		if (inMsgID == null || "".equals(inMsgID)) {
			inMsgID = MessageIdGenerator.generateCommonInMsgID(message
					.getOSsID());
			message.setInMsgID(inMsgID);
		}
		String msgID = message.getMsgID();
		if (msgID == null || "".equals(msgID)) {
			msgID = MessageIdGenerator.generateCommonMsgID(message.getOSsID());
			message.setMsgID(msgID);
		}

		int oA2P = gmmsUtility.getCustomerManager().getConnectedRelay(
				message.getOSsID(), message.getGmmsMsgType());
		if (message.getOA2P() <= 0) {
			message.setOA2P(oA2P);
		}
		if (message.getCurrentA2P() <= 0) {
			if (gmmsUtility.getCustomerManager().inCurrentA2P(oA2P)) {
				message.setCurrentA2P(oA2P);
			} else {
				message.setCurrentA2P(message.getRA2P());
			}
		}

		A2PCustomerInfo customerInfo = gmmsUtility.getCustomerManager()
				.getCustomerBySSID(message.getOSsID());
		if (customerInfo == null) {
			message.setOperatorPriority(5);
		} else {
			message.setOperatorPriority(customerInfo.getOperatorPriority());
		}
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
						.getMessageType())) {
			// PeeringTcpServer doesn't do throttlingControl and binaryFilter
			if (message.getOA2P() == message.getCurrentA2P()) {
				if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(message
						.getGmmsMsgType())
						&& !gmmsUtility.getCustomerManager()
								.getCustomerBySSID(message.getOSsID())
								.isSupportIncomingBinary()) {
					message.setStatusCode(GmmsStatus.BinaryFilter.getCode());
					if (log.isInfoEnabled()) {
						log.info(
								"{} (ssid) doesn't support binary SMS, just log CDR.",
								message, message.getOSsID());						
					}
				}
			}
		}
		// parse the UDH of message and set the related attributes of
		// concatenated short message
		try {
			parseCsmUdh(message);
		} catch (Exception e) {
			log.error(message, "Parse UDH error:", e);
		}

		// set inCsm
		if (CsmUtility.isConcatenatedMsg(message)) {
			message.setInCsm(true);
		}
	}

	public void handleInSubmit(GmmsMessage message, boolean isSendToSMQ)
			throws DataManagerException {
		CDRManager cdrManager = gmmsUtility.getCdrManager();

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())) {
			if (message.isInCsm()) {
				cdrManager.logInSubmitPartial(message);
			} else {				
				cdrManager.logInSubmit(message);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
				.getMessageType())) {
			if (message.isInCsm()) {
				cdrManager.logInDeliveryPartial(message);
			} else {
				cdrManager.logInDelivery(message);
			}

		} else {
			log.error(message, "Unknown message type");
			return;
		}
		if (isSendToSMQ) {
			insertMessageToDB(message, SMQ);
		} else {
			sendToPMQ(message);
		}
	}

	public void handleInSubmit4Csm(GmmsMessage message)
			throws DataManagerException {
		CDRManager cdrManager = gmmsUtility.getCdrManager();

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())) {			
			cdrManager.logInSubmit(message);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
				.getMessageType())) {
			cdrManager.logInDelivery(message);
		} else {
			log.error(message, "Unknown message type");
		}
	}

	/**
	 * 
	 * @param message
	 */
	public void sendToPMQ(GmmsMessage message) {
		if (isPMQEnable) {
			messageQueuePmq.put(message);
		}
	}

	class PmqMessageThread extends Thread {
		private PmqMessageWriter writer = null;

		public PmqMessageThread() {
			writer = PmqMessageWriter.getInstance();
			writer.initialize();
		}

		public void start() {
			Thread thread = new Thread(A2PThreadGroup.getInstance(), this,
					"PmqMessageThread");
			thread.start();
			log.info("PmqMessageThread Thread start.");
		}

		public void run() {
			GmmsMessage message = null;
			while (true) {
				try {
					message = (GmmsMessage) messageQueuePmq.get(200L);
					if (message != null) {
						writer.backupMessage(message);
					}
				} catch (Exception e) {
					log.error(e, e);
				}
			}
		}
	}

	class AsyncSqlWriterThread extends Thread {
		public void start() {
			Thread thread = new Thread(A2PThreadGroup.getInstance(), this, "AsyncSqlWriterThread");
			thread.start();
			log.info("AsyncSqlWriterThread Thread start.");
		}

		public void run() {
			java.util.List<String> batch = new java.util.ArrayList<String>();
			long lastExecuteTime = System.currentTimeMillis();
			while (runAsyncWriter || !asyncSqlQueue.isEmpty()) {
				try {
					String sql = asyncSqlQueue.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
					if (sql != null) {
						batch.add(sql);
					}
					
					long now = System.currentTimeMillis();
					if (batch.size() >= SQL_BATCH_SIZE || (batch.size() > 0 && (now - lastExecuteTime) >= SQL_MAX_WAIT_MS)) {
						executeBatch(batch);
						batch.clear();
						lastExecuteTime = now;
					}
				} catch (InterruptedException ie) {
					// Ignore InterruptedException and let loop continue
				} catch (Exception e) {
					log.error("Error in AsyncSqlWriterThread loop", e);
				}
			}
		}
		
		private void executeBatch(java.util.List<String> batch) {
			java.sql.Statement stmt = null;
			org.hibernate.Session sess = null;
			try {
				sess = currentSession();
				java.sql.Connection conn = sess.connection();
				boolean originalAutoCommit = conn.getAutoCommit();
				
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				for (String sql : batch) {
					stmt.addBatch(sql);
				}
				stmt.executeBatch();
				conn.commit();
				conn.setAutoCommit(originalAutoCommit);
			} catch (Exception e) {
				log.error("Error executing batch update of size " + batch.size(), e);
				if (!e.getMessage().contains("Data truncated") && !e.getMessage().contains("SQL syntax")) {
					MailSender.getInstance().sendAlertMail("A2P DB Batch Exception", e);
				}
			} finally {
				if (stmt != null) {
					try { stmt.close(); } catch (java.sql.SQLException ex) {}
				}
				closeSession();
			}
		}
	}

	public void handleInSubmitRes(GmmsMessage message) {
	}

	public void handleOutSubmitReq(GmmsMessage message) {
	}

	public void handleOutSubmit4CmccTestSms(GmmsMessage message) {
		message.setActionCode(-1);		
		gmmsUtility.getCdrManager().logOutSubmitRes(message);
		message.setRetriedNumber(0);
		message.setNextRetryTime(null);
		message.setDateIn(new Date());
		message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		message.setStatus(GmmsStatus.DELIVERED);
		message.setDeliveryChannel(module);
		sendDRMessage(message);
	}
	
	public void handleOutSubmitRes(GmmsMessage message) {
		handleOutSubmitRes(message, false);
	}

	/**
	 * 
	 * @param message
	 * @param checkReroute
	 */
	public void handleOutSubmitRes(GmmsMessage message, boolean checkReroute) {
		message.setActionCode(-1);
		boolean supportDR = !gmmsUtility.getCustomerManager()
				.isRssidNotSupportDR(message.getRSsID());
		String messageType = message.getMessageType();	
		GmmsMessage cdr = new GmmsMessage(message);
		// reroute check and process
		if (checkReroute) {				
			if (RouteResponse.RouteOK.equals(rerouteDispatcher.dispatch(message))) {
				// reroute success, return directly				
				if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {			
					if (supportDR)
						gmmsUtility.getCdrManager().logOutSubmitRes(cdr, supportDR);
					else
						gmmsUtility.getCdrManager().logOutSubmitRes(cdr);
				} else {
					if (supportDR)
						gmmsUtility.getCdrManager().logOutDeliveryRes(cdr,
								supportDR);
					else
						gmmsUtility.getCdrManager().logOutDeliveryRes(cdr);
				}
				return;
			}
		}

		if (needRetryForOutSubmitRes(message)) {									
			RetryPolicyManager.getInstance().doRetryPolicy(message);
			//TODO add retry flag for cdr
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
				if(message.getRetriedNumber()>1){
					message.setRetryFlag(true);
				}
				if (supportDR)
					gmmsUtility.getCdrManager().logOutSubmitRes(message, supportDR);
				else
					gmmsUtility.getCdrManager().logOutSubmitRes(message);
			} else {
				if (supportDR)
					gmmsUtility.getCdrManager().logOutDeliveryRes(message,
							supportDR);
				else
					gmmsUtility.getCdrManager().logOutDeliveryRes(message);
			}
			if (RetryPolicyManager.getInstance().isLastRetry(message)) {								
				if (message.getOriginalQueue() != null) {
					clearMessage(message, message.getOriginalQueue());
				}
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
					if (ocustomer.getSmsOptionSubmitErrorCodesForFakeDR()!=null 
							&& !ocustomer.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
							&& !ocustomer.isSmsOptionSendDRByInSubmitInCU()
							&& ocustomer.getSmsOptionSubmitErrorCodesForFakeDR().contains(message.getStatusCode())) {
						message.setStatus(GmmsStatus.DELIVERED);
					} else {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.UNDELIVERABLE);
						}
					}
					message.setRetriedNumber(0);
					message.setNextRetryTime(null);
					message.setDateIn(new Date());
					message.setDeliveryChannel(module);
					A2PCustomerInfo oinfo = ctm.getCustomerBySSID(message.getOSsID());
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 && ((message.getOA2P() == message.getCurrentA2P())) || ctm.vpOnSameA2P(message.getOA2P(),message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							if(!oinfo.isNullFakeDRByInSubmitRecPrefixInCU()
									||oinfo.isSmsOptionSendDRByInSubmitInCU()) {
								//not need to send dr
							}else {
								sendDRMessage(message);
							}
							
						}
					} else {
						if(!oinfo.isNullFakeDRByInSubmitRecPrefixInCU()
								||oinfo.isSmsOptionSendDRByInSubmitInCU()) {
							//not need to send dr
						}else {
							sendDRMessage(message);
						}
					}
				}
			} else {				
				if (message.getOriginalQueue() == null) {
					if (inRopBusyMode(message.getRSsID())) {
						insertMessageToDB(message, SMQ);
					} else {
						insertMessageToDB(message, RMQ);
					}
				} else {
					updateByInMsgId(message, message.getOriginalQueue());
				}
			}
		} else {			
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {			
				if (supportDR)
					gmmsUtility.getCdrManager().logOutSubmitRes(message, supportDR);
				else
					gmmsUtility.getCdrManager().logOutSubmitRes(message);
			} else {
				if (supportDR)
					gmmsUtility.getCdrManager().logOutDeliveryRes(message,
							supportDR);
				else
					gmmsUtility.getCdrManager().logOutDeliveryRes(message);
			}
			
			if (message.getOriginalQueue() != null) {
				clearMessage(message, message.getOriginalQueue());
			}
			A2PCustomerInfo customerInfo = ctm.getCustomerBySSID(message
					.getRSsID());
			message.setOutClientPull(customerInfo.getOutClientPull()
					.equalsIgnoreCase("1") ? true : false);
			message.setRetriedNumber(0);
			message.setNextRetryTime(null);
			message.setDateIn(new Date());
			A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message
					.getOSsID());
			if (message.getStatus().equals(GmmsStatus.SUCCESS)) {
				String omsgid = message.getOutMsgID();				
				if (supportDR) {										
					if (message.outClientPull()) {
						insertMessageToDB(message, QDQ);
					} else {
						//check Ocustomer support In DR or not , send accept InDR to ossid					
						//reroute message did not need to send accept DR
						
						/**if (message.getDeliveryReport() && !message.hasDRFailedReroute()
								&& !"SMPP".equalsIgnoreCase(oInfo.getProtocol())
								&& message.getSplitStatus() <= 1) {
							GmmsMessage acceptMsg = new GmmsMessage(message);
							acceptMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
							acceptMsg.setStatus(GmmsStatus.ACCEPT);
							acceptMsg.setDeliveryChannel(module);
							acceptMsg.setInMsgID(message.getInMsgID());
							if (message.getInClientPull() == 2
									&& (message.getOA2P() == message.getRA2P())) {
								updateMsgForDRQuery(acceptMsg);
							} else {
								sendDRMessage(acceptMsg);
							}
						}**/
						//adjust send success in dr by out submit in 20190202
						if (oInfo.isSmsOptionSendFakeDR() && 							 
								message.getDeliveryReport() &&								
								!message.hasDRFailedReroute() &&
								message.getSplitStatus() <= 1 && 
							    (!oInfo.isSmsOptionSendDRByInSubmitInCU()&&!oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())) && 								
								customerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())) {						
							GmmsMessage acceptMsg = new GmmsMessage(message);
							acceptMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
							if (GmmsUtility.isModifySuccessDR(oInfo.getSSID(), oInfo.getDrSucRatio(), oInfo.getDrBiasRatio())) {
								acceptMsg.setStatus(GmmsStatus.DELIVERED);
							}else {
								GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
								if(converDRStatus!=null) {
									acceptMsg.setStatus(converDRStatus);
								}else {
									acceptMsg.setStatus(GmmsStatus.UNDELIVERABLE);
								}
							}						
							acceptMsg.setDeliveryChannel(module);
							acceptMsg.setInMsgID(message.getInMsgID());
							if (message.getInClientPull() == 2
									&& (message.getOA2P() == message.getRA2P())) {
								updateMsgForDRQuery(acceptMsg);
							} else {
								if (message.hasFakeSendDR()&&(
										!oInfo.isSmsOptionSendDRByInSubmitInCU() && oInfo.isNullFakeDRByInSubmitRecPrefixInCU()
										)) {
									int sendDrImmediately = oInfo.getSendDelayDRImmediately();
									if(sendDrImmediately == 1){
										sendDRMessage(acceptMsg);
									}else{
										String delayTimeInterval = oInfo.getDrDelayTimeInSec();
										int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
										insertMsgForDRDelay(acceptMsg, delayTime);										
									}
									message.setFakeDR(false);
									
								}								
							}
						}
						if (isRedisEnable) {
							String temp = SerializableHandler
									.convertGmmsMessage2RedisMessage(message);
							Date expireDate = message.getExpiryDate();
							long expireThrottle = 10*60*1000L;
							if(expireDate != null) {
								long expireT = expireDate.getTime();
								expireThrottle = expireT-System.currentTimeMillis();
							}
							
							if (temp == null || expireThrottle<600000) {
								insertMessageToDB(message, WDQ);
							} else {
								int expirTime = gmmsUtility
										.getRedisExpireTime(message);
								String hashKey = gmmsUtility
										.getRedisDateIn(message);
								if (!redis.setPipeline(omsgid, temp, expirTime,
										hashKey)) {
									insertMessageToDB(message, WDQ);
									if (log.isInfoEnabled()) {
										log.info(
												"message set redis error, so insert wdq and the outmsgid is {}",
												omsgid);
									}
								}
							}
						} else {
							insertMessageToDB(message, WDQ);
						}

					}
					if (message.getInClientPull() == 2
							&& message.getDeliveryReport()
							&& message.getSplitStatus() < 2) {
						message.setStatus(GmmsStatus.SUCCESS);
						updateMsgForDRQuery(message);
					}
				} else if (message.getDeliveryReport()) {
					if ((message.getOA2P() != message.getRA2P())
							&& (message.getOA2P() == message.getCurrentA2P())) {
						if (isRedisEnable) {
							String temp = SerializableHandler
									.convertGmmsMessage2RedisMessage(message);
							if (temp == null) {
								insertMessageToDB(message, WDQ);
							} else {
								int expirTime = gmmsUtility
										.getRedisExpireTime(message);

								String hashKey = gmmsUtility
										.getRedisDateIn(message);
								if (!redis.setPipeline(omsgid, temp, expirTime,
										hashKey)) {
									insertMessageToDB(message, WDQ);
									if (log.isInfoEnabled()) {
										log.info(
												"message set redis error, so insert wdq and the outmsgid is {}",
												omsgid);
									}
								}
							}
						} else {
							insertMessageToDB(message, WDQ);
						}

						if (message.getInClientPull() == 2
								&& message.getDeliveryReport()
								&& message.getSplitStatus() < 2) {
							message.setStatus(GmmsStatus.SUCCESS);
							updateMsgForDRQuery(message);
						}

					} else if (message.getSplitStatus() < 2) {
						message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
						message.setStatus(GmmsStatus.DELIVERED);
						message.setDeliveryChannel(module);
						if (message.getInClientPull() == 2
								&& (message.getOA2P() == message.getRA2P())) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					}
				}
			} else {
				//TODO 判断客户fakedr类型，防止二次dr返回。		
				  if((oInfo.isSmsOptionSendDRByInSubmitInCU()
						  || oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()))
						  && message.getSplitStatus() < 2){ 
					  return;
				  }
				 
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					//message.setStatusCode(GmmsStatus.REJECTED.getCode());
					A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
					if (ocustomer.getSmsOptionSubmitErrorCodesForFakeDR()!=null 
							&& ocustomer.getSmsOptionSubmitErrorCodesForFakeDR().contains(message.getStatusCode())) {
						message.setStatus(GmmsStatus.DELIVERED);
					} else {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.UNDELIVERABLE);
						}
					}
					
					message.setDeliveryChannel(module);
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 &&(message.getOA2P() == message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							int sendDrImmediately = oInfo.getSendDelayDRImmediately();
							if(sendDrImmediately == 1){
								sendDRMessage(message);
							}else{
								String delayTimeInterval = oInfo.getDrDelayTimeInSec();
								int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
								if(delayTime == 0) {
									sendDRMessage(message);
								}else {
									insertMsgForDRDelay(message, delayTime);
								}										
							}
						}
					} else {
						int sendDrImmediately = oInfo.getSendDelayDRImmediately();
						if(sendDrImmediately == 1){
							sendDRMessage(message);
						}else{
							
							String delayTimeInterval = oInfo.getDrDelayTimeInSec();
							int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
							if(delayTime == 0) {
								sendDRMessage(message);
							}else {
								insertMsgForDRDelay(message, delayTime);
							}
																	
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * 
	 * @param message
	 * @param checkReroute
	 */
	public void handleOutSubmitResForInnack(GmmsMessage message) {
		message.setActionCode(-1);
		boolean supportDR = !gmmsUtility.getCustomerManager()
				.isRssidNotSupportDR(message.getRSsID());
		String messageType = message.getMessageType();	
		GmmsMessage cdr = new GmmsMessage(message);

		if (needRetryForOutSubmitRes(message)) {									
			RetryPolicyManager.getInstance().doRetryPolicy(message);
			//TODO add retry flag for cdr
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
				if(message.getRetriedNumber()>1){
					message.setRetryFlag(true);
				}
				if (supportDR)
					gmmsUtility.getCdrManager().logOutSubmitRes(message, supportDR);
				else
					gmmsUtility.getCdrManager().logOutSubmitRes(message);
			} else {
				if (supportDR)
					gmmsUtility.getCdrManager().logOutDeliveryRes(message,
							supportDR);
				else
					gmmsUtility.getCdrManager().logOutDeliveryRes(message);
			}
			if (RetryPolicyManager.getInstance().isLastRetry(message)) {								
				if (message.getOriginalQueue() != null) {
					clearMessage(message, message.getOriginalQueue());
				}
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
					if (ocustomer.getSmsOptionSubmitErrorCodesForFakeDR()!=null 
							&& !ocustomer.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
							&& !ocustomer.isSmsOptionSendDRByInSubmitInCU()
							&& ocustomer.getSmsOptionSubmitErrorCodesForFakeDR().contains(message.getStatusCode())) {
						message.setStatus(GmmsStatus.DELIVERED);
					} else {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.UNDELIVERABLE);
						}
					}
					message.setRetriedNumber(0);
					message.setNextRetryTime(null);
					message.setDateIn(new Date());
					message.setDeliveryChannel(module);
					A2PCustomerInfo oinfo = ctm.getCustomerBySSID(message.getOSsID());
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 && ((message.getOA2P() == message.getCurrentA2P())) || ctm.vpOnSameA2P(message.getOA2P(),message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							if(!oinfo.isNullFakeDRByInSubmitRecPrefixInCU()
									||oinfo.isSmsOptionSendDRByInSubmitInCU()) {
								//not need to send dr
							}else {
								sendDRMessage(message);
							}
							
						}
					} else {
						if(!oinfo.isNullFakeDRByInSubmitRecPrefixInCU()
								||oinfo.isSmsOptionSendDRByInSubmitInCU()) {
							//not need to send dr
						}else {
							sendDRMessage(message);
						}
					}
				}
			} else {				
				if (message.getOriginalQueue() == null) {
					if (inRopBusyMode(message.getRSsID())) {
						insertMessageToDB(message, SMQ);
					} else {
						insertMessageToDB(message, RMQ);
					}
				} else {
					updateByInMsgId(message, message.getOriginalQueue());
				}
			}
		} else {			
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {			
				if (supportDR)
					gmmsUtility.getCdrManager().logOutSubmitRes(message, supportDR);
				else
					gmmsUtility.getCdrManager().logOutSubmitRes(message);
			} else {
				if (supportDR)
					gmmsUtility.getCdrManager().logOutDeliveryRes(message,
							supportDR);
				else
					gmmsUtility.getCdrManager().logOutDeliveryRes(message);
			}
			
			if (message.getOriginalQueue() != null) {
				clearMessage(message, message.getOriginalQueue());
			}
			A2PCustomerInfo customerInfo = ctm.getCustomerBySSID(message
					.getRSsID());
			message.setOutClientPull(customerInfo.getOutClientPull()
					.equalsIgnoreCase("1") ? true : false);
			message.setRetriedNumber(0);
			message.setNextRetryTime(null);
			message.setDateIn(new Date());
			A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message
					.getOSsID());
			if (message.getStatus().equals(GmmsStatus.SUCCESS)) {
				String omsgid = message.getOutMsgID();				
				if (supportDR) {										
					if (message.outClientPull()) {
						insertMessageToDB(message, QDQ);
					} else {
						//check Ocustomer support In DR or not , send accept InDR to ossid					
						//reroute message did not need to send accept DR
						
						/**if (message.getDeliveryReport() && !message.hasDRFailedReroute()
								&& !"SMPP".equalsIgnoreCase(oInfo.getProtocol())
								&& message.getSplitStatus() <= 1) {
							GmmsMessage acceptMsg = new GmmsMessage(message);
							acceptMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
							acceptMsg.setStatus(GmmsStatus.ACCEPT);
							acceptMsg.setDeliveryChannel(module);
							acceptMsg.setInMsgID(message.getInMsgID());
							if (message.getInClientPull() == 2
									&& (message.getOA2P() == message.getRA2P())) {
								updateMsgForDRQuery(acceptMsg);
							} else {
								sendDRMessage(acceptMsg);
							}
						}**/
						//adjust send success in dr by out submit in 20190202
						if (oInfo.isSmsOptionSendFakeDR() && 							 
								message.getDeliveryReport() &&								
								!message.hasDRFailedReroute() &&
								message.getSplitStatus() <= 1 && 
							    (!oInfo.isSmsOptionSendDRByInSubmitInCU()&&!oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())) && 								
								customerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())) {						
							GmmsMessage acceptMsg = new GmmsMessage(message);
							acceptMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
							if (GmmsUtility.isModifySuccessDR(oInfo.getSSID(), oInfo.getDrSucRatio(), oInfo.getDrBiasRatio())) {
								acceptMsg.setStatus(GmmsStatus.DELIVERED);
							}else {
								GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
								if(converDRStatus!=null) {
									acceptMsg.setStatus(converDRStatus);
								}else {
									acceptMsg.setStatus(GmmsStatus.UNDELIVERABLE);
								}
							}						
							acceptMsg.setDeliveryChannel(module);
							acceptMsg.setInMsgID(message.getInMsgID());
							if (message.getInClientPull() == 2
									&& (message.getOA2P() == message.getRA2P())) {
								updateMsgForDRQuery(acceptMsg);
							} else {
								if (message.hasFakeSendDR()&&(
										!oInfo.isSmsOptionSendDRByInSubmitInCU() && oInfo.isNullFakeDRByInSubmitRecPrefixInCU()
										)) {
									int sendDrImmediately = oInfo.getSendDelayDRImmediately();
									if(sendDrImmediately == 1){
										sendDRMessage(acceptMsg);
									}else{
										String delayTimeInterval = oInfo.getDrDelayTimeInSec();
										int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
										insertMsgForDRDelay(acceptMsg, delayTime);										
									}
									message.setFakeDR(false);
									
								}								
							}
						}
						if (isRedisEnable) {
							String temp = SerializableHandler
									.convertGmmsMessage2RedisMessage(message);
							Date expireDate = message.getExpiryDate();
							long expireThrottle = 10*60*1000L;
							if(expireDate != null) {
								long expireT = expireDate.getTime();
								expireThrottle = expireT-System.currentTimeMillis();
							}
							
							if (temp == null || expireThrottle<600000) {
								insertMessageToDB(message, WDQ);
							} else {
								int expirTime = gmmsUtility
										.getRedisExpireTime(message);
								String hashKey = gmmsUtility
										.getRedisDateIn(message);
								if (!redis.setPipeline(omsgid, temp, expirTime,
										hashKey)) {
									insertMessageToDB(message, WDQ);
									if (log.isInfoEnabled()) {
										log.info(
												"message set redis error, so insert wdq and the outmsgid is {}",
												omsgid);
									}
								}
							}
						} else {
							insertMessageToDB(message, WDQ);
						}

					}
					if (message.getInClientPull() == 2
							&& message.getDeliveryReport()
							&& message.getSplitStatus() < 2) {
						message.setStatus(GmmsStatus.SUCCESS);
						updateMsgForDRQuery(message);
					}
				} else if (message.getDeliveryReport()) {
					if ((message.getOA2P() != message.getRA2P())
							&& (message.getOA2P() == message.getCurrentA2P())) {
						if (isRedisEnable) {
							String temp = SerializableHandler
									.convertGmmsMessage2RedisMessage(message);
							if (temp == null) {
								insertMessageToDB(message, WDQ);
							} else {
								int expirTime = gmmsUtility
										.getRedisExpireTime(message);

								String hashKey = gmmsUtility
										.getRedisDateIn(message);
								if (!redis.setPipeline(omsgid, temp, expirTime,
										hashKey)) {
									insertMessageToDB(message, WDQ);
									if (log.isInfoEnabled()) {
										log.info(
												"message set redis error, so insert wdq and the outmsgid is {}",
												omsgid);
									}
								}
							}
						} else {
							insertMessageToDB(message, WDQ);
						}

						if (message.getInClientPull() == 2
								&& message.getDeliveryReport()
								&& message.getSplitStatus() < 2) {
							message.setStatus(GmmsStatus.SUCCESS);
							updateMsgForDRQuery(message);
						}

					} else if (message.getSplitStatus() < 2) {
						message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
						message.setStatus(GmmsStatus.DELIVERED);
						message.setDeliveryChannel(module);
						if (message.getInClientPull() == 2
								&& (message.getOA2P() == message.getRA2P())) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					}
				}
			} else {				
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					//message.setStatusCode(GmmsStatus.REJECTED.getCode());
					A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
					if (ocustomer.getSmsOptionSubmitErrorCodesForFakeDR()!=null 
							&& ocustomer.getSmsOptionSubmitErrorCodesForFakeDR().contains(message.getStatusCode())) {
						message.setStatus(GmmsStatus.DELIVERED);
					} else {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.UNDELIVERABLE);
						}
					}
					
					message.setDeliveryChannel(module);
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 &&(message.getOA2P() == message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							int sendDrImmediately = oInfo.getSendDelayDRImmediately();
							if(sendDrImmediately == 1){
								sendDRMessage(message);
							}else{
								String delayTimeInterval = oInfo.getDrDelayTimeInSec();
								int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
								if(delayTime == 0) {
									sendDRMessage(message);
								}else {
									insertMsgForDRDelay(message, delayTime);
								}										
							}
						}
					} else {
						int sendDrImmediately = oInfo.getSendDelayDRImmediately();
						if(sendDrImmediately == 1){
							sendDRMessage(message);
						}else{
							
							String delayTimeInterval = oInfo.getDrDelayTimeInSec();
							int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
							if(delayTime == 0) {
								sendDRMessage(message);
							}else {
								insertMsgForDRDelay(message, delayTime);
							}
																	
						}
					}
				}
			}
		}
	}
	
	
	
	public boolean rerouteOutMsg(GmmsMessage message){		
		if (RouteResponse.RouteOK.equals(rerouteDispatcher.dispatch(message))) {
			// reroute success, return directly
			return true;
		}
		return false;
	}

	public boolean needRetry(GmmsMessage message) {
		
		return message.getStatus().equals(GmmsStatus.COMMUNICATION_ERROR)
				|| message.getStatus().equals(GmmsStatus.Throttled)
				|| message.getStatus().equals(GmmsStatus.SERVER_ERROR)
				|| message.getStatus().equals(GmmsStatus.UNKNOWN_ERROR)
				|| message.getStatus().equals(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT)
				|| message.getStatus().equals(GmmsStatus.SERVICE_ERROR);
	}
	
	public boolean needRetryForOutSubmitRes(GmmsMessage message) {
		return gmmsUtility.getCustomerManager()
		.getCustomerBySSID(message.getRSsID())
		.getSmsOptionSubmitRetryCode().contains(message.getStatusCode());
	}

	private boolean inRopBusyMode(int ssid) {
		return gmmsUtility.getCustomerManager().isInRopBusyMode(ssid);
	}

	private boolean inRopFailedMode(int ssid) {
		return gmmsUtility.getCustomerManager().isInRopFailedMode(ssid);
	}

	public void handleInDeliveryReportRes(GmmsMessage message) {
		message.setActionCode(-1);
		
		if (message.getCurrentA2P()!=message.getOA2P()&&
            ctm.vpOnSameA2P(message.getCurrentA2P(), message.getOA2P())) {
			 message.setCurrentA2P(message.getOA2P());
		 }

		gmmsUtility.getCdrManager().logInDeliveryReportRes(message);
		// Amy: Delete messages which have sent normal IN_DR;
		// Keep messages which have sent expired IN_DR.
		if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
				.getCode()) {
			RetryPolicyManager.getInstance().doRetryPolicy(message);
			// int defaultExpireTime = gmmsUtility.getExpireTimeInMinute();
			if (RetryPolicyManager.getInstance().isLastRetry(message)) {
				clearMessage(message, RDQ);
			} else {
				if (RDQ.equalsIgnoreCase(message.getOriginalQueue())) {
					updateByInMsgId(message, RDQ);
				} else {
					insertMessageToDB(message, RDQ);
				}
			}
		} else {
			if (RDQ.equalsIgnoreCase(message.getOriginalQueue())) {
				clearMessage(message, RDQ);
			}
		}
	}

	public void handleOutDeliveryReportReq(GmmsMessage message) {
		clearMessage(message, WDQ);
		if (GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
				message.getStatusText())) {
			message.setStatusText(GmmsStatus.getStatus(message.getStatusCode())
					.getText());
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
		} else {
			message.setStatusText(GmmsStatus.getStatus(message.getStatusCode())
					.getText());
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			if (message.getDeliveryReport() && message.getSplitStatus() <= 1) {
				if (message.getInClientPull() == 2) {
					if ((message.getOA2P() == message.getRA2P())							
							||((message.getOA2P() != message.getRA2P())
							&& (message.getOA2P() == message.getCurrentA2P() || ctm.vpOnSameA2P(message.getOA2P(),message.getCurrentA2P())))) {
						updateMsgForDRQuery(message);
					} else {
						sendDRMessage(message);
					}
				} else {
					sendDRMessage(message);
				}
			}
		}
	}

	public void handleOutDeliveryReportRes(GmmsMessage message) {
		message.setActionCode(-1);
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(message
				.getMessageType())) {
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			// fix query DR issue on 20080618
			if (GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode() != message
					.getStatusCode()
					&& GmmsStatus.ENROUTE.getCode() != message.getStatusCode()) {
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				message.setRetriedNumber(0);
				message.setNextRetryTime(null);
				clearMessage(message, QDQ);
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 && ((message.getOA2P() == message.getCurrentA2P()) || ctm.vpOnSameA2P(message.getOA2P(),message.getCurrentA2P())))) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					} else {
						sendDRMessage(message);
					}
				}
			} else {
				RetryPolicyManager.getInstance().doRetryPolicy(message);
				checkLastRetry(message);
			}
		} else {
			/*if current time is earlier than expired time of previous hop, 
			* A2P does not give in_dr until the message is expired.
			**/			
			if(message.getStatusCode() == GmmsStatus.EXPIRED.getCode()){
				long gmtNow = GmmsUtility.getInstance().getGMTTime().getTime();
				long expiryDate = message.getExpiryDate().getTime();
				if(gmtNow < expiryDate){
					if (log.isInfoEnabled()) {
						log.info(message, "A2P has received the expired OUT_DR message, " +
								"but for IN_DR of current message is not expired");
					}
					//just record the out_dr into CDR table with real status
					A2PCustomerInfo info = ctm.getCustomerBySSID(message.getOSsID());
					A2PCustomerInfo rCustomerInfo = ctm.getCustomerBySSID(message.getOSsID());
					if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
							&&info.isNullFakeDRByInSubmitRecPrefixInCU()
							||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
							||info.isSmsOptionSendDRByInSubmitInCU()
							) {
					}else {
						message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
						insertMessageToDB(message, WDQ);
						message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					}
					message.setStatusText(GmmsStatus.getStatus(message.getStatusCode()).getText());
					gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
					return;
				}
			}
			
			if (WDQ.equalsIgnoreCase(message.getOriginalQueue())) {
				clearMessage(message, WDQ);
			}
			if (GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
					message.getStatusText())) {
				message.setStatusText(GmmsStatus.getStatus(
						message.getStatusCode()).getText());
				gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			}else {
				message.setStatusText(GmmsStatus.getStatus(
						message.getStatusCode()).getText());
				gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
				//add retry message to backup route in 20180215
				/*if (!GmmsStatus.DELIVERED.getText().equalsIgnoreCase(
						message.getStatusText())) {
					if (!message.hasDRFailedReroute()) {
						GmmsMessage tmpMsg = new GmmsMessage(message);
						tmpMsg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
						tmpMsg.setStatus(GmmsStatus.DRFAILEDTOREROUTE_ERROR);
						tmpMsg.setDRFailedReroute(true);
						IOSMSRerouteDispatcher rerouteDispatcher = new IOSMSRerouteDispatcher();
						if (RouteResponse.RouteOK.equals(rerouteDispatcher.dispatch(tmpMsg))) {
							// reroute success, return directly
							return;
						}
					}
				}		*/	
				A2PCustomerInfo info = ctm.getCustomerBySSID(message.getOSsID());
				/*if(info.isSmsOptionSendDRByOutSubmit() || info.isSmsOptionSendDRByInSubmit()){
					// in dr has been sent by out submit, so doesn't need to send in dr.
				    return;			
				}*/
				
				if (message.getDeliveryReport()
						&& message.getSplitStatus() <= 1) {
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 &&(message.getOA2P() == message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					} else {
						sendDRMessage(message);
					}
				}
			}
		}
	}
	
	public void handleOutDeliveryReportResForInnerAck(GmmsMessage message) {
		message.setActionCode(-1);
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(message
				.getMessageType())) {
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			// fix query DR issue on 20080618
			if (GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode() != message
					.getStatusCode()
					&& GmmsStatus.ENROUTE.getCode() != message.getStatusCode()) {
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				message.setRetriedNumber(0);
				message.setNextRetryTime(null);
				clearMessage(message, QDQ);
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 && ((message.getOA2P() == message.getCurrentA2P()) || ctm.vpOnSameA2P(message.getOA2P(),message.getCurrentA2P())))) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					} else {
						sendDRMessage(message);
					}
				}
			} else {
				RetryPolicyManager.getInstance().doRetryPolicy(message);
				checkLastRetry(message);
			}
		} else {
			/*if current time is earlier than expired time of previous hop, 
			* A2P does not give in_dr until the message is expired.
			**/			
			if(message.getStatusCode() == GmmsStatus.EXPIRED.getCode()){
				long gmtNow = GmmsUtility.getInstance().getGMTTime().getTime();
				long expiryDate = message.getExpiryDate().getTime();
				if(gmtNow < expiryDate){
					if (log.isInfoEnabled()) {
						log.info(message, "A2P has received the expired OUT_DR message, " +
								"but for IN_DR of current message is not expired");
					}
					A2PCustomerInfo info = ctm.getCustomerBySSID(message.getOSsID());
					A2PCustomerInfo rCustomerInfo = ctm.getCustomerBySSID(message.getOSsID());
					if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
							&&info.isNullFakeDRByInSubmitRecPrefixInCU()
							||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
							||info.isSmsOptionSendDRByInSubmitInCU()
							) {
					}else {
						message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
						insertMessageToDB(message, WDQ);
						message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					}
					
					//just record the out_dr into CDR table with real status
					message.setStatusText(GmmsStatus.getStatus(message.getStatusCode()).getText());
					gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
					return;
				}
			}
			
			if (WDQ.equalsIgnoreCase(message.getOriginalQueue())) {
				clearMessage(message, WDQ);
			}
			if (GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
					message.getStatusText())) {
				message.setStatusText(GmmsStatus.getStatus(
						message.getStatusCode()).getText());
				gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			}else {
				message.setStatusText(GmmsStatus.getStatus(
						message.getStatusCode()).getText());
				gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);								
				A2PCustomerInfo info = ctm.getCustomerBySSID(message.getOSsID());
				/*if(info.isSmsOptionSendDRByOutSubmit()|| info.isSmsOptionSendDRByInSubmit()){
					// in dr has been sent by out submit, so doesn't need to send in dr.
				    return;			
				}*/
				if (message.getDeliveryReport()
						&& message.getSplitStatus() <= 1) {
					
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								|| ((message.getOA2P() != message.getRA2P()) 
									 &&(message.getOA2P() == message.getCurrentA2P()))) {
							updateMsgForDRQuery(message);
						} else {
							A2PCustomerInfo rCustomerInfo = ctm.getCustomerBySSID(message.getRSsID());
							if (rCustomerInfo!=null && rCustomerInfo.isNeedDrReroute()
									&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
								List<Integer> drRerouteCode = rCustomerInfo.getSmsOptionDRRerouteCode();
								if(drRerouteCode!=null && !drRerouteCode.isEmpty()){
									Date timeDate = message.getTimeStamp();									
									log.trace(message, "message timestamp is:{}, timestamp:{}, delayTime:{}", timeDate, timeDate.getTime(), rCustomerInfo.getMaxRerouteDRDelayTimeInSec());
									long delayTime = System.currentTimeMillis()-timeDate.getTime()-8*3600*1000-rCustomerInfo.getMaxRerouteDRDelayTimeInSec()*1000;
									if(drRerouteCode.contains(message.getStatusCode())&& delayTime<0){										
										GmmsStatus temp = message.getStatus();
										message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
										message.setStatus(GmmsStatus.UNASSIGNED);							
										if(!rerouteOutMsg(message)){
											message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
											message.setStatus(temp);
											if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 									
													&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
													&&info.isNullFakeDRByInSubmitRecPrefixInCU()
													&& !info.isSmsOptionSendDRByInSubmitInCU()
													&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
												message.setStatus(GmmsStatus.DELIVERED);								
											}
											if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
													&&info.isNullFakeDRByInSubmitRecPrefixInCU()
													||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
													||info.isSmsOptionSendDRByInSubmitInCU()
													) {
											}else {
												sendDRMessage(message);
											}
										}
									}else{
										if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
												&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
												&&info.isNullFakeDRByInSubmitRecPrefixInCU()
												&& !info.isSmsOptionSendDRByInSubmitInCU()
												&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
											message.setStatus(GmmsStatus.DELIVERED);								
										}
										if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
												&&info.isNullFakeDRByInSubmitRecPrefixInCU()
												||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
												||info.isSmsOptionSendDRByInSubmitInCU()
												) {
										}else {
											sendDRMessage(message);
										}
									}
								}else{
									Date timeDate = message.getTimeStamp();									
									log.trace(message, "message timestamp is:{}, timestamp:{}, delayTime:{}", timeDate, timeDate.getTime(), rCustomerInfo.getMaxRerouteDRDelayTimeInSec());								
									long delayTime = System.currentTimeMillis()-timeDate.getTime()-8*3600*1000-rCustomerInfo.getMaxRerouteDRDelayTimeInSec()*1000;
									if(GmmsStatus.DELIVERED.getCode() != message.getStatusCode() && delayTime<0){
										
										GmmsStatus temp = message.getStatus();
										message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
										message.setStatus(GmmsStatus.UNASSIGNED);							
										if(!rerouteOutMsg(message)){
											message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
											message.setStatus(temp);
											if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
													&&info.isNullFakeDRByInSubmitRecPrefixInCU()
													&& !info.isSmsOptionSendDRByInSubmitInCU()
													&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
													&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
												message.setStatus(GmmsStatus.DELIVERED);								
											}
											if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
													&&info.isNullFakeDRByInSubmitRecPrefixInCU()
													||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
													||info.isSmsOptionSendDRByInSubmitInCU()
													) {
											}else {
												sendDRMessage(message);
											}
										}
									}else{
										if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
												&&info.isNullFakeDRByInSubmitRecPrefixInCU()
												&& !info.isSmsOptionSendDRByInSubmitInCU()
												&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
												&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
											message.setStatus(GmmsStatus.DELIVERED);								
										}
										if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
												&&info.isNullFakeDRByInSubmitRecPrefixInCU()
												||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
												||info.isSmsOptionSendDRByInSubmitInCU()
												) {
										}else {
											sendDRMessage(message);
										}
									}
								}													
							}else {
								//modify to support add success ratio in 20190201
								if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
										&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
										&& info.isNullFakeDRByInSubmitRecPrefixInCU()
										&& !info.isSmsOptionSendDRByInSubmitInCU()
										&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
									message.setStatus(GmmsStatus.DELIVERED);								
								}
								if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
										&& info.isNullFakeDRByInSubmitRecPrefixInCU()
										||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
										||info.isSmsOptionSendDRByInSubmitInCU()
										) {
								}else {
									sendDRMessage(message);
								}
							}
						}
					} else {
						
						//add new feature of send msg to backup routing when dr failed at 11.19
						//TODO
						A2PCustomerInfo rCustomerInfo = ctm.getCustomerBySSID(message.getRSsID());
						Date timeDate = message.getTimeStamp();									
						long delayTime = System.currentTimeMillis()-timeDate.getTime()-8*3600*1000-rCustomerInfo.getMaxRerouteDRDelayTimeInSec()*1000;
						if (rCustomerInfo!=null && rCustomerInfo.isNeedDrReroute() 
								&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()
								&& delayTime<0) {
							/*String inContent = message.getInTextContent();
							if (inContent!=null && !inContent.equals(message.getTextContent())) {									
								message.setTextContent(inContent);	
								message.setContentType(message.getInContentType());
								try {
									message.setMessageSize(inContent.getBytes(message
											.getContentType()).length);
								} catch (Exception e) {
									// TODO: handle exception
								}									
							}*/
							List<Integer> drRerouteCode = rCustomerInfo.getSmsOptionDRRerouteCode();
							
							if(drRerouteCode!=null && !drRerouteCode.isEmpty()){
								if(drRerouteCode.contains(message.getStatusCode())){									
									GmmsStatus temp = message.getStatus();
									message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
									message.setStatus(GmmsStatus.UNASSIGNED);							
									if(!rerouteOutMsg(message)){
										message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
										message.setStatus(temp);
										if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 									
												&& info.isNullFakeDRByInSubmitRecPrefixInCU()
												&& !info.isSmsOptionSendDRByInSubmitInCU()
												&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
												&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
											message.setStatus(GmmsStatus.DELIVERED);								
										}
										if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
												&& info.isNullFakeDRByInSubmitRecPrefixInCU()
												||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
												||info.isSmsOptionSendDRByInSubmitInCU()
												) {
										}else {
											sendDRMessage(message);
										}
									}
								}else{
									if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
											&& info.isNullFakeDRByInSubmitRecPrefixInCU()
											&& !info.isSmsOptionSendDRByInSubmitInCU()
											&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
											&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
										message.setStatus(GmmsStatus.DELIVERED);								
									}
									if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
											&&info.isNullFakeDRByInSubmitRecPrefixInCU()
											||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
											||info.isSmsOptionSendDRByInSubmitInCU()
											) {
									}else {
										sendDRMessage(message);
									}
								}
							}else{
								GmmsStatus temp = message.getStatus();
								message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
								message.setStatus(GmmsStatus.UNASSIGNED);							
								if(!rerouteOutMsg(message)){
									message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
									message.setStatus(temp);
									if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
											&&info.isNullFakeDRByInSubmitRecPrefixInCU()
											&& !info.isSmsOptionSendDRByInSubmitInCU()
											&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
											&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
										message.setStatus(GmmsStatus.DELIVERED);								
									}
									if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
											&&info.isNullFakeDRByInSubmitRecPrefixInCU()
											||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
											||info.isSmsOptionSendDRByInSubmitInCU()
											) {
									}else {
										sendDRMessage(message);
									}
								}
							}
							
						}else {
							if (GmmsUtility.isModifySuccessDR(info.getSSID(), info.getDrSucRatio(), info.getDrBiasRatio()) 
									&& info.isNullFakeDRByInSubmitRecPrefixInCU()
									&& !info.isSmsOptionSendDRByInSubmitInCU()
									&& rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())
									&& GmmsStatus.DELIVERED.getCode() != message.getStatusCode()) {
								message.setStatus(GmmsStatus.DELIVERED);								
							}
							if((info.isSmsOptionSendFakeDR()&&rCustomerInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress()))
									&&info.isNullFakeDRByInSubmitRecPrefixInCU()
									||info.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress())
									||info.isSmsOptionSendDRByInSubmitInCU()
									) {
							}else {
								sendDRMessage(message);
							}
						}
						
					}
				}
			}
		}
	}
	
	public void sendDRMessageForServerReject(GmmsMessage message) {
		A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message
				.getOSsID());
		int sendDrImmediately = oInfo.getSendDelayDRImmediately();
		if(sendDrImmediately == 1){
			sendDRMessage(message);
		}else{
			
			String delayTimeInterval = oInfo.getDrDelayTimeInSec();
			int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
			if(delayTime == 0) {
				sendDRMessage(message);
			}else {
				insertMsgForDRDelay(message, delayTime);
			}
													
		}
	}


	/**
	 * process send DR, Wrapper of doSendDRMessage
	 * 
	 * @param message
	 */
	public void sendDRMessage(GmmsMessage message) {
		// concatenated message and is the first DR
		if (ctm == null) {
			ctm = gmmsUtility.getCustomerManager();
		}
		A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message.getOSsID());
		boolean isSend = false;
		if (oInfo!=null && oInfo.isSmsOptionSendFakeDR()) {
			//if msg hasFakeDR flag can be sent.
			if (message.hasFakeSendDR()) {
				isSend = true;
			}			
		}else {
			isSend = true;
		}
		
		if (isSend) {
			if (message.getSarMsgRefNum() != null
					&& message.getSarMsgRefNum().trim().length() > 0) {
				List<GmmsMessage> msgList = new ArrayList<GmmsMessage>();
				if (log.isTraceEnabled()) {
					log.trace(message, "sendDRMessage, before splitCsmDr:{}",
							message.toString());
				}
				CsmUtility.splitCsmDr(message, msgList);
				for (GmmsMessage msg : msgList) {
					doSendDRMessage(msg);
				}
			} else {
				doSendDRMessage(message);
			}
		}
	}
	
	/*public void sendDRMessageByInSubmit(GmmsMessage message, int delayTime) {
		if (delayTime>0) {
			insertMsgForDRDelay(message, delayTime);
		}else {
			// concatenated message and is the first DR
			if (message.getSarMsgRefNum() != null
					&& message.getSarMsgRefNum().trim().length() > 0) {
				List<GmmsMessage> msgList = new ArrayList<GmmsMessage>();
				if (log.isTraceEnabled()) {
					log.trace(message, "sendDRMessage, before splitCsmDr:{}",
							message.toString());
				}
				CsmUtility.splitCsmDr(message, msgList);
				for (GmmsMessage msg : msgList) {
					doSendDRMessage(msg);
				}
			} else {
				doSendDRMessage(message);
			}
		}				
	}*/

	private void doSendDRMessage(GmmsMessage message) {
		try {
			if (ctm == null) {
				ctm = gmmsUtility.getCustomerManager();
			}
			int cA2P = message.getCurrentA2P();
			int oA2P = message.getOA2P();
			if ((cA2P == oA2P) || ctm.vpOnSameA2P(cA2P, oA2P)) { // on the same A2P
				if (!ctm.isNotDRSupport(message.getOSsID())) {
					if (ctm.isInDRStoreMode(message.getRSsID())) {
						insertDRMessageToSDQ(message);
						return;
					}
					if (!processorHandler.putMsg(message)) {
						message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
								.getCode());
						handleMessageError(message);
					}
				} else {
					if (log.isInfoEnabled()) {
						log.info(
								message,
								"The ossid: {} of this message doesn't supprot DR",
								message.getOSsID());
					}
				}
			} else if (ctm.isA2P(oA2P) || ctm.isPartition(oA2P)) {
				if (!processorHandler.putMsg(message)) {
					message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
							.getCode());
					handleMessageError(message);
				}
			} else {
				log.warn(
						message,
						"cA2P and oA2P isn't the same A2P and oA2P is also not A2P or ASG or Partition.So this DR can't be sent out.");
				
				GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
				if(converDRStatus!=null) {
					message.setStatus(converDRStatus);
				}else {
					message.setStatus(GmmsStatus.UNKNOWN);
				}
				handleMessageError(message);
			}

		} catch (Exception ex) {
			log.error(ex, ex);
			this.insertMessageToDB(message, RDQ);// added by Jianming in v1.0.1
		}
	}

	public void handleMessageError(GmmsMessage message) {
		message.setActionCode(-1);
		CDRManager cdrManager = gmmsUtility.getCdrManager();
		boolean supportDR = !gmmsUtility.getCustomerManager()
				.isRssidNotSupportDR(message.getRSsID());
		String messageType = message.getMessageType();
		RetryPolicyManager.getInstance().doRetryPolicy(message);
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) {
			cdrManager.logInDeliveryReportRes(message);
			if (RetryPolicyManager.getInstance().isLastRetry(message)) {
				clearMessage(message, RDQ);
			} else {
				if (RDQ.equalsIgnoreCase(message.getOriginalQueue())) {
					updateByInMsgId(message, RDQ);
				} else {
					insertMessageToDB(message, RDQ);
				}
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(messageType)) {
			cdrManager.logOutDeliveryReportRes(message);
			checkLastRetry(message);
		} else {
			if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)) {
				if (supportDR)
					cdrManager.logOutDeliveryRes(message, supportDR);
				else
					cdrManager.logOutDeliveryRes(message);
			} else if (GmmsMessage.MSG_TYPE_SUBMIT
					.equalsIgnoreCase(messageType)) {				
				if (supportDR)
					cdrManager.logOutSubmitRes(message, supportDR);
				else
					cdrManager.logOutSubmitRes(message);
			}
			if (needRetry(message)) {
				log.debug(message,"is need retry? "+message);
				checkLastRetry(message);
			} else {
				if (message.getOriginalQueue() != null) {
					clearMessage(message, message.getOriginalQueue());
				}
				if (message.getDeliveryReport() && message.getSplitStatus() < 2) {
					message.setRetriedNumber(0);
					message.setNextRetryTime(null);
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					//message.setStatusCode(GmmsStatus.UNDELIVERABLE.getCode());
					GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
					if(converDRStatus!=null) {
						message.setStatus(converDRStatus);
					}else {
						message.setStatus(GmmsStatus.UNDELIVERABLE);
					}
					message.setDateIn(new Date());
					message.setDeliveryChannel(module);
					if (message.getInClientPull() == 2) {
						if ((message.getOA2P() == message.getRA2P())
								||((message.getOA2P() != message.getRA2P())
								    && ((message.getOA2P() == message.getCurrentA2P()) || ctm.vpOnSameA2P(message.getOA2P(), message.getCurrentA2P())))) {
							updateMsgForDRQuery(message);
						} else {
							sendDRMessage(message);
						}
					} else {
						sendDRMessage(message);
					}
				}
			}
		}
	}

	/**
	 * Abstract method add() adds a Data object into the database
	 * 
	 * @param data
	 *            Data
	 * @throws DataManagerException
	 */
	public void add(Data data) throws DataManagerException {
		GmmsMessage message = (GmmsMessage) data;
		String strSQL = "INSERT INTO messagestore SET "
				+ makeUpdateAllStmt(message);
		try {
			if (message.getMimeMultiPartData() == null
					&& message.getUdh() == null) {
				doInsert(strSQL);
			} else {
				strSQL = strSQL + ",Payload=?, UDH = ?";
				PreparedStatement ps = this.getCon().prepareStatement(strSQL);
				ps.setBytes(1, ((GmmsMessage) data).getMimeMultiPartData());
				ps.setBytes(2, ((GmmsMessage) data).getUdh());
				ps.executeUpdate();
			}
			if (log.isDebugEnabled()) {
				log.debug(message, "inserted into messagestore.");
			}
		} catch (Exception e) {
			log.error(message, "Error in executing:" + strSQL, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
			throw new DataManagerException(e);
		} finally {
			super.closeSession();// add by Levens in Jan.20 2006
		}
	}

	public void insertDRMessageToSDQ(GmmsMessage message) {
		String strSQL = "INSERT INTO " + SDQ + " SET "
				+ makeUpdateForSDQ(message);
		try {
			doInsert(strSQL);
			if (log.isDebugEnabled()) {
				log.debug(message, "insert SQL command is {}", strSQL);
			}
		} catch (Exception e) {
			log.error(message, "Error in executing:" + strSQL, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	public void insertMessageToDB(GmmsMessage message, String table) {

		String strSQL = "INSERT INTO " + table + " SET "
				+ makeUpdateAllStmt(message);
		try {
			if (message.getMimeMultiPartData() == null
					&& message.getUdh() == null) {
				if (!asyncSqlQueue.offer(strSQL)) {
					doInsert(strSQL);
				}
			} else {
				strSQL = strSQL + ",Payload=?, UDH = ?";
				PreparedStatement ps = this.getCon().prepareStatement(strSQL);
				ps.setBytes(1, message.getMimeMultiPartData());
				ps.setBytes(2, message.getUdh());
				ps.executeUpdate();
				try { ps.close(); } catch (Exception e) {}
			}
			if (log.isDebugEnabled()) {
				log.debug(message, "inserted into {}", table);
			}
		} catch (Exception e) {
			log.error(message, "Error in executing:" + strSQL, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession(); // add by Levens in Jan.20 2006
		}
	}

	/**
	 * create PMQ table for TimerTask
	 * 
	 * @param startIndex
	 * @param tableNumber
	 * @throws DataManagerException
	 */
	public void createPMQTable(int tableNumber) throws DataManagerException {
		String pefix = "PMQ_" + getNextDate() + "_0";
		for (int i = 1; i <= tableNumber; i++) {
			StringBuffer sb = new StringBuffer();
			sb.append("CREATE TABLE IF NOT EXISTS `").append(pefix + i)
					.append("` (")
					.append("`ConnectionID` varchar(100) default '',")
					.append("`ID` int(10) NOT NULL auto_increment,")
					.append("`SenderAddrType` varchar(10) default '',")
					.append("`InTransID` varchar(100) default '',")
					.append("`OutTransID` varchar(100) default '',")
					.append("`InMsgID` varchar(1024) default '',")
					.append("`OutMsgID` varchar(100) default '',")
					.append("`OSsID` int(11) default '0',")
					.append("`RecipientAddrType` varchar(10) default '',")
					.append("`RSsID` int(11) default '0',")
					.append("`GmmsMsgType` varchar(100) default '',")
					.append("`MessageType` varchar(100) default '',")
					.append("`MessageSize` int(11) default '0',")
					.append("`ProtocolVersion` varchar(100) default '',")
					.append("`SenderAddresses` varchar(100) default '',")
					.append("`RecipientAddresses` varchar(100) default '',")
					.append("`OriginalSenderAddr` varchar(100) default '',")
					.append("`OriginalRecipientAddr` varchar(100) default '',")
					.append("`TimeMark` datetime default NULL,")
					.append("`ExpiryDate` datetime default NULL,")
					.append("`DeliveryReport` tinyint(4) default '0',")
					.append("`Priority` varchar(100) default NULL,")
					.append("`ContentType` varchar(200) default '',")
					.append("`TextContent` varchar(2048) default '',")
					.append("`Payload` mediumblob,")
					.append("`StatusCode` varchar(100) default '',")
					.append("`StatusText` varchar(100) default '',")
					.append("`MilterActionCode` int(4) default '0',")
					.append("`DateIn` datetime default NULL,")
					.append("`InClientPull` tinyint(4) NOT NULL default '0',")
					.append("`OutClientPull` tinyint(4) NOT NULL default '0',")
					.append("`ActionCode` int(11) default '-1',")
					.append("`Current_A2P` int(11) default '0',")
					.append("`O_A2P` int(11) default '0',")
					.append("`R_A2P` int(11) default '0',")
					.append("`MsgID` varchar(1024) default '',")
					.append("`DeliveryChannel` varchar(100) default '0',")
					.append("`O_Operator` int(11) default '-1',")
					.append("`R_Operator` int(11) default '-1',")
					.append("`Split` int(11) default '0',")
					.append("`OMncMcc` varchar(20) default '',")
					.append("`RMncMcc` varchar(20) default '',")
					.append("`RetriedNumber` int(4) default '0',")
					.append("`NextRetryTime` datetime default NULL,")
					.append("`msgRefNum` varchar(100) default '',")
					.append("`totalSegments` int(4) default '0',")
					.append("`segmentSeqNum` int(4) default '0',")
					.append("`UDH` tinyblob,")
					.append("`operatorPriority` int(11) default NULL,")
					.append("`SpecialDCS` varchar(10) default '',")
					.append("`InCsm` tinyint(4) NOT NULL default '0',")
					.append("PRIMARY KEY  (`ID`),")
					.append("KEY `OutMsgID` (`OutMsgID`),")
					.append("KEY `InmsgID` (`InMsgID`)")
					.append(") ENGINE=MyISAM DEFAULT CHARSET=utf8;\n");
			String sqlStr = sb.toString();
			if (log.isDebugEnabled()) {
				log.debug("createPMQTable sql:{}", sqlStr);
			}
			try {
				doUpdate(sqlStr);
			} catch (Exception e) {
				if (!e.getMessage().contains("Data truncated")
						&& !e.getMessage().contains("SQL syntax")) {

					MailSender.getInstance().sendAlertMail(
							"A2P alert mail from "
									+ ModuleURI.self().getAddress()
									+ " for DB Exception", e);
				}
				throw new DataManagerException(e);
			} finally {
				super.closeSession();
			}
		}
	}

	/**
	 * added by Jianming in v1.0.1 get pmq table name
	 * 
	 * @param tableNumber
	 * @return
	 */
	public String getPMQTableName() {
		TaskConfiguration config = TaskConfiguration.getInstance();
		int tableNumber = Integer.parseInt(config.getProperty(
				"PMQ.CreatePMQTableTask.Number", "4"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String date = sdf.format(new java.util.Date());
		sdf = new SimpleDateFormat("HH");
		String hour = sdf.format(new java.util.Date());
		int iHour = Integer.parseInt(hour);
		int seqId = ((tableNumber * iHour) / 24) + 1;
		String name = "PMQ_" + date + "_0" + seqId;
		return name;
	}

	/**
	 * 
	 * @return
	 */
	private String getNextDate() {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, 1);
		Date nextDate = cal.getTime();
		String next_dateStr = new SimpleDateFormat("yyyyMMdd").format(nextDate);
		return next_dateStr;
	}

	/**
	 * 
	 * @param message
	 * @param table
	 */
	public void clearMessage(GmmsMessage message, String table) {
		String sqlStr = null;
		if (message.getOutMsgID() != null
				&& message.getOutMsgID().trim().length() > 0
				&& table.equalsIgnoreCase(WDQ)) {
			sqlStr = "DELETE FROM " + table + " WHERE OutMsgID="
					+ makeSqlStr(message.getOutMsgID());
		} else {
			int splitStatus = message.getSplitStatus();
			if (splitStatus <= 0) {
				sqlStr = "DELETE FROM " + table + " WHERE InMsgID="
						+ makeSqlStr(message.getInMsgID());
			} else if (splitStatus == 1) {
				sqlStr = "DELETE FROM " + table + " WHERE InMsgID="
						+ makeSqlStr(message.getInMsgID())
						+ " and (Split = 1 or Split = 0)";
			} else {
				sqlStr = "DELETE FROM " + table + " WHERE InMsgID="
						+ makeSqlStr(message.getInMsgID()) + " and Split = "
						+ splitStatus;
			}
		}

		try {
			if (log.isDebugEnabled()) {
				log.debug(message, "clear message, statuscode:{}, table={}",
						message.getStatusCode(), table);
			}
			doDelete(sqlStr);
		} catch (Exception e) {
			log.error(message, e, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	public void clearMessages(String tableName) {
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}

		Date date = new Date(System.currentTimeMillis()
				- Integer.parseInt(gmmsUtility.getModuleProperty("ClearTime",
						"120")) * 60 * 60 * 1000 - diff);
		String oldtime = formatMySqlDate(date);

		Date processedMsgDeleteDate = new Date(System.currentTimeMillis()
				- Integer.parseInt(gmmsUtility.getModuleProperty(
						"ClearProcessedMsgTime", "2")) * 60 * 60 * 1000 - diff);
		String processedMsgDeleteTime = formatMySqlDate(processedMsgDeleteDate);

		// handle expired DR
		Date DRexpiredate = new Date(System.currentTimeMillis()
				- Integer.parseInt(gmmsUtility.getModuleProperty("DRClearTime",
						"48")) * 60 * 60 * 1000 - diff);

		String DRtime = formatMySqlDate(DRexpiredate);

		String sqlStr = "DELETE FROM " + tableName + " WHERE (TimeMark < "
				+ oldtime + " )";

		if (WDQ.equalsIgnoreCase(tableName)) {
			sqlStr = sqlStr
					+ " OR (DeliveryReport=1 AND StatusCode=12000 AND TimeMark < "
					+ DRtime + " )";
		} else if (PMQ.equalsIgnoreCase(tableName)) {
			sqlStr = "DELETE FROM " + tableName + " WHERE (TimeMark < "
					+ processedMsgDeleteTime + " )";
		}
		if (log.isTraceEnabled()) {
			log.trace("clearMessages->sqlStr={}", sqlStr);
		}
		try {
			doDelete(sqlStr);
		} catch (Exception e) {
			log.error("Error occur when clearing messages." + sqlStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	public void clearMessages() {
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}

		Date date = new Date(System.currentTimeMillis()
				- Integer.parseInt(gmmsUtility.getModuleProperty("ClearTime",
						"120")) * 60 * 60 * 1000 - diff);
		String oldtime = formatMySqlDate(date);

		// handle expired DR
		Date DRexpiredate = new Date(System.currentTimeMillis()
				- Integer.parseInt(gmmsUtility.getModuleProperty("DRClearTime",
						"48")) * 60 * 60 * 1000 - diff);

		String DRtime = formatMySqlDate(DRexpiredate);

		String sqlStr = "DELETE FROM messagestore WHERE (DeliveryReport=0 AND StatusCode=10000) OR (DeliveryReport=1 AND StatusCode=12000 AND TimeMark < "
				+ DRtime + " ) OR (TimeMark < " + oldtime + " )";
		try {
			doDelete(sqlStr);
		} catch (Exception e) {
			log.error("Error occur when clearing messages." + sqlStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	public ArrayList<GmmsMessage> getResendMessage(String priorityList) {
		if (priorityList == null || "".equals(priorityList)) {
			return null;
		}

		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> messageIds = new ArrayList<Integer>();

		String sqlStr = "SELECT * FROM " + RMQ + " WHERE ActionCode != 0 AND "
				+ "now() > NextRetryTime AND OperatorPriority in ("
				+ priorityList + ") AND " + getNoExpireSql()
				+ " ORDER BY DateIn ASC limit 500 ";
		if (log.isTraceEnabled()) {
			log.trace("getResendMessage->sqlStr={}", sqlStr);
		}
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage newMsg = new GmmsMessage();
				assignValue(rs, newMsg);
				newMsg.setActionCode(0);
				newMsg.setOriginalQueue(RMQ);
				newMsg.setRowId(rs.getInt("ID"));
				String deliveryChannel = newMsg.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						newMsg.setDeliveryChannel(dms[0]);
					}
				}

				messages.add(newMsg);
				messageIds.add(rs.getInt("ID"));
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateActionCode(messageIds, RMQ);
			return messages;
		}
	}

	public ArrayList<GmmsMessage> getResendDR() {
		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> messageIds = new ArrayList<Integer>();
		String finalExpire = getNoFinalExpireSql();
		String before = getExpireSqlAddDescTime();
		String[] sqlStr = new String[2];

		sqlStr[0] = "SELECT * FROM " + RDQ + " WHERE ActionCode != 0 AND "
				+ "now() > NextRetryTime AND O_A2P IN ("
				+ gmmsUtility.getCustomerManager().getCurrentA2Ps() + ") AND "
				+ finalExpire + " ORDER BY DateIn ASC limit 500";

		sqlStr[1] = "SELECT * FROM " + RDQ + " WHERE ActionCode != 0 AND "
				+ "now() > NextRetryTime AND O_A2P NOT IN ("
				+ gmmsUtility.getCustomerManager().getCurrentA2Ps() + ") AND "
				+ before + " ORDER BY DateIn ASC limit 500";

		// String sqlStr = "SELECT * FROM " + RDQ +
		// " WHERE ActionCode != 0 AND " +
		// "now() > NextRetryTime AND " + getNoFinalExpireSql() +
		// " ORDER BY DateIn ASC limit 100 ";
		if (log.isTraceEnabled()) {
			for (String str : sqlStr) {
				log.trace("getResendDR->sqlStr={}", str);
			}
		}
		ResultSet rs = null;
		try {
			for (String str : sqlStr) {
				rs = doSelect(str);
				while (rs.next()) {
					GmmsMessage newMsg = new GmmsMessage();
					assignValue(rs, newMsg);
					newMsg.setActionCode(0);
					newMsg.setOriginalQueue(RDQ);
					newMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					newMsg.setStatusCode(getCodeFromState(newMsg
							.getStatusText()));
					newMsg.setRowId(rs.getInt("ID"));
					String deliveryChannel = newMsg.getDeliveryChannel();
					if (deliveryChannel != null) {
						String[] dms = deliveryChannel.split(":");
						if (dms != null && dms.length > 1) {
							newMsg.setDeliveryChannel(dms[0]);
						}
					}

					messages.add(newMsg);
					messageIds.add(rs.getInt("ID"));
				} // end of while
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset{}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateActionCode(messageIds, RDQ);
			return messages;
		}
	}

	public ArrayList<GmmsMessage> getStoredDR() {
		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<GmmsMessage> msgs = new ArrayList<GmmsMessage>();
		ArrayList<GmmsMessage> storedDRs = new ArrayList<GmmsMessage>();
		ArrayList<Integer> msgIDs = new ArrayList<Integer>();

		String sqlStr = "SELECT * FROM " + SDQ
				+ " ORDER BY DateIn ASC limit 100 ";

		String sqlStr2 = "SELECT * FROM " + WDQ + " WHERE OutMsgID =";
		if (log.isTraceEnabled()) {
			log.trace("getStoredDR->sqlStr={}", sqlStr);
		}

		ResultSet rs = null;

		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage tempDR = new GmmsMessage();
				assignValue4SDQ(rs, tempDR);
				msgIDs.add(rs.getInt("ID"));
				storedDRs.add(tempDR);
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}

		GmmsMessage message = null;
		for (GmmsMessage temp : storedDRs) {
			if (log.isInfoEnabled()) {
				log.info("handle a DR:{} ", temp.getOutMsgID());
			}
			if (isRedisEnable) {
				String object = redis.getString(temp.getOutMsgID());
				if (object != null) {
					message = SerializableHandler
							.convertRedisMssage2GmmsMessage(object);
					if (message != null) {
						String dateKey = gmmsUtility.getRedisDateIn(message);
						if (dateKey != null) {
							redis.delPipeline(temp.getOutMsgID(), dateKey);
						} else {
							redis.del(temp.getOutMsgID());
						}
					}
				} else {
					try {
						message = this.getGmmsMessageByOutMsgID(temp
								.getOutMsgID());
					} catch (DataManagerException e) {
						log.error(
								"Can not find the message in message Queue, and out msg id is {}",
								temp.getOutMsgID());
					}
				}
			} else {
				try {
					message = this.getGmmsMessageByOutMsgID(temp.getOutMsgID());
				} catch (DataManagerException e) {
					log.error(
							"Can not find the message in message Queue, and out msg id is {}",
							temp.getOutMsgID());
				}
			}
			if (message != null) {
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				message.setStatusCode(temp.getStatusCode());

				if (!GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
						message.getStatusText())) {
					message.setStatusText(GmmsStatus.getStatus(
							message.getStatusCode()).getText());
				}

				message.setOutTransID(temp.getOutTransID());
				message.setTransaction(temp.getTransaction());
				message.setDateIn(temp.getDateIn());
				String deliveryChannel = temp.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						message.setDeliveryChannel(dms[0] + ":" + dms[1]);
					}
				}

				messages.add(message);
			}
		}

		storedDRs.clear();

		if (msgIDs.size() > 0) {
			clearMessages(msgIDs, SDQ);
		}

		try {
			msgIDs.clear();
			Iterator<GmmsMessage> iter = messages.iterator();
			while (iter.hasNext()) {
				GmmsMessage dr = (GmmsMessage) iter.next();
				gmmsUtility.getCdrManager().logOutDeliveryReportRes4StoreDR(dr);
				String deliveryChannel = dr.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						dr.setDeliveryChannel(dms[0]);
					}
				}
				msgIDs.add(dr.getRowId());
				
				long gmtNow = gmmsUtility.getGMTTime().getTime();
				long expiryDate = dr.getExpiryDate().getTime();
				
				if (GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
						dr.getStatusText())) {
					iter.remove();
				}else if(gmtNow < expiryDate && GmmsStatus.EXPIRED.getCode() == dr.getStatusCode()){
					iter.remove();
					if(dr.getOriginalQueue() == null){
						dr.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
						dr.setDeliveryChannel("");
						insertMessageToDB(dr, WDQ);
					}
					msgIDs.remove(new Integer(dr.getRowId()));
				}else if (!dr.getDeliveryReport() || dr.getSplitStatus() > 1) {
					iter.remove();
				} else {
					if (dr.getInClientPull() == 2) {
						updateMsgForDRQuery(dr);
					} else {
						if (dr.getSarMsgRefNum() != null
								&& dr.getSarMsgRefNum().trim().length() > 0) {
							CsmUtility.splitCsmDr(dr, msgs);
						} else {
							msgs.add(dr);
						}
					}
				}
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
		}

		if (msgIDs.size() > 0) {
			clearMessages(msgIDs, WDQ);
			return msgs;
		} else {
			return null;
		}

	}

	public ArrayList<GmmsMessage> getQueryDR() {
		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> messageIds = new ArrayList<Integer>();

		String sqlStr = "SELECT * FROM " + QDQ + " WHERE ActionCode != 0 AND "
				+ "((StatusCode=0 AND " + getNoExpireSql() + " AND R_A2P in ("
				+ gmmsUtility.getCustomerManager().getCurrentA2Ps() + ")) OR "
				+ "((StatusCode=9005 OR StatusCode=10105) AND "
				+ getNoExpireSql() + " AND now() > NextRetryTime))"
				+ " ORDER BY DateIn ASC limit 100 ";
		log.trace("getQueryDR->sqlStr={}", sqlStr);
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage newMsg = new GmmsMessage();
				assignValue(rs, newMsg);
				newMsg.setActionCode(0);
				newMsg.setOriginalQueue(QDQ);
				newMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY);
				newMsg.setRowId(rs.getInt("ID"));
				String deliveryChannel = newMsg.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						newMsg.setDeliveryChannel(dms[0]);
					}
				}

				messages.add(newMsg);
				messageIds.add(rs.getInt("ID"));
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateActionCode(messageIds, QDQ);
			return messages;
		}
	}

	public ArrayList<GmmsMessage> getStoredMessage(boolean isOOP, String OPList) {
		if (OPList == null || "".equals(OPList)) {
			return null;
		}

		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> messageIds = new ArrayList<Integer>();

		// SendStoredMessageTask/SendROPFailedMessageTask/SendScheduledMessageTask share SMQ,  
		// Should limit search condition to avoid concurrency issue
		Date gmtNow = gmmsUtility.getGMTTime();
		String scheduleStr = "(scheduleDeliveryTime is null OR " 
			               + makeSqlStr(dateFormat.format(gmtNow)) + " >= scheduleDeliveryTime)";
		
		String sqlStr = "SELECT * FROM " + SMQ + " WHERE ActionCode != 0 AND "
				+ "(NextRetryTime is null OR now() > NextRetryTime) AND "
				+ scheduleStr + " AND "
				+(isOOP ? "OSsID" : "RSsID") + " in (" + OPList + ") AND "
				+ getNoExpireSql() + " ORDER BY DateIn ASC limit 100 ";
		if (log.isTraceEnabled()) {
			log.trace("getStoredMessage->sqlStr={}", sqlStr);
		}
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage newMsg = new GmmsMessage();
				assignValue(rs, newMsg);
				newMsg.setActionCode(0);
				newMsg.setOriginalQueue(SMQ);
				newMsg.setRowId(rs.getInt("ID"));
				messages.add(newMsg);
				String deliveryChannel = newMsg.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						newMsg.setDeliveryChannel(dms[0]);
					}
				}

				messageIds.add(rs.getInt("ID"));
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateActionCode(messageIds, SMQ);
			return messages;
		}
	}
	
	/**
	 * getScheduledMessage
	 * SendStoredMessageTask/SendROPFailedMessageTask/SendScheduledMessageTask share SMQ
	 * @param storedMessageTaskOPList
	 * @param ropFailedMessageTaskOPList
	 * @return
	 */
	public ArrayList<GmmsMessage> getScheduledMessage(String storedMessageTaskOPList, String ropFailedMessageTaskOPList) {
		storedMessageTaskOPList = makeSqlStr(storedMessageTaskOPList);
		ropFailedMessageTaskOPList = makeSqlStr(ropFailedMessageTaskOPList);

		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> messageIds = new ArrayList<Integer>();
		
		Date gmtNow = gmmsUtility.getGMTTime();
		String scheduleStr = "(scheduleDeliveryTime is not null AND " 
			               + makeSqlStr(dateFormat.format(gmtNow)) + " >= scheduleDeliveryTime)";

		// SendStoredMessageTask/SendROPFailedMessageTask/SendScheduledMessageTask share SMQ,  
		// Should limit search condition to avoid concurrency issue
		String sqlStr = "SELECT * FROM " + SMQ + " WHERE ActionCode != 0 AND "
				+ "(NextRetryTime is null OR now() > NextRetryTime) AND "
				+ scheduleStr + " AND "
				+ "OSsID not in (" + storedMessageTaskOPList + ") AND " 
				+ "RSsID not in (" + ropFailedMessageTaskOPList + ") AND "
				+ getNoExpireSql() + " ORDER BY DateIn ASC limit 100 ";
		if (log.isTraceEnabled()) {
			log.trace("getScheduledMessage->sqlStr={}", sqlStr);
		}
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage newMsg = new GmmsMessage();
				assignValue(rs, newMsg);
				newMsg.setActionCode(0);
				newMsg.setOriginalQueue(SMQ);
				newMsg.setRowId(rs.getInt("ID"));
				messages.add(newMsg);
				String deliveryChannel = newMsg.getDeliveryChannel();
				if (deliveryChannel != null) {
					String[] dms = deliveryChannel.split(":");
					if (dms != null && dms.length > 1) {
						newMsg.setDeliveryChannel(dms[0]);
					}
				}

				messageIds.add(rs.getInt("ID"));
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateActionCode(messageIds, SMQ);
			return messages;
		}
	}

	public ArrayList<GmmsMessage> getExpiredMessage(String table) {

		if (ctm == null) {
			ctm = gmmsUtility.getCustomerManager();
		}
		ArrayList<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		ArrayList<Integer> deleteMessageIds = new ArrayList<Integer>();
		ArrayList<Integer> updateMessageIds = new ArrayList<Integer>();
		int defaultExpireTime = gmmsUtility.getExpireTimeInMinute();
		int defaultFinalExpireTime = gmmsUtility.getFinalExpireTimeInMinute();
		String[] sqlStr = null;
		if (table.equalsIgnoreCase("WDQ")) {
			sqlStr = new String[2];
			String expire = getExpireSqlForWDQ();
			String before = getExpireSqlAddDescTime();
			String notFinalExpire = getNoFinalExpireSql();

			sqlStr[0] = "SELECT * FROM " + table
					+ " WHERE ActionCode != 0 AND "
					+ "DeliveryReport = 1 AND Split < 2 AND O_A2P IN ("
					+ gmmsUtility.getCustomerManager().getCurrentA2Ps()
					+ ") AND " + expire + " AND " + notFinalExpire
					+ " ORDER BY DateIn ASC limit 50 ";

			sqlStr[1] = "SELECT * FROM " + table
					+ " WHERE ActionCode != 0 AND "
					+ "DeliveryReport = 1 AND Split < 2 AND "
					+ "O_A2P NOT IN ("
					+ gmmsUtility.getCustomerManager().getCurrentA2Ps()
					+ ") AND " + expire + " AND " + before
					+ " ORDER BY DateIn ASC limit 50 ";

		} else {
			sqlStr = new String[1];
			sqlStr[0] = "SELECT * FROM " + table
					+ " WHERE ActionCode != 0 AND " + getExpireSql() + " AND "
					+ getNoFinalExpireSql()
					+ " AND DeliveryReport = 1 AND Split < 2 "
					+ " ORDER BY DateIn ASC limit 100 ";

		}
		if (log.isTraceEnabled()) {
			for (String str : sqlStr) {
				log.trace("getExpiredMessage->sqlStr={}", str);
			}
		}
		ResultSet rs = null;
		try {
			for (String str : sqlStr) {
				rs = doSelect(str);
				while (rs.next()) {
					GmmsMessage newMsg = new GmmsMessage();
					assignValue(rs, newMsg);
					newMsg.setActionCode(0);
					switch (newMsg.getStatusCode()) {
					case 0:
						if (ctm.isSupportExpiredDR(newMsg.getOSsID())) {
							newMsg.setStatus(GmmsStatus.EXPIRED);
						} else {
							newMsg.setStatus(GmmsStatus.DELIVERED);
						}
						break;
					case 9005:
					case 10105:
						if (ctm.isSupportExpiredDR(newMsg.getOSsID())) {
							newMsg.setStatus(GmmsStatus.EXPIRED);
						} else {
							newMsg.setStatus(GmmsStatus.DELIVERED);
						}
						break;
					case -2:
					case 105:
					case 2000:
					case 2010:
					case 2100:
					case 2110:
					case 2120:
					case 2130:
					case 2200:
					case 2210:
					case 2220:
					case 2300:
					case 5000:
					case 9000:
						newMsg.setStatus(GmmsStatus.UNDELIVERABLE);
						break;

					case 2500:
					case 2600:
					case 3000:
					case 4000:
						if (ctm.isSupportExpiredDR(newMsg.getOSsID())) {
							newMsg.setStatus(GmmsStatus.EXPIRED);
						} else {
							newMsg.setStatus(GmmsStatus.UNDELIVERABLE);
						}
						break;
					case -1:
						if (table.equalsIgnoreCase("CSMQ")) {
							if (ctm.isSupportExpiredDR(newMsg.getOSsID())) {
								newMsg.setStatus(GmmsStatus.EXPIRED);
							} else {
								newMsg.setStatus(GmmsStatus.UNDELIVERABLE);
							}
						}
						break;
					default:
						break;

					}
					newMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					newMsg.setRowId(rs.getInt("ID"));

					newMsg.setDeliveryChannel(module);
					// process for concatenated message DR: RMQ, WDQ, QDQ, CSMQ
					// concatenated message and is the first DR
					String sarRefNum = newMsg.getSarMsgRefNum();
					// don't use CsmUtility.isConcatenatedMsg, since assemble
					// will reset other two prams of CSM
					if (sarRefNum != null && sarRefNum.trim().length() > 0) {
						List<GmmsMessage> splitMsgList = new ArrayList<GmmsMessage>();
						if (log.isTraceEnabled()) {
							log.trace(newMsg,
									"getExpiredMessage before splitCsmDr:{}",
									newMsg.toString());
						}
						CsmUtility.splitCsmDr(newMsg, splitMsgList);
						messages.addAll(splitMsgList);
					} else {
						messages.add(newMsg);
					}

					if (WDQ.equalsIgnoreCase(table)
							&& isRSSIDLongerThanOSSID(newMsg,
									defaultExpireTime, defaultFinalExpireTime)) {
						updateMessageIds.add(rs.getInt("ID"));
					} else {
						deleteMessageIds.add(rs.getInt("ID"));
					}
				}
			}
		} catch (Exception ex) {
			log.error("Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					log.warn("Catch exception when close resultset {}",
							e.getMessage());
				}
			}
			super.closeSession();
		}
		if (messages.size() == 0) {
			return null;
		} else {
			updateStatusToRetrived(updateMessageIds, table);
			clearMessages(deleteMessageIds, table);
			return messages;
		}
	}

	private void updateActionCode(ArrayList<Integer> messageIds, String table) {
		String ids = messageIds.toString();
		String updateStr = "UPDATE " + table
				+ " set ActionCode=0 where ID in ("
				+ ids.substring(1, ids.length() - 1) + ")";
		try {
			doUpdate(updateStr);
		} catch (Exception e) {
			log.error("Error in executing:" + updateStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	private void updateStatusToRetrived(ArrayList<Integer> messageIds,
			String table) {
		if (messageIds == null || messageIds.size() <= 0) {
			return;
		}
		String ids = messageIds.toString();
		String updateStr = "UPDATE "
				+ table
				+ " set StatusCode=12000, StatusText='Retrieved', ActionCode=0 where ID in ("
				+ ids.substring(1, ids.length() - 1) + ")";
		try {
			doUpdate(updateStr);
		} catch (Exception e) {
			log.error("Error in executing:" + updateStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	private void clearMessages(ArrayList<Integer> messageIds, String table) {
		if (messageIds == null || messageIds.size() <= 0) {
			return;
		}
		String ids = messageIds.toString();
		String updateStr = "DELETE FROM " + table + " WHERE ID in ("
				+ ids.substring(1, ids.length() - 1) + ")";
		if (log.isTraceEnabled()) {
			log.trace("deltet table {} ->sqlStr={}", table, updateStr);
		}
		try {
			doUpdate(updateStr);
		} catch (Exception e) {
			log.error("Error in executing:" + updateStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	private String getExpireSqlAddDescTime() {
		long descend = Long.parseLong(gmmsUtility
				.getCommonProperty("DescendingTime")) * 60 * 1000; // The second
																	// A2P need
																	// before
																	// the first
																	// 10 mins.
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}

		java.util.Date gmtNow = new java.util.Date(now - diff - descend); // transfer
																			// localtime
																			// to
																			// GMT
		// character String of expire
		String expireStr = "(" + makeSqlStr(dateFormat.format(gmtNow))
				+ " < ExpiryDate)";
		return expireStr;

	}

	private String getExpireSql() {
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		java.util.Date gmtNow = new java.util.Date(now - diff); // transfer
																// localtime to
																// GMT
		// character String of expire
		String expireStr = "(" + makeSqlStr(dateFormat.format(gmtNow))
				+ " >= ExpiryDate)";
		return expireStr;
	}
	
	private String getExpireSqlForWDQ() {
		long now = new Date().getTime();
		java.util.Date gmtNow = new java.util.Date(now); // transfer
																// localtime to
																// GMT
		// character String of expire
		String expireStr = "(" + makeSqlStr(dateFormat.format(gmtNow))
				+ " >= ExpiryDate)";
		return expireStr;
	}

	private String getNoExpireSql() {
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		java.util.Date gmtNow = new java.util.Date(now - diff); // transfer
																// localtime to
																// GMT
		// character String of no expire
		String noExpireStr = "(" + makeSqlStr(dateFormat.format(gmtNow))
				+ " < ExpiryDate)";
		return noExpireStr;
	}

	private String getNoFinalExpireSql() {
		long now = new Date().getTime();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		int expireTime = gmmsUtility.getExpireTimeInMinute() * 60 * 1000;
		int finalExpireTime = gmmsUtility.getFinalExpireTimeInMinute() * 60 * 1000;

		// character String of noFinalExpireStr

		// FinalExpire=ExpiryDate + (finalExpireyTime - expiryTime)
		// NoFinalExpireSql = gmtNow < ExpiryDate + (finalExpireyTime -
		// expiryTime)
		// NoFinalExpireSql = gmtNow - (finalExpireyTime - expiryTime) <
		// ExpiryDate
		java.util.Date gmtExpire = new java.util.Date(now - diff
				- (finalExpireTime - expireTime)); // transfer localtime to GMT
		String noFinalExpireStr = "("
				+ makeSqlStr(dateFormat.format(gmtExpire)) + " < ExpiryDate)";
		return noFinalExpireStr;
	}

	/**
	 * Added by Bill, July, 2004 get Gmms message by outTransactionID and
	 * outMsgID
	 * 
	 * @param outTransactionID
	 *            String
	 * @param outMsgID
	 *            String
	 * @return GmmsMessage
	 * @throws DataManagerException
	 */
	public GmmsMessage getGmmsMessageByOutTransMsgID(String outTransactionID,
			String outMsgID) throws DataManagerException {
		return getGmmsMessageByOutTransMsgID(outTransactionID, outMsgID, WDQ);
	}

	public GmmsMessage getGmmsMessageByOutTransMsgID(String outTransactionID,
			String outMsgID, String table) throws DataManagerException {
		if (outTransactionID == null || outMsgID == null) {
			return null;
		}
		String sqlStr = "SELECT * FROM " + table + " WHERE OutTransID = "
				+ makeSqlStr(outTransactionID) + " and OutMsgID = "
				+ makeSqlStr(outMsgID);
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			if (rs.next()) {
				GmmsMessage msg = new GmmsMessage();
				assignValue(rs, msg);
				msg.setRowId(rs.getInt("ID"));
				return msg;
			}
			// rs.close(); //add by Sam 2005-06-24
		} catch (Exception e) {
			log.error("Error in executing:" + sqlStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
			throw new DataManagerException(e);
		}
		// add by Levens in Jan.20 2006
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception ex) {
					log.error(ex, ex);
					if (!ex.getMessage().contains("Data truncated")
							&& !ex.getMessage().contains("SQL syntax")) {

						MailSender.getInstance().sendAlertMail(
								"A2P alert mail from "
										+ ModuleURI.self().getAddress()
										+ " for DB Exception", ex);
					}
				}
			}
			super.closeSession();// add by Levens in Jan.20 2006
		}

		return null;
	}

	public GmmsMessage getGmmsMessageByOutMsgID(String outMsgID)
			throws DataManagerException {
		return getGmmsMessageByOutMsgID(outMsgID, WDQ);
	}

	/**
	 * Added by Bill, July, 2004 get Gmms message by outMsgID
	 * 
	 * @param outMsgID
	 *            String
	 * @return GmmsMessage
	 * @throws DataManagerException
	 */
	public GmmsMessage getGmmsMessageByOutMsgID(String outMsgID,
			String tableName) throws DataManagerException {
		GmmsMessage msg = null;
		ResultSet rs = null;

		try {
			if (outMsgID == null) {
				return null;
			}
			String sqlStr = "SELECT * FROM " + tableName + " WHERE OutMsgID = "
					+ makeSqlStr(outMsgID);

			rs = doSelect(sqlStr);
			if (rs.next()) {
				msg = new GmmsMessage();
				assignValue(rs, msg);
				msg.setRowId(rs.getInt("ID"));
				msg.setOriginalQueue(tableName);
				return msg;
			}
		} catch (Exception e) {
			log.error("Error when get GmmsMessage by outmsgid:" + outMsgID, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
			throw new DataManagerException(e);
		}
		// add by Levens in Jan.20 2006
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception ex) {
					log.error(ex, ex);
					if (!ex.getMessage().contains("Data truncated")
							&& !ex.getMessage().contains("SQL syntax")) {

						MailSender.getInstance().sendAlertMail(
								"A2P alert mail from "
										+ ModuleURI.self().getAddress()
										+ " for DB Exception", ex);
					}
				}
			}
			super.closeSession();// add by Levens in Jan.20 2006
			if (msg == null) {
				log.warn("Can't find the message from {} by outmsgid:{}",
						tableName, outMsgID);
			}
		}

		return msg;
	}

	/**
	 * return gmmsMessage by inMsgID
	 * 
	 * @param outMsgID
	 *            String
	 * @return GmmsMessage
	 * @throws DataManagerException
	 */
	public GmmsMessage getGmmsMessageByInMsgID(String outMsgID)
			throws DataManagerException {
		return getGmmsMessageByInMsgID(outMsgID, WDQ);
	}

	public GmmsMessage getGmmsMessageByInMsgID(String outMsgID, String table)
			throws DataManagerException {
		if (outMsgID == null) {
			return null;
		}

		String sqlStr = "SELECT * FROM " + table + " WHERE InMsgID = "
				+ makeSqlStr(outMsgID);
		ResultSet rs = null;
		try {
			rs = doSelect(sqlStr);
			if (rs.next()) {
				GmmsMessage msg = new GmmsMessage();
				assignValue(rs, msg);
				msg.setRowId(rs.getInt("ID"));
				return msg;
			}
			// rs.close(); //add by Sam 2005-06-24
		} catch (Exception e) {
			log.error("Error in executing:" + sqlStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
			throw new DataManagerException(e);
		}
		// add by Levens in Jan.20 2006
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception ex) {
					log.error(ex, ex);
					if (!ex.getMessage().contains("Data truncated")
							&& !ex.getMessage().contains("SQL syntax")) {

						MailSender.getInstance().sendAlertMail(
								"A2P alert mail from "
										+ ModuleURI.self().getAddress()
										+ " for DB Exception", ex);
					}
				}
			}
			super.closeSession();// add by Levens in Jan.20 2006
		}

		return null;
	}

	public void updateByInMsgId(GmmsMessage message, String table) {

		int splitStatus = message.getSplitStatus();
		String sqlStr = null;
		if (splitStatus <= 0) {
			sqlStr = "UPDATE " + table + " SET " + makeUpdateAllStmt(message)
					+ " WHERE InMsgID=" + makeSqlStr(message.getInMsgID());
		} else if (splitStatus == 1) {
			sqlStr = "UPDATE " + table + " SET " + makeUpdateAllStmt(message)
					+ " WHERE InMsgID=" + makeSqlStr(message.getInMsgID())
					+ " and (Split = 1 or Split = 0)";
		} else {
			sqlStr = "UPDATE " + table + " SET " + makeUpdateAllStmt(message)
					+ " WHERE InMsgID=" + makeSqlStr(message.getInMsgID())
					+ " and Split = " + splitStatus;
		}

		try {
			if (!asyncSqlQueue.offer(sqlStr)) {
				doUpdate(sqlStr);
			}
		} catch (Exception ex) {
			log.error(message, "Error in executing:" + sqlStr, ex);
			if (!ex.getMessage().contains("Data truncated")
					&& !ex.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", ex);
			}
		} finally {
			message.setContentIsChanged(false);
			super.closeSession(); // add by Levens in Jan.20 2006
		}
	}

	/**
	 * get the statecode from state of deliveryreport
	 * 
	 * @param state
	 *            String
	 * @return int
	 */
	private int getCodeFromState(String state) {
		if (state == null) {
			return 10900;
		} else if (GmmsStatus.DELIVERED.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.DELIVERED.getCode();
		} else if (GmmsStatus.EXPIRED.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.EXPIRED.getCode();
		} else if (GmmsStatus.DELETED.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.DELETED.getCode();
		} else if (GmmsStatus.UNDELIVERABLE.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.UNDELIVERABLE.getCode();
		} else if (GmmsStatus.REJECTED.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.REJECTED.getCode();
		} else if (GmmsStatus.UNKNOWN.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.UNKNOWN.getCode();
		} else if (GmmsStatus.ENROUTE.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.ENROUTE.getCode();
		}else if (GmmsStatus.ACCEPT.getText().equalsIgnoreCase(state)) {
			return GmmsStatus.ACCEPT.getCode();
		} else {
			return 10900;
		}
	}

	/**
	 * return status text by status code
	 * 
	 * @param code
	 *            int
	 * @return String
	 */
	private String getStateTextFromCode(int code) {
		switch (code) {
		case 10000:
		case 10105:
			return GmmsStatus.DELIVERED.getText();
		case 10200:
			return GmmsStatus.EXPIRED.getText();
		case 10300:
			return GmmsStatus.DELETED.getText();
		case 10400:
			return GmmsStatus.UNDELIVERABLE.getText();
		case 10500:
			return GmmsStatus.REJECTED.getText();
		case 10900:
		default:
			return GmmsStatus.UNKNOWN.getText();
		}
	}

	/**
	 * judge whether one message is expired or not.
	 * 
	 * @param expireTime
	 *            int : minute
	 * @param message
	 *            GmmsMessage
	 * @return boolean
	 */
	private boolean isExpired(int expireTime, GmmsMessage message) {
		long now = new Date().getTime();

		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}

		java.util.Date gmtNow = new java.util.Date(now - diff); // transfer
																// localtime to
																// GMT
		if (message.getExpiryDate() != null) {
			return gmtNow.after(message.getExpiryDate());
		} else {
			if (message.getTimeStamp() == null) {
				return true;
			} else {
				long nowTimemilliS = gmtNow.getTime();
				long timeMarkmilliS = message.getTimeStamp().getTime();
				return nowTimemilliS - timeMarkmilliS > (long) expireTime * 60 * 1000;
			}
		}
	}

	private boolean isRSSIDLongerThanOSSID(GmmsMessage message,
			int defaultExpireTime, int defaultFinalExpireTime) {
		long oExpireMillis = 0;
		long rFinalExpireMillis = 0;

		if (message.getExpiryDate() != null) {
			oExpireMillis = message.getExpiryDate().getTime();
		} else if (message.getTimeStamp() != null) {
			oExpireMillis = message.getTimeStamp().getTime()
					+ defaultExpireTime * 60 * 1000;
		} else {
			return true;
		}

		if (message.getRSsID() < 0 || message.getDateIn() == null) {
			return true;
		} else {
			int rFinalExpireTime = ctm.getCustomerBySSID(message.getRSsID())
					.getFinalExpireTime();
			if (rFinalExpireTime <= 0) {
				rFinalExpireTime = defaultFinalExpireTime;
			}
			long diff = local.getRawOffset();
			if (local.inDaylightTime(new Date())) {
				diff += local.getDSTSavings();
			}
			rFinalExpireMillis = message.getDateIn().getTime() - diff
					+ rFinalExpireTime * 60 * 1000;
		}

		return (rFinalExpireMillis - oExpireMillis > 0);
	}

	/**
	 * make sql string to operate on database
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return String
	 */
	private String makeUpdateStmt(GmmsMessage msg) {
		StringBuilder sb = new StringBuilder(1024);
		sb.append("InTransID=").append(makeSqlStrReplace(msg.getInTransID()))
				.append(", ");
		sb.append("OutTransID=").append(makeSqlStrReplace(msg.getOutTransID()))
				.append(", ");
		sb.append("MsgID=").append(makeSqlStr(msg.getMsgID())).append(",");
		sb.append("InMsgID=").append(makeSqlStrReplace(msg.getInMsgID()))
				.append(", ");
		sb.append("OutMsgID=").append(makeSqlStrReplace(msg.getOutMsgID()))
				.append(", ");
		sb.append("O_Operator=").append(msg.getOoperator()).append(", ");
		sb.append("R_operator=").append(msg.getRoperator()).append(", ");
		sb.append("OSsID=").append(msg.getOSsID()).append(", ");
		sb.append("RSsID=").append(msg.getRSsID()).append(", ");
		sb.append("GmmsMsgType=").append(makeSqlStr(msg.getGmmsMsgType()))
				.append(", ");
		sb.append("MessageType=").append(makeSqlStr(msg.getMessageType()))
				.append(", ");
		sb.append("MessageSize=").append(msg.getMessageSize()).append(", ");
		sb.append("ProtocolVersion=")
				.append(makeSqlStr(msg.getProtocolVersion())).append(", ");
		sb.append("SenderAddresses=")
				.append(makeSqlStrReplace(msg.getSenderAddress())).append(", ");
		sb.append("RecipientAddresses=")
				.append(makeSqlStrReplace(msg.getRecipientAddress()))
				.append(", ");
		sb.append("OriginalSenderAddr=")
				.append(makeSqlStrReplace(msg.getOriginalSenderAddr()))
				.append(", ");
		sb.append("OriginalRecipientAddr=")
				.append(makeSqlStrReplace(msg.getOriginalRecipientAddr()))
				.append(", ");
		sb.append("TimeMark=").append(formatMySqlDate(msg.getTimeStamp()))
				.append(", ");
		if (msg.getExpiryDate() == null) {
			int expireTime = gmmsUtility.getExpireTimeInMinute();
			Date expireDate = new Date(msg.getTimeStamp().getTime()
					+ expireTime * 60 * 1000);
			msg.setExpiryDate(expireDate);
		}
		sb.append("ExpiryDate=").append(formatMySqlDate(msg.getExpiryDate()))
				.append(", ");
		sb.append("DeliveryReport=")
				.append((msg.getDeliveryReport() ? "1" : "0")).append(", ");
		// added by linda to support custom retry
		sb.append("RetriedNumber=").append(msg.getRetriedNumber()).append(", ");
		sb.append("NextRetryTime=")
				.append(formatMySqlDate(msg.getNextRetryTime())).append(", ");
		if (msg.getOMncMcc() != null) {
			sb.append("OMncMcc=").append(makeSqlStr(msg.getOMncMcc()))
					.append(", ");
		}
		if (msg.getRMncMcc() != null) {
			sb.append("RMncMcc=").append(makeSqlStr(msg.getRMncMcc()))
					.append(", ");
		}
		// support Concatenated Message
		if (msg.getSarMsgRefNum() != null) {
			sb.append("msgRefNum= ").append(makeSqlStr(msg.getSarMsgRefNum()))
					.append(", ");
		}
		sb.append("totalSegments=").append(msg.getSarTotalSeqments())
				.append(", ");
		sb.append("segmentSeqNum=").append(msg.getSarSegmentSeqNum())
				.append(", ");
		sb.append("Priority=").append(msg.getPriority()).append(", ");
		sb.append("ContentType=").append(makeSqlStr(msg.getContentType()))
				.append(", ");
		sb.append("InContentType=").append(makeSqlStr(msg.getInContentType()))
		.append(", ");
		sb.append("RoutingSsIDs=").append(makeSqlStr(msg.getRoutingSsIDs()))
		.append(", ");
		sb.append("StatusCode=").append(msg.getStatusCode()).append(", ");
		sb.append("StatusText=").append(makeSqlStr(msg.getStatusText()))
				.append(", ");
		// sb.append("InClientPull=").append((msg.inClientPull() ? "1" : "0"))
		// .append(", ");
		sb.append("InClientPull=").append((msg.getInClientPull())).append(",");

		sb.append("OutClientPull=").append((msg.outClientPull() ? "1" : "0"))
				.append(", ");
		sb.append("DeliveryChannel=")
				.append(makeSqlStr(msg.getDeliveryChannel())).append(",");
		sb.append("SenderAddrType=")
				.append(makeSqlStr(msg.getSenderAddrType())).append(",");
		sb.append("RecipientAddrType=")
				.append(makeSqlStr(msg.getRecipientAddrType())).append(",");
		sb.append("SenderAddrTon=").append(makeSqlStr(msg.getSenderAddrTon()))
				.append(",");
		sb.append("RecipientAddrTon =")
				.append(makeSqlStr(msg.getRecipientAddrTon())).append(",");
		sb.append("MilterActionCode=").append(msg.getMilterActionCode())
				.append(",");
		sb.append("R_A2P=").append(msg.getRA2P()).append(",");
		sb.append("O_A2P=").append(msg.getOA2P()).append(",");
		sb.append("Current_A2P=").append(msg.getCurrentA2P()).append(",");
		sb.append("ActionCode=").append(msg.getActionCode()).append(",");
		sb.append("Split=").append(msg.getSplitStatus()).append(",");
		sb.append("ConnectionID=").append(makeSqlStr(msg.getConnectionID()))
				.append(",");
		sb.append("OperatorPriority=").append(msg.getOperatorPriority())
				.append(", ");
		sb.append("SpecialDCS=")
				.append(makeSqlStr(msg.getSpecialDataCodingScheme()))
				.append(", ");// added
								// by
								// Jianming
								// in
								// v1.0.1
		sb.append("DateIn=NOW() ").append(", ");
		sb.append("InCsm=").append((msg.isInCsm() ? "1" : "0"));
		sb.append(",").append("ServiceTypeID=").append(msg.getServiceTypeID());
		sb.append(",").append("ScheduleDeliveryTime=").append(formatMySqlDate(msg.getScheduleDeliveryTime()));
		return sb.toString();
	}

	/**
	 * make up sql string
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return String
	 */
	private String makeUpdateAllStmt(GmmsMessage msg) {
		StringBuilder sb = new StringBuilder(1024);
		sb.append(makeUpdateStmt(msg));
		sb.append(",")
		.append("InTextContent=")
		.append(makeSqlStrReplace(gmmsUtility.modifybackslash(
				msg.getInTextContent(), 1)));
		sb.append(",")
				.append("TextContent=")
				.append(makeSqlStrReplace(gmmsUtility.modifybackslash(
						msg.getTextContent(), 1)));
		return sb.toString();
	}

	private String makeUpdateForSDQ(GmmsMessage msg) {

		StringBuilder sb = new StringBuilder(1024);
		sb.append("RSsID=").append(msg.getRSsID()).append(", ");
		sb.append("OutMsgID=").append(makeSqlStrReplace(msg.getOutMsgID()))
				.append(", ");
		sb.append("TextContent=")
				.append(makeSqlStrReplace(gmmsUtility.modifybackslash(
						msg.getTextContent(), 1))).append(", ");
		sb.append("StatusCode=").append(msg.getStatusCode()).append(", ");
		sb.append("StatusText=").append(makeSqlStr(msg.getStatusText()))
				.append(", ");
		sb.append("DeliveryChannel=")
				.append(makeSqlStrReplace(msg.getDeliveryChannel()))
				.append(", ");
		sb.append("DateIn=NOW() ");

		return sb.toString();
	}

	private void assignValue4SDQ(ResultSet rs, GmmsMessage msg)
			throws SQLException {
		msg.setRSsID(rs.getInt("RSsID"));
		msg.setOutMsgID(rs.getString("OutMsgID"));
		msg.setStatusCode(rs.getInt("StatusCode"));
		msg.setStatusText(rs.getString("StatusText"));
		msg.setTextContent(rs.getString("TextContent"));
		msg.setDeliveryChannel(rs.getString("DeliveryChannel"));
		msg.setDateIn(rs.getTimestamp("DateIn"));
	}

	/**
	 * assign message fields value from database
	 * 
	 * @param rs
	 *            ResultSet
	 * @param msg
	 *            GmmsMessage
	 * @throws SQLException
	 */
	private void assignValue(ResultSet rs, GmmsMessage msg) throws SQLException {
		msg.setInTransID(rs.getString("InTransID"));
		msg.setOutTransID(rs.getString("OutTransID"));
		msg.setMsgID(rs.getString("MsgID"));
		msg.setInMsgID(rs.getString("InMsgID"));
		msg.setOutMsgID(rs.getString("OutMsgID"));
		msg.setOSsID(rs.getInt("OSsID"));
		msg.setRSsID(rs.getInt("RSsID"));
		msg.setGmmsMsgType(rs.getString("GmmsMsgType"));
		msg.setMessageType(rs.getString("MessageType"));
		msg.setMessageSize(rs.getInt("MessageSize"));
		msg.setProtocolVersion(rs.getString("ProtocolVersion"));
		msg.setSenderAddress(rs.getString("SenderAddresses"));
		msg.setRecipientAddress(rs.getString("RecipientAddresses"));
		msg.setTimeStamp(rs.getTimestamp("TimeMark"));
		msg.setExpiryDate(rs.getTimestamp("ExpiryDate"));
		msg.setDeliveryReport(rs.getInt("DeliveryReport") != 0);
		msg.setPriority(rs.getInt("Priority"));
		msg.setContentType(rs.getString("ContentType"));
		msg.setInContentType(rs.getString("InContentType"));
		msg.setRoutingSsIDs(rs.getString("RoutingSsIDs"));
		msg.setStatusCode(rs.getInt("StatusCode"));
		msg.setStatusText(rs.getString("StatusText"));
		msg.setTextContent(rs.getString("TextContent"));
		msg.setInTextContent(rs.getString("InTextContent"));
		msg.setMimeMultiPartData(rs.getBytes("Payload"));

		// msg.setInClientPull((rs.getInt("InClientPull")) != 0);

		msg.setInClientPull(rs.getInt("InClientPull"));

		msg.setOutClientPull((rs.getInt("OutClientPull")) != 0);
		msg.setDeliveryChannel(rs.getString("DeliveryChannel"));
		msg.setSenderAddrType(rs.getString("SenderAddrType"));
		msg.setRecipientAddrType(rs.getString("RecipientAddrType"));
		msg.setSenderAddrTon(rs.getString("SenderAddrTon"));
		msg.setRecipientAddrTon(rs.getString("RecipientAddrTon"));
		msg.setDateIn(rs.getTimestamp("DateIn"));
		msg.setMilterActionCode(rs.getInt("MilterActionCode"));
		msg.setRA2P(rs.getInt("R_A2P"));
		msg.setOA2P(rs.getInt("O_A2P"));
		msg.setCurrentA2P(rs.getInt("Current_A2P"));
		msg.setActionCode(rs.getInt("ActionCode"));
		msg.setOoperator(rs.getInt("O_Operator"));
		msg.setRoperator(rs.getInt("R_Operator"));
		msg.setSplitStatus(rs.getInt("Split"));
		// added by linda to support custom retry.
		msg.setRetriedNumber(rs.getInt("RetriedNumber"));
		msg.setNextRetryTime(rs.getTimestamp("NextRetryTime"));
		msg.setOMncMcc(rs.getString("OMncMcc"));
		msg.setRMncMcc(rs.getString("RMncMcc"));
		// added to support Concatenated Message
		msg.setSarMsgRefNum(rs.getString("msgRefNum"));
		msg.setSarSegmentSeqNum(rs.getInt("segmentSeqNum"));
		msg.setSarTotalSegments(rs.getInt("totalSegments"));
		msg.setUdh(rs.getBytes("UDH"));
		// added connectionID on 2007-12-04
		msg.setConnectionID(rs.getString("connectionID"));
		msg.setOperatorPriority(rs.getInt("OperatorPriority"));
		// added SpecialDCS by Jianming in v1.0.1
		msg.setSpecialDataCodingScheme(rs.getString("SpecialDCS"));
		msg.setOriginalSenderAddr(rs.getString("OriginalSenderAddr"));
		msg.setOriginalRecipientAddr(rs.getString("OriginalRecipientAddr"));
		msg.setInCsm((rs.getInt("InCsm")) != 0);
		msg.setServiceTypeID(rs.getInt("ServiceTypeID"));
		msg.setScheduleDeliveryTime(rs.getTimestamp("ScheduleDeliveryTime"));
	}

	public String makeSqlStrReplace(String strSQL) {
		if (strSQL == null) {
			return "NULL";
		}
		strSQL = strSQL.replaceAll("'", "''");
		return new StringBuilder("'").append(strSQL).append("'").toString();
	}

	public String makeSqlStr(String strSQL) {
		if (strSQL == null) {
			return "NULL";
		}
		return new StringBuilder("'").append(strSQL).append("'").toString();
	}

	private void checkLastRetry(GmmsMessage message) {
		boolean nextIsExpired = RetryPolicyManager.getInstance().isNextExpired(
				message);

		if (nextIsExpired
				|| RetryPolicyManager.getInstance().isLastRetry(message)) {
			if (message.getOriginalQueue() != null) {
				clearMessage(message, message.getOriginalQueue());
			}
			if (message.getDeliveryReport()) {
				if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
						.equalsIgnoreCase(message.getMessageType())) {
					message.setStatus(GmmsStatus.DELIVERED);
				} else {
					if (nextIsExpired) {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.EXPIRED);
						}						
					} else {
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(message.getStatusText());
						if(converDRStatus!=null) {
							message.setStatus(converDRStatus);
						}else {
							message.setStatus(GmmsStatus.UNDELIVERABLE);
						}
						
					}
				}
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				message.setRetriedNumber(0);
				message.setNextRetryTime(null);
				message.setDeliveryChannel(module);
				if (message.getInClientPull() == 2) {
					if ((message.getOA2P()== message.getRA2P())							
							||((message.getOA2P() != message.getRA2P())
							   && ((message.getOA2P() == message.getCurrentA2P()) || ctm.vpOnSameA2P(message.getOA2P(), message.getCurrentA2P())))) {
						updateMsgForDRQuery(message);
					} else {
						sendDRMessage(message);
					}
				} else {
					sendDRMessage(message);
				}
			}
		} else {
			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
					.equalsIgnoreCase(message.getMessageType()))
				updateByInMsgId(message, QDQ);
			else {
				if (message.getOriginalQueue() == null) {
					if (inRopBusyMode(message.getRSsID())
							|| inRopFailedMode(message.getRSsID())) {
						insertMessageToDB(message, SMQ);
					} else {
						insertMessageToDB(message, RMQ);
					}
				} else {
					updateByInMsgId(message, message.getOriginalQueue());
				}
			}
		}
	}

	public void parseCsmUdh(GmmsMessage message) {
		byte[] udh_byte;
		int element_length;
		int i = 1;

		if (message.getUdh() != null) {
			udh_byte = message.getUdh();

			while (i < udh_byte.length) {
				if (udh_byte[i] == 0x00) {
					element_length = udh_byte[i + 1] & 0xff;

					if (element_length == 3) {
						// set 3 optional parameters
						message.setSarMsgRefNum(HttpUtils.format2Digits(Integer
								.toHexString(udh_byte[i + 2])));
						message.setSarTotalSegments(udh_byte[i + 3] & 0xff);
						message.setSarSegmentSeqNum(udh_byte[i + 4] & 0xff);
						if (udh_byte.length == 6) {// UDH just contains only one
													// concatenation element
							message.setUdh(null);
						} else {
							// remove concatenation element
							byte[] udh_target = new byte[udh_byte.length - 5];
							System.arraycopy(udh_byte, 0, udh_target, 0, i);
							if (i < udh_byte.length - 5) {// judge concatenation
															// element is the
															// last element in
															// the UDH
								System.arraycopy(udh_byte, i + 5, udh_target,
										i, udh_target.length - i);
							}

							// update the UDH length
							udh_target[0] = (byte) (udh_target[0] - (byte) 5);
							message.setUdh(udh_target);

						}
						return;
					} else {
						log.error(
								"UDH of current message contains an error element length when concatenated short messages is 8-bit reference number, the message id is:{}",
								message.getMsgID());
						message.setUdh(null);
						return;
					}
				} else if (udh_byte[i] == 0x08) {
					element_length = udh_byte[i + 1] & 0xff;

					if (element_length == 4) {
						message.setSarMsgRefNum(HttpUtils.format2Digits(Integer
								.toHexString(udh_byte[i + 2]))
								+ HttpUtils.format2Digits(Integer
										.toHexString(udh_byte[i + 3])));
						message.setSarTotalSegments(udh_byte[i + 4] & 0xff);
						message.setSarSegmentSeqNum(udh_byte[i + 5] & 0xff);
						if (udh_byte.length == 7) {// UDH just contains only one
													// concatenation element
							message.setUdh(null);
						} else {
							// remove concatenation element
							byte[] udh_target = new byte[udh_byte.length - 6];
							System.arraycopy(udh_byte, 0, udh_target, 0, i);
							if (i < udh_byte.length - 6) {// judge concatenation
															// element is the
															// last element in
															// the UDH
								System.arraycopy(udh_byte, i + 6, udh_target,
										i, udh_target.length - i);
							}

							udh_target[0] = (byte) (udh_target[0] - (byte) 6);
							message.setUdh(udh_target);

						}
						return;
					} else {
						log.error(
								"UDH of current message contains an error element length when concatenated short message is 16-bit reference number, the message id is:{}",
								message.getMsgID());
						message.setUdh(null);
						return;
					}
				} else {
					// locate the next element
					element_length = udh_byte[i + 1] & 0xff; // ---
					i += element_length + 2;
				}
			}
		}
		// else indicates transferring message which maybe contain 3
		// parameters(:sar_msg_ref_num, sar_total_segments and
		// sar_segment_seqnum) or normal message without UDH
	}

	/**
	 * Store GmmsMessage to CSMQ
	 * 
	 * @param message
	 */
	public void sendToCsmq(GmmsMessage message) {

		if (message != null) {
			insertMessageToDB(message, CSMQ);
		}
	}

	/**
	 * get Messages From Csmq By CsmKeyInfo
	 * 
	 * @param csmKeyInfo
	 * @return
	 */
	public List<GmmsMessage> getMessagesFromCsmqByCsmKeyInfo(
			CsmKeyInfo csmKeyInfo) {
		if (csmKeyInfo == null) {
			return null;
		}
		String sqlStr = "SELECT * FROM " + CSMQ + " WHERE OSsID="
				+ csmKeyInfo.getoSsID() + " AND SenderAddresses="
				+ makeSqlStrReplace(csmKeyInfo.getSenderAddress())
				+ " AND RecipientAddresses="
				+ makeSqlStrReplace(csmKeyInfo.getRecipientAddress())
				+ " AND msgRefNum=" + makeSqlStr(csmKeyInfo.getSarMsgRefNum())
				+ " ORDER BY DateIn ASC limit 100";

		ResultSet rs = null;
		List<GmmsMessage> retList = new ArrayList<GmmsMessage>();
		try {
			rs = doSelect(sqlStr);
			while (rs.next()) {
				GmmsMessage msg = new GmmsMessage();
				assignValue(rs, msg);
				msg.setRowId(rs.getInt("ID"));
				retList.add(msg);
			}
		} catch (Exception e) {
			log.error("Error in executing:" + sqlStr, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {

				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception ex) {
					log.error(ex, ex);
					if (!ex.getMessage().contains("Data truncated")
							&& !ex.getMessage().contains("SQL syntax")) {

						MailSender.getInstance().sendAlertMail(
								"A2P alert mail from "
										+ ModuleURI.self().getAddress()
										+ " for DB Exception", ex);
					}
				}
			}
			super.closeSession();// add by Levens in Jan.20 2006
		}

		return retList;
	}

	/**
	 * clear messasage in CSMQ by CsmKeyInfo
	 * 
	 * @param csmKeyInfo
	 */
	public void clearCsmqByCsmKeyInfo(CsmKeyInfo csmKeyInfo,
			CsmValueInfoMark csmValueInfoMark) {
		if (csmKeyInfo == null || csmValueInfoMark == null) {
			return;
		}

		StringBuilder seqNumBuilder = new StringBuilder(50);
		Set<CsmValueInfo> csmvalueSet = csmValueInfoMark.getValueSet();
		for (Iterator<CsmValueInfo> iter = csmvalueSet.iterator(); iter
				.hasNext();) {
			seqNumBuilder.append(iter.next().getSarSegmentSeqNum()).append(",");
		}

		String seqNums = seqNumBuilder.substring(0, seqNumBuilder.length() - 1);
		if (seqNums.length() < 1) {
			return;
		}

		String sqlStr = "DELETE FROM " + CSMQ + " WHERE OSsID="
				+ csmKeyInfo.getoSsID() + " AND SenderAddresses="
				+ makeSqlStrReplace(csmKeyInfo.getSenderAddress())
				+ " AND RecipientAddresses="
				+ makeSqlStrReplace(csmKeyInfo.getRecipientAddress())
				+ " AND msgRefNum=" + makeSqlStr(csmKeyInfo.getSarMsgRefNum())
				+ " AND segmentSeqNum in (" + seqNums + ")";
		try {
			doDelete(sqlStr);
			if (log.isDebugEnabled()) {
				log.debug(" {} delete from {}", csmKeyInfo.toString(), CSMQ);
			}
		} catch (Exception e) {
			log.error(" " + csmKeyInfo.toString() + " " + e, e);
			if (!e.getMessage().contains("Data truncated")
					&& !e.getMessage().contains("SQL syntax")) {
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for DB Exception", e);
			}
		} finally {
			super.closeSession();
		}
	}

	public void insertInMsgForQueryDR(GmmsMessage msg) {
		if (isRedisEnable) {
			// String temp = SerializableHandler
			// .convertGmmsMessage2RedisMessage4REST(msg);
			String temp = SerializableHandler
					.convertGmmsMessage2RedisMessage(msg);
			if (temp == null) {
				insertMessageToDB(msg, IDDQ);
			} else {
				int expirTime = gmmsUtility.getRedisExpireTime(msg);
				boolean redisFlag = redis.setString(INMSGIDPREFIXFORDRQUERY
						+ msg.getInMsgID(), temp, expirTime);
				if (!redisFlag) {
					insertMessageToDB(msg, IDDQ);
					if (log.isInfoEnabled()) {
						log.info(
								"message set redis error, so insert IDDQ and the inmsgid is {}",
								msg.getInMsgID());
					}
				}
			}
		} else {
			insertMessageToDB(msg, IDDQ);
		}
	}

	private void updateDBForQueryDR(GmmsMessage msg) {
		if (isRedisEnable) {
			// String temp = SerializableHandler
			// .convertGmmsMessage2RedisMessage4REST(msg);
			String temp = SerializableHandler
					.convertGmmsMessage2RedisMessage(msg);
			if (log.isDebugEnabled()) {
				log.debug("temp={}", temp);
			}

			if (temp == null) {
				updateByInMsgId(msg, IDDQ);
			} else {
				int expirTime = gmmsUtility.getRedisExpireTime(msg);
				if (!redis.setString(
						this.INMSGIDPREFIXFORDRQUERY + msg.getInMsgID(), temp,
						expirTime)) {

					insertMessageToDB(msg, IDDQ);
					if (log.isInfoEnabled()) {
						log.info(
								"message set redis error, so insert IDDQ and the inmsgid is {}",
								msg.getInMsgID());
					}
				}
			}
		} else {
			updateByInMsgId(msg, IDDQ);
		}
	}
	
	private void insertMsgForDelayDR(GmmsMessage msg, long timemark) {
		if (isRedisEnable) {			
			String temp = SerializableHandler
					.convertGmmsMessage2RedisMessage(msg);
			if (log.isDebugEnabled()) {
				log.debug("insertMsgforDelayDR=[{}], delayTime={}", temp, timemark);
			}

			if (temp == null) {
				insertMessageToDB(msg, RDQ);
			} else {
				int expirTime = gmmsUtility.getRedisExpireTime(msg);
				if (!redis.setDelayDR(
						this.INMSGIDPREFIXFORDELAYDR + msg.getInMsgID(), temp,
						expirTime, timemark, msg.getOSsID())) {

					insertMessageToDB(msg, RDQ);
					if (log.isInfoEnabled()) {
						log.info(
								"message set redis error, so insert RDQ and the inmsgid is {}",
								msg.getInMsgID());
					}
				}
			}
		} else {
			insertMessageToDB(msg, RDQ);
		}
	}
	
	public void doInsertMsgForDRDelay(GmmsMessage message, int delayTime) {				
		long timemark = delayTime+System.currentTimeMillis()/1000;
		insertMsgForDelayDR(message, timemark);
	}
	
	public void insertMsgForDRDelay(GmmsMessage message, int delayTime) {
		if (message.getSarMsgRefNum() != null
				&& message.getSarMsgRefNum().trim().length() > 0) {
			List<GmmsMessage> msgList = new ArrayList<GmmsMessage>();
			CsmUtility.splitCsmDr(message, msgList);
			for (GmmsMessage msg : msgList) {
				doInsertMsgForDRDelay(msg, delayTime);
			}
		} else {
			doInsertMsgForDRDelay(message, delayTime);
		}
	}

	public void updateMsgForDRQuery(GmmsMessage message) {
		if (message.getSarMsgRefNum() != null
				&& message.getSarMsgRefNum().trim().length() > 0) {
			List<GmmsMessage> msgList = new ArrayList<GmmsMessage>();
			CsmUtility.splitCsmDr(message, msgList);
			for (GmmsMessage msg : msgList) {
				updateDBForQueryDR(msg);
			}
		} else {
			updateDBForQueryDR(message);
		}
	}

	public GmmsMessage getInMsgfromCache(String inMsgId) {
		GmmsMessage message = null;
		if (isRedisEnable) {
			String object = redis.getString(this.INMSGIDPREFIXFORDRQUERY
					+ inMsgId);
			if (object != null) {
				// message = SerializableHandler
				// .convertRedisMssage2GmmsMessage4REST(object);
				message = SerializableHandler
						.convertRedisMssage2GmmsMessage(object);
				return message;
			} else {
				if (log.isInfoEnabled()) {
					log.info(
							"Can not find the message in redis, and in msg id is {}",
							inMsgId);
				}
			}
		} else {
			try {
				message = this.getGmmsMessageByInMsgID(inMsgId, IDDQ);
			} catch (DataManagerException e) {
				log.error(
						"Can not find the message in message Queue, and in msg id is {}",
						inMsgId);
			}
		}
		return message;

	}

	// add by kevin for REST
	public void deleteInMsgFromCache(GmmsMessage msg) {

		if (isRedisEnable) {
			redis.del(INMSGIDPREFIXFORDRQUERY + msg.getInMsgID());
		} else {
			clearMessage(msg, IDDQ);
		}
	}
	
	public static void main(String[] args) {
		GmmsMessage msg = new GmmsMessage();
		msg.setStatus(GmmsStatus.TEMPLATE_FAIL);
		
		MessageStoreManager storeManager =new MessageStoreManager();
		String s="11,13,";
		System.out.println(s.contains("13,"));
		//System.out.println(storeManager.needRetry(msg));
	}
}
