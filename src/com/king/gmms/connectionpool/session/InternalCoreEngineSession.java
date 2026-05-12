package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.king.db.DataManagerException;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.InternalClientSessionThread;
import com.king.gmms.connectionpool.sessionthread.InternalServerSessionThread;
import com.king.gmms.customerconnectionfactory.InternalCoreEngineConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.milter.ContentScanMilter;
import com.king.gmms.processor.CsmProcessorHandler;
import com.king.gmms.processor.CsmUtility;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.gmms.protocol.tcp.internaltcp.CommandBind;
import com.king.gmms.protocol.tcp.internaltcp.CommandBindAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReport;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportQuery;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportQueryAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandKeepAliveAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandSubmit;
import com.king.gmms.protocol.tcp.internaltcp.CommandSubmitAck;
import com.king.gmms.protocol.tcp.internaltcp.InternalPdu4MQM;
import com.king.gmms.protocol.tcp.internaltcp.Pdu;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.routing.DeliveryRouterHandler;
import com.king.gmms.util.BufferMonitor;
import com.king.gmms.util.BufferMonitorWithSafeExit;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.message.gmms.SdqMessageManager;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;
import com.king.gmms.metrics.MetricsCollector;

public class InternalCoreEngineSession extends AbstractInternalSession{

	private static SystemLogger log = SystemLogger
			.getSystemLogger(InternalCoreEngineSession.class);
	private final static Object sync = new Object();
	private static final MetricsCollector metrics = MetricsCollector.getInstance();
    protected BufferMonitorWithSafeExit outBuffer = null;
    protected BufferMonitor inBuffer = null;
    protected int windowsSize = 50000;
    protected int bufferTimeout = 100000;
    private MessageStoreManager msgStoreManager = null;
    private DeliveryRouterHandler deliveryRouterHandler = null;
    private MessageProcessorHandler processorHandler = null;
    private CsmProcessorHandler csmHandler = null;
    private A2PCustomerManager ctm;
    private InternalCoreEngineConnectionFactory factory;
    private ModuleManager moduleManager;
    private SdqMessageManager sdqManager = null;
	private ContentScanMilter csm;
	private boolean isRedisEnable = false;
    private boolean isStoreDRModeEnable = false;
	private RedisClient redis = null;
	
	private static InternalCoreEngineSession reactiveInstance = null;
	public static synchronized InternalCoreEngineSession getReactiveInstance() {
		if (reactiveInstance == null) {
			reactiveInstance = new InternalCoreEngineSession();
		}
		return reactiveInstance;
	}
	
	protected InternalCoreEngineSession() {
		super();
		moduleManager = ModuleManager.getInstance();
		msgStoreManager = gmmsUtility.getMessageStoreManager();
		ctm = gmmsUtility.getCustomerManager();
		csm = ContentScanMilter.getInstance();
		factory = InternalCoreEngineConnectionFactory.getInstance();
		isStoreDRModeEnable = gmmsUtility.isStoreDRModeEnable();
		if ("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))) {
			isRedisEnable = true;
			redis = gmmsUtility.getRedisClient();
		}
		deliveryRouterHandler = DeliveryRouterHandler.getInstance();
		processorHandler = MessageProcessorHandler.getInstance();
		csmHandler = CsmProcessorHandler.getInstance();
		sdqManager = SdqMessageManager.getInstance();
	}
    
    public InternalCoreEngineSession(ConnectionInfo connectionInfo){
        super();
        isServer = false;
        moduleManager = ModuleManager.getInstance();
		int connectionSilentTime = Integer.parseInt(gmmsUtility
				.getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
        this.connectionInfo = connectionInfo;
        super.initReceivers();
        super.initSenders();
        transaction = new TransactionURI();
        transaction.setConnectionName(connectionInfo.getConnectionName());
        setSessionName(connectionInfo.getConnectionName());
		windowsSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty(
				"BufferWindowsSize", "50000"));
		bufferTimeout = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty(
				"BufferTimeout", "100")) * 1000;
		if("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))){
			isRedisEnable = true;
			redis = gmmsUtility.getRedisClient();			
        }
        isStoreDRModeEnable = gmmsUtility.isStoreDRModeEnable();
        ctm = gmmsUtility.getCustomerManager();
        msgStoreManager = gmmsUtility.getMessageStoreManager();
        csm = ContentScanMilter.getInstance();
        factory = InternalCoreEngineConnectionFactory.getInstance();
		sessionThread = new InternalClientSessionThread(this,
				connectionSilentTime);
        startBufferMonitor();
        start();
    }
    
    public InternalCoreEngineSession(Socket socket){
        super();
        // create a single thread pool first, then update after binded
        super.initReceivers();
        isServer = true;
        moduleManager = ModuleManager.getInstance();
		int connectionSilentTime = Integer.parseInt(gmmsUtility
				.getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
		transaction = new TransactionURI();
		windowsSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty(
				"BufferWindowsSize", "50000"));
		bufferTimeout = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty(
				"BufferTimeout", "100")) * 1000;
		
		msgStoreManager = gmmsUtility.getMessageStoreManager();		
		ctm = gmmsUtility.getCustomerManager();
		csm = ContentScanMilter.getInstance();
		factory = InternalCoreEngineConnectionFactory.getInstance();
		isStoreDRModeEnable = gmmsUtility.isStoreDRModeEnable();
		if("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))){
			isRedisEnable = true;
			redis = gmmsUtility.getRedisClient();			
		}
        try {
            if (socket != null) {
                createConnection(socket);
            }
		} catch (IOException ex) {
			log.error(ex, ex);
		}
		sessionThread = new InternalServerSessionThread(this,
				connectionSilentTime);
		start();
	}

	public void start() {
		synchronized (mutex) {
			if (keepRunning == true) {
				return;
			}
			keepRunning = true;
			msgQueue = new InternalConnectionMessageQueue(isServer);
			
			deliveryRouterHandler = DeliveryRouterHandler.getInstance();
			processorHandler = MessageProcessorHandler.getInstance();
			csmHandler = CsmProcessorHandler.getInstance();
			sdqManager = SdqMessageManager.getInstance();
			if (sessionThread != null) {
				sessionThread.start();
			}
		}
	}

	public boolean startBufferMonitor() {
		outBuffer = new BufferMonitorWithSafeExit(windowsSize);
		outBuffer.setListener(new TimeoutHandlerForOutBuffer());
		outBuffer.setWaitTime(200, TimeUnit.MILLISECONDS);
		outBuffer.setTimeout(bufferTimeout, TimeUnit.MILLISECONDS);
		outBuffer.startMonitor(sessionName+"_OutBuffer");

		inBuffer = new BufferMonitor(windowsSize);
		inBuffer.setListener(new TimeoutHandlerForInBuffer());
		inBuffer.setWaitTime(200, TimeUnit.MILLISECONDS);
		inBuffer.setTimeout(bufferTimeout, TimeUnit.MILLISECONDS);
		inBuffer.startMonitor(sessionName+"_InBuffer");
		return true;
	}

	public boolean receive(Object obj) {
		boolean result = false;
		if (obj == null) {
			return true;
		}
		long _receiveStart = System.nanoTime();
		Pdu pdu = (Pdu) obj;
		GmmsMessage msg = null;
		metrics.incrementCounter("core.receive.total");
		if(log.isTraceEnabled()){
			log.trace("Internal Core Engine Session receive a PDU {}", pdu
				.getCommandId());
		}
		if (Pdu.COMMAND_SUBMIT == pdu.getCommandId()) {
			metrics.incrementCounter("core.receive.submit");
			msg = pdu.convertToMsg(null);
			if (msg != null) {
				if(log.isInfoEnabled()){
					log.info(msg,"Convert Submit PDU to GmmsMessage.");
				}
				final GmmsMessage finalMsg = msg;
				processorHandler.getHandlerThreadPool().execute(new Runnable() {
					public void run() {
						processSubmit(finalMsg);
					}
				});
				result = true;
			}
		} else if (Pdu.COMMAND_SUBMIT4MQM == pdu.getCommandId()) {
			msg = pdu.convertToMsg(null);
			if (msg != null) {
				if(log.isDebugEnabled()){
					log.debug(msg, "Convert Submit4MQM PDU to GmmsMessage:{}",msg.toString4NewMsg());
				}
				final GmmsMessage finalMsg = msg;
				processorHandler.getHandlerThreadPool().execute(new Runnable() {
					public void run() {
						processSubmit4MQM(finalMsg);
					}
				});
				result = true;
			}
		} else if (Pdu.COMMAND_SUBMIT_ACK == pdu.getCommandId()) {
			msg = pdu.convertToMsg(outBuffer);
			if (msg != null) {
				if(log.isInfoEnabled()){
					log.info(msg,"Convert SubmitACK PDU to GmmsMessage, and outmsgid is {} and statuscode is : {}"
							 ,msg.getOutMsgID(),msg.getStatusCode());
				}
				result = processSubmitAck(msg);
			}
		} else if (Pdu.COMMAND_DELIVERY_REPORT == pdu.getCommandId()) {
			metrics.incrementCounter("core.receive.dr");
			msg = ((CommandDeliveryReport) pdu).convertToMsg(true);		
			if(log.isInfoEnabled()){
				log.info(msg, "Convert DeliveryReport PDU to GmmsMessage, and outmsgid is {} and statuscode is : {}"
					      ,msg.getOutMsgID(), msg.getStatusCode());
			}
			final GmmsMessage finalMsg = msg;
			final Pdu finalPdu = pdu;
			processorHandler.getHandlerThreadPool().execute(new Runnable() {
				public void run() {
					handleInboundDRInternal(finalMsg, finalPdu);
				}
			});
			result = true;
		} else if (Pdu.COMMAND_DELIVERY_REPORT4MQM == pdu.getCommandId()) {
			msg = pdu.convertToMsg(null);
			if (msg == null) {
				log.trace("Null DR msg when processDeliveryReport4MQM");
			} else {
				if(log.isInfoEnabled()){
					log.info(msg, "Convert DeliveryReport4MQM PDU to GmmsMessage:{}",msg.toString());
				}
			}
			result = processDeliveryReport4MQM(msg, pdu);
		}
		
		else if (Pdu.COMMAND_DELIVERY_REPORT_QUERY4MQM == pdu.getCommandId()) {
			msg = pdu.convertToMsg(null);
			if (msg == null) {
				log.info("Null DR msg when processDeliveryReport");
			} else {
				if (log.isInfoEnabled()) {
					log.info(
							msg,
							"Convert DeliveryReportQuery PDU to GmmsMessage and inmsgid is {} and statuscode is : {}",
							msg.getInMsgID(), msg.getStatusCode());
				}
			}
			result = processDeliveryReportQuery(msg, pdu);
		}
		
		else if (Pdu.COMMAND_DELIVERY_REPORT_QUERY == pdu.getCommandId()) {
			// logic to process OOP query delivery report for REST
			msg = ((CommandDeliveryReportQuery) pdu).convertToMsg();
			if (msg == null) {
				log.info("Null DR msg when processDeliveryReport");
			} else {
				if (log.isInfoEnabled()) {
					log.info(msg,
							"Convert DeliveryReportQuery PDU to GmmsMessage and inmsgid is {} and statuscode is : {}",
							msg.getInMsgID(), msg.getStatusCode());
				}
			}
			result = processRESTDeliveryReportQuery(msg, pdu);
		} else if (Pdu.COMMAND_DELIVERY_REPORT_ACK == pdu.getCommandId()) {
			metrics.incrementCounter("core.receive.dr_ack");
			msg = pdu.convertToMsg(outBuffer);
			if (msg != null) {
				if(log.isInfoEnabled()){
					log.info(msg,"Convert DeliveryReportAck PDU to GmmsMessage and inmsgid is {} and statuscode is {}"
							 ,msg.getInMsgID(),msg.getStatusCode());
				}
				result = processDeliveryReportAck(msg);
			}
		} else if (Pdu.COMMAND_DELIVERY_REPORT_QUERY_ACK == pdu.getCommandId()) {
			msg = pdu.convertToMsg(outBuffer);
			if (msg != null) {
				if(log.isInfoEnabled()){
					log.info(msg,"Convert DeliveryReportQueryAck PDU to GmmsMessage and outmsgid is {} and statuscode is {}"
							,msg.getOutMsgID(),msg.getStatusCode());
				}
				result = processDeliveryReportQueryAck(msg);
			}
		} else if (Pdu.COMMAND_ALIVE == pdu.getCommandId()) {
			CommandKeepAliveAck ack = new CommandKeepAliveAck();
			sendPdu(ack);
		} else if (Pdu.COMMAND_ALIVE_ACK == pdu.getCommandId()) {
			clearActiveTestCount();
		} else if (Pdu.COMMAND_BIND == pdu.getCommandId()) {
			result = processBind(pdu);
			if (!result) {
				stop();
			}
		}
		metrics.recordTime("core.receive", System.nanoTime() - _receiveStart);
		// Report buffer utilization gauges
		if (outBuffer != null) { metrics.setGauge("buffer.out." + sessionName, outBuffer.size()); }
		if (inBuffer != null) { metrics.setGauge("buffer.in." + sessionName, inBuffer.size()); }
		return result;
	}

	/**
	 * queue timeout
	 */
	public void timeout(Object msg) {
		GmmsMessage bufferedMsg = (GmmsMessage) msg;
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(bufferedMsg
				.getMessageType())) {
			bufferedMsg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
					.getCode());
			msgStoreManager.handleInDeliveryReportRes(bufferedMsg);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(bufferedMsg.getMessageType())) {
			bufferedMsg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			msgStoreManager.handleOutDeliveryReportRes(bufferedMsg);
		} else {
			bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			msgStoreManager.handleOutSubmitRes(bufferedMsg);
		}
	}

	public boolean submit(GmmsMessage message) {
		boolean result = false;
		long _submitStart = System.nanoTime();
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())) {
			metrics.incrementCounter("core.send.submit");
			result = sendNewMessage(message);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
				.equalsIgnoreCase(message.getMessageType())) {
			metrics.incrementCounter("core.send.dr");
			result = sendReport(message);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(message.getMessageType())) {
			metrics.incrementCounter("core.send.dr_query");
			result = sendReportQuery(message);
		} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message
				.getMessageType())
				|| (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(message.getMessageType()))) {
			metrics.incrementCounter("core.send.resp");
			result = sendResp(message);
		} else {
			log.warn(message, "Unknown Message Type:"
					+ message.getMessageType());
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			msgStoreManager.handleMessageError(message);
		}
		metrics.recordTime("core.submit", System.nanoTime() - _submitStart);
		return result;
	}

    /**
     * send SUBMIT request
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendNewMessage(GmmsMessage message) {
        boolean result = false;
        TcpByteBuffer submitBuffer = null;
        
        A2PCustomerInfo cst = ctm.getCustomerBySSID(message.getRSsID());		
        if (cst != null) {
        	//do senderId length check logic
        	try {
        		String senderAddr = message.getSenderAddress();
            	String min = cst.getOriNumberLenController().get("Min");
            	String max = cst.getOriNumberLenController().get("Max");
            	String suffix = cst.getOriNumberLenController().get("Suffix");
            	if (min!=null && senderAddr.length()<Integer.parseInt(min)) {
    				senderAddr= senderAddr+(suffix==null?"":suffix);
    			}
            	if (max!=null && senderAddr.length()>Integer.parseInt(max)){
					senderAddr = senderAddr.substring(0, Integer.parseInt(max));
				}
            	message.setSenderAddress(senderAddr);            	
			} catch (Exception e) {
				log.error("do sender address length check error", e);
			}
        	
        	
		}
        String[] address = new String[] { message.getSenderAddress(),
				message.getRecipientAddress() };
    	
        if (cst != null) {       	
        	//flag for replace
        	boolean isReplaceSenderID = false;
        	// senderId replacement
            Boolean checkAlphaOAddr = cst.isCheckAlphaWhenReplaceOAddr();
            // list info, 0: oadd_conf, 1: replaceAddr
            List<String> senderIdReplaceInfo = ctm.getSenderIdRelace(message);
            if (senderIdReplaceInfo != null && senderIdReplaceInfo.size()>0) {
            	if (checkAlphaOAddr) {
                	if (!gmmsUtility.isNumber(message.getSenderAddress())) {
                		isReplaceSenderID = replaceAddr(message, senderIdReplaceInfo, address, true);
                	}
                } else {
                	isReplaceSenderID = replaceAddr(message, senderIdReplaceInfo, address, true);
                }
            	
            }

    		// recipientId replacement
			// list info, 0: oadd_conf, 1: replaceAddr
			List<String> recipientIdReplaceInfo = ctm.getRecipientIdRelace(message);
			if (recipientIdReplaceInfo != null
					&& recipientIdReplaceInfo.size() > 0) {
				replaceAddr(message, recipientIdReplaceInfo, address, false);
			}
    		
			/*if(!isReplaceSenderID){
				// whether replace prefix for sender ID according to R_relay
	           
	            // sender ID does not change by R_relay, replace prefix by R_operator
				if(message.getSenderAddress().equals(address[0])){
					A2PCustomerInfo rop_cst = ctm.getCustomerBySSID(message.getRoperator());
		        	if(rop_cst != null){
		        		this.handleCommonPrefix4MT(message, address, rop_cst);
		        	} 
				} 
			} */ 
			// whether replace prefix for sender ID according to R_relay
			 this.handleCommonPrefix4MT(message,address, cst);
        }

        try {
            CommandSubmit pdu = new CommandSubmit();
            pdu.convertFromMsg(message,false);
            //for csm msg, the sender should keep consistent
            if(((message.getUdh()!=null && message.getUdh().length>0)
            		||(message.getSarTotalSeqments()>1))
            		&&(!message.getSenderAddress().equalsIgnoreCase(address[0]))){
            	//long msg
            	//senderAddress has been replace
            	String msgid = message.getMsgID();
            	// 6.1 Optimization: replaced synchronized(sync) global lock with
            	// atomic Redis SETNX. setnxString returns null if we set the key,
            	// or the existing value if key already existed.
            	String existingSender = GmmsUtility.getInstance().getRedisClient()
            			.setnxString("senderReplacement_" + msgid, address[0], 60 * 60 * 24);
            	if (existingSender == null) {
            		// We set it — use our sender
            		pdu.setSender(address[0]);
            	} else {
            		// Already existed — use the stored sender
            		pdu.setSender(existingSender);
            	}
            }else {
            	pdu.setSender(address[0]);
			}            
            pdu.setRecipient(address[1]);
            submitBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e,e);
        }
        
      //do the blacklist and whitelist feature first
    	//1. do the blacklist
        if(!"VIP".equalsIgnoreCase(cst.getCustomerTypeBySender())){
        	String recipientAddress = message.getRecipientAddress();
    		if (recipientAddress.startsWith("+")) {
    			recipientAddress = recipientAddress.substring(1);
    		}
    		if (recipientAddress.startsWith("00")) {
    			recipientAddress = recipientAddress.substring(2);
    		}
    		if (recipientAddress.startsWith("0")) {
    			recipientAddress = recipientAddress.substring(1);
    		}
        	if("NonPA".equalsIgnoreCase(cst.getCustomerTypeBySender()) 
        			&& recipientAddress.startsWith("65")){
        		if(ctm.doSenderBlacklistCheck(-2, message)){
	       			 message.setStatus(GmmsStatus.SENDER_ERROR_BY_BL);
	   	             msgStoreManager.handleOutSubmitRes(message);
	   	             return result;
       		    }
        	}else{
        		if(!ctm.doSenderBlacklistCheck(message.getRSsID(), message)){
            		if(ctm.doSenderBlacklistCheck(-2, message)){
            			 message.setStatus(GmmsStatus.SENDER_ERROR_BY_BL);
        	             msgStoreManager.handleOutSubmitRes(message);
        	             return result;
            		}
            	}else{
            		message.setStatus(GmmsStatus.SENDER_ERROR_BY_BL);
                    msgStoreManager.handleOutSubmitRes(message);
                    return result;
            	}
        	}
        	
        	
        	//2. do whitelist check
        	
        	if(!ctm.doVendorSenderWhitelistCheck(message.getRSsID(), message)){
        		message.setStatus(GmmsStatus.SENDER_ERROR_BY_WL);
                msgStoreManager.handleOutSubmitRes(message);
                return result;
        	}
        }
        
        
        //1. do the recipient blacklist
    	if(!ctm.doRecipientBlacklistCheck(message.getRSsID(), message)){
    		if(ctm.doRecipientBlacklistCheck(-2, message)){
    			 message.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_BL);
	             msgStoreManager.handleOutSubmitRes(message, true);
	             return result;
    		}
    	}else{
    		message.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_BL);
            msgStoreManager.handleOutSubmitRes(message, true);
            return result;
    	}
    	
    	//1. do the blacklist
    	if(!ctm.doContentBlacklistCheck(message.getRSsID(), message)){
    		if(ctm.doContentBlacklistCheck(-2, message)){
    			 message.setStatus(GmmsStatus.SPAMED_CTBL);
	             msgStoreManager.handleOutSubmitRes(message);
	             return result;
    		}
    	}else{
    		message.setStatus(GmmsStatus.SPAMED_CTBL);
            msgStoreManager.handleOutSubmitRes(message);
            return result;
    	}
    	
    	//2. do whitelist check
    	//disable 2023.04.04
    	/*if(!ctm.doVendorContentWhitelistCheck(message.getRSsID(), message)){
    		message.setStatus(GmmsStatus.SPAMED_CTWL);
            msgStoreManager.handleOutSubmitRes(message);
            return result;
    	}*/

        if (submitBuffer == null) {
            message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
            msgStoreManager.handleOutSubmitReq(message);
            msgStoreManager.handleOutSubmitRes(message);
            return result;
        }

		// 6.2 Optimization: replaced infinite busy-wait with bounded retry + backoff
		String bufferKey = message.getMsgID() + message.getSarSegmentSeqNum();
		int retries = 0;
		while (!outBuffer.put(bufferKey, message)) {
			if (++retries > 5000) {
				log.warn(message, "outBuffer full after 5000 retries, failing message");
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				msgStoreManager.handleOutSubmitRes(message);
				return result;
			}
			try { Thread.sleep(1); } catch (InterruptedException ie) {
				log.warn(message, "outBuffer put interrupted, failing message");
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				msgStoreManager.handleOutSubmitRes(message);
				return result;
			}
		}
        try {
            super.submit(submitBuffer.getBuffer());
            message.setOutTransID(String.valueOf(System.currentTimeMillis()));
            result = true;
		} catch (IOException ex) {
			log.warn(message,
					"IOException occured when submit new message to : "
							+ connectionInfo.getConnectionName(), ex);
			message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			msgStoreManager.handleOutSubmitRes(message);
			outBuffer
					.remove(message.getMsgID() + message.getSarSegmentSeqNum());
            stop();
            result = true;
        }
        return result;
    }

	private boolean replaceAddr(GmmsMessage message, List<String> replaceInfo,
			String[] address, boolean isOaddr) {
		
		if (replaceInfo != null && replaceInfo.size() > 1) {
			String prefix = replaceInfo.get(0);
			String replaceStr = replaceInfo.get(1);
			String originalAddr = (isOaddr? address[0] : address[1]);
			// wholely replace
			if("All".equalsIgnoreCase(prefix)){
				if (isOaddr) {
					address[0] = replaceStr;
					if (log.isInfoEnabled()) {
						log.info(message,
							"Sender address replaced, original value = {} , new value = {}"
							,message.getSenderAddress(),address[0]);
					}
				} else {
					address[1] = replaceStr;
					if (log.isInfoEnabled()) {
						log.info(message,
							"Recipient address replaced, original value = {}, new value = {}"
									, message.getRecipientAddress(),address[1]);
					}
				}
				
	            return true;
	        }
			// replace prefix
	        if (originalAddr != null && originalAddr.startsWith(prefix)) {
	        	if (isOaddr) {
					// address[0] = replaceStr +
					// originalAddr.substring(prefix.length());
					// always replace whole_Oaddr
					address[0] = replaceStr;
					if(log.isInfoEnabled()){
						log.info(message,
							"Sender address replaced, original value = {}, new value = {}"
							,message.getSenderAddress(),address[0]);
					}
	        	} else {
					address[1] = replaceStr
							+ originalAddr.substring(prefix.length());
					if(log.isInfoEnabled()){
						log.info(message,
							"Recipient address replaced, original value= {} , new value = {}"
							, message.getRecipientAddress(),address[1]);
					}
	        	}
	        	
	            return true;
	        }
		}
	    return false;
	}
    
    
    /**
     * send Delivery REPORT to the client
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendReport(GmmsMessage message) {
        if (message == null) {
        	if(log.isDebugEnabled()){
        		log.debug("No DR send to Client");
        	}
            return false;
        }
        boolean result = false;
        TcpByteBuffer reportBuffer = null;
        
        A2PCustomerInfo cst = ctm.getCustomerBySSID(message.getOSsID());
		String[] address = new String[] { message.getSenderAddress(),
				message.getRecipientAddress() };

        if (cst != null) {
            this.handleCommonPrefix4MT(message,address, cst);
        }
        
        try {
            CommandDeliveryReport pdu = new CommandDeliveryReport();
            pdu.convertFromMsg(message);
            pdu.setSender(address[0]);
            pdu.setRecipient(address[1]);
            reportBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e, e);
        }

        if (reportBuffer == null) {
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
					.getCode());
            msgStoreManager.handleInDeliveryReportRes(message);
            return result;
        }

        try {
            // 6.2 Optimization: bounded retry for DR buffer put
            String drBufKey = message.getInMsgID()+"_"+message.getStatusText();
            int drRetries = 0;
            while (!outBuffer.put(drBufKey, message)) {
                if (++drRetries > 5000) {
                    log.warn(message, "outBuffer full after 5000 retries for DR, failing message");
                    message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
                    msgStoreManager.handleInDeliveryReportRes(message);
                    return result;
                }
                try { Thread.sleep(1); } catch (InterruptedException ie) {
                    log.warn(message, "DR outBuffer put interrupted, failing message");
                    message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
                    msgStoreManager.handleInDeliveryReportRes(message);
                    return result;
                }
            }
            super.submit(reportBuffer.getBuffer());
            result = true;
		} catch (IOException ex) {
			log.warn(message, ex, ex);
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
					.getCode());
            msgStoreManager.handleInDeliveryReportRes(message);
            outBuffer.remove(message.getInMsgID()+"_"+message.getStatusText());
            stop();
            result = true;
        }
        return result;
    }
    
    /**
     * send Delivery REPORT to the client
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendReportQuery(GmmsMessage message) {
        if (message == null) {
        	if(log.isDebugEnabled()){
                log.debug("No DR send to Client");
        	}
            return false;
        }
        boolean result = false;
        TcpByteBuffer reportBuffer = null;
        
        A2PCustomerInfo cst = ctm.getCustomerBySSID(message.getOSsID());
		String[] address = new String[] { message.getSenderAddress(),
				message.getRecipientAddress() };

        if (cst != null) {
            this.handleCommonPrefix4MT(message,address, cst);
        }
        
        try {
            CommandDeliveryReportQuery pdu = new CommandDeliveryReportQuery();
            pdu.convertFromMsg(message);
            pdu.setSender(address[0]);
            pdu.setRecipient(address[1]);
            reportBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e, e);
        }

        if (reportBuffer == null) {
			message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
							.getCode());
            msgStoreManager.handleOutDeliveryReportRes(message);
            return result;
        }

        try {
            // 6.2 Optimization: bounded retry for DRQuery buffer put
            int drqRetries = 0;
            while (!outBuffer.put(message.getMsgID(), message)) {
                if (++drqRetries > 5000) {
                    log.warn(message, "outBuffer full after 5000 retries for DRQuery, failing message");
                    message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
                    msgStoreManager.handleOutDeliveryReportRes(message);
                    return result;
                }
                try { Thread.sleep(1); } catch (InterruptedException ie) {
                    log.warn(message, "DRQuery outBuffer put interrupted, failing message");
                    message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
                    msgStoreManager.handleOutDeliveryReportRes(message);
                    return result;
                }
            }
            super.submit(reportBuffer.getBuffer());
            result = true;
        }
        catch (IOException ex) {
            log.warn(message,ex,ex);
            message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
            msgStoreManager.handleOutDeliveryReportRes(message);
            outBuffer.remove(message.getMsgID());
            stop();
        }
        return result;
    }
    
    /**
     * send response to the client
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendResp(GmmsMessage message) {
        boolean result = false;
        Pdu ack = null;
        TcpByteBuffer responseBuffer = null;
        try {
			if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message
					.getMessageType())) {
                ack = new CommandSubmitAck();
                ack.convertFromMsg(message);
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
					.equalsIgnoreCase(message.getMessageType())) {
                ack = new CommandDeliveryReportAck();
                ((CommandDeliveryReportAck)ack).convertFromMsg(message,false);
            }
            responseBuffer = ack.toByteBuffer();
            if (log.isTraceEnabled()) {
                log.trace(message, "Send the response to Protocol module.");
            }
            super.submit(responseBuffer.getBuffer());
            result = true;

		} catch (Exception ex) {
			log.warn(message, ex, ex);
			if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message
					.getMessageType())) {
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
					.equalsIgnoreCase(message.getMessageType())) {
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
            }
            stop();
        }
        return result;
    }
    
    private boolean processErrorSubmit(GmmsMessage msg){
    	
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {			
			gmmsUtility.getCdrManager().logInSubmit(msg);
		} else {
			gmmsUtility.getCdrManager().logInDelivery(msg);
		}
		A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());
		if (cst!= null && cst.isSmsOptionSendFakeDR() && msg.getDeliveryReport()) {
			//TODO set in dr to redis for delay send dr by insubmit 2021.01.19						
			msg.setFakeDR(true);						
		}
		if (!cst.getProtocol().startsWith("SMPP") && msg.getDeliveryReport() 
				&&GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
			//http protocal, didn't need send response to commonHttpServer.			
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			msg.setRSsID(msg.getOSsID());
			msg.setOA2P(ctm.getCurrentA2P());
			msg.setTransaction(transaction);			
			msgStoreManager.sendDRMessage(msg);
    		return true;
		}else {
			CommandSubmitAck ack = new CommandSubmitAck();
	        try{
	        	ack.convertFromMsg(msg);
	        }catch(Exception e){
	        	log.warn(msg,"Fail to convert Message to PDU");
	        }
			if(!sendPdu(ack)){
				if(log.isInfoEnabled()){
					log.info(msg,"Failed to send response to function module, msg type is {}",msg.getMessageType());
				}
				return false;
			}else{
				return true;
			}
		}
        
    }
    private boolean processSuccessSubmit(GmmsMessage msg){
        CommandSubmitAck ack = new CommandSubmitAck();
        msg.setOriginalSenderAddr(msg.getSenderAddress());
        A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());       
        if (cst != null) {
        	if (cst.isSmsOptionSendFakeDR() && msg.getDeliveryReport()) {
    			//TODO set in dr to redis for delay send dr by insubmit 2021.01.19						
    			msg.setFakeDR(true);						
    		}        	
        	//msg.setOriginalSenderAddr(msg.getSenderAddress());
			String[] address = new String[] { msg.getSenderAddress(),
					msg.getRecipientAddress() };
            this.handleCommonPrefix4MO(address, cst);
            if(address[0] == null || "".equalsIgnoreCase(address[0])) {
            	msg.setSenderAddress("abc");
            }else {
            	msg.setSenderAddress(address[0]);
            }            
            msg.setRecipientAddress(address[1]);
            msg.setInClientPull(cst.getInClientPull());
            
             //TODO
        	//do the blacklist and whitelist feature first
        	//1. do the blacklist
            if(!"VIP".equalsIgnoreCase(cst.getCustomerTypeBySender())){
            	String recipientAddress = msg.getRecipientAddress();
        		if (recipientAddress.startsWith("+")) {
        			recipientAddress = recipientAddress.substring(1);
        		}
        		if (recipientAddress.startsWith("00")) {
        			recipientAddress = recipientAddress.substring(2);
        		}
        		if (recipientAddress.startsWith("0")) {
        			recipientAddress = recipientAddress.substring(1);
        		}
            	if("NonPA".equalsIgnoreCase(cst.getCustomerTypeBySender()) 
            			&& recipientAddress.startsWith("65")){
            		if(ctm.doSenderBlacklistCheck(-1, msg)){
            			try {
            				ack.convertFromMsg(msg);
        	    	        sendPdu(ack);
						} catch (Exception e) {
							// TODO: handle exception
						}            			
            			gmmsUtility.getCdrManager().logInSubmit(msg);
    					if (msg.getDeliveryReport()) {
    						msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
    						msg.setRSsID(msg.getOSsID());	
    						msg.setStatus(GmmsStatus.REJECTED_SDBL);						
    						msgStoreManager.sendDRMessageForServerReject(msg);						
    					}
    					return true;
            		}
            	}else{
            		if(!ctm.doSenderBlacklistCheck(msg.getOSsID(), msg)){
                		if(ctm.doSenderBlacklistCheck(-1, msg)){
                			try {
                				ack.convertFromMsg(msg);
            	    	        sendPdu(ack);
    						} catch (Exception e) {
    							// TODO: handle exception
    						}
                			gmmsUtility.getCdrManager().logInSubmit(msg);
        					if (msg.getDeliveryReport()) {
        						msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        						msg.setRSsID(msg.getOSsID());	
        						msg.setStatus(GmmsStatus.REJECTED_SDBL);						
        						msgStoreManager.sendDRMessageForServerReject(msg);						
        					}
        					
        					return true;
                		}
                	}else{
                		try {
            				ack.convertFromMsg(msg);
        	    	        sendPdu(ack);
						} catch (Exception e) {
							// TODO: handle exception
						}
                		gmmsUtility.getCdrManager().logInSubmit(msg);
        				if (msg.getDeliveryReport()) {
        					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        					msg.setRSsID(msg.getOSsID());	
        					msg.setStatus(GmmsStatus.REJECTED_SDBL);						
        					msgStoreManager.sendDRMessageForServerReject(msg);						
        				}
        				
        				return true;
                	}
            	} 	
            	//2. do whitelist check
            	
            	if(!ctm.doSenderWhitelistCheck(msg.getOSsID(), msg)){
            		try {
        				ack.convertFromMsg(msg);
    	    	        sendPdu(ack);
					} catch (Exception e) {
						// TODO: handle exception
					}
            		gmmsUtility.getCdrManager().logInSubmit(msg);
    				if (msg.getDeliveryReport()) {
    					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
    					msg.setRSsID(msg.getOSsID());	
    					msg.setStatus(GmmsStatus.REJECTED_SDWL);						
    					msgStoreManager.sendDRMessageForServerReject(msg);						
    				}
    				return true;
            	}
            }
        	
        	
        	//3. do content blacklist check
        	if(!ctm.doContentBlacklistCheck(msg.getOSsID(), msg)){
        		if(ctm.doContentBlacklistCheck(-1, msg)){
        			try {
        				ack.convertFromMsg(msg);
    	    	        sendPdu(ack);
					} catch (Exception e) {
						// TODO: handle exception
					}
        			gmmsUtility.getCdrManager().logInSubmit(msg);
					if (msg.getDeliveryReport()) {
						msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
						msg.setRSsID(msg.getOSsID());	
						msg.setStatus(GmmsStatus.REJECTED_CTBL);						
						msgStoreManager.sendDRMessageForServerReject(msg);						
					}
					return true;
        		}
        	}else{
        		try {
    				ack.convertFromMsg(msg);
	    	        sendPdu(ack);
				} catch (Exception e) {
					// TODO: handle exception
				}
        		gmmsUtility.getCdrManager().logInSubmit(msg);
				if (msg.getDeliveryReport()) {
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					msg.setRSsID(msg.getOSsID());	
					msg.setStatus(GmmsStatus.REJECTED_CTBL);						
					msgStoreManager.sendDRMessageForServerReject(msg);						
				}
				return true;
        	}
        	
        	//4. do content whitelist check
        	/*if(!ctm.doContentWhitelistCheck(msg.getOSsID(), msg)){
        		gmmsUtility.getCdrManager().logInSubmit(msg);
				if (msg.getDeliveryReport()) {
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					msg.setRSsID(msg.getOSsID());	
					msg.setStatus(GmmsStatus.REJECTED_CTWL);						
					msgStoreManager.sendDRMessage(msg);						
				}
				return true;
        	}*/
        	
            //add senderReplacement function 2020.09.01
            try {
            	if(!cst.isNeedSupportBlackList() || (gmmsUtility.preBlackListCheck(msg)&& cst.isNeedSupportBlackList())){
            		if (cst.isSmsOptionAdvancedSenderReplacement()) {
                		ctm.doSenderAddressReplace(msg.getOSsID(), msg);
    				}
            	}            					
			} catch (Exception e) {
				log.error("do sender replace in ocustomer failed", e);
			}
			if (msg.getExpiryDate() == null) {
				TimeZone local = TimeZone.getDefault();
				long now = new Date().getTime();
				long diff = local.getRawOffset();
				if (local.inDaylightTime(new Date(now))) {
					diff += local.getDSTSavings();
				}
				long gmtNow = now - diff;
				int expireTime = gmmsUtility.getExpireTimeInMinute();
				if (cst.getExpireTime() > 0) {
					expireTime = cst.getExpireTime();
				} else if (cst.getFinalExpireTime() > 0) {
					expireTime = cst.getFinalExpireTime() * 3 / 4;
				}
				msg.setExpiryDate(new Date(gmtNow + expireTime * 60 * 1000));
			}
        }
        
        
        if (gmmsUtility.preCheckMessage(msg)) {
            csm.processGMMS(msg);
        	msgStoreManager.prepareInSubmit(msg);
        	gmmsUtility.afterCheckMessage(msg);
        }
        
        try{
        	// all submit msg didn't need to send response to function module 2023.03.09
        	if (cst.getProtocol().startsWith("Common")) {
				//http protocal, didn't need send response to commonHttpServer.
        		if (msg.getStatus().getCode() == GmmsStatus.UNASSIGNED
						.getCode()) {
        			processInnerAck(msg);
				}else {					
					gmmsUtility.getCdrManager().logInSubmit(msg);
					if (msg.getDeliveryReport()) {
						msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
						msg.setRSsID(msg.getOSsID());	
						GmmsStatus converDRStatus = GmmsStatus.SubmitConvertErrorDRStatus(msg.getStatusText());
						if(converDRStatus!=null) {
							msg.setStatus(converDRStatus);
						}else {
							msg.setStatus(GmmsStatus.REJECTED);
						}
						msgStoreManager.sendDRMessageForServerReject(msg);						
					}					
				}
        		
			}else {
				if(inBuffer.put(msg.getMsgID(), msg)){
	        		ack.convertFromMsg(msg);
	    	        if(!sendPdu(ack)){
	    	        	inBuffer.remove(msg.getMsgID());
	    	        	msg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED.getCode());
						if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
								.getMessageType())) {
							
							gmmsUtility.getCdrManager().logInSubmit(msg);
						}
						if(log.isInfoEnabled()){
							log.info(msg,"Failed to send response to function module, msg type is {}",msg.getMessageType());
	    	        }
					}
				} else {
					if(log.isInfoEnabled()){
						log.info(msg,"Message Buffer is full, and return customer temp error, buffer size is {}", inBuffer.size());
					}
					ack.setStatusCode(GmmsStatus.SERVER_ERROR.getCode());
					ack.convertFromMsg(msg);
					if (!sendPdu(ack)) {
						msg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED.getCode());
						if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
								.getMessageType())) {							
							gmmsUtility.getCdrManager().logInSubmit(msg);
						}
						if(log.isInfoEnabled()){
							log.info(msg,"Failed to send response to function module, msg type is {}",msg.getMessageType());
	    				}
	        		}
	        	}
			}
        	
        }catch(Exception e){
        	log.warn(msg,"Fail to convert Message to PDU");
        }

        return true;
    }
    
    
    private boolean processSubmit(GmmsMessage msg) {
    	boolean result = false;
    	if(msg.getStatusCode() < 0 ){
    		result = processSuccessSubmit(msg);
    	}else{
    		msg.setStatusText(GmmsStatus.getStatus(msg.getStatusCode()).getText());
    		result = processErrorSubmit(msg);
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
    		// Error path: processErrorSubmit 内部会调 sendPdu(ack) 给 TCP 方向，
    		// Stream 路径下 Core 无 TCP 连接，sendPdu 会返回 false 但不影响业务。
    		// 对于 HTTP(Common) 协议，错误 DR 会正常通过 msgStoreManager 发出。
    		// 对于 SMPP 协议（来自 Stream），忽略 sendPdu 失败，只要 CDR 和错误 DR 正常处理即可。
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
        	return false;
        }
        if (cst.isSmsOptionSendFakeDR() && msg.getDeliveryReport()) {
    		msg.setFakeDR(true);
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

        	if ("NonPA".equalsIgnoreCase(cst.getCustomerTypeBySender()) && recipientAddress.startsWith("65")) {
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
        		processInnerAck(msg);
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
        if (cst != null && cst.isSmsOptionSendFakeDR() && msg.getDeliveryReport()) {
        	msg.setFakeDR(true);
        }
        // 对 HTTP 协议保持发送错误 DR
        if (cst != null && !cst.getProtocol().startsWith("SMPP") && msg.getDeliveryReport()
                && GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
        	msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        	msg.setRSsID(msg.getOSsID());
        	msg.setOA2P(ctm.getCurrentA2P());
        	msg.setTransaction(transaction);
        	msgStoreManager.sendDRMessage(msg);
        }
        // SMPP Stream 路径：不尝试 TCP sendPdu，直接返回 true
        return true;
    }
    /**
     * 
     * @param msg
     * @return
     */
    private boolean processSubmit4MQM(GmmsMessage msg) {
        boolean result = true;
        InternalPdu4MQM ack = new InternalPdu4MQM(Pdu.COMMAND_ACK4MQM);
        
        try{
        	ack.convertFromMsg(msg);
        }catch(Exception e){
        	log.warn(msg,"Fail to convert Message to PDU");
        	return result;
        }
    	
        if(sendPdu(ack)){
			if (CsmUtility.isConcatenatedMsg(msg) && msg.getRoperator() < 0) {
				if(!csmHandler.putMsg(msg)){
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					msgStoreManager.handleOutSubmitRes(msg);
				}
			}else{
				if(!deliveryRouterHandler.putMsg(msg)){
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					msgStoreManager.handleOutSubmitRes(msg);
				}
			}
		}

		return result;
	}

	private boolean processSubmitAck(GmmsMessage msg) {
		boolean result = false;
		try {
			A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getRSsID());
			msg.setOutClientPull("1".equals(cst.getOutClientPull()));
			msgStoreManager.handleOutSubmitRes(msg, true);
			result = true;
		} catch (Exception e) {
			log.error(msg, e, e);
			stop();
		}
		return result;

	}

	private boolean processStoredDeliveryReport(GmmsMessage msg, Pdu pdu) {
		boolean result = false;
		try {
			if (msg != null) {				
				
				if (msg.getDeliveryChannel()!=null &&
						msg.getDeliveryChannel().startsWith("CommonHttpServer")) {
					//http protocal, didn't need send response to commonHttpServer.
					processInnerAck(msg);	        		
				}else{
					result = sdqManager.insertSDQMessage(msg);
					if (pdu != null && pdu instanceof CommandDeliveryReport) {
						CommandDeliveryReportAck ack = new CommandDeliveryReportAck();
						CommandDeliveryReport reportPdu = (CommandDeliveryReport) pdu;
						ack.setMsgId(reportPdu.getMsgId());
						ack.setCustMsgId(reportPdu.getMsgId());
						ack.setTransactionId(reportPdu.getTransactionId());
						ack.setR_relay(reportPdu.getR_relay());
						ack.setO_relay(reportPdu.getO_relay());
						ack.setC_hub(reportPdu.getC_hub());
						ack.setO_hub(reportPdu.getO_hub());
						ack.setR_hub(reportPdu.getR_hub());
						ack.setTransaction(reportPdu.getTransaction());
						if (result) {
							ack.setStatusCode(0);
						} else {
							ack.setStatusCode(1);
						}

						sendPdu(ack);
					}
				}
				
			}
			result = true;
		} catch (Exception e) {
			log.warn(msg, e, e);
			stop();
			result = false;
		}
		return result;
	}

	private boolean processDeliveryReport(GmmsMessage msg, Pdu pdu) {
		boolean result = false;
		try {
			A2PCustomerInfo vdc = ctm.getCustomerBySSID(msg.getRSsID());
			
			if (msg != null) {
				msg.setInnerTransaction(transaction);
				processInnerAck(msg);
				//TODO
				/*if (msg.getDeliveryChannel()!=null &&
						msg.getDeliveryChannel().startsWith("CommonHttpServer")) {
					//http protocal, didn't need send response to commonHttpServer.
					processInnerAck(msg);	        		
				}else{
					if (inBuffer.put(msg.getOutMsgID(), msg)) {
						ack.convertFromMsg(msg, false);
						if(vdc.isSmsOptionIsVirtualDC()){
							ack.setR_relay(reportPdu.getR_relay());
						}
						if (!sendPdu(ack)) {
							if(log.isInfoEnabled()){
								log.info(msg, "Fail to process message");
							}
							if (inBuffer.remove(msg.getOutMsgID()) != null) {
								msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
								msg.setStatus(GmmsStatus.SUCCESS);
								msgStoreManager.insertMessageToDB(msg,
										msgStoreManager.WDQ);
							}
						}
					} else {
						ack.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
								.getCode());
						ack.convertFromMsg(msg, false);
						if(vdc.isSmsOptionIsVirtualDC()){
							ack.setR_relay(reportPdu.getR_relay());
						}
						sendPdu(ack);
					}
				}	*/																									
			} else {
				if (pdu != null && pdu instanceof CommandDeliveryReport) {
					CommandDeliveryReportAck ack = new CommandDeliveryReportAck();
					CommandDeliveryReport reportPdu = (CommandDeliveryReport) pdu;
					ack.setMsgId(reportPdu.getMsgId());
					ack.setCustMsgId(reportPdu.getMsgId());
					ack.setTransactionId(reportPdu.getTransactionId());
					ack.setR_relay(reportPdu.getR_relay());
					ack.setO_relay(reportPdu.getO_relay());
					ack.setC_hub(reportPdu.getC_hub());
					ack.setO_hub(reportPdu.getO_hub());
					ack.setR_hub(reportPdu.getR_hub());
					ack.setStatusCode(0);
					ack.setTransaction(reportPdu.getTransaction());
					sendPdu(ack);
				}
			}
			result = true;
		} catch (Exception e) {
			log.warn(msg, e, e);
			stop();
			result = false;
		}
		return result;
	}

	/**
	 * V4.5 Reactive DR Entry Point
	 * Shared logic between TCP path and Redis Stream path
	 */
	public void handleInboundDRInternal(GmmsMessage finalMsg, Pdu finalPdu) {
		GmmsMessage message = null;
		
		if (ctm.isInDRStoreMode(finalMsg.getRSsID())
				|| (isStoreDRModeEnable && gmmsUtility.isRunningStoreDRMode())) {
			processStoredDeliveryReport(finalMsg, finalPdu);
		}
		else{
			if(isRedisEnable){
				String object = redis.getString(finalMsg.getOutMsgID());
				if(object != null){
					message = SerializableHandler.convertRedisMssage2GmmsMessage(object);
					if(message!=null){
						if (GmmsStatus.ACCEPT.getCode() ==finalMsg.getStatus().getCode()) {
							//didn't do anything							
						}else {
							String dateKey  = gmmsUtility.getRedisDateIn(message);
							if(dateKey!=null){
								redis.delPipeline(finalMsg.getOutMsgID(),dateKey);
							}else{
								redis.del(finalMsg.getOutMsgID());
							}
						}
						
					}			
				}else{
					if(log.isInfoEnabled()){
						log.info("Can not find the message in redis, and out msg id is {}"
								, finalMsg.getOutMsgID());	
					}
					try {
						message = msgStoreManager.getGmmsMessageByOutMsgID(finalMsg.getOutMsgID());
					} catch (DataManagerException e) {
						log.error("Can not find the message in message Queue, and out msg id is {}"
										,finalMsg.getOutMsgID());
					}
				}
				
			}else{
				try {
					message = msgStoreManager.getGmmsMessageByOutMsgID(finalMsg.getOutMsgID());
				} catch (DataManagerException e) {
					log.error("Can not find the message in message Queue, and out msg id is {}"
									,finalMsg.getOutMsgID());
				}
			}
			if(message != null){					
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				message.setStatusCode(finalMsg.getStatusCode());
				if (!GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(
						message.getStatusText())) {
					message.setStatusText(GmmsStatus.getStatus(finalMsg.getStatusCode()).getText());
				}
				
				message.setOutTransID(finalMsg.getOutTransID());
				message.setTransaction(finalMsg.getTransaction());
				message.setDeliveryChannel(finalMsg.getDeliveryChannel());
				processDeliveryReport(message, finalPdu);
			} else {
				processStoredDeliveryReport(finalMsg, finalPdu);
			}
		}
	}


	
	private boolean processDeliveryReportQuery(GmmsMessage msg, Pdu pdu) {
		boolean result = false;
		try {
			InternalPdu4MQM ack = new InternalPdu4MQM(Pdu.COMMAND_ACK4MQM);
			if (msg != null) {
				msg.setInnerTransaction(transaction);
				ack.convertFromMsg(msg);
				if (sendPdu(ack)) {
					if (!processorHandler.putMsg(msg)) {
						msg.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
						msgStoreManager.handleOutDeliveryReportRes(msg);
					}
				} else {
					log.error(msg, "Fail to process message");
				}
			} 
			result = true;
		} catch (Exception e) {
			log.warn(msg, e, e);
			stop();
			result = false;
		}
		return result;
	}
	
	// REST
	private boolean processRESTDeliveryReportQuery(GmmsMessage msg, Pdu pdu) {
		boolean result = false;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Enter InternalCoreEngineSession.processRESTDeliveryReportQuery()");
			}
			CommandDeliveryReportQueryAck ack = new CommandDeliveryReportQueryAck();
			if (msg != null) {

				if (log.isDebugEnabled()) {
					log.debug("processRESTDeliveryReportQuery msg.getOSsid():"
							+ msg.getOSsID());
				}

				A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getOSsID());

				if (log.isDebugEnabled()) {
					log.debug("processRESTDeliveryReportQuery cst.getInClientPull():"
							+ cst.getInClientPull());
				}

				if (cst.getInClientPull() == 2) {

					GmmsMessage message = null;
					message = msgStoreManager.getInMsgfromCache(msg
							.getInMsgID());
					if (log.isDebugEnabled()) {
						log.debug("processRESTDeliveryReportQuery msg:"
								+ message);
					}

					if (message == null) {
						message = new GmmsMessage();
						message.setInMsgID(msg.getInMsgID());
						message.setMsgID(msg.getMsgID());
						message.setStatus(GmmsStatus.MSG_NOT_FOUND);//
						message.setInTransID(msg.getInTransID());
						message.setOSsID(msg.getOSsID());
						message.setRSsID(msg.getRSsID());
						message.setDeliveryChannel(msg.getDeliveryChannel());
					} else {

						if (message.getStatus() == GmmsStatus.UNASSIGNED
								|| message.getStatus() == GmmsStatus.INDELIVERY
								|| message.getStatus() == GmmsStatus.COMMUNICATION_ERROR
								|| message.getStatus() == GmmsStatus.Throttled
								|| message.getStatus() == GmmsStatus.SERVER_ERROR
								|| message.getStatus() == GmmsStatus.SERVICE_ERROR
								|| message.getStatus() == GmmsStatus.FAIL_QUERY_DELIVERREPORT) {
							message.setStatusCode(GmmsStatus.ENROUTE.getCode());
						} else if (message.getStatus() == GmmsStatus.SUCCESS) {
							message.setStatusCode(GmmsStatus.DELIVERED
									.getCode());
						} else if ((message.getStatus().getCode() >= 2000 && message
								.getStatus().getCode() <= 2300)
								|| message.getStatus() == GmmsStatus.AUTHENTICATION_ERROR
								|| message.getStatus() == GmmsStatus.UNKNOWN_ERROR) {
							message.setStatusCode(GmmsStatus.UNDELIVERABLE
									.getCode());
						}

					}

					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY);
					message.setTransaction(transaction);
					message.setInMsgID(msg.getInMsgID());
					message.setDeliveryChannel(msg.getDeliveryChannel());

					if (log.isDebugEnabled()) {
						log.debug("message.getMsgId:" + message.getMsgID()
								+ "msg.getMsgID:" + msg.getMsgID()
								+ "statusCode:" + message.getStatusCode() + ","
								+ message.getStatusText());
					}

					if (inBuffer.put(message.getMsgID(), message)) {
						ack.convertFromMsg(message, true);
						if (log.isDebugEnabled()) {
							log.debug("processRESTDeliveryReportQuery ack:"
									+ ack);
						}

						if (!sendPdu(ack)) {
							inBuffer.remove(message.getMsgID());
							msg.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
									.getCode());

							if (log.isInfoEnabled()) {
								log.info(
										msg,
										"Failed to send response to function module, msg type is {}",
										msg.getMessageType());
							}

						} else {
							log.debug("processRESTDeliveryReportQuery ack send success");
						}
					} else {
						if (log.isInfoEnabled()) {
							log.info(
									msg,
									"Message Buffer is full, and return customer temp error, buffer size is {}",
									inBuffer.size());
						}

						ack.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
								.getCode());
						ack.convertFromMsg(message);
						if (!sendPdu(ack)) {
							msg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED
									.getCode());

							if (log.isInfoEnabled()) {
								log.info(
										msg,
										"Failed to send response to function module, msg type is {}",
										msg.getMessageType());
							}
						}
					}
					result = true;
				}
			} else {
				result = false;
			}

		} catch (Exception e) {
			log.warn(msg, e, e);
			stop();
			result = false;
		}
		return result;
	}

	/**
	 * 
	 * @param msg
	 * @return
	 */
	private boolean processDeliveryReport4MQM(GmmsMessage msg, Pdu pdu) {
		boolean result = false;
		try {
			InternalPdu4MQM ack = new InternalPdu4MQM(Pdu.COMMAND_ACK4MQM);
			if (msg != null) {
				msg.setInnerTransaction(transaction);
				ack.convertFromMsg(msg);
				if (sendPdu(ack)) {
					if (!processorHandler.putMsg(msg)) {
						msg
								.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
										.getCode());
						msgStoreManager.handleInDeliveryReportRes(msg);
					}
				}
			} else {
				log
						.info("Message is null when to return Delivery Report ACK to MQM, PDU is "
								+ pdu.toString());
			}
			result = true;
		} catch (Exception e) {
			log.warn(msg, e, e);
			stop();
			result = false;
		}
		return result;
	}

	private boolean processDeliveryReportAck(GmmsMessage msg) {
		boolean result = false;
		try {
			msgStoreManager.handleInDeliveryReportRes(msg);
			result = true;
		} catch (Exception e) {
			log.error(msg, "Failed in processDeliveryReportAck.", e);
		}
		return result;
	}

	private boolean processDeliveryReportQueryAck(GmmsMessage msg) {
		boolean result = false;
		try {
			msgStoreManager.handleOutDeliveryReportRes(msg);
			result = true;
		} catch (Exception e) {
			log.error(msg, "Failed in processDeliveryReportAck.", e);
		}
		return result;
	}

	public boolean processInnerAck(GmmsMessage msg) {		
		
		boolean result = false;
		try {
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
					.getMessageType())) {
				if (msg.getStatus().getCode() == GmmsStatus.UNASSIGNED
						.getCode()) {
					Date scheduleDate = msg.getScheduleDeliveryTime();
					if (ctm.isInStoreMode(msg.getOSsID())) { //store mode
						if (log.isInfoEnabled()) {
							log.info(msg, "In store mode, send to SMQ");
						}
						msgStoreManager.handleInSubmit(msg, true);
					} else if ((scheduleDate != null && scheduleDate.after(gmmsUtility.getGMTTime()))) { //schedule delivery
						if (log.isInfoEnabled()) {
							log.info(msg, "Schedule delivery message, send to SMQ");
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
							if(!csmHandler.putMsg(msg)){
								msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
								msgStoreManager.handleOutSubmitResForInnack(msg);
							}
						}else{
							//TODO							
							if("SMPP".equalsIgnoreCase(oCustomer.getProtocol())){
								//TODO
								if(oCustomer.isNeedCheckMsgSize()) {
									if(!"ASCII".equalsIgnoreCase(msg.getContentType()) 
											&& msg.getMessageSize()>140){
										msg.setStatus(GmmsStatus.MSG_SIZE_OVERLENGTH);
										msg.setRSsID(gmmsUtility.getBlackholeSsid());
										msgStoreManager.handleOutSubmitResForInnack(msg);
										return true;
									}
								}
								//charge recipient 重复次数
								int count = oCustomer.getSmsOptionRecipientMaxSendCountIn24H();
								if(count>0){
									String key = "Duplicate:"+msg.getRecipientAddress();
									long currentCount = redis.incrString(key);
									if(currentCount == 1){
										redis.setExpire(key, 24*60*60);
									}
									if(currentCount>count){
										msg.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_MAX_COUNT);
										msg.setRSsID(gmmsUtility.getBlackholeSsid());
										msgStoreManager.handleOutSubmitResForInnack(msg);
										return true;
									}
								}
								
								
							}
							if(!deliveryRouterHandler.putMsg(msg)){
								msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
								msgStoreManager.handleOutSubmitResForInnack(msg);
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
											
    

	private boolean processBind(Pdu pdu) {
		boolean result = false;
		CommandBind bind = (CommandBind) pdu;
		String userName = bind.getUserName();
		CommandBindAck ack = new CommandBindAck();

        if (userName != null) {
            try {
				ConnectionInfo connInfo = moduleManager
						.getConnectionInfo(userName);
        		if(connInfo != null){
        			super.connectionInfo = connInfo;
	                setSessionName(connInfo.getConnectionName());
					transaction.setConnectionName(connectionInfo
							.getConnectionName());
	                if (factory.manageConnection(userName, this)) {
	                    setStatus(ConnectionStatus.CONNECT);
	        			initSenders();
	        			updateReceivers();
	                    startBufferMonitor();
	                    ack.setStatusCode(0);
	                    result = true;
	                }
	                else {
	                    ack.setStatusCode(1);
	                }
            	}		               
            	else{
                	ack.setStatusCode(1);
                }
            }
            catch (Exception e) {
                log.error("processBind exception:", e);
                setStatus(ConnectionStatus.DISCONNECT);
                ack.setStatusCode(1);
            }
        }
        else {
            log.warn("userName in BIND is null.");
            ack.setStatusCode(1);
        }
        try {
            submit(ack.toByteBuffer().getBuffer());
        }
        catch (IOException ex) {
            log.warn(ex, ex);
        }
        catch (Exception ex) {
            log.warn(ex, ex);
        }
        return result;
    }

    public boolean sendPdu(Pdu pdu){
    	if(pdu == null){
    		return false;
    	}
        boolean res = false;
        try {
            byte[] toPostData = pdu.toByteBuffer().getBuffer();
            res = super.submit(toPostData);
        }
        catch (IOException e) {
        	res = false;
        	stop();
        }
        catch (Exception e){
        	res = false;
        }
        return res;
    }
    /**
     * connection broken
     */
    public void connectionUnavailable() {
    	super.connectionUnavailable();
    	stop();
    }
    
    public void stop(){
    	synchronized(mutex){
	    	super.stop();
	    	if(isServer){
	    		if(msgQueue != null){
			        msgQueue.stopMessageQueue();
		        }
		        
		        if (executorServiceManager != null) {
					executorServiceManager.shutdown(receiverThreadPool);
				}
		        
		        if(outBuffer != null){
		        	outBuffer.writeAllToDB();
		        	outBuffer.stopMonitor();
		        	outBuffer = null;
		        }
		        if(inBuffer != null){
		        	inBuffer.sendbackBuffer();
		        	inBuffer.stopMonitor();
		        	inBuffer = null;
		        }
		        
		    	if(factory!=null && connectionInfo!=null){
		    		factory.connectionBroken(this.connectionInfo.getUserName(),this);
		    	}
	    	}
    	}
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
    /**
     * handle for common prefix for MT
     * @param msg GmmsMessage
     * @param cst GmmsCustomer
     * add by brush
     */
    private void handleCommonPrefix4MT(GmmsMessage msg,String[] address,A2PCustomerInfo cst){

        if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.
            getMessageType())){
            if(cst.isHandlePrefix4DRMO()){
                cst.handlePrefix4DrMo(address);
            }
        }
        else{
        	
        	//add senderReplacement function 2020.09.01
            try {
            	if (cst.isSmsOptionAdvancedSenderReplacement()) {
            		ctm.doSenderAddressReplace(cst.getSSID(), msg);
            		String sender = msg.getSenderAddress();
            		msg.setSenderAddress(address[0]);
            		address[0] = sender;
				}				
			} catch (Exception e) {
				log.error("do sender replace in ocustomer failed", e);
			}
        	
            if(cst.isHandlePrefix4NewMT()){
                cst.handlePrefix4NewMt(address);
            }
          
        }
    }
    
    class TimeoutHandlerForOutBuffer implements BufferTimeoutInterface{
    	
		public void timeout(Object key, GmmsMessage bufferedMsg) {
	        try {
	            if (bufferedMsg != null) {
	                if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(
	                    bufferedMsg.getMessageType())) {
	                    bufferedMsg.setStatusCode(GmmsStatus.
	                                              FAIL_SENDOUT_DELIVERYREPORT.
	                                              getCode());
	                    msgStoreManager.handleInDeliveryReportRes(bufferedMsg);
	                }else if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(
	                        bufferedMsg.getMessageType())) {
	                    bufferedMsg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
	                    msgStoreManager.handleOutDeliveryReportRes(bufferedMsg);
	                }
	                else {
	                    bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
	                    msgStoreManager.handleOutSubmitRes(bufferedMsg);
	                }
	            }
	        }
	        catch (Exception ex) {
	            log.error(bufferedMsg,ex, ex);
	        }
		}
		
    }
    
    class TimeoutHandlerForInBuffer implements BufferTimeoutInterface{
    	
		public void timeout(Object key, GmmsMessage bufferedMsg) {
	        try {
	            if (bufferedMsg != null) {
        			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(bufferedMsg.getMessageType())) {
        				bufferedMsg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED.getCode());        				
        				gmmsUtility.getCdrManager().logInSubmit(bufferedMsg);
        			} else if(GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(bufferedMsg.getMessageType())){
        				bufferedMsg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED.getCode());
        				gmmsUtility.getCdrManager().logInDelivery(bufferedMsg);
        			} else if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(bufferedMsg.getMessageType())){
        				bufferedMsg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
        				bufferedMsg.setStatus(GmmsStatus.SUCCESS);
        				if(!msgStoreManager.WDQ.equalsIgnoreCase(bufferedMsg.getOriginalQueue())){
        					msgStoreManager.insertMessageToDB(bufferedMsg,msgStoreManager.WDQ);
        				}
        			}
	            }
	        }
	        catch (Exception ex) {
	            log.error(bufferedMsg,ex, ex);
	        }
		}
    }
    
 
    
}
