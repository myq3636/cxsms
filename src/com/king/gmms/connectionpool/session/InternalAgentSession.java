package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.Socket;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.InternalClientSessionThread;
import com.king.gmms.connectionpool.sessionthread.InternalServerSessionThread;
import com.king.gmms.customerconnectionfactory.CustomerConnectionFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.tcp.internaltcp.CommandBind;
import com.king.gmms.protocol.tcp.internaltcp.CommandBindAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReport;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportQuery;
import com.king.gmms.protocol.tcp.internaltcp.CommandDeliveryReportQueryAck;

import com.king.gmms.protocol.tcp.internaltcp.CommandKeepAliveAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandSubmit;
import com.king.gmms.protocol.tcp.internaltcp.CommandSubmitAck;
import com.king.gmms.protocol.tcp.internaltcp.Pdu;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class InternalAgentSession extends AbstractInternalSession{
	
    private static SystemLogger log = SystemLogger.getSystemLogger(InternalAgentSession.class);
    private InternalAgentConnectionFactory agentConnectionFactory = null;
    
    /**
     * for client side
     * @param connectionInfo
     */
    public InternalAgentSession(ConnectionInfo connectionInfo){
    	super();
        if (connectionInfo == null) {
            return;
        }
        isServer = false;
        msgQueue = new InternalConnectionMessageQueue(isServer);
        transaction = new TransactionURI();
        transaction.setConnectionName(connectionInfo.getConnectionName());
        int connectionSilentTime = Integer.parseInt(gmmsUtility.getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
        this.connectionInfo = connectionInfo;
        super.initReceivers();
        this.initSenders();
        setSessionName(connectionInfo.getConnectionName());
        agentConnectionFactory = InternalAgentConnectionFactory.getInstance();
        sessionThread = new InternalClientSessionThread(this,connectionSilentTime);
        
        start();
    }
    /**
     * for server side
     * @param socket
     */
    public InternalAgentSession(Socket socket) {
        super();
        // create a single thread pool first, then update after binded
        super.initReceivers();
        
        isServer = true;
        msgQueue = new InternalConnectionMessageQueue(isServer);
        int connectionSilentTime = Integer.parseInt(gmmsUtility.
            getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
        transaction = new TransactionURI();
        try {
            if (socket != null) {
                createConnection(socket);
            }
        }
        catch (IOException ex) {
            log.error(ex, ex);
        }
        agentConnectionFactory = InternalAgentConnectionFactory.getInstance();
        sessionThread = new InternalServerSessionThread(this,
            connectionSilentTime);
        
        
        start();
    }
    
    public boolean receive(Object obj) {
        boolean result = false;
        if (obj == null) {
            return true;
        }
        Pdu pdu = (Pdu) obj;
        GmmsMessage msg = null;
        if(log.isTraceEnabled()){
    		log.trace("Internal Agent Session receive a PDU {}", pdu.getCommandId());
        }
        if (Pdu.COMMAND_SUBMIT == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
            if (msg != null) {
            	if(log.isInfoEnabled()){
					log.info(msg,"Convert SubmitSM PDU to GmmsMessage.");
            	}
                result = processSubmit(msg);
            }
        }
        else if (Pdu.COMMAND_SUBMIT_ACK == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
            if (msg != null) {
                this.processSubmitAck(msg);
            }
        }
        else if (Pdu.COMMAND_DELIVERY_REPORT == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
            if (msg != null) {
                this.processDeliveryReport(msg, pdu);
            }
        }
        else if (Pdu.COMMAND_DELIVERY_REPORT_QUERY == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
            if (msg != null) {
                this.processDeliveryReportQuery(msg, pdu);
            }
        }
        else if(pdu.COMMAND_DELIVERY_REPORT_QUERY_ACK==pdu.getCommandId())
        {
        	if(log.isDebugEnabled())
        	{
        		log.debug("InternalAgentSession receive delivery report ack from CoreEngine");
        	}
        	msg=((CommandDeliveryReportQueryAck)pdu).convertToMsg();
        	if(msg!=null)
        	{
        		processDeliveryReportQueryAck(msg,pdu);
        	}
        	
        }
        else if (Pdu.COMMAND_DELIVERY_REPORT_ACK == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
    		if(log.isDebugEnabled()){
                log.debug(msg,"processDeliveryReportAck before:{}",msg);
    		}
            if (msg != null) {
                this.processDeliveryReportAck(msg);
            }
        }
        else if (Pdu.COMMAND_ALIVE == pdu.getCommandId()) {
            CommandKeepAliveAck ack = new CommandKeepAliveAck();
            try {
                super.submit(ack.toByteBuffer().getBuffer());
            }
            catch (Exception ex) {
                log.warn("IOException occured when sending KeepAlive ACK", ex);
                stop();
            }
        }
        else if (Pdu.COMMAND_ALIVE_ACK == pdu.getCommandId()) {
            super.clearActiveTestCount();
        }
        else if (Pdu.COMMAND_BIND == pdu.getCommandId()) {
        	result = processBind(pdu);
        	if(!result){
        		stop();
        	}
        }
        return result;
    }
    /**
     * queue timeout
     */
    public void timeout(Object msg) {
		GmmsMessage message = (GmmsMessage)msg;
    	try {
            if (message != null) {
                if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType())) {
                	message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
                	message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
            		put2OperatorMessageQueue(message);
                }else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(message.getMessageType())){
                	message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
                	message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            		put2OperatorMessageQueue(message);
                }else {
                	message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
                }
            }
        }
        catch (Exception ex) {
            log.error(message,ex, ex);
        }
    }
    
    /**
     * 
     * @param message
     */
    private void put2OperatorMessageQueue(GmmsMessage message){
    	int ssid = -1;
    	CustomerConnectionFactory customerFactory = agentConnectionFactory.getCustomerFactory();
    	if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(message.getMessageType())
    		||GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message.getMessageType())
    		||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(message.getMessageType())) {
        	if(message.getOA2P()==message.getCurrentA2P() 
        		|| gmmsUtility.getCustomerManager().vpOnSameA2P(message.getOA2P(),message.getCurrentA2P())){
        		ssid = message.getOSsID();
        	}else{
        		ssid = message.getOA2P();
        	}
        }else if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType())
        		||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(message.getMessageType())
        		||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(message.getMessageType())){
        	if(message.getRA2P()==message.getCurrentA2P() 
        		|| gmmsUtility.getCustomerManager().vpOnSameA2P(message.getRA2P(),message.getCurrentA2P())){
        		ssid = message.getRSsID();
        	}else{
        		ssid = message.getRA2P();
        	}
        }
    	
    	/*
    	 *  Since ASG or Partitions in it use the same socket connect with A2P, 
         *  and A2P PeeringTcp2 Server manage TCP session with (O_A2P_SSID, TCP_session) pairs, 
         *  so when A2P send Response messages to Partitions, 
         *  it will find the session by the SSID of the ASG which this Partition locate in instead of SSID of the Partition.
    	 * */
        A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
        if (ctm.isPartition(ssid)) {
        	int partitionConnectdSsid = ctm.getVPConnectingRelaySsid(ssid);
        	if (ctm.isA2P(partitionConnectdSsid)) {
        		log.info(message, "Changer ssid from Partition: " + ssid + " to A2P: " + partitionConnectdSsid);
        		ssid = partitionConnectdSsid;
        	}
        }
    	
    	OperatorMessageQueue msgQueue = customerFactory.getOperatorMessageQueue(ssid);
    	if(msgQueue != null){
    		msgQueue.putMsg(message);    		
    	}else{
    		log.warn(message,"Can't find the operator message queue by ssid:{}",ssid);
    	}
    }

    /**
     * submit
     */
    public boolean submit(GmmsMessage message) {
        boolean result = false;
        String messageType = message.getMessageType();
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
            result = sendNewMessage(message);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) {
            result = sendReport(message);
        }
        else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(messageType)){
        	result = sendSubmitResp(message);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(messageType)){
        	result = sendReportAck(message);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(messageType)){
        	result = sendReportQueryAck(message);
        }
        //add by kevin for REST
        else if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(messageType))
        {
        	result=sendDRQueryMessage(message);
        }
        else {
            log.warn(message,
                     "Unknown Message Type:" + message.getMessageType());
            message.setStatus(GmmsStatus.UNKNOWN_ERROR);
        }
        return result;
    }

    private boolean processSubmit(GmmsMessage msg) {
    	
        boolean result = false;
        if (msg == null) {
            log.warn("Message is null in processSubmit method.");
            return result;
        }
        if(log.isTraceEnabled()){
    		log.trace(msg, "processSubmit: {}" , msg.toString());
        }
        
        try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
            result = true;
        }
        catch (Exception ex) {
            log.warn(ex, ex);
            stop();
        }
        return result;
    }
    /**
     * process submit response
     * @param msg
     * @return
     */
    private boolean processSubmitAck(GmmsMessage msg) {
    	
    	if(log.isTraceEnabled()){
    		log.trace(msg, "processSubmitAck: {}" , msg.toString());
    	}
    	
        boolean result = false;
        try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
        	result = true;
        }
        catch (Exception e) {
            log.error(msg,"msg.getOSsID():{}",msg.getOSsID(), e);
            stop();
        }
        return result;
    }

    private boolean processDeliveryReport(GmmsMessage msg, Pdu pdu) {
    	 boolean result = false;
         if (msg == null) {
             log.warn("Message is null in processDeliveryReport method.");
             return result;
         }
         try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
    		if(log.isInfoEnabled()){
				log.info(msg,"Internal Agent received DR,status:{}",msg.getStatusCode());
    		}
            result = true;
         }
         catch (Exception ex) {
             log.warn(ex, ex);
             stop();
         }
         return result;
    }

    private boolean processDeliveryReportQuery(GmmsMessage msg, Pdu pdu) {
   	 	boolean result = false;
        if (msg == null) {
            log.warn("Message is null in processDeliveryReport method.");
            return result;
        }
        try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
        	result = true;
        }
        catch (Exception ex) {
            log.warn(ex, ex);
            stop();
        }
        return result;
   }
    
    private boolean processDeliveryReportAck(GmmsMessage msg) {
        boolean result = false;
        if (msg == null) {
            log.warn("Message is null in processDeliveryReportAck method.");
            return result;
        }
        try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
            result = true;
        }
        catch (Exception ex) {
            log.warn(ex, ex);
            stop();
        }
        return result;
    }

    public boolean processBind(Pdu pdu) {
        boolean result = false;
        CommandBind bind = (CommandBind)pdu;
        String userName = bind.getUserName();
        CommandBindAck ack = new CommandBindAck();

        if (userName != null) {
            try {
        		ConnectionInfo connInfo = ModuleManager.getInstance().getConnectionInfo(userName);

                if(connInfo != null){
                	connectionInfo = connInfo;
                	setSessionName(connInfo.getConnectionName());
                	transaction.setConnectionName(connectionInfo.getConnectionName());
                    if (insertSession(userName)) {
                        setStatus(ConnectionStatus.CONNECT);
                    	initSenders();
                    	updateReceivers();
                        ack.setStatusCode(0);
                        result = true;
                    }
                    else {
                        ack.setStatusCode(1);
                    }

                } else {
                    log.warn("Can't find server for module: {}" , userName);
                    ack.setStatusCode(1);
                }
            }
            catch (Exception e) {
                log.error(e, e);
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
    /**
     * send SUBMIT request
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendNewMessage(GmmsMessage message) {
        boolean result = false;
        TcpByteBuffer submitBuffer = null;

        try {
            CommandSubmit pdu = new CommandSubmit();
            pdu.convertFromMsg(message);
            submitBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e,e);
            return result;
        }
        if (submitBuffer == null) {
            message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
            return result;
        }

        try {
            super.submit(submitBuffer.getBuffer());
            message.setOutTransID(String.valueOf(System.currentTimeMillis()));
            result = true;
        }
        catch (IOException ex) {
            log.warn(message,
                     "IOException occured when submit msg to Core engine: ",
                     ex);
            message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
            stop();
        }
        return result;
    }
    
    /**
     * send REPORT2 to the client
     *
     * @param message GmmsMessage
     * @return boolean
     */
    private boolean sendReport(GmmsMessage message) {
        if (message == null) {
            	log.trace("No DR send to Client");
            return false;
        }
        boolean result = false;
        TcpByteBuffer reportBuffer = null;
        try {
            CommandDeliveryReport pdu = new CommandDeliveryReport();
            pdu.convertFromMsg(message,false);
        	log.trace(message, "send DR PDU : {}",pdu);
            reportBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e, e);
            return result;
        }

        if (reportBuffer == null) {
            return result;
        }

        try {
            super.submit(reportBuffer.getBuffer());
            result = true;
        }
        catch (IOException ex) {
            log.error(message,ex,ex);
            message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.
                                  getCode());
            stop();
        }
        return result;
    }
    
    private boolean sendReportAck(GmmsMessage message){
        boolean result = false;
        Pdu ack = null;
        TcpByteBuffer responseBuffer = null;
        try {
            ack = new CommandDeliveryReportAck();
            ((CommandDeliveryReportAck)ack).convertFromMsg(message);
            responseBuffer = ack.toByteBuffer();
            if(log.isTraceEnabled()){
                log.trace(message, "Send the response to CoreEngine");
            }
            super.submit(responseBuffer.getBuffer());
            result = true;

        }
        catch (Exception ex) {
        	if(log.isDebugEnabled()){
        		log.debug(message,ex,ex);
        	}
            stop();
        }

        return result;
    }
    
    private boolean sendReportQueryAck(GmmsMessage message){
        boolean result = false;
        Pdu ack = null;
        TcpByteBuffer responseBuffer = null;
        try {
            ack = new CommandDeliveryReportQueryAck();
            ((CommandDeliveryReportQueryAck)ack).convertFromMsg(message);
            responseBuffer = ack.toByteBuffer();
            if(log.isTraceEnabled()){
                log.trace(message, "Send the DR Query response to Core Engine");
            }
            super.submit(responseBuffer.getBuffer());
            result = true;

        }
        catch (Exception ex) {
        	if(log.isDebugEnabled()){
        		log.debug(message,ex,ex);
        	}
            stop();
        }

        return result;
    }

    private boolean sendSubmitResp(GmmsMessage message) {
        boolean result = false;
        CommandSubmitAck ack = null;
        TcpByteBuffer responseBuffer = null;
        try {
            ack = new CommandSubmitAck();
            ack.convertFromMsg(message,false);
            responseBuffer = ack.toByteBuffer();
            log.trace(message, "Send the response:{} to CoreEngine.",ack.toString());
            super.submit(responseBuffer.getBuffer());
            result = true;

        }
        catch (Exception ex) {
        	if(log.isDebugEnabled()){
        		log.debug(message,ex,ex);
        	}
            stop();
        }

        return result;
    }
    
    /**
     * connection broken
     */
    public void connectionUnavailable() {
    	super.connectionUnavailable();
    	stop();
    }
    /**
     * insert internal session
     * @param moduleName
     * @return
     */
    private synchronized boolean insertSession(String moduleName) {
    	boolean result = false;
        try {
            result = agentConnectionFactory.manageConnection(moduleName, this);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
		if(log.isDebugEnabled()){
			log.debug("processBind result {} with moduleName {}",result,moduleName);
		}
        return result;
    }
    
    /**
     * stop bufferMonitor
     */
    public void stop(){
    	super.stop();
    	if(isServer){
	        if(msgQueue != null){
	        	msgQueue.stopMessageQueue();
	        }
	        
	        if (executorServiceManager != null) {
				executorServiceManager.shutdown(receiverThreadPool);
			}
	        
	        if(connectionInfo != null && agentConnectionFactory != null){
	        	agentConnectionFactory.connectionBroken(connectionInfo.getUserName(),this);
	        }
	        
	        
    	}
    }
    
    @Override
	public void initSenders() {
		int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
		ThreadPoolProfile senderProfile = new ThreadPoolProfileBuilder("IntSenderPool_" + sessionName)
		                                            .poolSize(connectionInfo.getMinSenderNum()).maxPoolSize(connectionInfo.getMaxSenderNum()).needSafeExit(false).build();
		// sender thread pool
		senderThreadPool = executorServiceManager.newExpiredThreadPool(this, "IntSender_" + sessionName, senderProfile, this, queueTimeout);
		
	}
    
    //add by kevin for REST
    private boolean sendDRQueryMessage(GmmsMessage message) {
        boolean result = false;
        TcpByteBuffer submitBuffer = null;

        try {
        	CommandDeliveryReportQuery pdu = new CommandDeliveryReportQuery();
            pdu.convertFromMsgForREST(message);
            submitBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e,e);
            return result;
        }
        if (submitBuffer == null) {
            message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
            return result;
        }

        try {
            super.submit(submitBuffer.getBuffer());
            message.setOutTransID(String.valueOf(System.currentTimeMillis()));
            result = true;
        }
        catch (IOException ex) {
            log.warn(message,
                     "IOException occured when submit msg to Core engine: ",
                     ex);
            message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
            stop();
        }
        return result;
    }
    
    //add by kevin for REST
    private boolean processDeliveryReportQueryAck(GmmsMessage msg, Pdu pdu) {
   
        if(log.isDebugEnabled()){
    		log.debug(msg, "processDeliveryReportQueryAck: {}" , msg.toString());
    	}
    	
        boolean result = false;
        try {
        	msg.setInnerTransaction(transaction);
    		put2OperatorMessageQueue(msg);
        	result = true;
        }
        catch (Exception e) {
            log.error(msg,"msg.getOSsID():{}",msg.getOSsID(), e);
            stop();
        }
        return result;
   }
}
