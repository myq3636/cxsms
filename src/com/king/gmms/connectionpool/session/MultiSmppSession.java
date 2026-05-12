package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.PhoneUtils;
import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerInterface;
import com.king.gmms.connectionpool.sessionthread.ServerSessionThread;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.routing.ADSServerMonitor;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.gmms.ha.systemmanagement.pdu.InBindAck;
import com.king.gmms.ha.systemmanagement.pdu.OutBindAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.smpp.MultiSmppPduProcessor;
import com.king.gmms.protocol.smpp.Smpp;
import com.king.gmms.protocol.smpp.pdu.BindReceiver;
import com.king.gmms.protocol.smpp.pdu.BindRequest;
import com.king.gmms.protocol.smpp.pdu.BindResponse;
import com.king.gmms.protocol.smpp.pdu.BindTransciever;
import com.king.gmms.protocol.smpp.pdu.BindTransmitter;
import com.king.gmms.protocol.smpp.pdu.DeliverSM;
import com.king.gmms.protocol.smpp.pdu.DeliverSMResp;
import com.king.gmms.protocol.smpp.pdu.EnquireLink;
import com.king.gmms.protocol.smpp.pdu.HeaderIncompleteException;
import com.king.gmms.protocol.smpp.pdu.MessageIncompleteException;
import com.king.gmms.protocol.smpp.pdu.Outbind;
import com.king.gmms.protocol.smpp.pdu.PDU;
import com.king.gmms.protocol.smpp.pdu.Request;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.protocol.smpp.pdu.SubmitSM;
import com.king.gmms.protocol.smpp.pdu.SubmitSMResp;
import com.king.gmms.protocol.smpp.pdu.WrongLengthOfStringException;
import com.king.gmms.protocol.smpp.util.Data;
import com.king.gmms.protocol.smpp.util.Unprocessed;
import com.king.gmms.protocol.smpp.version.SMPPVersion;
import com.king.gmms.strategy.StrategyType;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.ExceptionMessageManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.StringUtility;
import com.king.gmms.protocol.smpp.util.SmppByteBuffer;
import com.king.gmms.metrics.SmppPduLogger;


/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class MultiSmppSession extends AbstractCommonSession {
    private static SystemLogger log = SystemLogger.getSystemLogger(MultiSmppSession.class);
    private static final SmppPduLogger pduLogger = SmppPduLogger.getInstance();
    protected String gmmsSystemID = "AicGMMSServer";
    protected Smpp smpp = null;
    protected String ip = null;
    protected BindMode bindOption = BindMode.Transmitter; //option of binding
    protected SMPPVersion smppVersion = SMPPVersion.getDefaultVersion();
    protected String sourceSysID = null;
    protected Integer activeTestCount = 0;
    protected MultiSmppPduProcessor processor;
    protected Unprocessed unprocessed = new Unprocessed();
    protected SystemSession sysSession = null;
    protected SystemSessionFactory sysSessionFactory =null;
    protected SystemPdu msgResponse = null;
    protected boolean isEnableSysMgt = false;
    protected InternalAgentConnectionFactory agentFactory = null;
    protected MessageIdGenerator messageIdGenerator = MessageIdGenerator.getInstance();
    protected ExceptionMessageManager exMsgManager = ExceptionMessageManager.getInstance();
    
    //construct for server
    public MultiSmppSession(Socket socket) {
        super();
        
        // create a single thread pool first, then update after binded
        super.initReceivers();
        
        isServer = true;
        smpp = new Smpp();
        processor = new MultiSmppPduProcessor();
        transaction = new TransactionURI();
        isEnableSysMgt = gmmsUtility.isSystemManageEnable();
        try {
            if (socket != null) {
                createConnection(socket);
            }
            if(isEnableSysMgt){
            	sysSessionFactory = SystemSessionFactory.getInstance();
            	sysSession = sysSessionFactory.getSystemSessionForFunction();
            }
            ip = socket.getInetAddress().getHostAddress();
        }
        catch (Exception ex) {
            log.error(ex, ex);
        }
        
        agentFactory = InternalAgentConnectionFactory.getInstance();
        
        int connectionSilentTime = Integer.parseInt(gmmsUtility.
            getCommonProperty("MaxSilentTime", "600")) * 1000;
        sessionThread = new ServerSessionThread(this, connectionSilentTime);
        
        start();
    }

    //construct for client
    public MultiSmppSession(ConnectionInfo info) {
        super();
        if (info == null) {
            return;
        }
        this.isServer = false;
        setSessionName(info.getConnectionName());
        smpp = new Smpp();
        try {
            isServer = false;
            isEnableSysMgt = gmmsUtility.isSystemManageEnable();
            int buffersize = Integer.parseInt(gmmsUtility.getCommonProperty("ReadBuffersize", "10240"));
            this.setBuffersize(buffersize);
            bindOption = info.getBindMode();
            A2PCustomerManager customerInfoManager = gmmsUtility.getCustomerManager();

            customerInfo = (A2PMultiConnectionInfo) customerInfoManager.
                getCustomerBySSID(info.getSsid());
            if (customerInfo == null) {
                log.error(
                    "Can not get customer information from CCB by the ssid of connection info");
                return;
            }
            smpp.initSmppPara(customerInfo,info);
            intialize(info, customerInfo);
            super.initReceivers();
            transaction = new TransactionURI(connectionInfo.getConnectionName());
            initialVersion();
            processor = new MultiSmppPduProcessor(this);
            startBufferMonitor(customerInfo);
            if(customerInfo.isEnableGuavaBuffer()) {
            	startGuavaCache(customerInfo);
            }            
            if(isEnableSysMgt){
            	sysSessionFactory = SystemSessionFactory.getInstance();
            	sysSession = sysSessionFactory.getSystemSessionForFunction();
            }
            
            agentFactory = InternalAgentConnectionFactory.getInstance();
            
        }
        catch (Exception ex) {
            log.warn("construct MultiSmppSession error!");
        }

    }
    
    protected void initialVersion() {
        if (this.connectionInfo.getVersion() == 33) {
            smppVersion = new com.king.gmms.protocol.smpp.version.SMPPVersion33();
        } else {
            smppVersion = new com.king.gmms.protocol.smpp.version.SMPPVersion34();
        }
    }

    public Smpp getSmpp() {
        return this.smpp;
    }

    public SMPPVersion getSmppVersion() {
        return this.smppVersion;
    }

    public void setSessionThread(SessionThread thread) {
        this.sessionThread = thread;
    }

    public void setSystemID(String systemId) {
        this.gmmsSystemID = systemId;
    }

    public String getSourceSysID() {
        return this.sourceSysID;
    }
    
	public boolean createConnection() throws IOException {

        if (ConnectionStatus.CONNECT.equals(status) ||
            ConnectionStatus.RECOVER.equals(status)) {
        	log.warn("MultiSmppSession is already binded. And the current session number is {}",getSessionNum());
            return true;
        }
        int ssid = customerInfo.getSSID();
        int applySuccess = 0;// applyNewSession();
        if(applySuccess!=0){
        	if(applySuccess==3){
        		log.warn("ssid: {} applyNewSession failed!", ssid);
        	}
        	else if(applySuccess == 2){
        		if(log.isDebugEnabled()){
        			log.debug("ssid: {} The session number reach to max limitation, no need to setup the new session", ssid);
        		}
        	}else{
        		log.warn("ssid: {} System management refuses the connection and now there are {} sessions in connection pool!", ssid,
                        connectionManager.getSessionNum());
        	}
            if(isEnableSysMgt && applySuccess==1){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            return false;
        }
        
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(connectionInfo.getURL(),
					connectionInfo.getPort()), 500);
		} catch (IOException e) {
			if(log.isInfoEnabled()){
				log.info("Connect unsuccessfully with {}:{}", connectionInfo
					.getURL(), connectionInfo.getPort());
			}
			if (socket != null) {
				socket.close();
			}
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
			throw e;
		}
		synchronized (mutex) {
			connection = new TCPIPConnection(socket);
		}
		try{
			connection.setReceiver(this);
			connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
			connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
					TimeUnit.MILLISECONDS);
			connection.setSoTimeout(5, TimeUnit.SECONDS);
			connection.setConnectionName("" + sessionNum);
			if (this.buffersize > 0) {
				connection.setReadBufferSize(buffersize);
			}
			connection.open();
		}catch(IOException ex){
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
			throw ex;
		}catch(Exception e){
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            return false;
		}
		return true;
	}

    /**
     * connect
     *
     * @return boolean
     * @todo Implement this
     *   com.king.gmms.connectionpool.session.AbstractSession method
     */
    public boolean connect() {
    	
        int ssid = customerInfo.getSSID();
        
        try {
            BindRequest request = null;
            BindResponse response = null;
            request = getBindRequest();
            if (request == null) {
                return false;
            }

            request.version = this.smppVersion;

            request.setSystemId(connectionInfo.getUserName());
            request.setPassword(connectionInfo.getPassword());
            String systemType = (customerInfo.getSystemType() == null ? "" :
                                 customerInfo.getSystemType());

            request.setSystemType(systemType);
            request.setInterfaceVersion( (byte)this.smppVersion.getVersionID());
            String addressRange = (customerInfo.getExtProperty("AddressRange") == null ?
                                   "" :
                                   customerInfo.getExtProperty("AddressRange"));
            request.setAddressRange(customerInfo.getAddrTon(),
                                    customerInfo.getAddrNpi(), addressRange);

            log.warn("SMPP Client send a {} bind request to {}.", bindOption,
                     connectionInfo.getUserName());
            if (request != null) {
                ByteBuffer received = connection.sendAndReceive(request.getData().getBuffer());
                if (received != null) {
                    ArrayList list = parsePDU(received);
                    if (list != null && list.size() > 0) {
                        PDU pdu = (PDU) list.get(0);
                        if (pdu != null) {
                            lastActivity = System.currentTimeMillis();
                            if (pdu.getCommandId() == Data.BIND_TRANSCEIVER_RESP
                            		||pdu.getCommandId() == Data.BIND_TRANSMITTER_RESP
                            		||pdu.getCommandId() == Data.BIND_RECEIVER_RESP)
                            {
                            	response = (BindResponse)pdu;
                            }
                        }
                    }
                }
                else {
                    log.warn("Did not receive the Bind Response");
                }
            }
            if (response == null) {
                log.warn("{} session bind request receive a null bindresponse ",this.bindOption);
                if(isEnableSysMgt){
                	this.sysSession.outBindConfirm(false, msgResponse, ssid);
                }
                return false;
            }
            else {
            	log.warn("SMPP Bind {} session's response, and response is {}",this.bindOption,response.toString());
            }
            lastActivity = System.currentTimeMillis();

            int statusCode = response.getCommandStatus();
            if (statusCode == Data.ESME_ROK) { //bind succeed
                sourceSysID = response.getSystemId();
                log.warn("Bind to {} successfully.",connectionInfo.getUserName());
                clearActiveTestCount();
                if(isEnableSysMgt){
                    this.sysSession.outBindConfirm(true, msgResponse, ssid);
                }
                return true;
            }
            else { //bind fail
                log.warn("SMPP bind request REJECTED. Status code: {}", statusCode);
                if(isEnableSysMgt){
                    this.sysSession.outBindConfirm(false, msgResponse, ssid);
                }
                return false; // counter server rejects bind request, don't try it again.
            }
        }
        catch (Exception e) {
            log.error("SMPP session bind fail.",e);
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            return false;
        }
    }

    private BindRequest getBindRequest() {
        switch (this.bindOption) {
            case Transmitter: {
                return new BindTransmitter();
            }
            case Receiver: {
                return new BindReceiver();
            }
            case Transceiver: {
                return new BindTransciever();
            }
            default:
                return null;
        }
    }
    
    /**
     * apply new session</br>
     * @return
     * 0: success </br>
     * 1: apply got response, but statusCode indicate failed or cm.cfg inconsistent </br>
     * 2: session number reach to max limitation </br>
     * 3: apply no response
     */
    protected int applyNewSession(){
/*
        if(isEnableSysMgt){
        	if(connectionInfo.getSessionNum() > 0 && connectionManager != null){
        		Connection connection = connectionManager.getConnection(connectionInfo.getConnectionName());
        		if(connection != null){
    				int sessionNum = connection.getSessionNum();
    				if(sessionNum >= connectionInfo.getSessionNum()){
    					return 2;
    				}
        		}
        	}
        	msgResponse = sysSession.applyNewSession(isServer, connectionInfo, this.transaction);
        	if(msgResponse!=null){
        		int responseCode = -1;
        		if(isServer){
        			responseCode = ((InBindAck)msgResponse).getResponseCode();
        		}else{
        			responseCode = ((OutBindAck)msgResponse).getResponseCode();
        		}
            	return responseCode;
        	}else{
        		return 3;
        	}
        }else{
        	return 0;
        }
*/
        return 0;
    }
    /**
     * connectionUnavailable
     *
     * @todo Implement.gmms.connectionpool.Receiver method
     */
    public void connectionUnavailable() {
        if (!status.equals(ConnectionStatus.RETRY)) {
            stop();
        }
    }


    private void sendEnquireLink(EnquireLink request) throws Exception {
    	if(connection != null){
	    	connection.send(request.getData().getBuffer());
	        if (this.isServer) {
	        		log.trace("Smpp Server Send enquireLink");
	        }
	        else {
	        	if(log.isTraceEnabled()){
	        		log.trace("Send enquireLink of {} session to: {}",this.bindOption ,connectionInfo.getUserName());
	        	}
	        }
    	}
    }

    /**
     * enquireLink
     *
     * @return int
     * @throws Exception
     * @todo Implement this
     *   com.king.gmms.connectionpool.session.AbstractSession method
     */
    public int enquireLink() throws Exception {
        long currentTime = System.currentTimeMillis();
        long temp;
        synchronized (mutex) {
            //if activetest sent, retry time is 60s, else 3M.
            if (activeTestCount > 0) {
            	if(log.isInfoEnabled()){
					log.info("Session({}) does not receive the enquire link response!",sessionName);
            	}
            }
            temp = customerInfo.getEnquireLinkTime();
            if (currentTime - lastActivity >= temp ||(customerInfo.isKeepEnquireLink()&& currentTime-eqLastActivity>=temp)) { //need to send active test
                try {
                    EnquireLink request = new EnquireLink();
                    sendEnquireLink(request);
                    activeTestCount++;
                }
                catch (Exception e) {
                    log.error("Failed to send active test.", e);
                    stop();
                    throw e;
                }

            }
        }
        return activeTestCount;
    }

    /**
     * receive
     *
     * @param obj Object
     * @param exceptionFlag boolean
     * @return boolean
     * @todo Implement.gmms.connectionpool.Receiver method
     */
    public boolean receive(Object obj) {
        if (obj == null) {
            return true;
        }
        if(log.isTraceEnabled()){
            log.trace("Smpp session start at: {}", System.currentTimeMillis());
        }
        PDU pdu = (PDU) obj;
        // Log received PDU to dedicated SMPP PDU log file
        pduLogger.logReceived(isServer ? "server" : "client", sessionName, pdu);
        if(log.isTraceEnabled()){
            log.trace("Receive a pdu:{}", pdu.toString());
        }
        if (pdu.isRequest()) {
            processRequest( (Request) pdu);
        }
        else if (pdu.isResponse()) {
            processResponse( (Response) pdu);

        }
        else {
            log.warn("CommandID: {}", Integer.toString(pdu.getCommandId()));
            log.warn("SMPP processing a PDU with a command id . We can't handle it.");
        }
        return false;
    }

    private void processRequest(Request request) {
        if (this.isServer) {
            processServerRequest(request);
        }
        else {
            processClientRequest(request);
        }
    }

    private void processServerRequest(Request request) {
        int commandId = request.getCommandId();
        if (getStatus().equals(ConnectionStatus.INITIAL) ||
            getStatus().equals(ConnectionStatus.DISCONNECT)) {

            if (commandId == Data.BIND_TRANSMITTER ||
                commandId == Data.BIND_RECEIVER ||
                commandId == Data.BIND_TRANSCEIVER) { // smpp server receive bind request
                if (commandId == Data.BIND_RECEIVER) {
                    bindOption = BindMode.Receiver;
                }
                else if (commandId == Data.BIND_TRANSMITTER) {
                    bindOption = BindMode.Transmitter;
                }
                else {
                    bindOption = BindMode.Transceiver;
                }
                if (!handleBindRequest(request)) {
                    this.stop();
                }
            } //end of if bind request
            else if (commandId == Data.OUTBIND) { // smpp server receive outbind request
                handleOutBindRequest(request);
            }
            else { // got a non-bind request
                log.warn("SMPP Listener reject an non-bind request before session binded. Sequence={}" ,
                         request.getSequenceNumber());
                if (request.canResponse()) { // if reqeust responsable, send response to reject
                    Response response = request.getResponse();
                    response.setCommandStatus(Data.ESME_RINVBNDSTS); //incorrect BIND status for given command
                    this.respond4Bind(response);
                } // else: request not respondable, silently discard.
                stop();
            }

        }//end of if initial or disconnect
        else { // already bound, can receive other PDUs
                request.setVersion(smppVersion);
                GmmsMessage msg = null;
                if (commandId == Data.SUBMIT_SM) {
                	if(checkThrottlingControl(request)){
                		if(log.isInfoEnabled()){
                        	log.info("Request PDU is blocked by throttling control, and sequence number is {}", request.getSequenceNumber());
                		}
                        return;
                	}                
                	
                	msg = processor.handleSubmitSM(request);
                	
                	// V4.0 Sticky Routing: Tag message with source session context
                	if (msg != null) {
                		msg.setInnerTransaction(transaction);
                	}

                	if(log.isInfoEnabled()){
    					log.info(msg, "Smpp receive message :{}",msg);
                	}
                	String msgType = msg.getMessageType();
                	if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msgType) ||
                            GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msgType)) {
                        	if(customerInfo.isSmsOptionChargeByGateWay()){
                            try {
                            	Long stock = gmmsUtility.getRedisClient().stock("stock:"+customerInfo.getSSID(), 1);
          					     if (stock==null || stock<0) {
          					    	 if(log.isInfoEnabled()){
          		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
          		                		}					    	 					    	
          					    	 Response response = request.getResponse();
     				                 response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RPROVNOTALLWD);
     				                 connection.send(response.getData().getBuffer()); 
          							 return;
          						 }
							} catch (Exception e) {
								log.warn(msg,"Fail to send response");
								return;
							}       					     
                       	  }
                        	if(customerInfo.isSmsOptionChargeCountryByGateWay()){
                                try {
                                	String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
                                	Long stock = gmmsUtility.getRedisClient().stock("stock:"+customerInfo.getSSID()+":"+cc, 1);
              					     if (stock!=null && stock<0) {
              					    	 if(log.isInfoEnabled()){
              		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
              		                		}					    	 					    	
              					    	 Response response = request.getResponse();
         				                 response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RPROVNOTALLWD);
         				                 connection.send(response.getData().getBuffer()); 
              							 return;
              						 }
    							} catch (Exception e) {
    								log.warn(msg,"Fail to send response");
    								return;
    							}       					     
                           	  }
                        
                        }
                	
                	// V4.0 异步化：将消息提交到 Redis Stream (Submit-MQ)
                    boolean produceSuccess = StreamQueueManager.getInstance().produceSubmitMessage(msg);
                    if(!produceSuccess){
                        log.error(msg, "Failed to produce SUBMIT_SM to Redis Stream!");
                        Response response = request.getResponse();
                        try{
	                        response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RSYSERR);
	                        connection.send(response.getData().getBuffer());
                        }catch(Exception e){
                        	log.warn(msg,"Fail to send error response");
                        }
                    } else {
                        // V4.0 异步化：入队成功后立即向 ESME 确认
                        Response response = request.getResponse();
                        try {
                            if (response instanceof SubmitSMResp) {
                                ((SubmitSMResp) response).setMessageId(msg.getMsgID());
                            }
                            response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_ROK);
                            connection.send(response.getData().getBuffer());
                            if(log.isInfoEnabled()){
                                log.info(msg, "SUBMIT_SM accepted asynchronously, MsgID: {}", msg.getMsgID());
                            }
                        } catch (Exception e) {
                            log.warn(msg, "Fail to send success response");
                        }
                    }
                }
                else if (commandId == Data.DELIVER_SM) {
                	if(checkThrottlingControl(request)){
                		if(log.isInfoEnabled()){
                			log.info("Request PDU is blocked by throttling control, and sequence number is {}", request.getSequenceNumber());
                		}
                        return;
                	}
                	msg = processor.handleDeliverSM(request);

                	// V4.0 Sticky Routing: Tag message with source session context
                	if (msg != null) {
                		msg.setInnerTransaction(transaction);
                	}

                	if(log.isInfoEnabled()){
                		log.info(msg, "Smpp receive message :{}",msg);
                	}
                	
                	String msgType = msg.getMessageType();

                	if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msgType) ||
                            GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msgType)) {
                        	if(customerInfo.isSmsOptionChargeByGateWay()){
                            try {
                            	Long stock = gmmsUtility.getRedisClient().stock("stock:"+customerInfo.getSSID(), 1);
          					     if (stock == null || stock<0) {
          					    	 if(log.isInfoEnabled()){
          		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
          		                		}					    	 					    	
          					    	 Response response = request.getResponse();
     				                 response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RPROVNOTALLWD);
     				                 connection.send(response.getData().getBuffer()); 
          							 return;
          						 }
							} catch (Exception e) {
								log.warn(msg,"Fail to send response");
								return;
							}       					     
                       	  }
                        	
                    	if(customerInfo.isSmsOptionChargeCountryByGateWay()){
                            try {
                            	String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
                            	Long stock = gmmsUtility.getRedisClient().stock("stock:"+customerInfo.getSSID()+":"+cc, 1);
          					     if (stock!=null && stock<0) {
          					    	 if(log.isInfoEnabled()){
          		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
          		                		}					    	 					    	
          					    	 Response response = request.getResponse();
     				                 response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RPROVNOTALLWD);
     				                 connection.send(response.getData().getBuffer()); 
          							 return;
          						 }
							} catch (Exception e) {
								log.warn(msg,"Fail to send response");
								return;
							}       					     
                       	  }
                        }
                	
                	// V4.0 异步化：将消息提交到 Redis Stream (Submit-MQ)
                    boolean produceSuccess = StreamQueueManager.getInstance().produceSubmitMessage(msg);
                    if(!produceSuccess){
                        log.error(msg, "Failed to produce DELIVER_SM to Redis Stream!");
                        Response response = request.getResponse();
                        try{
	                        response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RSYSERR);
	                        connection.send(response.getData().getBuffer());
                        }catch(Exception e){
                        	log.warn(msg,"Fail to send error response");
                        }
                    } else {
                        // V4.0 异步化：入队成功后立即向 ESME 确认
                        Response response = request.getResponse();
                        try {
                            // DELIVER_SM_RESP usually doesn't have a mandatory message_id field but some systems use it
                            response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_ROK);
                            connection.send(response.getData().getBuffer());
                            if(log.isInfoEnabled()){
                                log.info(msg, "DELIVER_SM accepted asynchronously, MsgID: {}", msg.getMsgID());
                            }
                        } catch (Exception e) {
                            log.warn(msg, "Fail to send success response");
                        }
                    }
                }
                else if (commandId == Data.UNBIND) {
                    // unbind causes stopping of the session
                	log.warn("unbind causes stopping of the session");
                    stop();
                }
                else if(commandId == Data.ENQUIRE_LINK){
                	handleEnquire_Link(request);
                }                
                else { // all other Requests that we can't handle
                    // send system error back.
                    Response response = request.getResponse();
                    response.setCommandStatus(Data.ESME_RSYSERR);
                    if(log.isInfoEnabled()){
    					log.info("Unknow CommandId, Just response it.Sequence={}", request.getSequenceNumber());
                    }
                }
            }
    }
    /**
     * put message to Router queue
     * @param msg
     */
    private boolean putGmmsMessage2RouterQueue(GmmsMessage msg){
    	if(msg == null){
    		return false;
    	}
    	ModuleManager moduleManager = ModuleManager.getInstance();
    	String routerQueue = null;
    	OperatorMessageQueue msgQueue = null;
    	String moduleName = null;
    	ModuleURI moduleURI = transaction.getModule();
		if(moduleURI!=null){
			moduleName = moduleURI.getModule();
		}
		String deliverChannelQueue = null;
    	if(GmmsMessage.MSG_TYPE_INNER_ACK.equalsIgnoreCase(msg.getMessageType())
    		||GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())
    		||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())){
    		TransactionURI innerTransaction = msg.getInnerTransaction();
    		if(innerTransaction == null){
    			if(log.isInfoEnabled()){
					log.info(msg,"Cannot get the inner transaction");
    			}
				return false;
    		}
    		routerQueue = innerTransaction.getConnectionName();  
    		deliverChannelQueue = routerQueue;
    		msgQueue = agentFactory.getMessageQueue(msg, routerQueue);
    	}else{
        	routerQueue = moduleManager.selectRouter(msg);
        	msgQueue = agentFactory.getMessageQueue(msg, routerQueue);
        	deliverChannelQueue = routerQueue;
        	if(msgQueue == null){
            	String aliveRouterQueue = moduleManager.selectAliveRouter(routerQueue,msg);
            	msgQueue = agentFactory.getMessageQueue(msg, aliveRouterQueue);
            	if(msgQueue == null){
            		ArrayList<String> failedRouters = new ArrayList<String>();
            		failedRouters.add(routerQueue);
            		failedRouters.add(aliveRouterQueue);
            		aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            		while(aliveRouterQueue != null){
            			msgQueue = agentFactory.getMessageQueue(msg, aliveRouterQueue);
            			if(msgQueue == null){
            				failedRouters.add(aliveRouterQueue);
            				aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            			}else{
            				break;
            			}
            		}
            	}
            	deliverChannelQueue = aliveRouterQueue;
            	if(log.isDebugEnabled()){
            		log.debug(msg,"putGmmsMessage2RouterQueue aliveRouterQueue={}",aliveRouterQueue);
            	}
        	}
    	}
    	
		/*
		 * if(log.isDebugEnabled()){
		 * log.debug(msg,"putGmmsMessage2RouterQueue transaction={};msgType={}"
		 * ,transaction,msg.getMessageType()); }
		 */
    	if(msgQueue == null){
    		if(log.isInfoEnabled()){
				log.info(msg,"Can not find the alive delivery router");
    		}
    		msg.setDeliveryChannel(moduleName);
    		return false;
    	}else{
    		msg.setDeliveryChannel(moduleName+":"+deliverChannelQueue);
    		if(log.isInfoEnabled()){
    			log.info(msg,"Send {} to {}",msg.getMessageType(),routerQueue);
        	}
    		return msgQueue.putMsg(msg);
    	}
    }
    /**
     * handle outbind request
     *
     * @param request Request
     */
    public void handleOutBindRequest(Request request) {
        int status = smpp.checkOutBindRequest((Outbind) request);
        if (status != Data.ESME_ROK) {
            log.info("SMPP Listener got a error outbind request. Reject it.");
            stop(); // outbind failed, reject it.
        }
        else if(checkIdentity((Outbind) request)) {
            setStatus(ConnectionStatus.CONNECT);
            BindReceiver bindRecReq = new BindReceiver();
            serverRequest(bindRecReq);
        }
        else{
        	log.info("SMPP Listener got a error outbind request. Reject it.");
            stop();
        }

    }
    
    private static volatile String cachedThcon = "1";
    private static volatile long lastThconCheckTime = 0;

    private String getThconStatus() {
        long now = System.currentTimeMillis();
        if (now - lastThconCheckTime > 50000) {
            try {
                String value = GmmsUtility.getInstance().getRedisClient().getString("thcon");
                if (value != null) {
                    cachedThcon = value;
                } else {
                    cachedThcon = "";
                }
                lastThconCheckTime = now;
            } catch (Exception e) {
                // Ignore transient Redis errors; reuse last cached value
            }
        }
        return cachedThcon;
    }
    
    private boolean checkThrottlingControl(Request request){
        try {
            if (customerInfo != null) {
            	if(!StringUtility.stringIsNotEmpty(getThconStatus())){
            		Response response = request.getResponse();
                	response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RTHROTTLED);
                	connection.send(response.getData().getBuffer());
                	return true;
            	}
                if (!ThrottlingControl.getInstance().isAllowedToReceive(customerInfo.getSSID())) {
                	Response response = request.getResponse();
                	response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RTHROTTLED);
                	connection.send(response.getData().getBuffer());
                	return true;
                }
            }
        }
        catch (Exception e) {
            log.warn(
                "when processing throttling control with SubmitSM occur error",
                e);
        }
        return false;
    }

    private void serverRequest(Request request) {
        try {
        	connection.send(request.getData().getBuffer());
        }
        catch (Exception e) {
            if (connectionInfo != null && connectionInfo.getUserName() != null) {
                log.warn("The session from {} has been broken, stop it!",connectionInfo.getUserName(), e);
            }
            else {
                log.warn("The session has been broken, stop it!", e);
            }
            stop();
        }

    }

    private boolean checkIdentity(Outbind request){

        ConnectionInfo conn = this.connectionInfo;
        A2PMultiConnectionInfo cusInfo = null;
        A2PCustomerManager customerInfoManager = gmmsUtility.
                getCustomerManager();
        if(conn == null){

            ArrayList infos = customerInfoManager.getConnectionInfobyServerInfo(
                request.getSystemId(), null, ip);
            int i = 0;
            if (infos != null && infos.size() > 0) {
                for (; i < infos.size(); i++) {
                    conn = (ConnectionInfo) infos.get(i);
                    cusInfo = (A2PMultiConnectionInfo) customerInfoManager.
                        getCustomerBySSID(conn.getSsid());
                    if (cusInfo == null)
                        continue;

                    if (conn.getPassword().equals(request.getPassword())) {
                        this.connectionInfo = conn;
                        this.customerInfo = cusInfo;
                        return true;
                    }
                }
            }

        }
        else {
            cusInfo = (A2PMultiConnectionInfo) customerInfoManager.
                getCustomerBySSID(conn.getSsid());
            if (cusInfo == null)
                return false;
            if (conn.getPassword().equals(request.getPassword())) {
                this.connectionInfo = conn;
                this.customerInfo = cusInfo;
                return true;
            }

        } //end of else
        return false;

    }


    private boolean handleBindRequest(Request request) {
        boolean result = false;
        BindResponse bindResponse = null;
        ConnectionInfo connInfo = null;
        byte btVer = ( (BindRequest) request).getInterfaceVersion();
        String sysid = ( (BindRequest) request).getSystemId();
        int status = smpp.checkBindRequest( (BindRequest) request);
        if (status != Data.ESME_ROK) {
            // if have invalid fields
        	log.warn("SystemId or Password is absent in BindRequest,Sequence={}",
                        request.getSequenceNumber());
            bindResponse = (BindResponse) ( (BindRequest) request).getResponse();
            bindResponse.setCommandStatus(status);
            try {
                bindResponse.setSystemId(sysid);
            }
            catch (WrongLengthOfStringException e) {
                log.error("King System ID Length is invalid.", e);
            }
        }
        else {
            bindResponse = (BindResponse) request.getResponse();
            A2PCustomerManager customerInfoManager = gmmsUtility.getCustomerManager();
            A2PMultiConnectionInfo cusInfo = null;
            
            String passwd = ( (BindRequest) request).getPassword();
            //add by bruce for check version

            ArrayList infos = customerInfoManager.getConnectionInfobyServerInfo(
                sysid, null, ip);
            if (infos != null && infos.size() > 0) {
        		if(log.isDebugEnabled()){
        			log.debug("Get Connection info list and list size is {}" ,
                            infos.size());
        		}
                try {
                    int i = 0;
                    for (; i < infos.size(); i++) {

                        connInfo = (ConnectionInfo) infos.get(i);
                        cusInfo = (A2PMultiConnectionInfo) customerInfoManager.
                            getCustomerBySSID(connInfo.getSsid());
                        if (cusInfo == null){
                            continue;
                        }
                        //if the customerInfo is SingleConnection then do not check password,version and conninfo
                        if (!connInfo.getPassword().equals(passwd)) {
                            bindResponse.setCommandStatus(Data.ESME_RINVPASWD);
                            continue;
                        }

                        if (!checkVersion(btVer, connInfo)) {
                            bindResponse.setCommandStatus(Data.ESME_RBINDFAIL);
                            log.warn("The Bind({}) version is not consistent with customer configuration, and Bind unsuccessfully!", sysid);
                            continue;
                        }

                        if (cusInfo.getConnectionType() != 1) {
                            if (!checkConnInfo(connInfo)) {
                                bindResponse.setCommandStatus(Data.ESME_RBINDFAIL);
                                log.warn("The Bind({}) mode is not consistent with customer configuration, and Bind unsuccessfully!", sysid);
                                continue;
                            }
                        }

                        MultiSmppServerFactory sessionFactory = MultiSmppServerFactory.getInstance();

                        String name = null;
                        if (cusInfo.getConnectionType() == 3) {
                            name = customerInfoManager.getNodeIDByConnectionID(connInfo.
                                getConnectionName());
                        }
                        else {
                            name = connInfo.getConnectionName();
                        }
                        if (name != null) {
                            connectionManager = sessionFactory.
                                getConnectionManager(connInfo.
                                getSsid(),
                                name);
                        }
                        else {
                            log.warn(
                                "Do not get the node name by " + connInfo.
                                getConnectionName() +
                                ", and Bind unsuccessfully!");
                            bindResponse.setCommandStatus(Data.ESME_RBINDFAIL);
                            break;

                        }
                        if (connectionManager == null) {
                            log.warn(
                                "Do not get the connection manager by " +
                                connInfo.
                                getSsid() + "_" + name +
                                ", and Bind unsuccessfully!");
                            bindResponse.setCommandStatus(Data.ESME_RBINDFAIL);
                            break;
                        }

                        if (cusInfo.getConnectionType() == 1) {
                            ConnectionInfo newConnInfo = new ConnectionInfo(
                                connInfo);
                            updateConnInfo(newConnInfo);

                            if (!connectionManager.contain(newConnInfo.
                                                           getConnectionName())) {

                                com.king.gmms.connectionpool.connection.
                                    Connection conn = new
                                    MultiServerConnectionImpl(true);
                                conn.initialize(newConnInfo);
                                connectionManager.insertConnection(conn);
                                ( (SingleNodeCustomerInfo) cusInfo).addIncomingConnection(newConnInfo.
                                    getConnectionName(), newConnInfo);

                            }
                            setSessionName(newConnInfo.getConnectionName());
                            intialize(newConnInfo, cusInfo);
                            transaction.setConnectionName(newConnInfo.getConnectionName());

                        }else{
                            setSessionName(connInfo.getConnectionName());
                            intialize(connInfo, cusInfo);
                            transaction.setConnectionName(connInfo.getConnectionName());
                        }
                        if (!connectionManager.insertSession(this.connectionInfo.
                            getConnectionName(), this)) {
                            bindResponse.setCommandStatus(Data.ESME_RBINDFAIL);
                            setStatus(ConnectionStatus.DISCONNECT);
                            log.warn("Server refuses the connection[{}] and now there are {} sessions in connection pool!",this.connectionInfo.
                                    getConnectionName(), connectionManager.getSessionNum());
                            break;
                        }
                        else {
                            initialVersion();
                            this.sourceSysID = sysid;
                            this.processor.init(this);
                            if (customerInfo.getConnectionType() == 3) {
                                String type = ((MultiNodeCustomerInfo)customerInfo).getSubmitNodePolicy();
                                if (! ( (NodeConnectionManagerInterface)
                                       connectionManager).getNodeStatus(
                                           connectionInfo.getConnectionName())
                                    && StrategyType.getStrategyType(type).equals(StrategyType.Primary)) {
                                	log.warn("The session({}) status is set:  RECOVER and the original status is DISCONNECT",getSessionName());
                                    setStatus(ConnectionStatus.RECOVER);

                                } //end of
                                else {
                                    setStatus(ConnectionStatus.CONNECT);
                                }

                            }
                            else {
                                bindResponse.setCommandStatus(Data.ESME_ROK);
                                respond4Bind(bindResponse);
                                setStatus(ConnectionStatus.CONNECT);
                                
                                // V4.0 Async Routing: Start listening for DRs for this SSID on this node
                                com.king.gmms.messagequeue.DRStreamConsumer.getInstance().registerSSID(this.connectionInfo.getSsid());
                                
                                i = infos.size();
                                break;
                            }
                            smpp.initSmppPara(cusInfo,this.connectionInfo);
                            startBufferMonitor(customerInfo);
                            updateReceivers();
                            result = true;
                            break;
                        }

                    } //end of for loop
                }//end of try
                catch (Exception e) {
                    log.error(e, e);
                }
            }//end of if has conninfo
            else {
                bindResponse.setCommandStatus(Data.ESME_RINVSYSID);
            }
        }//end of else

        if (btVer > 0x33 && this.smppVersion != null) {
            bindResponse.setScInterfaceVersion( (byte) smppVersion.
                                               getVersionID());
        }
        try {
            bindResponse.setSystemId(sysid);
        }
        catch (WrongLengthOfStringException e) {
            log.warn("King System ID Length is invalid.", e);
        }
        this.respond4Bind(bindResponse);
        if(this.isEnableSysMgt && (Data.ESME_ROK == bindResponse.getCommandStatus())){
           this.sysSession.inBindConfirm(transaction,connInfo.getSsid(), 0);
        }
        return result;
    }

    private void updateConnInfo(ConnectionInfo connInfo){
        String conName = SystemConstants.SINGLE_CONNECTION_NAME;
        switch(this.bindOption){
            case Receiver: {
                connInfo.setBindMode(BindMode.Transmitter);
                connInfo.setConnectionName(conName);
                break;
            }
            case Transmitter:{
                connInfo.setBindMode(BindMode.Receiver);
                connInfo.setConnectionName(conName+"_R");
                break;
            }
            case Transceiver:{
                connInfo.setBindMode(BindMode.Transceiver);
                connInfo.setConnectionName(conName);
                break;
            }
        }
    }

    private boolean checkConnInfo(ConnectionInfo connInfo){
        boolean result = false;
        switch(this.bindOption){
            case Receiver:
                if(connInfo.getBindMode().toString().equalsIgnoreCase("t")){
                    result = true;
                }
                break;
            case Transmitter:
                if(connInfo.getBindMode().toString().equalsIgnoreCase("r")){
                    result = true;
                }
                break;
            case Transceiver:
                if(connInfo.getBindMode().toString().equalsIgnoreCase("tr")){
                    result = true;
                }
                break;
        }

        return result;
    }

    private boolean checkVersion(byte btVer, ConnectionInfo info) {
        boolean result = false;

        if (btVer <= 0x33) {
            btVer = 0x33;
        }
        else {
            btVer = 0x34;
        }
        int version = Integer.parseInt(Integer.toString(info.getVersion()),16);

        if (btVer == version)
            result = true;

        return result;
    }

    private void processClientRequest(Request request) {
        int commandId = request.getCommandId();
        if (commandId == Data.ENQUIRE_LINK) {
            handleEnquire_Link(request);
        }
        else if (commandId == Data.DELIVER_SM) {
        	if(checkThrottlingControl(request)){
        		if(log.isInfoEnabled()){
					log.info("Request PDU is blocked by throttling control, and sequence number is {}", request.getSequenceNumber());
        		}
        		return;
        	}
        	if(log.isTraceEnabled()){
        		log.trace("processClientRequest request:{};transaction={}",request.debugString(),transaction);
        	}
        	GmmsMessage msg = processor.handleDeliver_SM4Client(request);
            if(!putGmmsMessage2RouterQueue(msg)){
                Response response = request.getResponse();
                try{
                    response.setCommandStatus(com.king.gmms.protocol.smpp.util.Data.ESME_RX_T_APPN);
                    connection.send(response.getData().getBuffer());
                }catch(Exception e){
                	log.warn(msg,"Fail to send response");
                }
                if(log.isInfoEnabled()){
					log.info(msg,"send response ok.");
                }
            }else{
            	//send response with out wait to dr response from core
            	GmmsMessage respMsg = new GmmsMessage(msg);
            	if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())){
            		respMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            	}            	
            	sendDeliverSMRespWithoutInnerack(respMsg);
            }
        }
    }

    private void handleEnquire_Link(PDU pdu) {
        EnquireLink request = (EnquireLink) pdu;
        Response response = request.getResponse();
        if (response != null) {
            try {
            	connection.send(response.getData().getBuffer());
            }
            catch (IOException e) {
                log.error(
                    "An error occured while SMPP Client tried to send a EnquireLinkResp.",
                    e);
                stop();
            }
            catch (Exception e) {
                log.error(
                    "An error occured while SMPP Client tried to send a EnquireLinkResp.",
                    e);
            }
        }

    }

    public void clearActiveTestCount() {
        synchronized (mutex) {
            activeTestCount = 0;
            eqLastActivity = System.currentTimeMillis();
        }
    }


    private void processResponse(Response response) {
        int commandId = response.getCommandId();
        if (commandId == Data.ENQUIRE_LINK_RESP) {
        	if(log.isTraceEnabled()){
        		log.trace("SmppClient receives alive response:{}", response);
        	}
            clearActiveTestCount();
        }
        else if (commandId == Data.BIND_RECEIVER_RESP) {
            // ESME will send a deliverSM to SMPP server
        }
        else if (commandId == Data.SUBMIT_SM_RESP) {
            handleSubmit_SM_Resp(response);
        }
        else if (commandId == Data.DELIVER_SM_RESP) {
            handleDeliver_SM_Resp(response);
        }
        else {
            log.warn("CommandID: {}", Integer.toString(response.getCommandId()));
            log.warn(
                "SMPP processing a PDU with a command id . We can't handle it.");
        }
    }

    private void handleDeliver_SM_Resp(Response response) {
        Integer sequence = response.getSequenceNumber();
        GmmsMessage bufferedMsg = bufferMonitor.remove(Integer.toString(sequence));
        if (bufferedMsg != null) {
            processor.handleDeliver_SM_Resp( (DeliverSMResp) response,
                                            bufferedMsg);
            if(!putGmmsMessage2RouterQueue(bufferedMsg)){
            	log.warn("Can't put the message into message queue of agent, sequence={}" ,
                        Integer.toString(sequence));
            }
        }
        else {
            log.warn(
                "Can't find the original message from bufferMonitor, sequence={}",
                Integer.toString(sequence));
          //no matter in_dr_response or out_submit_response, log sequenceNum into OutTransID, ssid into rssid.
            GmmsMessage msg = new GmmsMessage();
            msg.setOutTransID(Integer.toString(sequence)); 
            msg.setMessageType("response");
            msg.setRSsID(this.customerInfo.getSSID());
            String dc_Name = this.customerInfo.getShortName();
            msg.setStatusCode(response.getCommandStatus());
            
            String outMsgID = null;
            SMPPVersion ver = this.smppVersion;
            if (ver != null && ver.getVersionID() == 0x33) {
          	  outMsgID = ( (DeliverSMResp) response).getMessageId();
                msg.setOutMsgID(outMsgID);
            }
            exMsgManager.insertExceptionMessage(msg,dc_Name);
        }
    }

    private void handleSubmit_SM_Resp(Response response) {
        Integer sequence = response.getSequenceNumber();
        GmmsMessage bufferedMsg = null;
        if(customerInfo.isEnableGuavaBuffer()) {
        	bufferedMsg = guavaCache.get(Integer.toString(sequence));
        }else {
        	bufferedMsg = bufferMonitor.remove(Integer.toString(sequence));
        }
        if (bufferedMsg != null) {
            processor.handleSubmit_SM_Resp( (SubmitSMResp) response,
                                           bufferedMsg);
            if(!putGmmsMessage2RouterQueue(bufferedMsg)){
            	log.warn("Can't put the message into message queue of agent, sequence={}" ,
                        Integer.toString(sequence));
            }
        } else {
            log.warn("Can't find the original message from bufferMonitor, sequence={}",
                Integer.toString(sequence));
            
            GmmsMessage msg = new GmmsMessage();
            //no matter in_dr_response or out_submit_response, log sequenceNum into OutTransID, ssid into rssid.
            msg.setOutTransID(Integer.toString(sequence)); 
            msg.setMessageType("response");
            msg.setRSsID(this.customerInfo.getSSID());            
            String dc_Name = this.customerInfo.getShortName();
            msg.setStatusCode(response.getCommandStatus());
            SubmitSMResp resp = (SubmitSMResp) response;
            
            String outMsgID = null;
            if (resp.getMessageId() != null && resp.getMessageId().length() > 0) {
            	outMsgID = resp.getMessageId();
                try {
                    if (this.smppVersion.getVersionID() == 52) {
                        if (this.customerInfo.isChlSMPPMsgIDParse()) {
                            outMsgID = Long.toString(Long.valueOf(outMsgID, 16));
                        }
                        if (this.customerInfo.getSMPPIsPadZero4SR()) {
                            int lenDiff = 10 - outMsgID.length();
                            if (lenDiff > 0) {
                                for (int i = 0; i < lenDiff; i++) {
                                    outMsgID = "0" + outMsgID;
                                }
                            }
                        }
                    }
                }
                catch (Exception ex) {
                    log.error(ex, ex);
                }
                msg.setOutMsgID(outMsgID);
            }
            exMsgManager.insertExceptionMessage(msg,dc_Name);
        }
    }

    @Override
    public void stop() {
        synchronized (mutex) {
            if (keepRunning == false) {
                return;
            }
            try {
                if(ConnectionStatus.CONNECT.equals(status)||ConnectionStatus.RECOVER.equals(status)){
                	if(log.isInfoEnabled()){
                		log.info("SMSCSession stopping and TransactionURI is:{}" , getTransactionURI());
                	}
                	if(this.isEnableSysMgt){
                		if(connectionInfo == null){
                			if(log.isDebugEnabled()){
                        		log.debug("connectionInfo==null when stopSession.");
                			}
                    	}else{
                    		this.sysSession.clearSession(this.connectionInfo,this.transaction);
                    	}
                	}
                }
                
                if(isServer){
                	if (executorServiceManager != null) {
        				executorServiceManager.shutdown(receiverThreadPool);
                    }
                	
                	if(bufferMonitor != null){
	                	bufferMonitor.sendbackBuffer();
	                	bufferMonitor.stopMonitor();
	                	bufferMonitor = null;
                	}
                	
                	if(guavaCache !=null) {
                		guavaCache.shutdown();
                	}
                }
                
            	if (sessionThread != null) {
                    sessionThread.stopThread();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
                
            }
            catch (Exception ex) {
                log.warn(ex, ex);
            }
        }
    }
    /**
     * destroy sessionThread
     */
    public void destroy() {
    	synchronized (mutex) {
            if (keepRunning == false) {
                return;
            }
            keepRunning = false;
            try {
            	if(this.isEnableSysMgt 
                		&& (ConnectionStatus.CONNECT.equals(status)
                			||ConnectionStatus.RETRY.equals(status)
                			||ConnectionStatus.RECOVER.equals(status))){
            		if(connectionInfo == null){
            			if(log.isDebugEnabled()){
                    		log.debug("connectionInfo==null when stopSession.");
            			}
                	}else{
                		this.sysSession.clearSession(this.connectionInfo,this.transaction);
                	}
            	}
            	
            	if (executorServiceManager != null) {
    				executorServiceManager.shutdown(receiverThreadPool);
    			}
            	
            	if (sessionThread != null) {
                    sessionThread.stopThread();
                    sessionThread.interrupt();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            	if(bufferMonitor != null){
                	bufferMonitor.sendbackBuffer();
                	bufferMonitor.stopMonitor();
            	}
            	if(guavaCache !=null) {
            		guavaCache.shutdown();
            	}
                if(log.isInfoEnabled()){
					log.info("SMSCSession stopping and name is:{}" , this.getSessionName());
                }
            }
            catch (Exception ex) {
                log.warn(ex, ex);
            }
        }
	}
    /**
     * submit
     *
     * @param msg GmmsMessage
     * @return boolean
     * @throws IOException
     * @todo Implement.gmms.connectionpool.session.Session
     * method
     */
    public boolean submit(GmmsMessage msg) throws IOException {
        if(this.isServer){
            return serverSubmit(msg);
        }else{
            return clientSubmit(msg);
        }

    }

    private boolean clientSubmit(GmmsMessage msg)throws IOException{
        if (msg == null) { //message is null
            return false;
        }
        String messageType = msg.getMessageType();
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType) ||
            GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)) {
            return clientSendSubmitSM(msg);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) { 
        	//it's a deliveryReport request
            return clientSendDR(msg);
        } else if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(messageType)
        		  || GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(messageType)) {
        	return sendDeliverSMResp(msg);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(messageType)) { 
        	if(log.isInfoEnabled()){
				log.info(msg, "SMPP does not support to query delivery report");
        	}
            return true;
        }
        return false;
    }


    private boolean clientSendDR(GmmsMessage msg) throws IOException {

        boolean result = false;
        String key = null;
        //create smpp SubmitSM for delivery report
        SubmitSM request = smpp.createSmppSubmitSM4dr(msg, customerInfo);
        if(request == null){
        	return false;
        }
        try {
            request.assignSequenceNumber();

            if (gmmsUtility.getCustomerManager().getTransparency(msg.getOSsID()) > 0) {
                smpp.addMncMccForRequest(msg, request);
            }
            
            key = Integer.toString(request.getSequenceNumber());
            while (isKeepRunning() && !bufferMonitor.put(key, msg))
                ;
            if(isKeepRunning()){
                connection.send(request.getData().getBuffer());
                pduLogger.logSent(isServer ? "server" : "client", sessionName, request);
                if(log.isInfoEnabled()){
					log.info(msg,"send dr, inTransID {}", key);
                }
                result = true;
            }
            else{
                log.info(msg,"connection is broken.");
                result = false;
            }

        }
        catch (IOException e) {
            log.error(msg, e, e);
            stop();
            result = false;
            throw e;
        }
        catch (Exception e) {
            log.error(msg, e, e);
            result = false;

        }finally{
            if(!result){
                try {
                    bufferMonitor.remove(key);
                }
                catch (Exception ex) {

                }
            }
        }
        return result;

    }
    
    private boolean clientSendSubmitSM(GmmsMessage msg)throws IOException{

        boolean result = false;
        String key = null;
        boolean isEnableGuavaBuffer = customerInfo.isEnableGuavaBuffer();
        try {
            SubmitSM request = null;
            // ***** try to create a SMPP SubmitSM PDU *****

            request = smpp.createSmppSubmitSM(msg, this.customerInfo);

            if (request == null) {
                return result;
            }
            
            if (gmmsUtility.getCustomerManager().getTransparency(msg.getRSsID()) > 0) { //if the r_ssid need transparency
                smpp.addMncMccForRequest(msg, request);
            }
            
            request.assignSequenceNumber();

            key = Integer.toString(request.getSequenceNumber());            
            boolean isPutBuffer = false;
            while (isKeepRunning() && !isPutBuffer) {
            	if(isEnableGuavaBuffer) {
            		isPutBuffer = guavaCache.put(key, msg);
            	}else {
            		isPutBuffer = bufferMonitor.put(key, msg);
            	}
            	
            }

            if(isKeepRunning()){
            	connection.send(request.getData().getBuffer());
            	pduLogger.logSent(isServer ? "server" : "client", sessionName, request);
                msg.setOutTransID(key);
                result = true;

            }else{
                log.warn(msg,"The session is broken, send unsuccessfully!");
                result = false;
            }

        }
        catch (IOException e) {
            log.error(msg, e, e);
            stop();
            result = false;
            throw e;
        }
        catch (Exception e) {
            log.error(msg, e, e);
            result = false;

        }finally{
            if(!result){
                try {
                	if(isEnableGuavaBuffer) {
                		guavaCache.remove(key);
                	}else {
                		bufferMonitor.remove(key);
                	}
                }
                catch (Exception ex) {

                }
            }
        }
        return result;
    }

    /**
     * send submit response
     * @param respMsg
     * @return
     */
    private boolean sendSubmitSMResp(GmmsMessage respMsg) {
    	SubmitSMResp submitSMResp = null;
    	if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(respMsg.getMessageType())){
    		submitSMResp = smpp.createSmppSubmitSMResp4dr(respMsg,Data.ESME_ROK);
    	}else{
    		submitSMResp = smpp.createSmppSubmitSMResp(respMsg,customerInfo);
    	}
    	return this.respond(submitSMResp,respMsg);
    }
    
    private boolean sendSubmitSMRespWithoutInnerack(GmmsMessage respMsg) {
    	SubmitSMResp submitSMResp = null;
    	if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(respMsg.getMessageType())){
    		submitSMResp = smpp.createSmppSubmitSMResp4dr(respMsg,Data.ESME_ROK);
    	}else{
    		submitSMResp = smpp.createSmppSubmitSMResp(respMsg,customerInfo);
    	}
    	return this.respondWithoutInnerack(submitSMResp,respMsg);
    }
   
    private boolean sendDeliverySM(GmmsMessage deliverMsg, SMPPVersion ver) {
        boolean result = false;
        String key = null;
        DeliverSM delivSm = smpp.createSmppDeliverSM(deliverMsg,
                                                     this.customerInfo);
        if(delivSm == null)
            return result;

        delivSm.setVersion(ver);
        //create out msgid for SMPP3.4, set into ReceiptedMessageId in Deliver_Sm
        if (ver != null && ver.getVersionID() == 0x34) {
            //modified by Jianming,V1.0.1
            //tranfer to hex format
        	String decMsgId = messageIdGenerator.generateDecID(moduleIndex);
            try{
            	if(customerInfo.getSMPPIsGenHexMsgId()){//modified by Jianming in v1.0.1
            		long decId = Long.parseLong(decMsgId);
                	String hexMsgId = Long.toHexString(decId).toUpperCase();
                    //tranfer to hex format
                    delivSm.setReceiptedMessageId(hexMsgId);
            	}else{
            		delivSm.setReceiptedMessageId(decMsgId);
            	}
            }
            catch (Exception ex) {
                log.error(deliverMsg, ex, ex);
            }
            deliverMsg.setOutMsgID(decMsgId);
        }
        try {
            if (gmmsUtility.getCustomerManager().getTransparency(deliverMsg.getOoperator()) > 0) {
                smpp.addMncMccForRequest(deliverMsg, delivSm);
            }
            delivSm.assignSequenceNumber();
            key = Integer.toString(delivSm.getSequenceNumber());
            while (isKeepRunning() &&
                   !bufferMonitor.put(key, deliverMsg))
                ;

            if (isKeepRunning()) {
                connection.send(delivSm.getData().getBuffer());
                pduLogger.logSent(isServer ? "server" : "client", sessionName, delivSm);
                deliverMsg.setOutTransID(key);
                result = true;
            }
            else {
                log.warn(deliverMsg,"The session is broken, send unsuccessfully!");
                result = false;
            }

        }
        catch (IOException ex) {
            log.error(deliverMsg, ex, ex);
            result = false;
            stop();
        }
        catch (Exception ex) {
            log.error(deliverMsg, ex, ex);
            result = false;
            stop();
        }
        finally {
            if (!result) {
                try {
                    bufferMonitor.remove(key);
                }
                catch (Exception ex) {

                }
            }

        }
        return result;
    }
    /**
     * send DR response
     * @param respMsg
     * @return
     */
    private boolean sendDeliverSMResp(GmmsMessage respMsg) {
    	DeliverSMResp deliverSMResp = null;
    	if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(respMsg.getMessageType())){
    		deliverSMResp = smpp.createSmppDeliverSMResp4dr(respMsg,Data.ESME_ROK);
    	}else{
    		deliverSMResp = smpp.createSmppDeliverSMResp(respMsg,customerInfo);
    	}
    	if(log.isDebugEnabled()){
    		log.debug(respMsg,"sendDeliverSMResp = {};respMsg = {}", deliverSMResp , respMsg);
    	}
    	return this.respond(deliverSMResp,respMsg);
    }
    
    private boolean sendDeliverSMRespWithoutInnerack(GmmsMessage respMsg) {
    	DeliverSMResp deliverSMResp = null;
    	if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(respMsg.getMessageType())){
    		deliverSMResp = smpp.createSmppDeliverSMResp4dr(respMsg,Data.ESME_ROK);
    	}else{
    		deliverSMResp = smpp.createSmppDeliverSMResp(respMsg,customerInfo);
    	}
    	if(log.isDebugEnabled()){
    		log.debug(respMsg,"sendDeliverSMResp = {};respMsg = {}", deliverSMResp , respMsg);
    	}
    	return this.respondWithoutInnerack(deliverSMResp,respMsg);
    }
    
    private boolean serverSubmit(GmmsMessage msg){

        msg.setProtocolVersion(Integer.toString(smppVersion.getVersionID()));

        String msgType = msg.getMessageType();

        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msgType) ||
            GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msgType)) {
            return sendDeliverySM(msg, smppVersion);
        } else  if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msgType)||
        			GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msgType)) {
        	 return this.sendSubmitSMResp(msg);
        } else {
            return sendDeliveryDR(msg,smppVersion);
        }
    }

    private boolean sendDeliveryDR(GmmsMessage deliverMsg, SMPPVersion ver) {
        boolean result = false;
        String key = null;
        deliverMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        DeliverSM delivSm = smpp.createSmppDeliverSM4dr(deliverMsg,this.customerInfo);
        if(delivSm == null){
        	return false;
        }
        try {
        	if (gmmsUtility.getCustomerManager().getTransparency(deliverMsg.getOSsID()) > 0) {
                smpp.addMncMccForRequest(deliverMsg, delivSm);
            }
            delivSm.setVersion(ver);
            delivSm.assignSequenceNumber();
            key = Integer.toString(delivSm.getSequenceNumber());
            while (isKeepRunning() &&
                   !bufferMonitor.put(key, deliverMsg))
                ;
            if(log.isDebugEnabled()){
        		log.debug(deliverMsg,"bufferMonitor put deliverMsg with SequenceNumber = {}", key);
            }
            if (isKeepRunning()) {
                connection.send(delivSm.getData().getBuffer());
                pduLogger.logSent(isServer ? "server" : "client", sessionName, delivSm);
                deliverMsg.setInTransID(key);
                if(log.isInfoEnabled()){
					log.info(deliverMsg,"send dr, inTransID {}", key);
                }
                result = true;
            }
            else {
                log.warn(deliverMsg,"The session is broken, send unsuccessfully!");
                result = false;
            }
            // For all the messages will be inserted into messagesore,
            // we will remove the 00 if it is existed.
            deliverMsg.setActionCode( -1);
        }
        catch (IOException ex) {
            log.error(deliverMsg, ex, ex);
            result = false;
            stop();
        }
        catch (Exception ex) {
            log.error(deliverMsg, ex, ex);
            result = false;
            stop();
        }
        finally {
            if (!result) {
                try {
                    bufferMonitor.remove(key);
                }
                catch (Exception ex) {

                }
            }

        }
        return result;
    }



    /**
     * submitAndRec
     *
     * @param msg GmmsMessage
     * @return ByteBuffer
     * @throws IOException
     * @todo Implement.gmms.connectionpool.session.Session
     *   method
     */
    public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
        /**
         * as have not any class call the method
         * so have not implement the method for smpp session.
         */
        return null;
    }
    public boolean respond4Bind(Response response) {
        boolean result = true;
        try {
            connection.send(response.getData().getBuffer());
            pduLogger.logSent(isServer ? "server" : "client", sessionName, response);
        }
        catch (Exception ex) {
            result = false;
            log.error("An error occured while SMPP tried to send a Bind Resp.",ex);
             stop();
        }
        if(log.isInfoEnabled()){
			log.info("send bind response {}",response.debugString());
        }
        return result;

    }

    public boolean respond(Response response,GmmsMessage msg) {
        boolean result = true;
        try {
        	connection.send(response.getData().getBuffer());
        	if(log.isInfoEnabled()){
				log.info(msg,"Send {} response to customer, status {}",msg.getMessageType(),response.getCommandStatus());
        	}
        }
        catch (Exception ex) {
            result = false;
            log.warn(msg,"An error occured while SMPP tried to send a Resp.",ex);
            stop();
        }
        return result;
    }
    
    public boolean respondWithoutInnerack(Response response,GmmsMessage msg) {
        boolean result = true;
        try {
        	connection.send(response.getData().getBuffer());
        	pduLogger.logSent(isServer ? "server" : "client", sessionName, response);
        	if(log.isInfoEnabled()){
				log.info(msg,"Send {} response to customer, status {}",msg.getMessageType(),response.getCommandStatus());
        	}

            /*else if(msg.getMessageType().equals(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)) {
            	msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
            	 if(!putGmmsMessage2RouterQueue(msg)){
                 	if(log.isInfoEnabled()){
                 		log.info(msg,"Fail to send back the inner ack({}) to core engine",GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
                 	}
                 }
            }*/
        }
        catch (Exception ex) {
            result = false;
            log.warn(msg,"An error occured while SMPP tried to send a Resp.",ex);
            stop();
        }
        return result;
    }

    /**
     * timeout
     *
     * @param obj Object
     * @param msg GmmsMessage
     * @todo Implement.gmms.util.BufferTimeoutInterface
     *   method
     */
    public void timeout(Object obj, GmmsMessage msg) {
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
        	
        	String dc_Name = this.customerInfo.getShortName();
        	//log timeout submit or delivery message into file
        	this.logExceptionMsg(msg,dc_Name);
        	
			msg.setStatus(GmmsStatus.SUBMIT_RESP_ERROR);
	        msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
			String dc_Name = this.customerInfo.getShortName();
			//log timeout Delivery Report message into file
			this.logExceptionMsg(msg,dc_Name);
			
			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
	        msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		}else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
	        msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		}
		else if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) 
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())){
			msg.setStatusCode(1);
		}
        if(log.isInfoEnabled()){
        	log.info(msg,"{} do not receive the response after timeout", msg.getMessageType());
        }
        if(!putGmmsMessage2RouterQueue(msg)){
        	log.warn(msg, "Failed to send {} msg.",msg.getMessageType());
        }
    }

    // DC name /
    public void logExceptionMsg(GmmsMessage msg,String name){
    	if(exMsgManager == null){
    		exMsgManager =  ExceptionMessageManager.getInstance();
    	}
    	if(!exMsgManager.insertExceptionMessage(msg,name)){
	    	log.error(msg,"put message into exMsgQueue failed!");        
		}else{
			log.info(msg,"put message into exMsgQueue successfully.");
		}
    }
    
	/*
	 * public ArrayList parsePDU(ByteBuffer buffer) { if (buffer == null) { return
	 * null; } ArrayList<PDU> pdus = new ArrayList<PDU> ();
	 * 
	 * try { //if there is unprocessed bytes[] if (unprocessed.getHasUnprocessed())
	 * { //judge if the data in unprocessed is expired if (
	 * (unprocessed.getLastTimeReceived() + 120000) < System.currentTimeMillis()) {
	 * unprocessed.reset(); if(log.isInfoEnabled()){
	 * log.info("The unprocessed is expired, abandon it."); } } } //byte[] bytes =
	 * buffer.array(); //new byte[] bytes; if (buffer.hasArray()) { //
	 * 如果是堆缓冲区并可以直接访问，拿到有效部分 bytes = new byte[buffer.remaining()];
	 * buffer.get(bytes); } else { // 处理如果是直接内存或 ReadOnly Buffer 的情况 bytes = new
	 * byte[buffer.remaining()]; buffer.get(bytes); }
	 * 
	 * 
	 * 
	 * // add the new received bytes[] to unprocessed if (bytes.length > 0) {
	 * unprocessed.getUnprocessed().appendBytes(bytes);
	 * unprocessed.setLastTimeReceived(); unprocessed.check(); }
	 * 
	 * //create Pdus by unprocessed PDU pdu; while (unprocessed.getHasUnprocessed())
	 * { pdu = PDU.createPDU(unprocessed.getUnprocessed()); unprocessed.check(); if
	 * (pdu != null) { pdus.add(pdu); } } } catch (HeaderIncompleteException
	 * headerEx) { log.warn("HeaderIncompleteException when parse the buffer.");
	 * unprocessed.check(); } catch (MessageIncompleteException messageEx) {
	 * log.warn("MessageIncompleteException when parse the buffer.");
	 * unprocessed.check(); } catch (Exception e) { log.error(e, e);
	 * log.warn("There is an Exception while create PDU, drop all bytes.");
	 * unprocessed.reset(); } finally { return pdus; } }
	 */
    
    @Override
    @SuppressWarnings("unchecked")
    public ArrayList parsePDU(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ArrayList<PDU> pdus = new ArrayList<PDU>();

        try {
            // 1. 处理历史滞留数据的超时
            if (unprocessed.getHasUnprocessed()) {
                if ((unprocessed.getLastTimeReceived() + 60000) < System.currentTimeMillis()) {
                    unprocessed.reset();
                    if (log.isInfoEnabled()) {
                        log.info("The unprocessed is expired, abandon it.");
                    }
                }
            }

            // 只要我们还有网络数据未读完，或者还有历史半包未消耗，就一直循环
            while (buffer.hasRemaining() || unprocessed.getHasUnprocessed()) {
                
                // 情况A：存在历史遗留碎片数据，我们不得不走慢速拼接通道 (Fallback)
                if (unprocessed.getHasUnprocessed()) {
                    // 把当前的 ByteBuffer 剩余字节全部榨干追加进去
                    if (buffer.hasRemaining()) {
                        byte[] remainingBytes = new byte[buffer.remaining()];
                        buffer.get(remainingBytes); // 消耗 buffer
                        unprocessed.getUnprocessed().appendBytes(remainingBytes);
                        unprocessed.setLastTimeReceived();
                    }
                    unprocessed.check();

                    // 循环从 unprocessed 榨取
                    while (unprocessed.getHasUnprocessed()) {
                        PDU pdu = PDU.createPDU(unprocessed.getUnprocessed());
                        unprocessed.check();
                        if (pdu != null) {
                            pdus.add(pdu);
                        } else {
                            // 半包无法组成完整 PDU，跳出等待下次网络数据包
                            break; 
                        }
                    }
                    // 因为网络包已在上方全部清空到 unprocessed 里了，所以只能跳出外层循环
                    break;
                }

                // 情况B：【极速通道 (Fast-Path)】 unprocessed 是空的，网络包是全新的
                // 此时我们要极其高效地拆解 buffer
                
                // 连读取长度字段的 4 个字节都不够（极端罕见），全丢进暂存区缓存
                if (buffer.remaining() < 4) {
                    byte[] partial = new byte[buffer.remaining()];
                    buffer.get(partial);
                    unprocessed.getUnprocessed().appendBytes(partial);
                    unprocessed.setLastTimeReceived();
                    unprocessed.check();
                    break;
                }

                // SMPP 默认 Big-Endian，通过 ByteBuffer.getInt 直接 "偷看" 头 4 字节，指针原封不动！
                int nextPduLength = buffer.getInt(buffer.position()); 
                
                // 长度合法性校验（防脏数据污染内存，最大PDU通常在 8KB 以内）
                if (nextPduLength < 16 /* Header长度 */ || nextPduLength > 16384) {
                    log.error("Invalid SMPP PDU length ({}), maybe stream corrupted. Drop remaining bytes.", nextPduLength);
                    buffer.position(buffer.limit()); // 消费废弃整个缓冲区
                    unprocessed.reset();
                    break;
                }

                if (buffer.remaining() >= nextPduLength) {
                    // 这个 ByteBuffer 里包含了【至少一整个】 PDU
                    // 精准扣出这个 PDU 的字节数组，不多取一丁点
                    byte[] pduBytes = new byte[nextPduLength];
                    buffer.get(pduBytes);
                    
                    // 用提取好的完美尺寸数组交给 createPDU，原汁原味避开 N^2 的循环切片拷贝灾难！
                    SmppByteBuffer singlePduBuf = new SmppByteBuffer(pduBytes);
                    PDU pdu = PDU.createPDU(singlePduBuf);
                    if (pdu != null) {
                        pdus.add(pdu);
                    }
                } else {
                    // 包切断了，一个 PDU 竟然跨越了当前的网络包
                    // 需要把这些残余的部分放入暂存区，等下一个网络包到了自动触发上面的（情况A）
                    byte[] partial = new byte[buffer.remaining()];
                    buffer.get(partial);
                    unprocessed.getUnprocessed().appendBytes(partial);
                    unprocessed.setLastTimeReceived();
                    unprocessed.check();
                    break;
                }
            }
            
        } catch (HeaderIncompleteException | MessageIncompleteException messageEx) {
            // 半包情况（这在极速通道基本被规避了，只有在慢速拼接通道可能会抛出）
            unprocessed.check();
        } catch (Exception e) {
            log.error(e, e);
            log.warn("There is an Exception while create PDU, drop all bytes.");
            unprocessed.reset();
        }

        return pdus;
    }


	@Override
	public OperatorMessageQueue getOperatorMessageQueue() {
		// TODO Auto-generated method stub
		return null;
	}
}
