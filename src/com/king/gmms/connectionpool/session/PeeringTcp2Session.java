package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.ClientSessionThread;
import com.king.gmms.connectionpool.sessionthread.ServerSessionThread;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.PeeringTcp2ServerFactory;
import com.king.gmms.domain.*;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.processor.CsmUtility;
import com.king.gmms.protocol.tcp.peering20.*;
import com.king.gmms.protocol.tcp.peering20.exception.*;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

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
public class PeeringTcp2Session extends AbstractCommonSession {

    private static SystemLogger log = SystemLogger.getSystemLogger(PeeringTcp2Session.class);
    private Integer pendingAliveCount = 0;
    protected A2PCustomerManager ctm = null;
    private Unprocessed unprocessed = new Unprocessed();
    private int currentVersion = Pdu.VERSION_2_0;
    private int gmdVersion = Pdu.VERSION_2_0;
    private boolean isA2PP2PPeering = false;

    public PeeringTcp2Session(Socket socket) {
        super();
        // create a single thread pool first, then update after binded
        super.initReceivers();
        
        ctm = gmmsUtility.getCustomerManager();
        int connectionSilentTime = Integer.parseInt(gmmsUtility.
            getCommonProperty("MaxSilentTime", "600")) * 1000;
        transaction = new TransactionURI();
        isServer = true;
        if (ctm.getPeeringTcpVersion() > 0) {
            gmdVersion = ctm.getPeeringTcpVersion();
        }
        currentVersion = gmdVersion;
        try {
            if (socket != null) {
                createConnection(socket);
            }
        }
        catch (IOException ex) {
            log.error(ex, ex);
        }
        sessionThread = new ServerSessionThread(this,
            connectionSilentTime);
        
        start();
    }

    public PeeringTcp2Session(ConnectionInfo info) {
        super();
        if (info == null) {
            return;
        }
        ctm = gmmsUtility.getCustomerManager();
        setSessionName(info.getConnectionName());
        isServer = false;
        if (ctm.getPeeringTcpVersion() > 0) {
            gmdVersion = ctm.getPeeringTcpVersion();
        }
        currentVersion = gmdVersion;
        try {
            customerInfo = (A2PMultiConnectionInfo)ctm.getCustomerBySSID(info.getSsid());
            if (customerInfo == null) {
                log.error(
                    "Can not get customer information from CCB by the ssid of connection info");
                return;
            }
            intialize(info, customerInfo);
            super.initReceivers();
            startBufferMonitor(customerInfo);
            transaction = new TransactionURI(connectionInfo.getConnectionName());
        }
        catch (Exception e) {
            log.error(e, e);
            return;
        }
        int enquireLinkFailCount = Integer.parseInt(gmmsUtility.
            getModuleProperty(
                "PendingAliveCount",
                "3"));
        sessionThread = new ClientSessionThread(this,
            customerInfo.getReconnectInterval(), enquireLinkFailCount);
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
    		log.trace("PeeringTcp Receive a PDU {}", pdu.getCommandId());
        }
        if (Pdu.COMMAND_SUBMIT == pdu.getCommandId()) {
            pdu.setCustomerInfo(customerInfo);
            msg = pdu.convertToMsg(null);
            result = processSubmit(msg);
            
        }
        else if (Pdu.COMMAND_SUBMIT_ACK == pdu.getCommandId()) {
            msg = pdu.convertToMsg(bufferMonitor);
            if(msg==null){
            	result = false;
            	log.warn("Can't put the Null message into message queue of agent");
            }else{
                result = putGmmsMessage2RouterQueue(msg);
            	if(!result){
                 	log.warn("Can't put the message into message queue of agent, sequence={}",
                             msg.getOutTransID());
                 }
            }
        }
        else if (Pdu.COMMAND_DELIVERY_REPORT == pdu.getCommandId()) {
            msg = pdu.convertToMsg(null);
            result = processDeliveryReport(msg);
        }
        else if (Pdu.COMMAND_DELIVERY_REPORT_ACK == pdu.getCommandId()) {
            msg = pdu.convertToMsg(bufferMonitor);
            if(msg==null){
            	result = false;
            	log.warn("Can't put the Null message into message queue of agent");
            }else{
                result = putGmmsMessage2RouterQueue(msg);
            	if(!result){
                 	log.warn("Can't put the message into message queue of agent, sequence={}" ,
                             msg.getOutTransID());
                 }
            }
        }else if (Pdu.COMMAND_ALIVE == pdu.getCommandId()) {
            CommandKeepAliveAck ack = new CommandKeepAliveAck(currentVersion);
            try {
                super.submit(ack.toByteBuffer().getBuffer());
            }
            catch (Exception ex) {
                log.warn("IOException occured when sending KeepAlive ACK", ex);
                stop();
            }
        }
        else if (Pdu.COMMAND_ALIVE_ACK == pdu.getCommandId()) {
            clearActiveTestCount();
        }
        else if (Pdu.COMMAND_BIND == pdu.getCommandId()) {
            result = processBind(pdu);
            if(!result){
            	stop();
            }
        }
        return result;
    }

    public ArrayList parsePDU(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ArrayList<Pdu> pdus = new ArrayList<Pdu> ();

        try {
            //if there is unprocessed bytes[]
            if (unprocessed.getHasUnprocessed()) {
                //judge if the data in unprocessed is expired
                if ( (unprocessed.getLastTimeReceived() + 60000) <
                    System.currentTimeMillis()) {
                    unprocessed.reset();
                		log.info("The unprocessed is expired, abandon it.");
                }
            }
            byte[] bytes = buffer.array();
            // add the new received bytes[] to unprocessed
            if (buffer != null && bytes.length > 0) {
                TcpByteBuffer tcpBuffer = new TcpByteBuffer(bytes);
                unprocessed.getUnprocessed().appendBytes(tcpBuffer);
                unprocessed.setLastTimeReceived();
                unprocessed.check();
            }

            //create Pdus by unprocessed
            Pdu pdu;
            while (unprocessed.getHasUnprocessed()) {
                pdu = Pdu.createPdu(unprocessed.getUnprocessed(),
                                    currentVersion);
                unprocessed.check();
                if (pdu != null) {
                    pdus.add(pdu);
                }
            }
        }
        catch (HeaderIncompleteException headerEx) {
            log.warn("HeaderIncompleteException when parse the buffer.");
            unprocessed.check();
        }
        catch (MessageIncompleteException messageEx) {
            log.warn("MessageIncompleteException when parse the buffer.");
            unprocessed.check();
        }
        catch (Exception e) {
            log.error(e, e);
            log.warn("There is an Exception while create PDU, drop all bytes.");
            unprocessed.reset();
        }
        finally {
            return pdus;
        }
    }

    /**
     * judge whether A2PP2PPeering
     * @param playerType
     * @param protocol
     * @return
     */
    private boolean isA2PP2PPeering(String playerType, String protocol) {
    	if(log.isInfoEnabled()){
			log.info("playerType={}, protocol={}",playerType, protocol);
    	}
    	if (("3rdPartyHUB".equalsIgnoreCase(playerType) || "3rdHUB".equalsIgnoreCase(playerType))
    			&& protocol!=null && protocol.trim().equalsIgnoreCase(SystemConstants.A2P_P2P_PEERING_PROTOCOL)) {
    		return true;
    	}
    	return false;
    }

    public boolean connect() {
        boolean result = false;
        unprocessed.reset();
        CommandBind request = new CommandBind();
        
        if (isA2PP2PPeering(customerInfo.getA2PPlayerType(), customerInfo.getProtocol())) {        	
        	// A2P P2P peering use iosmsSsid to bind
        	int iosmsSsid = -1;
        	if(customerInfo.isSmsOptionIsVirtualDC()){
        		iosmsSsid = customerInfo.getIosmsSsid();
        	}else{
        		iosmsSsid = ctm.getIosmsSsidBySsid(ctm.getCurrentA2P());
        	}
        	if (iosmsSsid > 0) {
        		request.setUserName("" + iosmsSsid);
        		if(log.isInfoEnabled()){
					log.info("UserName: {} ,Version: {}",iosmsSsid, gmdVersion);
        		}
        		isA2PP2PPeering = true;
        	} else {
        		log.warn("Cant't get IOSMSSSID by current A2P's Ssid:{}", customerInfo.getSSID());
        		return false;
        	}
        } else {
        	request.setUserName("" + ctm.getCurrentA2P());
        	if(log.isInfoEnabled()){
				log.info("UserName: {} ,Version: {}",ctm.getCurrentA2P(), gmdVersion);
        	}
        }
        
        request.setVersion(gmdVersion);
        request.setTimestamp(Long.toString(System.currentTimeMillis()));
    	
        try {
            if (request != null) {
                ByteBuffer received = connection.sendAndReceive(request.
                    toByteBuffer().getBuffer());
                if (received != null) {
                    ArrayList list = parsePDU(received);
                    if (list != null && list.size() > 0) {
                        Pdu pdu = (Pdu) list.get(0);
                        if (pdu != null) {
                            lastActivity = System.currentTimeMillis();
                            if (pdu.getHeader().getCommandId() ==
                                Pdu.COMMAND_BIND_ACK) {
                                if ( ( (CommandBindAck) pdu).getStatusCode() ==
                                    0) {
                                    if (isKeepRunning()) {
                                        currentVersion = ((CommandBindAck)pdu).getVersion();
                                        setStatus(ConnectionStatus.CONNECT);
                                        clearActiveTestCount();
                                        result = true;
                                    }
                                } else {
                                    log.warn("receive COMMAND_BIND_ACK, BIND failed");
                                }
                            }
                        }
                    }
                }
                else {
                    log.warn("Did not receive the COMMAND_BIND_ACK");
                }
            }
        }
        catch (Exception e) {
            log.error("Tcp session bind fail.", e);
            return false;
        }
        return result;
    }

    public int enquireLink() throws Exception{
        long currentTime = System.currentTimeMillis();
        synchronized (mutex) {
            if (pendingAliveCount > 0) {
            	if(log.isInfoEnabled()){
					log.info("Session({}) does not receive the enquire link response!",sessionName);
            	}
            }
            if (currentTime - lastActivity >= customerInfo.getEnquireLinkTime()) {
                try {
                    CommandKeepAlive activeTest = new CommandKeepAlive(
                        currentVersion);
                        log.debug("Send an alive request to keep session ...");
                    super.submit(activeTest.toByteBuffer().getBuffer());
                    pendingAliveCount++;
                    //lastActivity = System.currentTimeMillis();
                }
                catch (Exception ex) {
                    log.error("IOException occured when sending alive request ",
                              ex);
                    stop();
                    throw ex;
                }
            }
        }
        return pendingAliveCount;
    }

    public void timeout(Object key, GmmsMessage msg) {
        try {
        	 if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
     				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
     			msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
     	        msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
     		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
     			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
     	        msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
     		}else if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) 
     				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())){
     			msg.setStatusCode(1);
     	        msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
     		}
             if(!putGmmsMessage2RouterQueue(msg)){
            	 if(log.isInfoEnabled()){
 					log.info(msg, "Failed to send {} msg.",msg.getMessageType());
            	 }
             }
     		bufferMonitor.remove(key);
        }
        catch (Exception ex) {
            log.error(msg,ex, ex);
        }
    }
    /**
     * queue timeout
     */
    public void timeout(Object msg) {
    	this.timeout(null, (GmmsMessage )msg);
    }
    /**
     * deliver
     *
     * @param message GmmsMessage
     * @return boolean
     */
    public boolean submit(GmmsMessage message) {
        boolean result = false;
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.
            getMessageType())) {
            result = sendNewMessage(message);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(
            message.
            getMessageType())) {
            result = sendReport(message);
        }
        else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message.getMessageType()) ||
                 (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(message.getMessageType()))) {
            result = sendResp(message);
        }
        else {
        	if(log.isInfoEnabled()){
				log.info(message, "Unknown Message Type:{}" , message.getMessageType());
        	}
            message.setStatus(GmmsStatus.UNKNOWN_ERROR);
        }
        return result;
    }

    public java.nio.ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
        return null;
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
        String outMsgID = message.getOutMsgID();
        if (outMsgID == null) {
            int cA2P = message.getCurrentA2P();
            int rA2P = message.getRA2P();
            if(cA2P == rA2P || ctm.vpOnSameA2P(cA2P , rA2P)){
                outMsgID = MessageIdGenerator.generateCommonOutMsgID(message.getRSsID());
            } else {
                outMsgID = MessageIdGenerator.generateCommonOutMsgID(message.getCurrentA2P());
            }
            message.setOutMsgID(outMsgID);
        }

        String bufferMonitorKey = message.getMsgID();

        try {
        	CommandSubmit pdu = new CommandSubmit(currentVersion);  
            pdu.setCustomerInfo(customerInfo);
            pdu.convertFromMsg(message);
            if (isA2PP2PPeering(customerInfo.getA2PPlayerType(), customerInfo.getProtocol())) {
            	if(customerInfo.isSmsOptionIsVirtualDC()){
            		// convert oop ssid to vitual DC Ssid
            		int vssid = customerInfo.getIosmsSsid();            		
            		if (vssid > 0) {
                		pdu.setO_op(vssid);
                		if(log.isDebugEnabled()){
        					log.debug(message, "Message Rssid={}, Virtual DC ioSmsid={}" , message.getRSsID(),vssid);
                		}
                	} else {
                		if(log.isInfoEnabled()){
                			log.info(message, "can't get Virtual DC iosmsSsid by ssid:{} ", customerInfo.getSSID());
                		}
                		return false;
                	}
            	}else{
            		// convert oop ssid to iosmsSsid
                	int iosmsSsidOop = ctm.getIosmsSsidBySsid(message.getOoperator());
                	if (iosmsSsidOop > 0) {
                		pdu.setO_op(iosmsSsidOop);
                		if(log.isDebugEnabled()){
        					log.debug(message, "Oop IosmsSsid={}, Ssid={}" ,iosmsSsidOop, message.getOoperator());
                		}
                	} else {
                		if(log.isInfoEnabled()){
                			log.info(message, "can't get iosmsSsid by ssid:{}" , message.getOoperator());
                		}
                		return false;
                	}
            	}
                	
            	// convert rop ssid to iosmsSsid
            	int iosmsSsidRop = ctm.getIosmsSsidBySsid(message.getRoperator());
            	if (iosmsSsidRop > 0) {
            		pdu.setR_op(iosmsSsidRop);
            		if(log.isDebugEnabled()){
    					log.debug(message, "Rop IosmsSsid={}, Ssid={}", iosmsSsidRop , message.getRoperator());
            		}
            	} else {
            		if(log.isInfoEnabled()){
            			log.info(message, "can't get iosmsSsid by ssid:{}", message.getRoperator());
            		}
            		return false;
            	}
            	
            	pdu.setO_hub(-1);
            	pdu.setR_hub(-1);
            	pdu.setO_relay(-1);
            	pdu.setR_relay(-1);
            	if(log.isTraceEnabled()){
            		log.trace(message, " CommandSubmit, o_hub/relay, r_hub/relay is set to -1.");
            	}
            	
            	// concatenated sms may have same msgid, so use outMsgId as msgId in PDU
            	pdu.setMsgId(message.getOutMsgID());
            	bufferMonitorKey = message.getOutMsgID();
            	
            	// ATOP-343 filter CSMS parameters
            	if(!ctm.isNotSupportUDH(message.getRSsID()) && customerInfo.isUdhConcatenated()){
            		// transfer only by UDH, remove parameters
            		pdu.setRefNum(null);
            		pdu.setTotalSegments(-1);
            		pdu.setSeqNum(-1);
            	} else {
            		// csm refnum, asg/gmd use decimal, a2p use hexadecimal
                	String refNum = message.getSarMsgRefNum();
                	if (refNum != null && refNum.trim().length() > 0 && message.getSarTotalSeqments()>1){
                		try{
                    		short pduRefNum = Short.parseShort(refNum.trim(), 16);
                    		pdu.setRefNum(pduRefNum + "");
                    	}catch(Exception e){
                    		log.warn(message, "CommandSubmit setRefNum error. refNum={}" , refNum);
                    		pdu.setRefNum(null);
                    	}
                	}
            	}
            	
            	// don't send this field to P2P since P2P don't recognize it.
                pdu.setServiceTypeID(-1);
                pdu.setScheduleDeliveryTime(null);
            }
            
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

        while (!bufferMonitor.put(bufferMonitorKey, message)) {
            ;
        }
        try {
            super.submit(submitBuffer.getBuffer());
            message.setOutTransID(String.valueOf(System.currentTimeMillis()));
            result = true;
        }
        catch (IOException ex) {
            log.warn(message,
                     "IOException occured when submit msg to PeerTcp Server: ",
                     ex);
            message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
            bufferMonitor.remove(bufferMonitorKey);
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
            CommandDeliveryReport pdu = new CommandDeliveryReport(
                currentVersion);
            pdu.convertFromMsg(message);
            reportBuffer = pdu.toByteBuffer();
        }
        catch (Exception e) {
            log.error(message,e, e);
            return result;
        }

        if (reportBuffer == null) {
            return result;
        }
        if(log.isDebugEnabled()){
        	log.debug(message,"sendReport:{}",message.toString4NewMsg());
        }
        try {
            while (!bufferMonitor.put(message.getInMsgID(), message)) {
                ;
            }
            super.submit(reportBuffer.getBuffer());
            result = true;
        }
        catch (IOException ex) {
            log.warn(message,ex,ex);
            message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.
                                  getCode());
            bufferMonitor.remove(message.getInMsgID());
            stop();
        }
        return result;
    }
    /**
     * 
     * @param message
     * @return
     */
    private boolean sendResp(GmmsMessage message) {
        Pdu ack = null;
        try {
            if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message.
                getMessageType())) {
                ack = new CommandSubmitAck(currentVersion);
                ack.convertFromMsg(message);
            }
            else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(
                message.getMessageType())) {
                ack = new CommandDeliveryReportAck(currentVersion);
                if (message != null) {
                    ack.convertFromMsg(message);
                } else {
                        log.info("send dr Resp failed because of null message");
                }
            }
        } catch (Exception ex) {
            log.warn(message,ex,ex);
            if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(message.
                getMessageType())) {
                message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
            }
            else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.
                     equalsIgnoreCase(message.getMessageType())) {
                message.setStatusCode(GmmsStatus.
                                      FAIL_SENDOUT_DELIVERYREPORT.getCode());
            }
            stop();
        }
        return this.respond(ack, message);
    }
    /**
     * 
     * @param msg
     * @return
     */
    private boolean processSubmit(GmmsMessage msg) {
        boolean result = false;
        if (msg == null) {
            log.warn("Message is null in processSubmit method.");
            return result;
        }
        try {
        	msg.setTransaction(transaction);
            if(msg.getOSsID() <= 0) {
                msg.setOSsID(customerInfo.getSSID());
            }
            if(msg.getOoperator() <= 0 && gmmsUtility.getCustomerManager().isOperator(msg.getOSsID())) {
                msg.setOoperator(msg.getOSsID());
            }
            
            
            if (isA2PP2PPeering(customerInfo.getA2PPlayerType(), customerInfo.getProtocol())) {
            	// convert oop iosmsSsid to ssid
            	int o_iosmsSsid = msg.getOoperator();
            	int ssidOop = ctm.getSsidByIosmsSsid(o_iosmsSsid);
            	if (ssidOop > 0) {
            		msg.setOoperator(ssidOop);
            		if(log.isInfoEnabled()){
    					log.info(msg, "Oop IosmsSsid={}, Ssid={}" , o_iosmsSsid , ssidOop);
            		}
            	} else {
            		log.warn(msg, "can't get ssid by iosmsSsid:{}" , o_iosmsSsid);
            		msg.setOoperator(-1);
            	}
            	
            	// convert rop iosmsSsid to ssid
            	int r_iosmsSsid = msg.getRoperator();
            	int ssidRop = ctm.getSsidByIosmsSsid(r_iosmsSsid);
            	if (ssidRop > 0) {
            		msg.setRoperator(ssidRop);
            		if(log.isInfoEnabled()){
    					log.info(msg, "Rop IosmsSsid={}, Ssid=" , r_iosmsSsid , ssidRop);
            		}
            	} else {
            		log.warn(msg, "can't get ssid by iosmsSsid:{}" , r_iosmsSsid);
            		msg.setRoperator(-1);
            	}
            	
            	// router will set it later
            	msg.setOSsID(customerInfo.getSSID());
            	msg.setRSsID(-1);
            	msg.setRA2P(-1);
            	// oa2p, currenta2p will be reset in MessageStoreManager.prepareInSubmit
            	msg.setCurrentA2P(-1);
            	msg.setOA2P(-1);
            	
            	// csm refnum, asg/gmd use decimal, a2p use hexadecimal
            	String refNum = msg.getSarMsgRefNum();
            	if (refNum != null && refNum.trim().length() > 0){
            		try {
            			msg.setSarMsgRefNum(CsmUtility.short2Hex(Short.parseShort(refNum.trim())));
            		} catch (Exception e) {
            			log.warn(msg, "setSarMsgRefNum error. refNum={}" , refNum);
            			msg.setSarMsgRefNum(null);
            		}
            	}
            }
            if(log.isInfoEnabled()){
            	log.info(msg,"Convert SubmitSM PDU to GmmsMessage:{}",msg.toString4NewMsg());
            }
            
            // check throttle
            if (customerInfo != null) {
				if (!ThrottlingControl.getInstance().isAllowedToReceive(ctm.getCurrentA2P())) {
					CommandSubmitAck ack = new CommandSubmitAck(currentVersion);
	                msg.setStatus(GmmsStatus.Throttled);
	                try{
	                	ack.convertFromMsg(msg);
	                    connection.send(ack.toByteBuffer().getBuffer());
	                }catch(Exception e){
	                	log.warn(msg,"Fail to send CommandSubmitAck");
	                }
	                if (log.isInfoEnabled()) {
						log.info(msg, "submit refuced by incoming throttling control");
					}
	                return result;
				}
            }
            
            result = putGmmsMessage2RouterQueue(msg);
            if(!result){
                CommandSubmitAck ack = new CommandSubmitAck(currentVersion);
                msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
                try{
                	ack.convertFromMsg(msg);
                    connection.send(ack.toByteBuffer().getBuffer());
                }catch(Exception e){
                	log.warn(msg,"Fail to send response");
                }
            }
        }
        catch (Exception ex) {
            log.warn(ex, ex);
            stop();
        }
        return result;
    }


    private boolean processDeliveryReport(GmmsMessage msg) {
        boolean result = false;
        try {
        	msg.setTransaction(transaction);
        	// ATOP-402
    		// due to redis introduced new logic, 
    		// we need to set r_a2p, c_a2p when Peeringserver receive in_dr.
        	if(isA2PP2PPeering){
        		msg.setRSsID(customerInfo.getSSID());
        	}else{
        		msg.setRA2P(customerInfo.getSSID());
        		msg.setCurrentA2P(ctm.getCurrentA2P());
        		msg.setOA2P(ctm.getCurrentA2P());
        	}
        	
        	// check throttle
            if (customerInfo != null) {
				if (!ThrottlingControl.getInstance().isAllowedToReceive(ctm.getCurrentA2P())) {
	                CommandDeliveryReportAck ack = new CommandDeliveryReportAck(currentVersion);
	                try{
	                	ack.convertFromMsg(msg);
	                	ack.setStatusCode(1); // error
	                    connection.send(ack.toByteBuffer().getBuffer());
	                }catch(Exception e){
	                	log.warn(msg,"Fail to send CommandDeliveryReportAck");
	                }
	                if (log.isInfoEnabled()) {
						log.info(msg, "DR refuced by incoming throttling control");
					}
	                return result;
				} 
            }
            
            result = putGmmsMessage2RouterQueue(msg);
            if(!result){
                CommandDeliveryReportAck ack = new CommandDeliveryReportAck(currentVersion);
                msg.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
                try{
                	ack.convertFromMsg(msg);
                	ack.setStatusCode(1); // error
                    connection.send(ack.toByteBuffer().getBuffer());
                }catch(Exception e){
                	log.warn(msg,"Fail to send DR response");
                }
            }
        }
        catch (Exception e) {
            log.warn(msg,e, e);
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
            A2PCustomerInfo server = null;
            try {
            	if(log.isInfoEnabled()){
					log.info("processBind username={}", userName);
            	}
                int ssid = Integer.parseInt(userName);
                // check whether ssid is an iosmsssid
            	int ssidTemp = ctm.getSsidByIosmsSsid(ssid);
            	
            	// MMVD is conf as an 3rdpartyhub, like MMVD_HK_GMD
            	// P2P is conf as an 3rdpartyhub with protocol Peering2
            	if (ssidTemp >0) {
            		server = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssidTemp);
            		// A2P P2P
            		if (server!=null && isA2PP2PPeering(server.getA2PPlayerType(), server.getProtocol())) {
            			if(log.isInfoEnabled()){
        					log.info("processBind iosmsid={},ssid={}" ,ssid, ssidTemp);
            			}
            			ssid = ssidTemp;
            			isA2PP2PPeering = true;
            		} else {
            			server = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
            		}
            	} else {
            		server = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
            	}
                
                if(server != null && server.getServerID() != null){
                	SingleNodeCustomerInfo serverInfo = (SingleNodeCustomerInfo)server;
                    Map<String, ConnectionInfo> conMap = serverInfo.getConnectionMap(true);
                    connectionInfo = conMap.get(SystemConstants.SINGLE_CONNECTION_NAME);
                    String connectionName = connectionInfo.getConnectionName();
                    connectionManager = PeeringTcp2ServerFactory.getInstance().getConnectionManager(ssid,connectionName);
                    if (gmdVersion > bind.getVersion()) {
                        currentVersion = bind.getVersion();
                    } else {
                        currentVersion = gmdVersion;
                    }
                    ack.setVersion(currentVersion);
                    transaction.setConnectionName(connectionName);
                    intialize(connectionInfo, serverInfo);
                    setSessionName(connectionName);
                    if (connectionManager.insertSession(connectionName, this)) {
                        setStatus(ConnectionStatus.CONNECT);
                        updateReceivers();
                        ack.setStatusCode(0);
                        result = true;
                    }
                    else {
                        ack.setStatusCode(1);
                    }

                } else {
                    log.warn("Can not find Server for ssid: {}", userName);
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
        	if(log.isInfoEnabled()){
				log.info("CommandBindAck is:{}" ,ack.toString());
        	}
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
     * 
     * @param response
     * @param msg
     * @return
     */
    public boolean respond(Pdu response,GmmsMessage msg) {
        boolean result = true;
        try {
        	connection.send(response.toByteBuffer().getBuffer());
            if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT_RESP)) {
                msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
                putGmmsMessage2RouterQueue(msg);
            }else if(msg.getMessageType().equals(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)) {
            	msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
                putGmmsMessage2RouterQueue(msg);
            }
        }
        catch (Exception ex) {
            result = false;
            log.error(
                "An error occured while SMPP tried to send a Resp.",
                ex);
            if(this.isServer){
                stop();
            }

        }
        return result;
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
    	InternalAgentConnectionFactory factory = InternalAgentConnectionFactory.getInstance();
    	OperatorMessageQueue msgQueue = null;
    	String deliveryChannelQueue = null;
    	String routerQueue = null;
    	String moduleName = null;
    	ModuleURI moduleURI = transaction.getModule();
		if(moduleURI!=null){
			moduleName = moduleURI.getModule();
		}
    	if(GmmsMessage.MSG_TYPE_INNER_ACK.equalsIgnoreCase(msg.getMessageType())
    		|| GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())
    		||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())){
    		
    		TransactionURI innerTransaction = msg.getInnerTransaction();
    		if(innerTransaction == null){
    			if(log.isInfoEnabled()){
					log.info(msg,"Cannot get the inner transaction");
    			}
				return false;
    		}
    		routerQueue = innerTransaction.getConnectionName();
    		msgQueue = factory.getMessageQueue(msg, routerQueue);
    		deliveryChannelQueue = routerQueue;
    	}else{
        	routerQueue = moduleManager.selectRouter(msg);
        	msgQueue = factory.getMessageQueue(msg, routerQueue);
        	deliveryChannelQueue = routerQueue;
        	if(msgQueue == null){
            	String aliveRouterQueue = moduleManager.selectAliveRouter(routerQueue,msg);
            	msgQueue = factory.getMessageQueue(msg, aliveRouterQueue);
            	if(msgQueue == null){
            		ArrayList<String> failedRouters = new ArrayList<String>();
            		failedRouters.add(routerQueue);
            		failedRouters.add(aliveRouterQueue);
            		aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            		while(aliveRouterQueue != null){
            			msgQueue = factory.getMessageQueue(msg, aliveRouterQueue);
            			if(msgQueue == null){
            				failedRouters.add(aliveRouterQueue);
            				aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            			}else{
            				break;
            			}
            		}
            	}
            	deliveryChannelQueue = aliveRouterQueue;
        	}
    	}
		/*
		 * if(log.isInfoEnabled()){ log.info(
		 * msg,"putGmmsMessage2RouterQueue transaction={};msgType={};outmsgid={};routerQueue={}"
		 * ,transaction,msg.getMessageType(),msg.getOutMsgID(),deliveryChannelQueue); }
		 */     	
    	if(msgQueue == null){
    		if(log.isInfoEnabled()){
				log.info(msg,"Can not find the alive delivery router");
    		}
    		msg.setDeliveryChannel(moduleName);
    		return false;
    	}else{
    		msg.setDeliveryChannel(moduleName+":"+deliveryChannelQueue);
    		return msgQueue.putMsg(msg);
    	}
    }
    private void clearActiveTestCount() {
        synchronized (mutex) {
            pendingAliveCount = 0;
        }
    }
    /**
     * destroy session
     */
    public void destroy() {
		synchronized (mutex) {
			if (keepRunning == false) {
				return;
			}
			keepRunning = false;
			try{
				
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
            }
            catch (Exception ex) {
                log.warn(ex, ex);
            }
		}
	}
	@Override
	public OperatorMessageQueue getOperatorMessageQueue() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void stop() {
		synchronized (mutex) {
			if (keepRunning == false) {
				return;
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
            	
            }
			
			if (sessionThread != null) {
				sessionThread.stopThread();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
			
			
		}
	}
}
