package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class WebServiceSession extends Session {
	private HttpInterface hi;
	private static SystemLogger log = SystemLogger.getSystemLogger(WebServiceSession.class);
	private A2PCustomerInfo cst = null;
    protected GmmsUtility gmmsUtility;
    protected String module = "";

	public WebServiceSession(A2PCustomerInfo info) {
		cst = info;
		gmmsUtility = GmmsUtility.getInstance();
		module = System.getProperty("module");
		init(info);
		try{
			int httpConnTimeOut = Integer.parseInt(gmmsUtility.getModuleProperty(
					"ConnectTimeout", "30")) * 1000;
			int httpReadTimeOut = Integer.parseInt(gmmsUtility.getModuleProperty(
					"ReadTimeout", "60")) * 1000;
			System.setProperty("sun.net.client.defaultConnectTimeout", String.valueOf(httpConnTimeOut));
			System.setProperty("sun.net.client.defaultReadTimeout", String.valueOf(httpReadTimeOut));
			if(info.isSMSOptionIsSupportHttps()){
				SslConfiguration config = cst.getSslConfiguration()== null? gmmsUtility.getSslConfiguration():cst.getSslConfiguration();
				System.setProperty("javax.net.ssl.trustStore", config.getTrustStorePath());  
		        System.setProperty("javax.net.ssl.trustStorePassword", config.getTrustStorePassword()); 
		        HostnameVerifier hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						// TODO Auto-generated method stub
						log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
						return urlHostName.equals(session.getPeerHost());
					}
			     };
			    // Install the all-trusting host verifier
			    HttpsURLConnection.setDefaultHostnameVerifier(hv);
			}

		}catch(Exception e){
			log.warn("Set failed for TimeOut:",e);
		}
	}

	private void init(A2PCustomerInfo info) {
		if (null == info) {
			log.error("A2PCustomerInfo is null!");
			return;
		}
		String interfaceName = info.getProtocol().trim();
		HttpInterfaceManager him = gmmsUtility.getHttpInterfaceManager();
		if (null == him) {
			log.error("Get null HttpInterfaceManager instance!");
			return;
		}
		him.initHttpInterfaceMap(interfaceName);
		try {
			hi = him.getHttpInterfaceMap().get(interfaceName);
		} catch (Exception e) {
			log.error("Can not find the Interface, and name is "
					+ interfaceName, e);
		}
	}
	/**
	 * submit4webservice
	 * @param msg
	 * @return
	 * @throws IOException
	 */
	public boolean submit(GmmsMessage msg) throws IOException{
		GmmsMessage resp = null;
		try{
			if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpPdu submitReq = hi.getMtSubmitRequest();
				if (submitReq.hasHandlerClass()) {
					String className = submitReq.getHandlerClass();
					Object[] args = { msg, cst };
					resp = (GmmsMessage) hi.invokeHandler(className,
							HttpConstants.WS_HANDLER_METHOD_NEWREQUEST, args);
					if(log.isDebugEnabled()){
		        		log.debug(msg, "Used handlerclass:{},invoke method:{}",
							className ,HttpConstants.WS_HANDLER_METHOD_NEWREQUEST);
					}
				} else {
					//
				}
			} else if (msg.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				HttpPdu drReq = hi.getMoDRRequest();
				if (hi.getMoDRRequest().hasHandlerClass()) {
					String className = drReq.getHandlerClass();
					Object[] args = { msg, cst };
					resp = (GmmsMessage) hi.invokeHandler(className,
							HttpConstants.WS_HANDLER_METHOD_DRREQUEST, args);
					log.debug(msg, "Used handlerclass:{},invoke method:{}",
							className ,HttpConstants.WS_HANDLER_METHOD_DRREQUEST);
				} else {
				}
			} else if (msg.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				HttpPdu drReq = hi.getMtDRRequest();
				if (drReq.hasHandlerClass()) {
					String className = drReq.getHandlerClass();
					Object[] args = { msg, cst };
					resp = (GmmsMessage)  hi.invokeHandler(className,
							HttpConstants.WS_HANDLER_METHOD_DRREQUEST, args);
					log.debug(msg, "Used handlerclass:{},invoke method:{}",
							className ,HttpConstants.WS_HANDLER_METHOD_DRREQUEST);
				}	
			}else if(msg.getMessageType().equals(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)
					||msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT_RESP) ){
				if(!"".equalsIgnoreCase(cst.getHttpQueryMessageFlag())){					
					msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
					msg.setStatusCode(0);
					if (!putGmmsMessage2RouterQueue(msg)) {
						if (log.isInfoEnabled()) {
							log.info(msg, "Send response to core engine failed!");
						}
					}
					return true;
				}else{
					log.error(msg, "Unsupport message type:"+msg.getMessageType());
				}
			} else {
			log.error(msg, "Unsupport message type:"
					+ msg.getMessageType());
			}
		}catch(Exception e){
			log.warn(msg,e.getMessage());
		}
		if (resp == null) {
			if(log.isDebugEnabled()){
				log.debug(msg,"Get null response!");
			}
			if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
				if (msg.getOutMsgID() == null) {
					msg.setOutMsgID(msg.getMsgID());
				}
				resp=msg;
			}else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				msg.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
				if (msg.getOutMsgID() == null) {
					msg.setOutMsgID(msg.getMsgID());
				}
				resp=msg;
			}else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
				msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
				if (msg.getOutMsgID() == null) {
					msg.setOutMsgID(msg.getMsgID());
				}
				resp=msg;
			}
		}
		if (!putGmmsMessage2RouterQueue(resp)) {
			if(log.isInfoEnabled()){
				log.info(msg, "Send response to core engine failed!");
			}
		}
		return true;
	}
	 /**
     * put message to Router queue
     * @param msg
     */
    protected boolean putGmmsMessage2RouterQueue(GmmsMessage msg){
    	if(msg == null){
    		return false;
    	}
    	ModuleManager moduleManager = ModuleManager.getInstance();
    	InternalAgentConnectionFactory factory = InternalAgentConnectionFactory.getInstance();
    	String routerQueue = null;
    	String deliveryChannelQueue = null;
    	OperatorMessageQueue msgQueue = null;
    	if(GmmsMessage.MSG_TYPE_INNER_ACK.equalsIgnoreCase(msg.getMessageType())
    			||GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())
    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())
    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msg.getMessageType())){
    		TransactionURI innerTransaction = msg.getInnerTransaction();
    		if(innerTransaction == null){
    			if(log.isInfoEnabled()){
					log.info(msg,"Cannot get the inner transaction");
    			}
				return false;
    		}
    		routerQueue = innerTransaction.getConnectionName();
        	msgQueue = factory.getMessageQueue(msg, routerQueue);
        	if(log.isInfoEnabled()){
        		log.info(msg, "Send {} to {}",msg.getMessageType(), routerQueue);
        	}
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
        	if(log.isInfoEnabled()){
        		log.info(msg, "Send {} to {}",msg.toString(), deliveryChannelQueue);
        	}
    	}
    	
    	if(msgQueue == null){
        	log.warn(msg,"Can not find the alive delivery router");
        	msg.setDeliveryChannel(module);
    		return false;
    	}else{
    		msg.setDeliveryChannel(module+":"+deliveryChannelQueue);
    		return msgQueue.putMsg(msg);
    	}
    }
    
	@Override
	public boolean connect() {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * Query the batch MO messages
	 * 
	 * */
	@SuppressWarnings("unchecked")
	public void queryMORequest(){
		if(cst == null){
			log.error("Current web service session has no coresponding customer info.");
			return;
		}
		List<GmmsMessage> msgs = null;
		
		HttpPdu moRequest = hi.getMoSubmitRequest();
		GmmsMessage msg = null;
		if (moRequest.hasHandlerClass()) {
			String className = moRequest.getHandlerClass();
			Object[] args = {msg, cst };
			Object c = hi.invokeHandler(className,
					HttpConstants.WS_HANDLER_METHOD_PARSEMOLISTREQUEST, args);
			if (c !=null && c.getClass().getName().equals("java.util.ArrayList")) {
				msgs = (List<GmmsMessage>) c;
			}
			if (log.isDebugEnabled()) {
				log.debug("Get "+hi.getInterfaceName()+" MO Request messages used handlerclass:" + className
						+ ",invoke method:"
						+ HttpConstants.WS_HANDLER_METHOD_PARSEMOLISTREQUEST);
			}
		}else{
			log.warn(hi.getInterfaceName()+" MO Submit Request has no handler class.");
		}

		if (msgs == null) {
			return;
		} else {
			for (GmmsMessage message : msgs) {				
				if (!putGmmsMessage2RouterQueue(message)) {
					if (log.isInfoEnabled()) {
						log.info(message,
								"Send response to core engine failed!");
					}
				}
			}
		}		
	}
	
	
	/**
	 * Query the batch DR messages
	 * */
	@SuppressWarnings("unchecked")
	public void queryBatchDR(){
		if(cst == null){
			log.error("Current web service session has no coresponding customer info.");
			return;
		}
		List<GmmsMessage> msgs = null;
		
		HttpPdu mtRequest = hi.getMtDRRequest();
		
		if (mtRequest.hasHandlerClass()) {
			String className = mtRequest.getHandlerClass();
			GmmsMessage msg = null;
			Object[] args = { msg,cst };
			Object c = hi.invokeHandler(className,
					HttpConstants.WS_HANDLER_METHOD_PARSMTELISTREQUEST, args);
			if (c !=null && c.getClass().getName().equals("java.util.ArrayList")) {
				msgs = (List<GmmsMessage>) c;
			}
			if (log.isDebugEnabled()) {
				log.debug("Get "+hi.getInterfaceName()+"  MT Dr messages used handlerclass:" + className
						+ ",invoke method:"
						+ HttpConstants.WS_HANDLER_METHOD_PARSMTELISTREQUEST);
			}
		}else{
			log.warn(hi.getInterfaceName()+" MT Dr Request has no handler class.");
		}

		if (msgs == null) {
			return;
		} else {
			for (GmmsMessage message : msgs) {
				if (!putGmmsMessage2RouterQueue(message)) {
					if (log.isInfoEnabled()) {
						log.info(message,
								"Send response to core engine failed!");
					}
				}
			}
		}
		
	}
	

	@Override
	public boolean createConnection() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int enquireLink() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLastActivity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public OperatorMessageQueue getOperatorMessageQueue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSessionName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getSessionNum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ConnectionStatus getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TransactionURI getTransactionURI() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isFakeSession() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isKeepRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isServer() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setConnectionManager(ConnectionManager connManager) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setKeepRunning(boolean keepRunning) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSessionName(String connectionName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStatus(ConnectionStatus status) {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}


	@Override
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void connectionUnavailable() {
		// TODO Auto-generated method stub

	}

	@Override
	public void parse(ByteBuffer buffer) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean receive(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void initReceivers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateReceivers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initSenders() {
		// TODO Auto-generated method stub
		
	}

}
