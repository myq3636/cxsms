/**
 * Author: frank.xue@King.com
 * Date: 2006-7-17
 * Time: 15:11:26
 * Document Version: 0.1
 */

package com.king.gmms.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.db.DatabaseStatus;
import com.king.framework.A2PService;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.SingletonSystemCommandListener;
import com.king.gmms.GmmsUtility;
import com.king.gmms.customerconnectionfactory.CustomerConnectionFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.SystemListener;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.throttle.ReportInMsgCountTimer;
import com.king.gmms.throttle.ResetDynamicCustInThresholdTimer;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.rest.util.StringUtility;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class AbstractHttpServer extends HttpServlet implements
		A2PService, Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractHttpServer.class);
	protected static GmmsUtility gmmsUtility;
	protected int cmdPort;
	protected static SingletonSystemCommandListener cmdListener;
	protected static InternalAgentListener agentListener = null;
	protected static InternalAgentConnectionFactory agentFactory = null;
	protected static Object mutex = new Object();
	protected String protocol = null;
	protected static volatile boolean initAgent = false;
	protected static volatile boolean isRunning = false;
	protected static int timeout = 10000;
	protected SystemListener systemListener = null;
	protected SystemSession systemSession = null; // system client
	protected boolean isEnableSysMgt = false;
    protected boolean canHandover = false;
	protected String moduleName;
	protected static Map<String, Integer> urlMap = null;
	protected static int httpConnectionNumberMaximum = 100;
	protected static Map<String, Integer> httpCustomConnectionNumMap = null;
	protected ReportInMsgCountTimer reportInMsgCountTimer = null;
	protected ResetDynamicCustInThresholdTimer resetDynamicCustInThresholdTimer = null;

	private static String comma = ",";
	private static String semi = ";";

	public void init() {
		try {
			urlMap = new ConcurrentHashMap<String, Integer>();
			httpCustomConnectionNumMap = new ConcurrentHashMap<String, Integer>();
			protocol = getServletConfig().getInitParameter("protocol");
			String a2phome = System.getProperty("a2p_home");
			if (!a2phome.endsWith("/")) {
				a2phome = a2phome + "/";
			}
			moduleName = System.getProperty("module");
			gmmsUtility = GmmsUtility.getInstance();
			gmmsUtility.initUtility(a2phome + "Gmms/GmmsConfig.properties");
			gmmsUtility.initCDRManager();
			//gmmsUtility.initRedisClient("M");
			ModuleConnectionInfo cinfo = gmmsUtility.getModuleManager().getConnectionInfo(moduleName);
			cmdPort = cinfo.getCmdPort();
			log.trace("a2p_home/module/cmdPort:{}",a2phome,moduleName,cmdPort);
			timeout = Integer.parseInt(gmmsUtility.getModuleProperty(
					"ServletTimeout", "10000"));
			try {
				httpConnectionNumberMaximum = Integer.parseInt(gmmsUtility
						.getCommonProperty("HttpConnectionMaxNumber", "100"));
			} catch (Exception e) {
				log.error(e, e);
			}
			if (httpConnectionNumberMaximum <= 0) {
				httpConnectionNumberMaximum = 100;
			}
			log.debug("httpConnectionMaxNumber is: {}",	httpConnectionNumberMaximum);
			isEnableSysMgt = gmmsUtility.isSystemManageEnable();
			canHandover = gmmsUtility.isDBHandover();
			if (canHandover || isEnableSysMgt) {
				systemListener = SystemListener.getInstance();
				try {
					SystemSessionFactory sysFactory = SystemSessionFactory
							.getInstance();
					systemSession = sysFactory.getSystemSessionForFunction();
					reportInMsgCountTimer = new ReportInMsgCountTimer(systemSession, 
							gmmsUtility.getReportModuleIncomingMsgCountInterval());
					resetDynamicCustInThresholdTimer = 
						new ResetDynamicCustInThresholdTimer(gmmsUtility.getDynamicCustInThresholdExipreTime()/3);
				} catch (Exception e) {
					log.warn(e, e);
				}
			}
			String customIp = gmmsUtility.getCommonProperty("HttpCustomConnectionNumber");
			if (customIp != null) {
				String[] list = customIp.split(semi);
				if (list != null) {
					for (int i = 0; i < list.length; i++) {
						String[] slist = list[i].split(comma);
						if (slist != null && slist.length == 2
								&& slist[0] != null && slist[1] != null) {
							String ip = slist[0].trim();
							Integer count = null;
							try {
								count = Integer.parseInt(slist[1].trim());
							} catch (Exception e) {
								log.error(e, e);
							}

							if (count != null && count > 0) {
								log
										.debug(
												"custom ip url: {}, and max number: {}",
												ip, count);
								httpCustomConnectionNumMap.put(ip, count);
							}
						}
					}
				}
			}

		} catch (Exception ex) {
			log.error("Error when init HttpServer:", ex);
		}
	}

	/**
	 * start service
	 * 
	 * @param className
	 * @return
	 */
	public boolean startService() {
		if (isRunning) {
			return true;
		}
		try {
			DatabaseStatus dbstatus = DatabaseStatus.MASTER_USED;
			String redisStatus = "M"; // V4.0 Default to Master, synchronized via Redis Pub/Sub
			/*
			 * V4.0 微服务化：关闭旧式 TCP SystemListener，由 RedisControlSubscriber 接管 
			if ((canHandover ||isEnableSysMgt) && !systemListener.isRunning()) {
				systemListener.start();
				if (systemSession != null) {
					ModuleRegisterAck ack = systemSession.moduleRegisterInDetail();
		        	if(ack!=null){
		        		String dbstatusStr = ack.getDbStatus();
		        		dbstatus = DatabaseStatus.get(dbstatusStr);
		        		redisStatus = ack.getRedisStatus();
		        	}
					reportInMsgCountTimer.startTimer("reportInMsgCountTimer");
					resetDynamicCustInThresholdTimer.startTimer("resetDynamicCustInThresholdTimer");
				}
			}
			*/
			gmmsUtility.initRedisClient(redisStatus);
			gmmsUtility.initDBManager(dbstatus);
			System.out.println("Starting service:"
					+ System.getProperty("module") + "...");
			new Thread(A2PThreadGroup.getInstance(), this).start();
			System.out.println("Start service success!");
			isRunning = true;
			return true;
		} catch (Exception e) {
			System.out.println("Start service fail!");
			e.printStackTrace();
			return false;
		}
	}

	public void run() {
		try{
			if(!SingletonSystemCommandListener.isRunning()){
				SingletonSystemCommandListener.setPort(cmdPort);
				cmdListener = SingletonSystemCommandListener.startCommandListener();
			}
		}catch(Exception e){
			log.warn(e.getMessage());
		}
	}

	/**
	 * start agent message queue and listener
	 */
	protected void startAgentConnection(CustomerConnectionFactory factory) {
		if (initAgent) {
			return;
		}
		agentFactory = InternalAgentConnectionFactory.getInstance();
		agentFactory.setCustomerFactory(factory);
		ModuleManager moduleManager = ModuleManager.getInstance();
		List<String> moduleNameList = moduleManager.getRouterModules();
		if (moduleNameList != null) {
			for (String routerModuleName : moduleNameList) {
				agentFactory.initInternalConnectionFactory(routerModuleName);
			}
		}
		ModuleConnectionInfo cinfo = moduleManager.getConnectionInfo(moduleName);
		int inSessionNum = cinfo.getInSessionNum();
		if(inSessionNum>0){
			agentListener = InternalAgentListener.getInstance();
			agentListener.start();
		}
		initAgent = true;
	}

	/**
	 * put message to Router queue
	 * 
	 * @param msg
	 */
	protected boolean putGmmsMessage2RouterQueue(GmmsMessage msg) {
		if (msg == null) {
			return false;
		}
		ModuleManager moduleManager = ModuleManager.getInstance();
		InternalAgentConnectionFactory factory = InternalAgentConnectionFactory
				.getInstance();
		String routerQueue = null;
		String deliveryChannelQueue = null;
		OperatorMessageQueue msgQueue = null;
		if (GmmsMessage.MSG_TYPE_INNER_ACK.equalsIgnoreCase(msg
				.getMessageType())) {
			TransactionURI innerTransaction = msg.getInnerTransaction();
			if (innerTransaction != null) {
				routerQueue = innerTransaction.getConnectionName();
				msgQueue = factory.getMessageQueue(msg, routerQueue);
				deliveryChannelQueue = routerQueue;
			} else {
				return false;
			}
		} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(msg.getMessageType())) {
			TransactionURI transaction = msg.getInnerTransaction();
			if (transaction != null) {
				routerQueue = transaction.getConnectionName();
				msgQueue = factory.getMessageQueue(msg, routerQueue);
			}
			deliveryChannelQueue = routerQueue;
		} else {
			routerQueue = moduleManager.selectRouter(msg);
			msgQueue = factory.getMessageQueue(msg, routerQueue);
			deliveryChannelQueue = routerQueue;
			if (msgQueue == null) {
				String aliveRouterQueue = moduleManager.selectAliveRouter(routerQueue, msg);
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
		if(log.isInfoEnabled()){
			log.info(msg, "Send {} to {}",msg.getMessageType(), deliveryChannelQueue);
		}		
		if (msgQueue == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "Can not find the alive delivery router");
			}
			msg.setDeliveryChannel(moduleName);
			return false;
		} else {
			msg.setDeliveryChannel(moduleName+":"+deliveryChannelQueue);
			return msgQueue.putMsg(msg);
		}
	}

	/**
	 * stop service
	 * 
	 * @param className
	 * @return
	 */
	public boolean stopService() {
		try {
			if (canHandover ||isEnableSysMgt) {
				beforeStop();
				systemListener.stop();
				if (systemSession != null) {
					systemSession.shutdown();
				}
			}
			if(cmdListener!=null){
				cmdListener.stopService();
			}
			if(agentListener!=null){
				agentListener.stop();
			}
			
			if (log.isInfoEnabled())  {
				log.info("AbstractHttpServer stopService");
			}
            
			return true;
		} catch (Exception e) {
			System.out.println("Stop service fail!");
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void destroy() {
		gmmsUtility.close();
		stopService();
	}

	/**
	 * send stop request
	 */
	public void beforeStop() {
		if (canHandover || isEnableSysMgt) {
			reportInMsgCountTimer.stopTimer();
			resetDynamicCustInThresholdTimer.stopTimer();
			if(systemSession != null){
				systemSession.moduleStop();
			}
		}
	}

	public abstract void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException;

	public void doGet(HttpServletRequest request, HttpServletResponse response) {		
		try {
			processRequest(request, response);
		} catch (Exception e) {
			log.error(e, e);
			this.response(request, response, 500);
		}
		/*String url = this.getIpAddr(request);
		log.debug("Access IP url = {}", url);
		if (urlMap == null) {
			log.error("urlMap is null");
			urlMap = new ConcurrentHashMap<String, Integer>();
		}
		int urlCount = 0;
		if (url != null) {
			if (urlMap.get(url) != null) {
				urlCount = urlMap.get(url);
				log.debug("access IP submit count: {}", urlCount);
				urlMap.put(url, urlMap.get(url) + 1);
			} else {
				urlMap.put(url, 1);
			}
		}
		Integer index = null;
		if (httpCustomConnectionNumMap != null) {
			index = httpCustomConnectionNumMap.get(url);
		}
		if (index != null && urlCount < index) {

			log.debug(
					"{} access limit by HttpCustomConnectionNumber parameter.",
					url);
			try {
				processRequest(request, response);
			} catch (Exception e) {
				log.error(e, e);
				this.response(request, response, 500);
			}
		} else if (index == null && urlCount < httpConnectionNumberMaximum) {

			log.debug("{} access limit by HttpConnectionMaxNumber parameter.",
					url);
			try {
				processRequest(request, response);
			} catch (Exception e) {
				log.error(e, e);
				this.response(request, response, 500);
			}
		} else {
			log
					.debug(
							"a2p reject the url {} access, and the current connection number is {}",
							url, urlCount);
			this.response(request, response, 403);
		}
		if (urlMap.get(url) != null) {

			log.debug("access IP dec count: {}", urlMap.get(url));

			if (urlMap.get(url) >= 1) {
				urlMap.put(url, urlMap.get(url) - 1);
			} else {
				urlMap.put(url, 0);
			}

		}*/
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		this.doGet(request, response);
	}

	private String getIpAddr(HttpServletRequest request) {

		String ip = request.getHeader("x-forwarded-for");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		} else {
			if (ip.indexOf(",") != -1) {
				ip = ip.substring(0, ip.indexOf(","));
			}
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}

		return ip;
	}

	public void response(HttpServletRequest request,
			HttpServletResponse response, int resp) {
		response.setStatus(resp);
	}
	
	public String makeResponse(GmmsMessage msg) {
		 switch (msg.getStatusCode()) {
        case -1:
            return "OK";
        case -2:
            return "";
        case 0:
            return "OK";
        case 2100:
        	return "Error:Invalid Message Field";
        case 2110:
        	return "Error:Sender Address Error";        
        case 2120:
        	return "Error:Recipient Address Error";
        case 2122:
        	return "Error:Recipient Address Over Max Count";
        case 2130: 
        	return "Error:Invalid Message Format";
        case 2200:
        	return "Error:Message Policy Denied";
        case 2600:
        	return "Error:Message Throttling Reject";
        case 5000:
        	return "Error:Invalid product token";
        case 5100:
        	return "Error:Insufficient balance";
        case 9000:
        	return "Error:ERROR";
        default:
        	return "Error:ERROR";
   
		 }
	}
	
	public String makeJsonResponse(GmmsMessage msg) {
		switch (msg.getStatusCode()) {
	      case -1:
	          return "\"status\":\"OK\"";
	      case -2:
	          return "";
	      case 0:
	          return "\"status\":\"OK\"";
	      case 2100:
	      	   return "\"status\":\"Error\",\"reason\":\"Invalid Message Field\"";
	      case 2110:
	      	   return "\"status\":\"Error\",\"reason\":\"Sender Address Error\"";        
	      case 2120:
	      	   return "\"status\":\"Error\",\"reason\":\"Recipient Address Error\"";
	      case 2122:
	      	   return "\"status\":\"Error\",\"reason\":\"Recipient Address Over Max Count\"";
	      case 2130: 
	      	   return "\"status\":\"Error\",\"reason\":\"Invalid Message Format\"";
	      case 2200:
	      	   return "\"status\":\"Error\",\"reason\":\"Message Policy Denied\"";
	      case 2600:
	          return "\"status\":\"Error\",\"reason\":\"Message Throttling Reject\"";
	      case 5000:
	      	   return "\"status\":\"Error\",\"reason\":\"Invalid product token\"";
	      case 5100:
	      	   return "\"status\":\"Error\",\"reason\":\"Insufficient balance\"";
	      case 9000:
	      	   return "\"status\":\"Error\",\"reason\":\"Unknown ERROR\"";
	      default:
	      	   return "\"status\":\"Error\",\"reason\":\"Unknown ERROR\"";
	 
			 }
	}
	
	public String makeAliCainiaoResponse(GmmsMessage msg) {		 		 
		 switch (msg.getStatusCode()) {       
	       case 0:
	           return "\"code\":\"OK\",\"message\":\"请求成功\"";
	       case 2100:
	       	   return "\"code\":\"Invalid.Parameter\",\"message\":\"参数非法\"";
	       case 2110:
	       	   return "\"code\":\"Invalid.Parameter\",\"message\":\"参数非法\"";        
	       case 2120:
	       	   return "\"code\":\"Invalid.Parameter\",\"message\":\"参数非法\"";
	       case 2122:
	       	   return "\"code\":\"Request.Frequent\",\"message\":\"请求过多\"";
	       case 2130: 
	       	   return "\"code\":\"Invalid.Parameter\",\"message\":\"参数非法\"";
	       case 2200:
	       	   return "\"code\":\"Invalid.Parameter\",\"message\":\"参数非法\"";
	       case 2600:
	           return "\"code\":\"Flow.Limit\",\"message\":\"业务流控\"";
	       case 5000:
	       	   return "\"code\":\"Verification.Fail\",\"message\":\"认证未通过\"";       
	       default:
	       	   return "\"code\":\"System.Error\",\"message\":\"系统错误\"";
	  
			 }
	}
	
	protected boolean checkIncomingThrottlingControl(int ssid, GmmsMessage msg) {
		boolean ret = false;
		
		if(!StringUtility.stringIsNotEmpty(GmmsUtility.getInstance().getRedisClient().getString("thcon"))){    		
        	return ret;
    	}
		
		if (ThrottlingControl.getInstance().isAllowedToReceive(ssid)) {
			ret = true;
		} else {
			if (log.isInfoEnabled()) {
				log.info(msg, "refuced by incoming throttling control");
			}
		}
		return ret;
	}
}