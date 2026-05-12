package com.king.gmms.customerconnectionfactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.listener.commonhttpserver.ServletResponseParameter;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionReceiverMessageQueue;

public class CommonHttpServerFactory implements CustomerConnectionFactory {
	private Map<String,ServletResponseParameter> servletCache = null;
	protected HashMap<Integer, OperatorMessageQueue> ssid2messageQueues;
	protected A2PCustomerManager cim;
	private static CommonHttpServerFactory instance = null;
	private static SystemLogger log = SystemLogger.getSystemLogger(CommonHttpServerFactory.class);
    private GmmsUtility gmmsUtility = null;

	/**
	 * singleton mode
	 */
	private CommonHttpServerFactory(){
		servletCache = new ConcurrentHashMap<String,ServletResponseParameter>();
		
		gmmsUtility = GmmsUtility.getInstance();
		cim = gmmsUtility.getCustomerManager();
        ssid2messageQueues = new HashMap<Integer, OperatorMessageQueue> ();
	}
	
	public static synchronized CommonHttpServerFactory getInstance() {
		if(instance == null){
			instance = new CommonHttpServerFactory();
		}
        return instance;
    }
	
	public ShortConnectionReceiverMessageQueue constructOperatorMessageQueue(int ssid) {
		ShortConnectionReceiverMessageQueue queue = null;
		try{
	    	A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
	    	int minRecNum = info.getMinReceiverNumber();
			int maxRecNum = info.getMaxReceiverNumber();
			
			queue = new ShortConnectionReceiverMessageQueue(info, minRecNum, maxRecNum);
	        ssid2messageQueues.put(ssid, queue);
		}catch(Exception e){
			log.error(e,e);
		}
		return queue;
	}
	/**
	 * get servlet 
	 */
	public ServletResponseParameter getServletParam(String msgId){
		if(servletCache.containsKey(msgId)){
			return servletCache.remove(msgId);
		}
		return null;
	}
	/**
	 * put to cache
	 */
	public void putServletParam(String msgId,ServletResponseParameter servletParam){
		if(servletCache.containsKey(msgId)){
			return;
		}else{
			servletCache.put(msgId, servletParam);
		}
	}
	/**
	 * remove
	 */
	public void removeServlet(String msgId){
		if(servletCache.containsKey(msgId)){
			servletCache.remove(msgId);
		}
	}
	/**
	 * clear cache
	 * @return
	 */
	public void clearCache(){
		servletCache.clear();
	}
	//@Override
	public ConnectionManager getConnectionManager(int ssid, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	//@Override
	public ConnectionManager getConnectionManagerBySSID(int ssid) {
		// TODO Auto-generated method stub
		return null;
	}

	public OperatorMessageQueue getOperatorMessageQueue(int ssid) {
		OperatorMessageQueue queue = ssid2messageQueues.get(ssid);
		if(queue == null){
			queue = constructOperatorMessageQueue(ssid);
			return queue;
		}else{
			return queue;
		}
	}

	//@Override
	public void putConnectionManager(int ssid, String name,
			ConnectionManager ncm) {
		// TODO Auto-generated method stub
		
	}
}
