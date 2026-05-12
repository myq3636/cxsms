package com.king.gmms.customerconnectionfactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.client.QueryHttpMessageThread;
import com.king.gmms.connectionpool.sessionfactory.CommonHttpSessionFactory;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.messagequeue.CustomerMessageQueue;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;
import com.king.message.gmms.GmmsMessage;
import com.king.gmms.messagequeue.HttpClientConsumer;

public class CommonHttpClientFactory extends ShortConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpClientFactory.class);
	private static CommonHttpClientFactory instance = new CommonHttpClientFactory();
	
	private ConcurrentHashMap<Integer,QueryHttpMessageThread[]> queryThreads = null;
	private ConcurrentHashMap<String,String> queryMinID = null;  //save queryID,  value type: <shortName:queryID>
	private HttpClientConsumer clientConsumer = null;
	private CommonHttpClientFactory() {
		super();
		queryThreads = new ConcurrentHashMap<Integer,QueryHttpMessageThread[]>();
		queryMinID = new ConcurrentHashMap<String,String>();
		isServer = false;
		
		// V4.1 Initialize and start the Redis Stream consumer for outbound tasks
		clientConsumer = new HttpClientConsumer();
		clientConsumer.start();
	}

	protected void startOperatorMessageQueue(OperatorMessageQueue queue,
			int ssid) {

		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		if (info != null) {
			try {
				SessionFactory factory = new CommonHttpSessionFactory(info);
				((ShortConnectionCustomerMessageQueue) queue)
						.startMessageQueue(factory);
			} catch (Exception ex) {
				log.debug(ex, ex);
			}
		} else {

			log.debug("get not get customer by ssid:{}", ssid);

		}

	}
	
	public void initQueryMsgThread(){
		ArrayList<Integer> alSsid = cim.getSsidByQueryMsg();
		this.startQueryMessageThread(alSsid);
	}
	
    private void startQueryMessageThread(ArrayList<Integer> alSsid){    	
    	 	               
         if (alSsid != null && alSsid.size() > 0) {
             A2PCustomerInfo ci;
             for (int i = 0; i < alSsid.size(); i++) {
                 int ssid = alSsid.get(i);	                    	                                      
                 ci = gmmsUtility.getCustomerManager().getCustomerBySSID(
                     ssid);               
                 String queryMsgFlag = ci.getHttpQueryMessageFlag();                 
                 if(!"".equalsIgnoreCase(queryMsgFlag) && ( ( (A2PSingleConnectionInfo) ci).isChlInit()) && cim.inCurrentA2P(cim.getConnectedRelay(ssid,GmmsMessage.AIC_MSG_TYPE_TEXT))){
                 	QueryHttpMessageThread[] queryMsgThread = new QueryHttpMessageThread[2];
                 	 /**
                 	 *  queryMsgFlag configure item value:	                    	 
						 *	MtDr: new QueryHttpMessageThread for query MT dr message  
						 *	Mo: new QueryHttpMessageThread for query MO request message
						 *	Both: new 2 QueryHttpMessageThread for query MO request and MT dr message 
						 *  WemediaDr: new QueryHttpMessageThread for query dr message in Wemedia operator				 
                 	 * */
                 	String module = System.getProperty("module");
                    String drModule = gmmsUtility.getCommonProperty("SMSQueryDRHttpModule", "CommonHttpClient");                                  	
                 	if("SouthDR".equalsIgnoreCase(queryMsgFlag)){ 
                 		if(!gmmsUtility.isSystemManageEnable()&&!module.equalsIgnoreCase(drModule)){
                 			continue;
                 		}
                 		queryMsgThread[0] = new QueryHttpMessageThread(ci);
                 		queryMsgThread[0].setDr(true);
                 	}else {
                 		queryMsgThread[0] = new QueryHttpMessageThread(ci);	                    		
                 		if(queryMsgFlag.equalsIgnoreCase("Mo")){
                 			queryMsgThread[0].setMo(true);
             			}else if(queryMsgFlag.equalsIgnoreCase("MtDr")){
             				queryMsgThread[0].setDr(true);
             			}else if(queryMsgFlag.equalsIgnoreCase("Both")){
             				queryMsgThread[0].setMo(true);
             				queryMsgThread[1] = new QueryHttpMessageThread(ci); 
             				queryMsgThread[1].setDr(true);
             				queryMsgThread[1].setWs(true);
             			}else{
             				log.warn("it is an invalid configuration for SMSOptinIsSupportHttpQueryMessage item.");
             				queryMsgThread[0] = null;
             			}
                 	}
                 	
                 	if(log.isInfoEnabled()){
     					log.info("queryDR thread init success by ssid: {}", ssid);
                 	}
                 	for(QueryHttpMessageThread thread:queryMsgThread){
                 		if(thread != null){
	                    		new Thread(A2PThreadGroup.getInstance(), thread, "QueryHttpMessageThread_"+ssid).start();
	                    	}	
                 	}
                 	this.queryThreads.put(ssid, queryMsgThread);
                 }
                 
             }
         } //end of size > 0
         else {
         		log.info("No client is queryDR started directly.");
         }              
    }
    
    public void reloadQueryMessageThread(int ssid){
    	ArrayList<Integer> ssids = new ArrayList<Integer>();
    	ssids.add(ssid);
    	startQueryMessageThread(ssids);
    }
    
    public void clearThreads(int ssid){
    	QueryHttpMessageThread[] queryMsgThread = queryThreads.remove(ssid);
    	if(queryMsgThread!=null&&queryMsgThread.length>0){
    		for(int i =0;i<queryMsgThread.length;i++){
    			if(queryMsgThread[i]!=null){
    				queryMsgThread[i].stopThread();
    			}
    			
    		}
    	}
    }
    
	public ConcurrentHashMap<String, String> getQueryMinID() {
		return queryMinID;
	}

	public void setQueryMinID(String shortName, String value) {
		this.queryMinID.put(shortName, value);
	}

	public static CommonHttpClientFactory getInstance() {
		return instance;
	}

}
