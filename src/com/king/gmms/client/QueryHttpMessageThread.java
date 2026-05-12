package com.king.gmms.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.connectionpool.session.CommonHttpSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.WebServiceSession;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionHttpConfirm;
import com.king.gmms.ha.systemmanagement.pdu.QueryHttpAck;
import com.king.gmms.ha.systemmanagement.pdu.QueryHttpRequest;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.message.gmms.GmmsMessage;

public class QueryHttpMessageThread extends Thread {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(QueryHttpMessageThread.class);

	protected volatile boolean running;

	private A2PCustomerInfo ci = null;
	private Session session = null;
	private int queryInterval = 60*1000;
	private Object sync = new Object();

	private Map<String, SystemPdu> bufferMap = null;
	private Map<String, String> queryFlagMap = null;
	private boolean isWs = false;
	private boolean isMo = false;
	private boolean isDr = false;
	private SystemSessionFactory sysFactory;
	private SystemSession systemSession;
	private CommonHttpClientFactory commHttpClientFactory;
    private String queryFlag = "";
	public boolean isWs() {
		return isWs;
	}

	public void setWs(boolean isWs) {
		this.isWs = isWs;
	}

	public boolean isMo() {
		return isMo;
	}

	public void setMo(boolean isMo) {
		this.isMo = isMo;
	}

	public boolean isDr() {
		return isDr;
	}

	public void setDr(boolean isDr) {
		this.isDr = isDr;
	}

	/**
	 * @see java.lang.Runnable#run()
	 */
	public void run() {

		if(isMo){
			queryFlag = "MO";
		}else if(isDr){
			queryFlag = "MTDR";
		}
		while (running) {
			try {
				Thread.sleep(queryInterval);
				//whether system support HA Model;
				boolean isEnableSysMgt = false;
				boolean isqueryFlag = false;
				String queryMinIDKey = ci.getShortName()+"_"+queryFlag;
				if(GmmsUtility.getInstance().isSystemManageEnable()){
					if(applyHttpQueryFlag()){
						isEnableSysMgt = true;
						isqueryFlag = true;
					}
				}else{
					isqueryFlag = true;
					String minID = readFile(queryMinIDKey);
					if(minID!=null){
						this.commHttpClientFactory.setQueryMinID(queryMinIDKey, minID);
					}
					
				}
				if (isqueryFlag) {
					if (isWs) {
						synchronized (sync) {
							if (isMo) {
								((WebServiceSession) session).queryMORequest();
							} else if (isDr) {
								((WebServiceSession) session).queryBatchDR();
							}
						}
					} else {
						GmmsMessage msg = new GmmsMessage();
						msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY);
						msg.setMsgID(this.commHttpClientFactory.getQueryMinID().get(queryMinIDKey));
						if (log.isInfoEnabled()) {
							log.info("ssid: {} submit DR request:{}",ci.getSSID(), msg.getMsgID());
						}
						synchronized (sync) {
							((CommonHttpSession) session).queryMessage(msg);
						}
					}
					if(isEnableSysMgt){
						this.connectionHttpConfirm();
					}else if(this.commHttpClientFactory.getQueryMinID().get(queryMinIDKey)!=null){
						writeFile(queryMinIDKey,this.commHttpClientFactory.getQueryMinID().get(queryMinIDKey));
					}					
				}

			} catch (Exception e) {
				log.warn(e, e);
			}
		}
		if (log.isInfoEnabled()) {
			log.info(Thread.currentThread().getName() + " stopped");
		}
	}

	public QueryHttpMessageThread(A2PCustomerInfo ci) {
		super();
		this.ci = ci;
		if (HttpConstants.HTTP_METHOD_WEBSERVICE.equalsIgnoreCase(ci
				.getHttpMethod())) {
			session = new WebServiceSession(ci);
			this.setWs(true);
		} else {
			session = new CommonHttpSession(ci);
		}

		queryInterval = ci.getMsgQueryInterval();
		if (queryInterval == 0) {
			queryInterval = 60*1000;
		}
		running = true;
		sysFactory = SystemSessionFactory.getInstance();
		systemSession = sysFactory.getSystemSessionForFunction();
		commHttpClientFactory = CommonHttpClientFactory.getInstance();
		bufferMap = new ConcurrentHashMap<String, SystemPdu>();
		queryFlagMap = new ConcurrentHashMap<String, String>();
		
	}

	public boolean isRunning() {
		return running;
	}

	public synchronized void stopThread() {
		running = false;
		try {
			interrupt();
		} catch (Exception e) {
		}
	}

	private boolean applyHttpQueryFlag() {		
		QueryHttpRequest qhr = new QueryHttpRequest();
		qhr.setQueryMethod(queryFlag);
		qhr.setSsid(this.ci.getSSID());	
		QueryHttpAck qha = (QueryHttpAck)systemSession.queryFlag(qhr);		
		if (qha != null) {	
			String value = qha.getValue();
			if (value == null || value.contains("false")) {
				return false;
			} else if (value.contains("true")) {
				String minID[] = value.split(":");
				if (minID.length == 2) {
					commHttpClientFactory.setQueryMinID(
							ci.getShortName()+"_"+queryFlag, minID[1]);
				}
				return true;
			}
		}
		return false;
	}
	private void connectionHttpConfirm(){
		ConnectionHttpConfirm chc = new ConnectionHttpConfirm();	
		chc.setQueryMethod(queryFlag);
		chc.setSsid(this.ci.getSSID());
		String value = "true:"+commHttpClientFactory.getQueryMinID().get(ci.getShortName()+"_"+queryFlag);
		chc.setValue(value);
		try {
			this.systemSession.send(chc);
		} catch (IOException e) {
			log.error("send ConnectionHttpConfirm message error!");
		}
	}
	
	private String readFile(String path) {
		String interffaceConfig = System.getProperty("a2p_home") + "ha/" + path;
		File file = new File(interffaceConfig);
		String minId = "0";
		BufferedReader bw = null;
		try {
			bw = new BufferedReader(new FileReader(file));
			minId = bw.readLine();
		} catch (IOException e) {
			log.error("file is not exist and Exception is {}", e.getMessage());
			return "0";
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (Exception e) {

				}
			}
		}
		return minId;
	}
	
	private boolean writeFile(String path, String nextId) {
		String interfaceConfig = System.getProperty("a2p_home") + "ha/" + path;
		File file = new File(interfaceConfig);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				log.error("file create error");
				return false;
			}
		}
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(file);
			fileWriter.write(nextId);
			fileWriter.flush();
		} catch (IOException e) {
			String mailSubject = "A2P alert mail for"+ path +"interface";
			String mailText = "IOException nextId can not write to file"
					+ e.getMessage();
			log
					.error(
							"IOException nextId can not write to file and Exception is {}",
							e.getMessage());
			MailSender.getInstance().sendAlertMail(mailSubject, mailText, null);
			return false;
		} finally {
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (Exception e) {

				}
			}
		}
		return true;
	}
	
	public static void main(String[] args) {
		String s = "ture:"+null;
	}

}
