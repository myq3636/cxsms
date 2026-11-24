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
import com.king.gmms.connectionpool.session.CommonHttpSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.WebServiceSession;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpConstants;
import com.king.message.gmms.GmmsMessage;

public class QueryHttpMessageThread extends Thread {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(QueryHttpMessageThread.class);

	protected volatile boolean running;

	private A2PCustomerInfo ci = null;
	private Session session = null;
	private int queryInterval = 60*1000;
	private Object sync = new Object();

	private boolean isWs = false;
	private boolean isMo = false;
	private boolean isDr = false;
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
				boolean isqueryFlag = true;
				String queryMinIDKey = ci.getShortName()+"_"+queryFlag;
				// the query position of this host (ha/{shortName}_{flag})
				String minID = readFile(queryMinIDKey);
				if(minID!=null){
					this.commHttpClientFactory.setQueryMinID(queryMinIDKey, minID);
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
					if(this.commHttpClientFactory.getQueryMinID().get(queryMinIDKey)!=null){
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
		commHttpClientFactory = CommonHttpClientFactory.getInstance();
		
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
			log
					.error(
							"IOException nextId can not write to file and Exception is {}",
							e.getMessage());
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
