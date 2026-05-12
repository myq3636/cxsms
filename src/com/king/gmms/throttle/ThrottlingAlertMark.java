package com.king.gmms.throttle;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.ModuleURI;

/**
 * <p>
 * Title: ThrottlingAlertMark
 * </p>
 * <p>
 * Description: store the alert mark of throttling
 * </p>
 * <p>
 * Copyright: Copyright (c) 2001-2012
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version
 * @author bensonchen
 */
public class ThrottlingAlertMark {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ThrottlingAlertMark.class);
	/**
	 * consecutive num of throttling control time window
	 */
	private int consecutiveNum;

	/**
	 * num of already sent alert mail
	 */
	private int alertMailNum;

	/**
	 * the time of throttling control in last window
	 */
	private long lastWindowFirstControlTimeMillis;
	
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private A2PCustomerManager custManager = null;
	private String hostAddress;
	
	private Object mutex = new Object();
	
	public ThrottlingAlertMark() {
		consecutiveNum = 0;
		alertMailNum = 0;
		lastWindowFirstControlTimeMillis = 0;
		custManager = gmmsUtility.getCustomerManager();
		hostAddress = ModuleURI.self().getAddress();
	}
	
	/**
	 * process sending alert mail when throttling control occurs
	 * 
	 * @param ssid
	 * @param isIncoming
	 */
	public void processAlertMail(int ssid, boolean isIncoming, long slingWinStartTime, int currentThrottleNum) {
		long throttlingTime = System.currentTimeMillis();
		boolean sendAlert = false;
		synchronized(mutex) {
			// if is init state
			if (0 == lastWindowFirstControlTimeMillis) {
				// init state
				consecutiveNum =1;
				lastWindowFirstControlTimeMillis = slingWinStartTime;
				if (log.isTraceEnabled()) {
			 		log.trace("Ssid: {} ThrottlingAlertMark init, slingWinStartTime={}", ssid, slingWinStartTime);
				}
				return;
			}

			long timeSpace = throttlingTime - lastWindowFirstControlTimeMillis;

			// set consecutive num and alert mail num
			if (timeSpace < ThrottlingTimemark.Period) {
				return;
			} else if ((timeSpace >= ThrottlingTimemark.Period)
					&& (timeSpace < 2 * ThrottlingTimemark.Period)) {
				// throttling control is consecutive, increase consecutiveNum
				consecutiveNum++;
				lastWindowFirstControlTimeMillis += ThrottlingTimemark.Period;
			} else if (timeSpace >= 2 * ThrottlingTimemark.Period) {
				// throttling control is not consecutive ,so reset consecutive num
				// and alert mail num
				consecutiveNum =1;
				alertMailNum = 0;
				lastWindowFirstControlTimeMillis = slingWinStartTime;

				if (log.isTraceEnabled()) {
					log.trace("Ssid: {} ThrottlingAlertMark reset occurs. timeSpace={}, slingWinStartTime={}",
							ssid, timeSpace, slingWinStartTime);
				}

				return;
			}

			if ((consecutiveNum >= gmmsUtility.getConSecThrottleWinNumToAlert())
					&& (alertMailNum < gmmsUtility.getMaxThrottleAlertMailNum())) {
				sendAlert = true;
				alertMailNum++;
				// reset consecutiveNum to initial state
				consecutiveNum = 1;
			}
		}
		
		if (sendAlert) {
			// make alert mail content and subject
			StringBuilder mailTextBuffer = new StringBuilder();
			StringBuilder mailSubject = new StringBuilder();
			A2PCustomerInfo server = custManager.getCustomerBySSID(ssid);
			String shortName = server.getShortName();
			String moduleName = System.getProperty("module");
			if (isIncoming) {
				int configThrottlingNum = server.getIncomingThrottlingNum();
				int defaultThrottlingNum = gmmsUtility.getDefaultCustIncomingThreshold();
				mailTextBuffer.append(shortName).append(" refused by Incoming Throttling Control. Ssid=")
						.append(ssid).append(", SMSOptionIncomingThrottlingNum=").append(configThrottlingNum)
						.append(", DefaultCustIncomingThreshold=").append(defaultThrottlingNum)
						.append(", current throttlingNum=").append(currentThrottleNum);
				
				mailSubject = mailSubject.append("A2P alert mail from ")
				                         .append(hostAddress)
				                         .append(" ")
				                         .append(moduleName != null ? moduleName : "")
				                         .append(" for Incoming Throttling Control: ")
				                         .append(shortName);
			} else {
				int configThrottlingNum = server.getOutgoingThrottlingNum();
				mailTextBuffer.append(shortName).append(" refused by Outgoing Throttling Control. Ssid=")
						.append(ssid).append(", SMSOptionOutgoingThrottlingNum=").append(configThrottlingNum);
				
				mailSubject = mailSubject.append("A2P alert mail from ")
				                         .append(hostAddress)
				                         .append(" ")
				                         .append(moduleName != null ? moduleName : "")
				                         .append(" for Outgoing Throttling Control: ")
				                         .append(shortName);
			}

			// send alert mail
			mailTextBuffer.append(", AlertMailNumId=").append(alertMailNum).append(".");
			String mailText = mailTextBuffer.toString();
			log.warn(mailText);
			MailSender.getInstance().sendAlertMail(mailSubject.toString(), mailText, null);
		}
	}

}