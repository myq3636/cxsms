/**
 * Copyright 2000-2014 King Inc. All rights reserved.
 */
package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;

/**
 * Route the messages to backup channels, find rssid, ra2p
 * @author bensonchen
 * @version 1.0.0
 */
public class IOSMSBackupChannelRouter extends ChannelRouter {
	
	private static SystemLogger log = SystemLogger
			.getSystemLogger(IOSMSBackupChannelRouter.class);
	/**
	 * Constructor
	 */
	public IOSMSBackupChannelRouter() {
	}

	/**
	 * 
	 * @param msg
	 * @return
	 * @see com.king.gmms.routing.ChannelRouter#dispatch(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public boolean dispatch(GmmsMessage msg) {
		if (msg == null) {
			return false;
		}
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
			int rSsid = 0;
			int ossid = msg.getOSsID();
			String reRoutessid = msg.getRoutingSsIDs();
			rSsid = ctm.getCustomerBackupSenderRoutingRelay(reRoutessid,ossid, msg);
			if (rSsid>0) {
				log.info(msg,"get Rssid: {} by backup sender routingRelay",rSsid);
			}else{
				rSsid = ctm.getCustomerBackupContentKeywordRoutingRelay(reRoutessid,ossid, msg);
				if (rSsid>0) {
					log.info(msg,"get Rssid: {} by backup content keyword routingRelay",rSsid);
				}else {
					rSsid = ctm.getCustomerBackupRoutingRelay(reRoutessid,ossid, msg);
					if(log.isInfoEnabled()){
						log.info(msg,"get Rssid: {} by backup routingRelay",rSsid);
					}
				}
			}
			if(rSsid!=0) {
				A2PCustomerInfo rCustomer = ctm.getCustomerBySSID(rSsid);
				if(!rCustomer.isRSupportContentLenghtAllow(msg)) {
					log.info(msg,"msg deny by specail content len check for Rssid: {}, so set ssid {} to 0",rSsid, rSsid);
					rSsid=0;
				}
			}
			msg.setRSsID(rSsid);			
			return routebySsid(rSsid, msg);
		}
		return false;
		
	}
	
	@Override
	protected boolean routebySsid(int ssid, GmmsMessage msg) {
		if (ssid == 0 || ssid == -1) { // Dead channel
			return false;
		}
		
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
			//int nextA2P = ctm.getConnectedRelay(ssid, msg.getGmmsMsgType());
			int nextA2P = ctm.getCurrentA2P();
			if (nextA2P > 0) {
				msg.setRA2P(nextA2P);
				return true;
			}
		}
		return false;
	}
}
