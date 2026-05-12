/**
 * Copyright 2000-2014 King Inc. All rights reserved.
 */

package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * Rereoute message dispatcher
 * @author bensonchen
 * @version 1.0.0
 */
public class IOSMSRerouteDispatcher extends MessageDispatcher {

	private static SystemLogger log = SystemLogger.getSystemLogger(IOSMSRerouteDispatcher.class);
	
	private ChannelRouter backupChannelRouter = null;
	private MessageProcessorHandler msgProcessorhandler = null;

	/**
	 * Constructor
	 */
	public IOSMSRerouteDispatcher() {
		super();
		try {
			super.channelRouter = new IOSMSChannelRouter();
			backupChannelRouter = new IOSMSBackupChannelRouter();
			msgProcessorhandler = MessageProcessorHandler.getInstance();
		} catch (Exception ex) {
			log.fatal("Fail to init IOSMSBackupDispatcher", ex);
		}
	}

	public RouteResponse dispatch(GmmsMessage msg) {
		// already know rop 
		 
		// already done pc.policyControl

		if (!reroute(msg)) {
			return RouteResponse.RouteFailed;
		}

		return RouteResponse.RouteOK;

	}
	
	/**
	 * reroute process
	 * @param message
	 * @return true: reroute success </br>
	 *         false: reroute failed or not need reroute, use the legacy process in caller
	 */
	private boolean reroute(GmmsMessage message) {
		if ((message.getSplitStatus() <= 0 // not split msg
				||message.getSplitStatus()>0 && message.getSarSegmentSeqNum()==1)
				&& (!(message.getStatus().equals(GmmsStatus.SUCCESS) // statusCode need reroute
						|| message.getStatus().equals(GmmsStatus.SUBMIT_RESP_ERROR)
						|| message.getStatus().equals(GmmsStatus.SUBMIT_NOTPAID)
						|| message.getStatus().equals(GmmsStatus.DELIVERED)))) {
			// tmpMsg is used to set temporary rssid, ra2p, should not use it to do anything else
			String inContent = message.getInTextContent();			
			GmmsMessage tmpMsg = new GmmsMessage(message);	
			if (inContent!=null && !inContent.equals(message.getTextContent())) {
				tmpMsg.setTextContent(inContent);				
			}					
			//tmpMsg.setRSsID(0);
			tmpMsg.setRA2P(0);
			if(!backupChannelRouter.dispatch(tmpMsg)){
				//dead node
				return false;
			}
			//do system vendor replacement
			ctm.getSystemRoutingReplace(tmpMsg);
			int r_ssid_backup = tmpMsg.getRSsID();
			String routingSsIDs = message.getRoutingSsIDs();
			if (r_ssid_backup > 0) {
				if (routingSsIDs!=null && routingSsIDs.contains(","+r_ssid_backup+",")) {
					/*if (message.getSplitStatus() <= 0 ){
						// already reroute, set rssid to primary DC, do retry policy in the caller
						tmpMsg.setRSsID(0);
						tmpMsg.setRA2P(0);
						channelRouter.dispatch(tmpMsg);
						int rssid_primary = tmpMsg.getRSsID();
						if (rssid_primary > 0) {
							message.setRSsID(rssid_primary);
							A2PCustomerInfo customerInfo = ctm.getCustomerBySSID(rssid_primary);
							message.setOutClientPull("1".equals(customerInfo.getOutClientPull()));
							if (log.isInfoEnabled()) {
								log.info(message, "message rerouted to primary channel, set rssid={}", rssid_primary);
							}
						}
					}*/
					if (log.isDebugEnabled()) {
						log.debug(message, "the message not to rerouted again");
					}
					// if primary DC not need split, but backupDc split the message, should not set dc to primary
					// use backupDC retry policy
					
					return false;
				} else {
					// reroute
					String routingSsids = message.getRoutingSsIDs();
					if (routingSsids == null || "".equalsIgnoreCase(routingSsids)) {
						message.setRoutingSsIDs(","+r_ssid_backup+",");
					}else {
						message.setRoutingSsIDs(routingSsids+","+r_ssid_backup+",");
					}
					int r_a2p_backup = tmpMsg.getRA2P();
					// backupRelay and primaryRelay should connect to the same A2P
					if (r_a2p_backup > 0 && r_a2p_backup == message.getRA2P()) {
						// set reroute properties
						message.setRSsID(r_ssid_backup);
						A2PCustomerInfo customerInfo = ctm.getCustomerBySSID(message.getRSsID());
						message.setOutClientPull("1".equals(customerInfo.getOutClientPull()));
						// original value would be MultiSmppServer1:CoreEngine1:MultiSmppClient1
						String deliveryChannel = message.getDeliveryChannel();
						if (deliveryChannel != null) {
							String[] dms = deliveryChannel.split(":");
							if (dms != null && dms.length > 2) {
								message.setDeliveryChannel(dms[0] + ":" + dms[1]);
							}
						}
						//reroute for backup template
						if (inContent!=null && !inContent.equals(message.getTextContent())) {
							message.setTextContent(inContent);	
							message.setContentType(message.getInContentType());
							if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(message.getInContentType())){
								 message.setGsm7bit(false);
							 }
							try {
								message.setMessageSize(inContent.getBytes(message
										.getInContentType()).length);
							} catch (Exception e) {
								// TODO: handle exception
							}				
						}
						
						if (log.isInfoEnabled()) {
							log.info(message, "message rerouted to backup channel, set rssid={}", r_ssid_backup);
						}
						//设置msgsplit值为0
						message.setSplitStatus(0);
						if (!msgProcessorhandler.putMsg(message)) {
							message.setStatus(GmmsStatus.SERVER_ERROR);
							if (log.isInfoEnabled()) {
								log.info(message, "reroute message put to MessageProcessorHandler failed");
							}
							return false;
						}
						return true;
					} else { // not connect to same A2P
						if (log.isInfoEnabled()) {
							log.info(message, "reroute failed due to backupRelay={} is connected to {}, not {}", r_ssid_backup, r_a2p_backup, message.getRA2P());
						}
						return false;
					}
				}
			}
				
		}
		return false;
	}

}
