package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * <p>
 * Title: A2PSecondChannelRouter
 * </p>
 * <p>
 * Description: ChannelRoute of a2p, route the messages to second-time channels
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 6.1
 * @author: Neal
 */
public class A2PSecondChannelRouter extends ChannelRouter {
	// private static Logger log = Logger.getLogger(IOSMSChannelRouter.class);
	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PSecondChannelRouter.class);

	/**
	 * Constructor
	 */
	public A2PSecondChannelRouter() {
		// channelFactory = IOSMSDeliveryChannelFactory.getInstance();
	}

	/**
	 * dipatch single message
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public boolean dispatch(GmmsMessage msg) {
		if (msg == null) {
			return false;
		}
		// message type is submit
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
						.getMessageType())) {
			// check whether the R_SSID exists or not
			if (msg.getRSsID() > 0) {
				// this is resend msg, or this msg is come from other A2P.
				if ((msg.getRA2P() == msg.getCurrentA2P() || ctm.vpOnSameA2P(msg.getRA2P(),msg.getCurrentA2P()))
						&& ctm.isInRopFailedMode(msg.getRSsID())) {
					// msm.insertMessageToDB(msg, "SMQ");
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg,"Insert into SMQ, Rssid is in ROPFailed mode: {}",msg.getRSsID());
					}
					return false;
				}
				return routebySsid(msg.getRSsID(), msg);
			} else { // this is new message, route it
				// get channel by message
				int rSsid = 0;
				int ossid = msg.getOSsID();
				//TODO
				rSsid = ctm.getSecCustomerSenderRoutingRelay(ossid, msg);
				if (rSsid>0) {
					log.info(msg,"get Rssid: {} by second sender routingRelay",rSsid);
				}else{
					rSsid = ctm.getSecCustomerContentKeywordRoutingRelay(ossid, msg);
					if (rSsid>0) {
						log.info(msg,"get Rssid: {} by second content keyword routingRelay",rSsid);
					}else {
						rSsid = ctm.getSecCustomerRoutingRelay(ossid, msg);
						if(log.isInfoEnabled()){
							log.info(msg,"get Rssid: {} by second routingRelay",rSsid);
						}
					}
				}
				//check routing special msg size check.
				if(rSsid!=0) {
					A2PCustomerInfo rCustomer = ctm.getCustomerBySSID(rSsid);
					if(!rCustomer.isRSupportContentLenghtAllow(msg)) {
						log.info(msg,"msg deny by specail content len check for Rssid: {}, so set ssid {} to 0",rSsid, rSsid);
						rSsid=0;
					}
				}
				int opId = msg.getRoperator();
				msg.setRSsID(rSsid);
				//set opId to real op id
				msg.setRoperator(opId);
				if(rSsid>0) {
					String routingSsids = msg.getRoutingSsIDs();
					if (routingSsids == null || "".equalsIgnoreCase(routingSsids)) {
						msg.setRoutingSsIDs(","+rSsid+",");
					}else {
						msg.setRoutingSsIDs(routingSsids+","+rSsid+",");
					}
				}				
				if (ctm.isInRopFailedMode(rSsid)) {
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg,"Insert into SMQ, Rssid is in ROPFailed mode: {}",rSsid);
					}
					return false;
				}
				return routebySsid(rSsid, msg);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			return routebySsid(msg.getRSsID(), msg);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_READ_REPLY_REPORT.equalsIgnoreCase(msg
						.getMessageType())) {
			if (msg.getOSsID() > 0) {
				return routebySsid(msg.getOSsID(), msg);
			} else {
				log.warn("Cannot send this DR or RR message: {}", msg);
				return false;
			}
		} else {
			log.warn("Cannot handle this message type: {} in IO-SMS service",
					msg.getMessageType());
			return false;
		}
	}
}
