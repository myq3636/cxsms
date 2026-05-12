package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.*;
import com.king.message.gmms.*;

/**
 * <p>
 * Title: ChannelRouter
 * </p>
 * <p>
 * Description: ChannelRoute, route the o_operator and r_operator of the
 * messages
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 1.0
 * @author: Neal
 */
public abstract class ChannelRouter {
	// private static Logger log = Logger.getLogger(ChannelRouter.class);
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ChannelRouter.class);
	protected A2PCustomerManager ctm = null;
	protected GmmsUtility gmmsUtility = null;
	protected MessageStoreManager msm = null;
	// protected DeliveryChannelFactory channelFactory = null;
	// protected DeliveryChannel channel = null;
	protected String channelName = null;
	protected MessageAddressInterpreter interpreter = null;

	/**
	 * constructor
	 */
	public ChannelRouter() {
		gmmsUtility = GmmsUtility.getInstance();
		interpreter = gmmsUtility.getMessageAddressInterpreter();
		this.ctm = gmmsUtility.getCustomerManager();
		this.msm = gmmsUtility.getMessageStoreManager();
	}

	/**
	 * abstract class, to dipatch single message
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public abstract boolean dispatch(GmmsMessage msg);

	/**
	 * Send the message to Peering GMD/AMR by ssid
	 * 
	 * @param ssid
	 *            int
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	// protected boolean send2Peering(int ssid, GmmsMessage msg) {
	// if(ctm.isGmd(ssid) || ctm.isPartition(ssid) ||ctm.isAsg(ssid)) {
	// send2PeeringTcp(ssid, msg);
	// return true;
	// }
	// if (ctm.isAmr(ssid)) {
	// send2Amr(ssid, msg);
	// return true;
	// }
	// else {
	// if(GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
	// msg.setStatus(GmmsStatus.SERVER_ERROR);
	// }
	// else
	// if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType()))
	// {
	// msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
	// }
	// else { //invalid message type
	// log.warn("Unknown Message Type! when update the fail status");
	// msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
	// }
	// //gmmsUtility.getMessageStoreManager().handleMessageError(msg);
	// log.warn("no match ssid in peering info!");
	// return false;
	// }
	// }

	// protected void send2PeeringTcp(int ssid, GmmsMessage msg) {
	// Server server = null;
	// try {
	// server = gmmsUtility.getServerManager().getServerBySSID(ssid);
	// msg.setDeliveryChannel(PeeringTcpChannel.class.getSimpleName());
	//
	// if(log.isDebugEnabled()) {
	// log.debug("This msg (" + msg +
	// " ) will be delivered to peering tcp Channel." +
	// server.getChlURL() + ":" + server.getPort());
	// }
	// }
	// catch(DataManagerException ex) {
	// log.error(ex, ex);
	// }
	// }

	/**
	 * Added by Neal to send the message to Peering AMR 2005-6-27 directly send
	 * this message to AMR.
	 * 
	 * @param ssid
	 *            int: the ssid of AMR.
	 * @param msg
	 *            GmmsMessage
	 */
	// protected void send2Amr(int ssid, GmmsMessage msg) {
	// Server amrServer = null;
	// try {
	// amrServer = gmmsUtility.getServerManager().getServerBySSID(ssid);
	// String url = amrServer.getChlURL()[0];
	// msg.setDeliveryChannel(AmrDeliveryChannel.class.getSimpleName());
	// //amrDeliveryChannel.addMessage(String.valueOf(ssid), server, msg);
	// if(log.isDebugEnabled()) {
	// log.debug("This msg (" + msg + " ) will be delivered to AMR Channel:" +
	// url);
	// }
	// }
	// catch(DataManagerException ex) {
	// log.error(ex, ex);
	// }
	// }

	/**
	 * routh message mainly by the existed ssid.
	 * 
	 * @param ssid
	 *            int: the ssid of CP or DC
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	protected boolean routebySsid(int ssid, GmmsMessage msg) {
		if (ssid == 0) { // Dead channel
			log.trace(msg, "Routing rA2P failed,for rssid is 0.");
			msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			return false;
		}

		int cA2P = msg.getCurrentA2P();
		int nextA2P = 0;

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
						.getMessageType())) {
			nextA2P = msg.getRA2P();

			if (nextA2P <= 0) {
				nextA2P = ctm.getCurrentA2P();
			}
			if (nextA2P > 0 && msg.getRA2P() <= 0) {
				msg.setRA2P(nextA2P);
			}
			if (ctm.vpOnSameA2P(msg.getCurrentA2P(),msg.getRA2P())){
                msg.setCurrentA2P(msg.getRA2P());
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			nextA2P = msg.getRA2P();
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			nextA2P = msg.getOA2P();
		}
		if ((cA2P == nextA2P) || gmmsUtility.getCustomerManager().vpOnSameA2P(cA2P, nextA2P)) {
			// the channel may has been created if this message firstly is
			// routed, then
			// invoke this method.
			// it may not be created, for example, resend or R_A2P send msg to
			// third part.
			return true;
		}
		if (ctm.isA2P(nextA2P) || ctm.isPartition(nextA2P)) {
			return true;
		}

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.SERVER_ERROR);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
		} else { // invalid message type
			log.warn(msg, "Unknown Message Type! when update the fail status");
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
		log.info(msg,"rA2P({}) and cA2P({}) isn't on the same physical A2P and rA2P({}) is also not a GMD|ASG|Partition,so terminate this message."
						,msg.getRA2P(), msg.getCurrentA2P(),msg.getRA2P());
		return false;
	}

	/**
	 * To get the channel by operator's information
	 * 
	 * @param operatorSsid
	 *            int
	 * @param msg
	 *            GmmsMessage
	 * @return int
	 */
	protected int getChannelByOperator(int operatorSsid, GmmsMessage msg) {
		// channel's ssid
		int channelSsid = 0;
		// if dead channel
		if (operatorSsid == 0) {
			return channelSsid;
		}

		// if the operator is not really an operator
		if (!ctm.isOperator(operatorSsid)) {
			if(log.isInfoEnabled()){
				log.info(msg, "Failed to get rssid,for rop({}) is not an operator." , operatorSsid);
			}
			return channelSsid;
		}

		// modified by Tommy for support CU, 2006-04-18
		channelSsid = ctm.getConnectedRelay(operatorSsid, msg.getGmmsMsgType());
		// if the operator is not connected with any DC
		if (channelSsid == 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "Failed to get rssid by connectedRelay rop, for rop({}) is not connected to King.",operatorSsid);
			}
			return channelSsid;
		}
		// The operator is connected to King directly.
		if (ctm.isA2P(channelSsid) || ctm.isPartition(channelSsid)) {
			return operatorSsid;
		}
		// get the channel's ssid by operator
		return channelSsid;
	}

}
