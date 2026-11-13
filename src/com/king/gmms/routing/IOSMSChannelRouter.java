package com.king.gmms.routing;

import com.king.framework.SystemLogger;
import com.king.gmms.routing.rule.RuleRouter;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * <p>
 * Title: IOSMSChannelRouter
 * </p>
 * <p>
 * Description: ChannelRoute of IOSMS service, route the messages to channels
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
public class IOSMSChannelRouter extends ChannelRouter {
	// private static Logger log = Logger.getLogger(IOSMSChannelRouter.class);
	private static SystemLogger log = SystemLogger
			.getSystemLogger(IOSMSChannelRouter.class);

	/**
	 * Constructor
	 */
	public IOSMSChannelRouter() {
		// channelFactory = IOSMSDeliveryChannelFactory.getInstance();
	}

	/** routingSsIDs: the channels the MT was sent to, ",81,,82," (CDR MsgID, reroute) */
	static void addRoutingSsid(GmmsMessage msg, int rssid) {
		String routingSsids = msg.getRoutingSsIDs();
		if (routingSsids == null || "".equalsIgnoreCase(routingSsids)) {
			msg.setRoutingSsIDs("," + rssid + ",");
		} else if (!routingSsids.contains("," + rssid + ",")) {
			msg.setRoutingSsIDs(routingSsids + "," + rssid + ",");
		}
	}

	/** the A2P checks of {@link ChannelRouter#routebySsid} for a channel picked by a reroute */
	boolean routeToChannel(int rssid, GmmsMessage msg) {
		return routebySsid(rssid, msg);
	}

	/**
	 * dipatch single message
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public boolean dispatch(GmmsMessage msg) {
		return dispatch(msg, false);
	}

	/**
	 * Route one message. A new MT: by the routing rules (docs/路由重构设计.md 2.1-2.3, {@link RuleRouter#main});
	 * second: the recipient is in the customer's second-routing cache, the second layer is tried first.
	 * An MT that has its channel already (sent again, a retry, from another A2P) goes to that channel.
	 */
	public boolean dispatch(GmmsMessage msg, boolean second) {
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
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg,"Insert into SMQ, Rssid is in ROPFailed mode: {}",msg.getRSsID());
					}
					return false;
				}
				return routebySsid(msg.getRSsID(), msg);
			}
			RuleRouter router = new RuleRouter(ctm);
			RuleRouter.Choice c = router.main(msg, second);
			com.king.gmms.routing.rule.RoutingRule.Layer mainLayer = null;
			if (c.status == RuleRouter.Status.UNAVAILABLE || c.status == RuleRouter.Status.NO_RULE) {
				// no usable channel in the main route: the backup levels, as the old routing did (docs/路由重构设计.md 2.3)
				String why = c.status == RuleRouter.Status.NO_RULE ? "no main routing rule matched" : c.describe();
				if (c.status == RuleRouter.Status.UNAVAILABLE && (c.rssid <= 0 || ctm.getCustomerBySSID(c.rssid) == null)) {
					logNoChannel(msg, c.rssid);
				} else if (c.status == RuleRouter.Status.UNAVAILABLE) {
					long n = com.king.gmms.util.LogRateLimiter.allow("mt-main-unusable:" + c.rssid, 600000L);
					if (n > 0) {
						log.warn(msg, "[MT-ROUTE] main route of ossid={} not usable ({}), backup routes used ({} MT(s) since the last line)",
								msg.getOSsID(), why, n);
					}
				}
				RuleRouter.Choice b = router.backupForNewMt(msg, c, second);
				if (!b.ok()) {
					log.info(msg, "[MT-ROUTE] ossid={} not routed: {}; no backup route either (recipient={}, sender={}, opid={})",
							msg.getOSsID(), why, msg.getRecipientAddress(), msg.getSenderAddress(), msg.getRoperator());
					msg.setRSsID(0);
					return routebySsid(0, msg);
				}
				log.info(msg, "[MT-ROUTE] ossid={} {}: {}", msg.getOSsID(), why, b.describe());
				mainLayer = b.rule.layer();
				c = b;
			}
			msg.setRSsID(c.rssid);
			addRoutingSsid(msg, c.rssid);
			msg.setRoutingLevel(c.level);
			msg.setRoutingRule(c.ruleText(mainLayer));
			if (c.level > 0) {
				return routebySsid(c.rssid, msg);
			}
			if (c.status == RuleRouter.Status.ROP_FAILED) {
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				log.info(msg, "[MT-ROUTE] ossid={} rssid={} {}: insert into SMQ", msg.getOSsID(), c.rssid, c.describe());
				return false;
			}
			if (log.isInfoEnabled()) {
				log.info(msg, "[MT-ROUTE] ossid={} rssid={} {}", msg.getOSsID(), c.rssid, c.describe());
			}
			return routebySsid(c.rssid, msg);
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
