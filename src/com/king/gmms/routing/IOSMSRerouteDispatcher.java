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
	
	private MessageProcessorHandler msgProcessorhandler = null;

	/**
	 * Constructor
	 */
	public IOSMSRerouteDispatcher() {
		super();
		try {
			super.channelRouter = new IOSMSChannelRouter();
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
			tmpMsg.setRA2P(0);
			// the next backup level (docs/路由重构设计.md 2.4): from the level the MT is at, skipping the channels used
			java.util.Set<Integer> used = usedChannels(message.getRoutingSsIDs());
			A2PCustomerInfo oCustomer = ctm.getCustomerBySSID(message.getOSsID());
			int maxReroute = oCustomer == null ? 10 : oCustomer.getSmsOptionMaxReroute();
			if (used.size() - 1 >= maxReroute) {
				log.info(message, "[MT-REROUTE] not rerouted: rerouted {} times already, SMSOptionMaxReroute={}", used.size() - 1, maxReroute);
				return false;
			}
			com.king.gmms.routing.rule.RoutingRule.Layer mainLayer = mainLayer(message.getRoutingRule());
			com.king.gmms.routing.rule.RuleRouter.Choice c = new com.king.gmms.routing.rule.RuleRouter(ctm)
					.backup(tmpMsg, message.getRoutingLevel() + 1, mainLayer, used);
			if (!c.ok()) {
				log.info(message, "[MT-REROUTE] not rerouted: no backup level after level {} (rssid={}, status={})",
						message.getRoutingLevel(), message.getRSsID(), message.getStatusCode());
				return false;
			}
			int r_ssid_backup = c.rssid;
			int from = message.getRSsID();
			tmpMsg.setRSsID(r_ssid_backup);
			((IOSMSChannelRouter) channelRouter).routeToChannel(r_ssid_backup, tmpMsg);
			if (r_ssid_backup > 0) {
				{
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
						
						log.info(message, "[MT-REROUTE] level={} from={} to={} code={} attempt={} {}", c.level, from, r_ssid_backup,
								message.getStatusCode(), used.size(), c.describe());
						message.setRoutingLevel(c.level);
						message.setRoutingRule(c.ruleText(mainLayer));
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


	/** the channels in routingSsIDs (",81,,82,") */
	static java.util.Set<Integer> usedChannels(String routingSsIDs) {
		java.util.Set<Integer> out = new java.util.LinkedHashSet<Integer>();
		if (routingSsIDs != null) {
			for (String p : routingSsIDs.split(",")) {
				try {
					if (!p.trim().isEmpty()) {
						out.add(Integer.parseInt(p.trim()));
					}
				} catch (NumberFormatException e) {
					// not a channel
				}
			}
		}
		return out;
	}

	/** the layer the main route came from: routingRule "{layer}|..." (null: the main layer, e.g. retried from the DB) */
	static com.king.gmms.routing.rule.RoutingRule.Layer mainLayer(String routingRule) {
		if (routingRule == null || routingRule.indexOf('|') < 0) {
			return null;
		}
		return com.king.gmms.routing.rule.RoutingRule.Layer.byLabel(routingRule.substring(0, routingRule.indexOf('|')));
	}
}
