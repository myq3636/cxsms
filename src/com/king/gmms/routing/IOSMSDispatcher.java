package com.king.gmms.routing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.EnumberDTO;
import com.king.gmms.processor.DBBackupHandler;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>
 * Title: IOSMSDispatcher
 * </p>
 * <p>
 * Description:IOSMS Message Dispatche
 * </p>
 * <p>
 * Copyright: Copyright (c) 2001-2010
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 6.1
 * @author: Neal
 */

public class IOSMSDispatcher extends MessageDispatcher {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(IOSMSDispatcher.class);
	private DBBackupHandler dbHandler = null;
	private ChannelRouter backupChannelRouter = null;
	private ChannelRouter opChannelRouter = null;
	private ChannelRouter numChannelRouter = null;
	private ChannelRouter backupOPChannelRouter = null;
	private A2PSecondChannelRouter a2pSecondChannelRouter = null;
	/**
	 * Constructor
	 */
	public IOSMSDispatcher() {
		super();
		try {
			super.deliveryPolicy = new IOSMSDeliveryPolicy();
			super.channelRouter = new IOSMSChannelRouter();
			a2pSecondChannelRouter = new A2PSecondChannelRouter();
			backupChannelRouter = new IOSMSBackupChannelRouter();
			opChannelRouter = new SMSOPChannelRouter();
			numChannelRouter = new SMSNumberChannelRouter();
			backupOPChannelRouter = new SMSOPBackupChannelRouter();
			super.operatorRouter = new IOSMSOperatorRouter(false);
			dbHandler = DBBackupHandler.getInstance();
		} catch (Exception ex) {
			log.fatal("Fail to init IOSMSDispatcher", ex);
			System.exit(-1);
		}
	}

	public List<GmmsMessage> getQueryResults(BufferMonitor buffer) {

		List<GmmsMessage> msgs = new ArrayList<GmmsMessage>();
		List<Map> result = ((IOSMSOperatorRouter) operatorRouter)
				.getDnsQueryResult();
		if (result.size() > 0) {
			if(log.isDebugEnabled()){
				log.debug("get asy result size:{}", result.size());
			}
		}
		RouteResponse res = RouteResponse.RouteOK;
		for (Map map : result) {
			Iterator it = map.keySet().iterator();
			int op = (Integer) it.next();
			GmmsMessage msg = (GmmsMessage) map.get(op);
			if (op > 0) {
				if (msg.getOoperator() > 0) {
					msg.setRoperator(op);
				} else {
					msg.setOoperator(op);
					res = ((IOSMSOperatorRouter) operatorRouter)
							.iosmsRouteToOperator(msg);
				}
				if (res == RouteResponse.RouteOK) {
					msgs.add(msg);
				}
			}
			if (op < 0 || res == RouteResponse.RouteFailed) {
				if(log.isInfoEnabled()){
					log.info(msg, "message get op {} error, update to db!",op);
				}
				dbHandler.putMsg(msg);
				if (buffer != null) {
					buffer.remove(msg.getInMsgID());
				}
			}
		}
		result.clear();
		return msgs;
	}

	public RouteResponse dispatch(GmmsMessage msg) {

		/*RouteResponse res = ((IOSMSOperatorRouter) operatorRouter)
				.iosmsRouteToOperator(msg);
		if (res != RouteResponse.RouteOK)
			return res;*/
       
		/*if (!pc.policyControl(msg, deliveryPolicy)) {
			return RouteResponse.RouteFailed;
		}*/
		//get mncmcc by enum query
		A2PCustomerInfo oCustomer = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(msg.getOSsID());
		if(oCustomer.isNeedEnumQueryRecPrefix(msg.getRecipientAddress())
				) {
			
			try {
				EnumberDTO enumberDTo = null;
				if(oCustomer.isNeedIR21EnumQueryRecPrefix(msg.getRecipientAddress())) {
					String enumAddress = oCustomer.getEnumIR21RoutingURL();
					if(enumAddress == null || enumAddress.isEmpty()) {
						enumAddress = GmmsUtility.getInstance().getCommonProperty("DNSAddress", "http://47.242.122.229:5200/mnp/realenumquery2");
					}
					enumberDTo = ((IOSMSOperatorRouter) operatorRouter).getDnsClient().queryIR21ByHost(msg, msg.getRecipientAddress(), "arpa", enumAddress);
				}else {
					String enumAddress = oCustomer.getEnumRoutingURL();
					if(enumAddress == null || enumAddress.isEmpty()) {
						enumAddress = GmmsUtility.getInstance().getCommonProperty("mnpqueryurl", "http://47.242.122.229:5200/mnp/realenumquery2");
					}					
					enumberDTo = ((IOSMSOperatorRouter) operatorRouter).getDnsClient().queryMNC(msg, enumAddress);
				}
				
				msg.setRMncMcc(enumberDTo.getMccmnc());
				if(enumberDTo!=null && enumberDTo.getOpId()>0) {
					msg.setRoperator(enumberDTo.getPrefix()*10000+enumberDTo.getOpId());
				}				
			} catch(Exception e) {
				log.error(msg," enum query msg error ", e);
			}			
		}
		if (oCustomer.isSmsOptionNeedSupportSecondRouting()
				&&oCustomer.getReicipienSecondRoutingtCache()!=null
				&&oCustomer.getReicipienSecondRoutingtCache().asMap().containsKey("SecRouting_"+msg.getRecipientAddress())
				&&oCustomer.isNeedSecondRoutingRecPrefix(msg.getRecipientAddress())
				) {
			if(a2pSecondChannelRouter.dispatch(msg)) {
				//routing replacement
				ctm.getSystemRoutingReplace(msg);
				return RouteResponse.RouteOK;
			}else {
				if(!numChannelRouter.dispatch(msg)) {
					if(msg.getRoperator()>0) {					
						if (!opChannelRouter.dispatch(msg)) {
							if (!backupOPChannelRouter.dispatch(msg)) {
								if (!channelRouter.dispatch(msg)) {
									if (!backupChannelRouter.dispatch(msg)) {
										return RouteResponse.RouteFailed;
									}			
							    }
							}			
					    }
					}else {
						if (!channelRouter.dispatch(msg)) {
							if (!backupChannelRouter.dispatch(msg)) {
								return RouteResponse.RouteFailed;
							}			
					    }
					}
				}				
			}			
		}else {
			if(!numChannelRouter.dispatch(msg)) {
				if(msg.getRoperator()>0) {				
					if (!opChannelRouter.dispatch(msg)) {
						if (!backupOPChannelRouter.dispatch(msg)) {
							if (!channelRouter.dispatch(msg)) {
								if (!backupChannelRouter.dispatch(msg)) {
									return RouteResponse.RouteFailed;
								}			
						    }
						}			
				    }				
				}else if (!channelRouter.dispatch(msg)) {
					if (!backupChannelRouter.dispatch(msg)) {
						return RouteResponse.RouteFailed;
					}			
			    }
			}
		}
		
		String routingSsids = msg.getRoutingSsIDs();
		if (routingSsids == null || "".equalsIgnoreCase(routingSsids)) {
			msg.setRoutingSsIDs(","+msg.getRSsID()+",");
		}else if(msg.getRSsID()>0 && !msg.getRoutingSsIDs().contains(","+msg.getRSsID()+",")){
			msg.setRoutingSsIDs(routingSsids+","+msg.getRSsID()+",");
		}
		A2PCustomerInfo rCustomer = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(msg.getRSsID());
		
		if(rCustomer!=null && "blackhole".equalsIgnoreCase(rCustomer.getRole()) 
				&&oCustomer!=null 
				&&oCustomer.isSmsOptionRecipientNotContinueFilter() 
				&& oCustomer.getReicipientCache()!=null ){
			oCustomer.getReicipientCache().put(msg.getRecipientAddress(), "true");
		}
		
		//routing replacement
		ctm.getSystemRoutingReplace(msg);
		
		if(oCustomer.isSmsOptionNeedSupportSecondRouting()
				&&oCustomer.getReicipienSecondRoutingtCache()!=null
				&&oCustomer.isNeedSecondRoutingRecPrefix(msg.getRecipientAddress())
				) {
			oCustomer.getReicipienSecondRoutingtCache().put("SecRouting_"+msg.getRecipientAddress(), "true");
		}
		return RouteResponse.RouteOK;

	}

}
