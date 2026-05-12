package com.king.gmms.routing.nmg;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.gmms.routing.DNSClient;
import com.king.gmms.routing.OperatorRouter;
import com.king.gmms.routing.RouteResponse;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class OTTSMSOperatorRouter extends OperatorRouter {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(OTTSMSOperatorRouter.class);
	private NMGClient nmgClient;
	private DNSClient dnsClient;

	/**
	 * Constructor
	 * 
	 */
	public OTTSMSOperatorRouter() {
		nmgClient = new NMGClient();
//		dnsClient = new DNSClient(false);
		dnsClient = new DNSClient(true);
	}

	/**
	 * To get operators' information and set into GmmsMessage's own field
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public RouteResponse ottsmsRouteToOperator(GmmsMessage msg) {
		int oOperator = getOoperatorSYN(msg);
		if (oOperator < 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "OTTSMSOperatorRouter failed to query oOperator:{}",oOperator);
			}
			return RouteResponse.NMG_RouteFailed;
		}

		int rOperator = getRoperator(msg);
		if (rOperator < 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "OTTSMSOperatorRouter failed to query rOperator:{}",rOperator);
			}
			msg.setStatus(GmmsStatus.SERVER_ERROR);
			return RouteResponse.NMG_RouteFailed;
		}
		if (rOperator == 0) {
			return RouteResponse.NMG_ASYQueryOP;
		}

		return RouteResponse.NMG_RouteOK;
	}

	public int getRoperatorSYN(GmmsMessage msg) {
		return msg.getRoperator();
	}

	public void getNmgQueryResult(List<GmmsMessage> msgs, List<Pdu> pduList) {
		nmgClient.getAsyQueryResult(msgs, pduList);
	}

	public int getRoperator(GmmsMessage msg) {
		int rOperator = msg.getRoperator();
		if (rOperator > 0) {
			return rOperator;
		}

		return nmgClient.asyQueryRop(msg);
	}

	public RouteResponse applySenderNumber(GmmsMessage msg) {

		// already checked whether rent addr in NMGUtility.needRentAddr()
		int resp = nmgClient.asyApplySenderNumber(msg);

		if (resp < 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "OTTSMSOperatorRouter failed to applySenderNumber, and response is {}",resp);
			}
			return RouteResponse.NMG_RouteFailed;
		}
		if (resp == 0) {
			return RouteResponse.NMG_ASYQueryOP;
		}

		return RouteResponse.NMG_RouteOK;
	}

	/**
	 * @param msg
	 * @return
	 * @see com.king.gmms.routing.OperatorRouter#getOoperator(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public int getOoperator(GmmsMessage msg) {
		return -1;
	}

	/**
	 * @param msg
	 * @return
	 * @see com.king.gmms.routing.OperatorRouter#getOoperatorSYN(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public int getOoperatorSYN(GmmsMessage msg) {
		int oOperator = msg.getOoperator();
		if (oOperator > 0) {
			return oOperator;
		}
		if (gmmsUtility.getCustomerManager().isOperator(msg.getOSsID())) {
			msg.setOoperator(msg.getOSsID());
			return msg.getOoperator();
		}
		if (!gmmsUtility.getCustomerManager().isHub(msg.getOSsID())
				&& !gmmsUtility.getCustomerManager().isChannel(msg.getOSsID())) {
			msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
			return msg.getOoperator();
		}
		try {
			if (!gmmsUtility.getCustomerManager().getCustomerBySSID(
					msg.getOSsID()).isParseOoperator()) {
				msg.setOoperator(msg.getOSsID());
				return msg.getOoperator();
			}
			oOperator = getOperatorByDNS(dnsClient.queryMncMcc(msg
					.getSenderAddress(), super.defaultSuffix), msg);
		} catch (com.king.db.DataManagerException exp) {
			msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
			log.error(msg, exp, exp);
		}
		if (oOperator > 0)
			msg.setOoperator(oOperator);
		else {
			if (oOperator == -4 || oOperator == -3 || oOperator == -2) {
				msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
			} else if (oOperator == -1) {
				msg.setStatus(GmmsStatus.SERVER_ERROR);
			} else {
				msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
				log.warn(msg,"OTTSMSOperatorRouter: Unknow Error when query DNS by sender address.");
			}
		}
		return msg.getOoperator();

	}
}
