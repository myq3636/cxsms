/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.routing.nmg;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.gmms.routing.IOSMSChannelRouter;
import com.king.gmms.routing.IOSMSDeliveryPolicy;
import com.king.gmms.routing.MessageDispatcher;
import com.king.gmms.routing.RouteResponse;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class OTTSMSDispatcher extends MessageDispatcher {
	
    private static SystemLogger log = SystemLogger.getSystemLogger(OTTSMSDispatcher.class);

    /**
     * Constructor
     */
    public OTTSMSDispatcher() {
        super();
        try {
            operatorRouter = new OTTSMSOperatorRouter();
            channelRouter = new IOSMSChannelRouter();
            deliveryPolicy = new IOSMSDeliveryPolicy();
        }
        catch (Exception ex) {
            log.fatal("Fail to init OTTSMSDispatcher", ex);
            System.exit( -1);
        }
    }

    public void getQueryResults(List<GmmsMessage> msgs, List<Pdu> pdus) {
    	
        ((OTTSMSOperatorRouter) operatorRouter).getNmgQueryResult(msgs, pdus);
    }
    
	public RouteResponse dispatch(GmmsMessage msg) {

		RouteResponse res = ((OTTSMSOperatorRouter) operatorRouter).ottsmsRouteToOperator(msg);
		if(res != RouteResponse.NMG_RouteOK)
            return res;

        if(!pc.policyControl(msg, deliveryPolicy)){
            return RouteResponse.NMG_RouteFailed;
        }

        if(!channelRouter.dispatch(msg)){
            return RouteResponse.NMG_RouteFailed;
        }
        
        return RouteResponse.NMG_RouteOK;

	}
	
	public RouteResponse applySenderNumber(GmmsMessage msg) {
		RouteResponse res = ((OTTSMSOperatorRouter) operatorRouter).applySenderNumber(msg);
		return res;
	}

}
