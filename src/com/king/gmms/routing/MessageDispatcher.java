package com.king.gmms.routing;

/**
 * <p>Title: MessageDispatcher </p>
 * <p>Description: The dispatcher to dispatch the messages</p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 5.0
 */




import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.routing.RouteResponse;
import com.king.message.gmms.GmmsMessage;

public abstract class MessageDispatcher {
    protected A2PCustomerManager ctm = null;
    protected String systemType = null;

    protected GmmsUtility utility = null;


    protected DeliveryPolicy deliveryPolicy = null;
    protected ChannelRouter channelRouter = null;
    protected OperatorRouter operatorRouter = null;
    protected PolicyControl pc = null;

    /**
     * Creates a new instance of MessageDispatcher
     */
    public MessageDispatcher() {
        utility = GmmsUtility.getInstance();
        this.ctm = utility.getCustomerManager();
        pc = new PolicyControl();
    }

    /**
     * dispatch message
     *
     * return: -1: error updated db, 1: dispatch ok send , 0: ignore message dispatch other
     */
    public abstract RouteResponse dispatch(GmmsMessage msg);

}
