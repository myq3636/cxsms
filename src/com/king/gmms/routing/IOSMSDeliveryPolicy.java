package com.king.gmms.routing;

/**
 * <p>Title: IOSMSdeliveryPolicy</p>
 * <p>Description: this class provide all the methods which return the deliver
 *                 policy in IO-SMS service. All the listener of IO-SMS must
 *                 implement</p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 5.0
 */

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 * <p/>
 * <p>Description: delivery policy of IOSMS</p>
 * <p/>
 * <p>Copyright: Copyright (c) 2001-2010</p>
 * <p/>
 * <p>Company: King</p>
 *
 * @version 6.1
 * @author: Neal
 */
public class IOSMSDeliveryPolicy extends DeliveryPolicy {
    A2PCustomerManager cm = null;

    /**
     * Constructor
     */
    public IOSMSDeliveryPolicy() {
        cm = GmmsUtility.getInstance().getCustomerManager();
    }

    /**
     * judge if the message is allowed to send
     *
     * @param msg GmmsMessage
     * @return boolean
     */
    public boolean isPermit(GmmsMessage msg) {
        if(!GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())) {
            return true;
        }
        return cm.isPolicyControlPermit(msg);
    }
}
