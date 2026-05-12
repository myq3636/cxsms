package com.king.gmms.routing;

//import org.apache.log4j.Logger;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * <p>Title: </p>
 * <p/>
 * <p>Description: </p>
 * <p/>
 * <p>Copyright: Copyright (c) 2006</p>
 * <p/>
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class PolicyControl {
    private GmmsUtility gmmsUtility = null;
    //private static Logger log = Logger.getLogger(PolicyControl.class);
    private static SystemLogger log = SystemLogger.getSystemLogger(PolicyControl.class);

    public PolicyControl() {
        gmmsUtility = GmmsUtility.getInstance();
    }

    /**
     * judge if the messages are allowed to send
     *
     * @param msgs           GmmsMessage[]
     * @param deliveryPolicy DeliveryPolicy
     */
    public boolean policyControl(GmmsMessage msg,
                                 DeliveryPolicy deliveryPolicy) {
        boolean result = true;
        try{
            // Add by Tommy to support send DR to MMVD
            if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.
                MSG_TYPE_DELIVERY_REPORT) ||
                msg.getMessageType().equalsIgnoreCase(GmmsMessage.
                MSG_TYPE_DELIVERY_REPORT_QUERY)) {
                return true;
            }
            if (msg.getOoperator() == -1 || msg.getRoperator() == -1) {
//            log.warn(msg,"oop or rop of this message is absent,so just return.oop:"+msg.getOoperator()+",rop:"+ msg.getRoperator());
                result = false;
                return false;
            }
            if (!deliveryPolicy.isPermit(msg)) {
                msg.setStatus(GmmsStatus.POLICY_DENIED);
                //gmmsUtility.getMessageStoreManager().handleMessageError(msg);
//            log.warn(msg,"Policy denied for this message.");
                result = false;
                return false;
            }
//        log.info(msg,"This message is allowed by PolicyControl.");
            return true;
        }
        finally{
            if( !result ){
                log.warn(msg, "Denied by policy control:oop={},ossid={},rop={}",
                         msg.getOoperator(), msg.getOSsID(), msg.getRoperator());
            }
        }
    }
}
