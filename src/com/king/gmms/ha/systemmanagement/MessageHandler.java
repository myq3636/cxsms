package com.king.gmms.ha.systemmanagement;

import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;



/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public interface MessageHandler {
    public SystemPdu process(SystemPdu message);
}
