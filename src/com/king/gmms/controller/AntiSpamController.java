package com.king.gmms.controller;

import com.king.gmms.domain.*;
import com.king.message.gmms.GmmsMessage;

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
public class AntiSpamController {
    public AntiSpamController() {
    }

    public boolean needAntiSpam(A2PCustomerInfo server, GmmsMessage msg) {
        if(server == null || msg == null) {
            return false;
        }
        if(server.isNeedAntiSpam() && !msg.hasAntiSpam()) {
            return true;
        }
        else {
            return false;
        }
    }
}
