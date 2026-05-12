package com.king.gmms.milter;

import com.king.framework.SystemLogger;
import com.king.gmms.*;

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
public abstract class LocalMilter implements Milter {
    protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
    protected static SystemLogger log;

    public LocalMilter() {
    }

}
