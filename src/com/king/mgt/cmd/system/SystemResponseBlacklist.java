package com.king.mgt.cmd.system;

import java.nio.ByteBuffer;

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
public class SystemResponseBlacklist extends SystemResponse{
    public SystemResponseBlacklist() {
        super();
    }
    public boolean parseArgs(ByteBuffer buf)
    {  // no addional args
        if (buf.position()==buf.array().length)
            return true;
        return false;
    }
}
