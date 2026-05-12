package com.king.framework.lifecycle.cmd;

import java.util.List;

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
public class SystemResponseWhitelist extends SystemResponse{
    public SystemResponseWhitelist(SystemCommandWhitelist req, int result, List args) {
        super(req, result, args);
    }

    public void genBody() {
        // no addional bodys.
        return;
    }

    public byte mapResultCode(int code) {
        return (byte)(code==0 ? 0 : 1);
    }
}
