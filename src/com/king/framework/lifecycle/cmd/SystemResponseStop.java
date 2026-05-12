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
public class SystemResponseStop
    extends SystemResponse {

    public SystemResponseStop(SystemCommandStop req, int result, List args){
        super(req,result,args);
    }
    /**
     * genBody
     *
     * @todo Implement this
     */
    public void genBody() {
        return;
    }

    /**
     * mapResultCode
     *
     * @param code int
     * @return byte
     * @todo Implement this
     */
    public byte mapResultCode(int code) {
        return (byte)code;
    }
}
