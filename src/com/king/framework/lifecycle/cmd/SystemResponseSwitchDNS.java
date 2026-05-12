package com.king.framework.lifecycle.cmd;

import java.util.List;

public class SystemResponseSwitchDNS extends SystemResponse {

	 public SystemResponseSwitchDNS(SystemCommandSwitchDNS req, int result, List args){
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
	     *   com.king.framework.lifecycle.cmd.SystemResponse method
	     */
	    public byte mapResultCode(int code) {
	        return (byte)code;
	    }

}
