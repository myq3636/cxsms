package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemResponseSwitchRedis extends SystemResponse {

	public SystemResponseSwitchRedis(SystemCommandSwitchRedis req, int result, List args){
        super(req,result,args);
    }
    /**
     * genBody
     *
     * @todo Implement this
     */
    public void genBody() {
    	String info = "\nExecute command successfully."+"\n";
		body= new ByteBuffer(info.getBytes());
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
