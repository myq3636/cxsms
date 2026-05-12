/**
 */
package com.king.framework.lifecycle.cmd;

import java.util.List;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class SystemResponseContentTpl extends SystemResponse {

	/**
	 * @param req
	 * @param result
	 * @param args
	 */
	public SystemResponseContentTpl(SystemCommand req, int result, List args) {
		super(req, result, args);
	}

	/** 
	 * 
	 * @see com.king.framework.lifecycle.cmd.SystemResponse#genBody()
	 */
	@Override
	public void genBody() {
		return;
	}

	/** 
	 * @param code
	 * @return
	 * @see com.king.framework.lifecycle.cmd.SystemResponse#mapResultCode(int)
	 */
	@Override
	public byte mapResultCode(int code) {
		return (byte)(code==0 ? 0 : 1);
	}

}
