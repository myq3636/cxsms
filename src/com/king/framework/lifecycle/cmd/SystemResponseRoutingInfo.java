package com.king.framework.lifecycle.cmd;

import java.util.List;

public class SystemResponseRoutingInfo extends SystemResponse {

	public SystemResponseRoutingInfo(SystemCommandRoutingInfo req, int result,
			List args) {
		super(req, result, args);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void genBody() {
		// TODO Auto-generated method stub

	}

	@Override
	public byte mapResultCode(int code) {
		// TODO Auto-generated method stub
		return (byte)(code==0 ? 0 : 1);
	}

}
