package com.king.framework.lifecycle.cmd;

import java.util.List;

public class SystemResponseCustomer extends SystemResponse {

	public SystemResponseCustomer(SystemCommandCustomer req, int result,
			List args) {
		super(req, result, args);
	}

	@Override
	public void genBody() {

	}

	@Override
	public byte mapResultCode(int code) {
		return (byte)(code==0 ? 0 : 1);
	}

}
