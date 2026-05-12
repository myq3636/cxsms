package com.king.framework.lifecycle.cmd;

import java.util.List;

public class SystemResponsePhonePrefix extends SystemResponse {

	public SystemResponsePhonePrefix(SystemCommandPhonePrefix req, int result,
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
