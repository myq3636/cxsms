package com.king.mgt.cmd.user;

/** drError -a: RELOAD_DRERROR, reloads conf/drErrorCode.cfg and conf/drErrorMapping.cfg (Core, Server) */
public class UserCommandDrErrorReload extends UserCommandModuleReload {
	public static final String keyword = "drError";

	protected String controlName() {
		return "RELOAD_DRERROR";
	}
}
