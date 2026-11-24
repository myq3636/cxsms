package com.king.mgt.cmd.user;

/** modules -a: RELOAD_MODULES, reloads the module registry in Redis (ModuleRegistry) */
public class UserCommandModulesReload extends UserCommandModuleReload {
	public static final String keyword = "modules";

	protected String controlName() {
		return "RELOAD_MODULES";
	}
}
