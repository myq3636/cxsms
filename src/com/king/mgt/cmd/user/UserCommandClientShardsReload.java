package com.king.mgt.cmd.user;

/** clientShards -a [module ...]: RELOAD_CLIENT_SHARDS, reloads the shards of the Clients (system:client:shards:{module}) */
public class UserCommandClientShardsReload extends UserCommandModuleReload {
	public static final String keyword = "clientShards";

	protected String controlName() {
		return "RELOAD_CLIENT_SHARDS";
	}

	protected boolean takesTargets() {
		return true;
	}
}

