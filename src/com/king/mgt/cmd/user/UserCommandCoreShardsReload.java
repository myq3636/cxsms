package com.king.mgt.cmd.user;

/** coreShards -a [module ...]: RELOAD_CORE_SHARDS, reloads the shards of the Cores (CoreShard.*, system:core:shards:{module}) */
public class UserCommandCoreShardsReload extends UserCommandModuleReload {
	public static final String keyword = "coreShards";

	protected String controlName() {
		return "RELOAD_CORE_SHARDS";
	}

	protected boolean takesTargets() {
		return true;
	}
}

