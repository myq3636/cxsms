package com.king.mgt.cmd.user;

/**
 * A reload that has only a Redis control command (handled by RedisControlSubscriber itself, no file to
 * download): "keyword -a", or "keyword -a module ..." for the ones that can name their modules (published as
 * COMMAND@module,...:-a).
 */
public abstract class UserCommandModuleReload extends UserCommand {
	public static final int TYPE_ACTIVE = 6;
	private String targets = null;

	public UserCommandModuleReload() {
		super(UserCommand.Notify_All);
	}

	/** the control command, e.g. RELOAD_DRERROR */
	protected abstract String controlName();

	/** true when the command can name the modules it is for */
	protected boolean takesTargets() {
		return false;
	}

	public boolean parseArgs(String[] args) {
		if (args.length < 2 || !args[1].equals("-a")) {
			return false;
		}
		if (args.length > 2 && !takesTargets()) {
			return false;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 2; i < args.length; i++) {
			String target = args[i].trim();
			if (target.length() == 0) {
				continue;
			}
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(target);
		}
		targets = sb.length() == 0 ? null : sb.toString();
		this.type = TYPE_ACTIVE;
		return true;
	}

	/** what RedisControlCommands publishes */
	public String controlCommand() {
		return controlName() + (targets == null ? "" : "@" + targets) + ":-a";
	}

	public boolean process() {
		// published on the control channel (RedisControlCommands), not run here
		return false;
	}
}
