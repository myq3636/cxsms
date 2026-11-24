package com.king.mgt.processor;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.util.InfoTable;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
/**
 * The smsui commands run in smsui itself (help, quit). The ones for the modules are published on the
 * control channel (RedisControlCommands); the commands sent by TCP to each module are gone
 * (docs/流模式下系统管理和模块间连接代码清理分析.md 3.4).
 */
public class CommandProcessor {
	private UserCommand cmd = null;

	public CommandProcessor(UserCommand cmd) {
		this.cmd = cmd;
	}

	public boolean handleCommandHelp() {
		cmd.append(InfoTable.getInstance().get(InfoTable.HELP_INFO));
		return true;
	}

	public boolean handleCommandQuit() {

		cmd.append(cmd.getContext().getCurUserIP());
		cmd.append(" close the connection. login time:");
		cmd.append(cmd.getContext().getCurUserLoginTime().toString());
		cmd.setIsQuit(true);
		return true;

	}
}
