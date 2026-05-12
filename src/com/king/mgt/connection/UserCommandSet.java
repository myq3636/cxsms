package com.king.mgt.connection;

import java.util.HashMap;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.cmd.user.UserCommandAmrT2I;
import com.king.mgt.cmd.user.UserCommandAntiSpam;
import com.king.mgt.cmd.user.UserCommandBlacklist;
import com.king.mgt.cmd.user.UserCommandContentBlacklist;
import com.king.mgt.cmd.user.UserCommandContentTpl;
import com.king.mgt.cmd.user.UserCommandContentWhitelist;
import com.king.mgt.cmd.user.UserCommandCustomer;
import com.king.mgt.cmd.user.UserCommandGwVersion;
import com.king.mgt.cmd.user.UserCommandHelp;
import com.king.mgt.cmd.user.UserCommandList;
import com.king.mgt.cmd.user.UserCommandPhonePrefix;
import com.king.mgt.cmd.user.UserCommandQuit;
import com.king.mgt.cmd.user.UserCommandRecipientAddressRule;
import com.king.mgt.cmd.user.UserCommandRecipientBlacklist;
import com.king.mgt.cmd.user.UserCommandRestart;
import com.king.mgt.cmd.user.UserCommandRoutingInfo;
import com.king.mgt.cmd.user.UserCommandSenderBlacklist;
import com.king.mgt.cmd.user.UserCommandSenderWhitelist;
import com.king.mgt.cmd.user.UserCommandStartup;
import com.king.mgt.cmd.user.UserCommandStop;
import com.king.mgt.cmd.user.UserCommandSwitchDB;
import com.king.mgt.cmd.user.UserCommandSwitchDNS;
import com.king.mgt.cmd.user.UserCommandSwitchRedis;
import com.king.mgt.cmd.user.UserCommandVendorContentReplacement;
import com.king.mgt.cmd.user.UserCommandVendorReplacement;
import com.king.mgt.cmd.user.UserCommandWhitelist;
import com.king.mgt.context.ContextManager;
import com.king.mgt.util.UserInterfaceUtility;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2008</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class UserCommandSet {
    private static SystemLogger log=SystemLogger.getSystemLogger(UserCommandSet.class);
    private HashMap<String, Class> commandSet=new HashMap<String, Class>();
    public UserCommandSet() {
    }

    public UserCommand parseCommand(String cmdLine, ContextManager context) {
        String[] args = cmdLine.split(" ");
        if (args.length < 1) {
            return null;
        }
        String keyword = args[0].trim();
        try {
            Class cmdClz = commandSet.get(keyword);
            if (cmdClz == null)
                return null;
            UserCommand cmd = (UserCommand)cmdClz.newInstance();
            if (!cmd.parseArgs(args))
                return null;
            cmd.setGroupId(UserInterfaceUtility.getInstance().genCommandGroupID());
            log.info("create groupid:{}",cmd.getGroupId());
            cmd.setCmdLine(cmdLine);
            cmd.setContext(context);
            return cmd;
        } catch (Exception ex) {
            log.error(ex, ex);
            return null;
        }
    }
    public void init()
    {
        commandSet.put(UserCommandBlacklist.keyword, UserCommandBlacklist.class);
        commandSet.put(UserCommandAmrT2I.keyword,UserCommandAmrT2I.class);
        commandSet.put(UserCommandCustomer.keyword, UserCommandCustomer.class);
        commandSet.put(UserCommandHelp.keyword, UserCommandHelp.class);
        commandSet.put(UserCommandStop.keyword,UserCommandStop.class);
        commandSet.put(UserCommandList.keyword,UserCommandList.class);
        commandSet.put(UserCommandQuit.keyword, UserCommandQuit.class);
        commandSet.put(UserCommandStartup.keyword, UserCommandStartup.class);
        commandSet.put(UserCommandRestart.keyword, UserCommandRestart.class);
        commandSet.put(UserCommandRoutingInfo.keyword, UserCommandRoutingInfo.class);
        commandSet.put(UserCommandAntiSpam.keyword, UserCommandAntiSpam.class);
        commandSet.put(UserCommandContentTpl.keyword, UserCommandContentTpl.class);
        commandSet.put(UserCommandSwitchDB.keyword, UserCommandSwitchDB.class);
        commandSet.put(UserCommandSwitchRedis.keyword, UserCommandSwitchRedis.class);
        commandSet.put(UserCommandSwitchDNS.keyword, UserCommandSwitchDNS.class);
        commandSet.put(UserCommandGwVersion.keyword, UserCommandGwVersion.class);
        commandSet.put(UserCommandPhonePrefix.keyword, UserCommandPhonePrefix.class);
        commandSet.put(UserCommandRecipientAddressRule.keyword, UserCommandRecipientAddressRule.class);
        commandSet.put(UserCommandWhitelist.keyword, UserCommandWhitelist.class);
        commandSet.put(UserCommandSenderWhitelist.keyword, UserCommandSenderWhitelist.class);
        commandSet.put(UserCommandSenderBlacklist.keyword, UserCommandSenderBlacklist.class);
        commandSet.put(UserCommandContentWhitelist.keyword, UserCommandContentWhitelist.class);
        commandSet.put(UserCommandContentBlacklist.keyword, UserCommandContentBlacklist.class);
        commandSet.put(UserCommandRecipientBlacklist.keyword, UserCommandRecipientBlacklist.class);
        commandSet.put(UserCommandVendorContentReplacement.keyword, UserCommandVendorContentReplacement.class);
        commandSet.put(UserCommandVendorReplacement.keyword, UserCommandVendorReplacement.class);
    }
}
