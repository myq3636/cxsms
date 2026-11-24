package com.king.mgt.connection;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.cmd.user.UserCommandModuleReload;
import com.king.redis.RedisClient;

/**
 * The smsui commands sent to the modules: published on the control channel of the state Redis, heard by
 * every module (RedisControlSubscriber). Telnet and CM tools sessions both send them this way (the commands
 * by TCP to each module's command port are gone, docs/流模式下系统管理和模块间连接代码清理分析.md 3.4).
 */
public final class RedisControlCommands {

    private static SystemLogger log = SystemLogger.getSystemLogger(RedisControlCommands.class);
    static final int TYPE_RELOAD = 4;
    static final int TYPE_ACTIVE = 6;

    private RedisControlCommands() {
    }

    /** true when the command goes to the modules (a reload, shutdown or switch), false for a local one */
    public static boolean handles(UserCommand cmd) {
        return toRedisControlCommand(cmd) != null;
    }

    /**
     * A reload downloads its file first (not published when that fails); the result is appended to the
     * command's response.
     */
    public static boolean publish(UserCommand cmd) {
        String controlCommand = toRedisControlCommand(cmd);
        if (controlCommand == null) {
            return false;
        }
        String configKeyword = getReloadConfigKeyword(cmd);
        if (configKeyword != null && cmd.getType() == TYPE_RELOAD) {
            FTP4Client ftp = new FTP4Client();
            if (!ftp.downloadConfigFile(configKeyword)) {
                cmd.append("Download ");
                cmd.append(configKeyword);
                cmd.append(" config file failed, redis control command not published.\n");
                return false;
            }
        }
        Long subscribers = publishControlCommand(controlCommand);
        cmd.append("Published redis control command [");
        cmd.append(controlCommand);
        cmd.append("] to ");
        cmd.append(String.valueOf(subscribers));
        cmd.append(" subscriber(s).\n");
        if (subscribers == null || subscribers.longValue() == 0) {
            cmd.append("No redis subscriber received this command, please check module RedisControlSubscriber status.\n");
        }
        return true;
    }

    static Long publishControlCommand(String command) {
        try {
            RedisClient redisClient = RedisClient.getInstance();
            if (redisClient.getStateRedis() == null) {
                log.warn("Redis state connection is not initialized before publish control command, initializing now. command={}", command);
                GmmsUtility.getInstance().initRedisClient("M");
            }
            Long subscribers = RedisClient.getInstance().publishState(ModuleStatusReporter.CONTROL_CHANNEL, command);
            if (subscribers == null || subscribers.longValue() < 0) {
                log.error("Publish redis control command did not execute. command={}, subscribers={}", command, subscribers);
            } else {
                log.info("Published redis control command. command={}, subscribers={}", command, subscribers);
            }
            return subscribers;
        } catch (Exception e) {
            log.error("Publish redis control command failed: " + command, e);
            return -1L;
        }
    }

    static String toRedisControlCommand(UserCommand cmd) {
        if (cmd instanceof UserCommandModuleReload) {
            return ((UserCommandModuleReload) cmd).controlCommand();
        }
        String name = cmd.getClass().getSimpleName();
        int type = cmd.getType();
        if ("UserCommandStop".equals(name)) {
            return buildShutdownControlCommand(cmd);
        }
        if ("UserCommandSwitchDB".equals(name)) {
            return "SWITCH_DB:" + getSecondArg(cmd);
        }
        if ("UserCommandSwitchRedis".equals(name)) {
            return "SWITCH_REDIS:" + getSecondArg(cmd);
        }
        if ("UserCommandSwitchDNS".equals(name)) {
            return "SWITCH_DNS:" + getSecondArg(cmd);
        }
        String reloadCommand = getRedisReloadCommand(name);
        if (reloadCommand == null) {
            return null;
        }
        if (type == TYPE_RELOAD) {
            return reloadCommand + ":-r";
        }
        if (type == TYPE_ACTIVE) {
            return reloadCommand + ":-a";
        }
        return null;
    }

    private static String getRedisReloadCommand(String commandClassName) {
        if ("UserCommandCustomer".equals(commandClassName)) return "RELOAD_CUSTOMER";
        if ("UserCommandRoutingInfo".equals(commandClassName)) return "RELOAD_ROUTING";
        if ("UserCommandAntiSpam".equals(commandClassName)) return "RELOAD_ANTISPAM";
        if ("UserCommandContentTpl".equals(commandClassName)) return "RELOAD_CONTENT_TEMPLATE";
        if ("UserCommandPhonePrefix".equals(commandClassName)) return "RELOAD_PHONE_PREFIX";
        if ("UserCommandBlacklist".equals(commandClassName)) return "RELOAD_BLACKLIST";
        if ("UserCommandWhitelist".equals(commandClassName)) return "RELOAD_WHITELIST";
        if ("UserCommandSenderBlacklist".equals(commandClassName)) return "RELOAD_SENDER_BLACKLIST";
        if ("UserCommandSenderWhitelist".equals(commandClassName)) return "RELOAD_SENDER_WHITELIST";
        if ("UserCommandContentBlacklist".equals(commandClassName)) return "RELOAD_CONTENT_BLACKLIST";
        if ("UserCommandContentWhitelist".equals(commandClassName)) return "RELOAD_CONTENT_WHITELIST";
        if ("UserCommandRecipientAddressRule".equals(commandClassName)) return "RELOAD_RECIPIENT_RULE";
        if ("UserCommandRecipientBlacklist".equals(commandClassName)) return "RELOAD_RECIPIENT_BLACKLIST";
        if ("UserCommandVendorContentReplacement".equals(commandClassName)) return "RELOAD_VENDOR_REPLACE";
        if ("UserCommandVendorReplacement".equals(commandClassName)) return "RELOAD_SYSTEM_REPLACE";
        return null;
    }

    private static String getReloadConfigKeyword(UserCommand cmd) {
        String name = cmd.getClass().getSimpleName();
        if ("UserCommandCustomer".equals(name)) return "customer";
        if ("UserCommandRoutingInfo".equals(name)) return "routingInfo";
        if ("UserCommandAntiSpam".equals(name)) return "antiSpam";
        if ("UserCommandContentTpl".equals(name)) return "contentTemplate";
        if ("UserCommandPhonePrefix".equals(name)) return "senderReplacement";
        if ("UserCommandBlacklist".equals(name)) return "blacklist";
        if ("UserCommandWhitelist".equals(name)) return "whitelist";
        if ("UserCommandSenderBlacklist".equals(name)) return "senderBL";
        if ("UserCommandSenderWhitelist".equals(name)) return "senderWL";
        if ("UserCommandContentBlacklist".equals(name)) return "contentBL";
        if ("UserCommandContentWhitelist".equals(name)) return "contentWL";
        if ("UserCommandRecipientAddressRule".equals(name)) return "recipientRule";
        if ("UserCommandRecipientBlacklist".equals(name)) return "recipientBL";
        if ("UserCommandVendorContentReplacement".equals(name)) return "vendorContentRP";
        if ("UserCommandVendorReplacement".equals(name)) return "vendorRP";
        return null;
    }

    private static String buildShutdownControlCommand(UserCommand cmd) {
        List args = cmd.getArgs();
        if (args == null || args.size() < 2) {
            return "SHUTDOWN";
        }
        String second = String.valueOf(args.get(1)).trim();
        if ("-a".equalsIgnoreCase(second)) {
            return "SHUTDOWN";
        }
        StringBuffer targets = new StringBuffer();
        int start = "-ha".equalsIgnoreCase(second) ? 2 : 1;
        for (int i = start; i < args.size(); i++) {
            String target = String.valueOf(args.get(i)).trim();
            if (target.length() == 0) {
                continue;
            }
            if (targets.length() > 0) {
                targets.append(",");
            }
            targets.append(target);
        }
        if (targets.length() == 0) {
            return "SHUTDOWN";
        }
        return "SHUTDOWN@" + targets.toString();
    }

    private static String getSecondArg(UserCommand cmd) {
        List args = cmd.getArgs();
        if (args != null && args.size() > 1) {
            return String.valueOf(args.get(1)).trim();
        }
        return "";
    }
}
