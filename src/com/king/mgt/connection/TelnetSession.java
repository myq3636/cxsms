package com.king.mgt.connection;

import java.util.List;
import java.net.Socket;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.GmmsUtility;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.context.ContextManager;
import com.king.mgt.connection.FTP4Client;
import com.king.mgt.util.InfoTable;
import com.king.mgt.util.MailSender;
import com.king.redis.RedisClient;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class TelnetSession extends UserSession{

    private static SystemLogger log = SystemLogger.getSystemLogger(TelnetSession.class);
    private static final int TYPE_RELOAD = 4;
    private static final int TYPE_ACTIVE = 6;

    public TelnetSession(ContextManager context, Socket socket, int timeout) {
        super(context, socket, timeout);
    }

    public void service() {
    	MailSender mailSender=util.getMailSender();
        sendResponse("Welcome! Connection from "+context.getCurUserIP()+".\n"+context.getPrompt());
//        sendResponse("If want to start modules,please start in such order:\n1.SystemManager,2.DeliveryRouter,3.Servers,4.Clients,5.MsgQueueMonitor.\n"+context.getPrompt());
        while(connected){

            String input = getInput();
            if(input == null){
                log.info("receive data null!");
                continue;
            }
            log.trace("Input command: {}",input);
            String inputLine = input.trim();
            if (publishRawRedisControlCommand(inputLine)) {
                sendResponse("\n Command completed  successfully!");
                sendResponse("\n"+context.getPrompt());
                continue;
            }
            UserCommand cmd = this.commandSet.parseCommand(input.trim(),
                                               context);
            if (cmd == null) {
                sendResponse(info.get(InfoTable.SYNTAX_ERROR));
                sendResponse("\n"+context.getPrompt());
                continue;
            }
            log.trace("UserCommandType: {}",cmd.getType());

            boolean success = publishRedisControlCommand(cmd);
            if (!success) {
                success = cmd.process();
            }
            if (!success) {
                mailSender.sendAlertMail(cmd, InfoTable.SYSTEM_RESPONSE_ERROR);                
            }
            sendResponse(cmd.getResp());

            if (cmd.isQuit()) {
                break;
            }
            sendResponse("\n Command completed  successfully!");
            sendResponse("\n"+context.getPrompt());

        }
    }

    private boolean publishRawRedisControlCommand(String inputLine) {
        if (inputLine == null || inputLine.length() == 0) {
            return false;
        }
        String cmd = inputLine.trim().toUpperCase();
        if (!cmd.startsWith("STATUS_REFRESH")
                && !cmd.startsWith("STATUS_REREGISTER")
                && !cmd.startsWith("STATUS_OFFLINE")
                && !cmd.startsWith("RELOAD_GMMS_CONFIG")
                && !cmd.startsWith("GMMSCONFIG")) {
            return false;
        }
        if (cmd.startsWith("GMMSCONFIG")) {
            cmd = buildGmmsConfigControlCommand(inputLine);
        }
        Long subscribers = publishControlCommand(cmd);
        sendResponse("Published redis control command [" + cmd + "] to " + subscribers + " subscriber(s).");
        return true;
    }

    private String buildGmmsConfigControlCommand(String inputLine) {
        String args = "-a";
        String targets = null;
        String[] parts = inputLine.trim().split("\\s+");
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.length() == 0) {
                continue;
            }
            if (part.startsWith("@")) {
                targets = part.substring(1);
            } else {
                args = part;
            }
        }
        String command = "RELOAD_GMMS_CONFIG";
        if (targets != null && targets.length() > 0) {
            command = command + "@" + targets;
        }
        return command + ":" + args;
    }

    private boolean publishRedisControlCommand(UserCommand cmd) {
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

    private Long publishControlCommand(String command) {
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

    private String toRedisControlCommand(UserCommand cmd) {
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

    private String getRedisReloadCommand(String commandClassName) {
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

    private String getReloadConfigKeyword(UserCommand cmd) {
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

    private String buildShutdownControlCommand(UserCommand cmd) {
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

    private String getSecondArg(UserCommand cmd) {
        List args = cmd.getArgs();
        if (args != null && args.size() > 1) {
            return String.valueOf(args.get(1)).trim();
        }
        return "";
    }

    private void sendResponse(String str){
        if(str != null){
            log.trace("send response to user:{}",str);
            try {
                writer.write(str);
                writer.flush();
            }
            catch (Exception ex) {
                log.info("Telnet session write error:{}", ex);
                closeConnection();
            }
        }
    }

    private String getInput() {
        char[] buf = new char[1024];
        int len = 0;
        String input = null;
        try {
            len = reader.read(buf, 0, buf.length);
            if (len < 0) {
                log.trace("Connection Reset.");
                closeConnection();
            }
            input = new String(buf,0,len);
            return input;

        }
        catch (java.net.SocketTimeoutException ex) {
            log.info("Telnet Connection Timeout.");
            closeConnection();
        }
        catch (Exception ex) {
            log.info("Telnet session error:{}", ex);
            closeConnection();
        }
        return input;
    }
}
