package com.king.mgt.connection;

import java.net.Socket;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.context.ContextManager;
import com.king.mgt.util.InfoTable;

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

    public TelnetSession(ContextManager context, Socket socket, int timeout) {
        super(context, socket, timeout);
    }

    public void service() {
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

            boolean success = RedisControlCommands.handles(cmd) ? RedisControlCommands.publish(cmd) : cmd.process();
            if (!success) {
                log.warn("[MGT-ALERT] user={} command={}: {}", cmd.getContext() == null ? null : cmd.getContext().getCurUser(), cmd.getCmdLine(), info.get(InfoTable.SYSTEM_RESPONSE_ERROR));                
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
        Long subscribers = RedisControlCommands.publishControlCommand(cmd);
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
