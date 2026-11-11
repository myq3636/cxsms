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
public class CMToolsSession extends UserSession{
    private static SystemLogger log = SystemLogger.getSystemLogger(CMToolsSession.class);
    public CMToolsSession(ContextManager context, Socket socket, int timeout) {
        super(context, socket, timeout);
    }

    public void service() {
        UserCommand cmd=null;
        try {
            char[] buf=new char[1024];
            int len=0;
            try{
                len=reader.read(buf,0,buf.length);
            }
            catch(java.net.SocketTimeoutException ex)
            {
                log.info("CMTools Connection Timeout.");
                return;
            }
            if (len<0)
            {
                log.trace("Connection Reset.");
                return;
            }
            String input = new String(buf,0,len);
            log.trace("Input command: {}",input);
            cmd = this.commandSet.parseCommand(input.trim(),
                context);
            if (cmd == null) {
                writer.write(info.get(InfoTable.SYNTAX_ERROR));
                writer.flush();
                return;
            }
            log.trace("UserCommandType: {}",cmd.getType());
            writer.write(info.get(InfoTable.OK));
            writer.flush();
            closeConnection();

            if (!cmd.process()) {
                log.warn("[MGT-ALERT] user={} command={}: {}", cmd.getContext() == null ? null : cmd.getContext().getCurUser(), cmd.getCmdLine(), info.get(InfoTable.SYSTEM_RESPONSE_ERROR));
                return;
            }
            // Dispatch and notify modules.
           /* if (util.isEnableNotification() &&
                cmd.getNotifyType() != UserCommand.Notify_None) {
                // We need to notify.
                if (!dispatcher.dispatch(cmd, context)) {
                    log.warn("[MGT-ALERT] user={} command={}: {}", cmd.getContext() == null ? null : cmd.getContext().getCurUser(), cmd.getCmdLine(), info.get(InfoTable.INTERNAL_ERROR));
                    return;
                }
                SystemCommandSender sender = new SystemCommandSender();
                sender.send(cmd.getSystemCommands());
                if (!cmd.isSuccess()) {
                    log.warn("[MGT-ALERT] command={}: failed on some modules", cmd.getCmdLine());
                    return;
                }
            }*/
            log.trace("Operation Done.");

        }
        catch(Exception ex)
        {
            log.error(ex,ex);
            log.warn("[MGT-ALERT] user={} command={}: {}", cmd.getContext() == null ? null : cmd.getContext().getCurUser(), cmd.getCmdLine(), info.get(InfoTable.INTERNAL_ERROR));
            return;
        }
    }
}
