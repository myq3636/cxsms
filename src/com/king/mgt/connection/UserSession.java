package com.king.mgt.connection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import com.king.framework.SystemLogger;
import com.king.mgt.context.ContextManager;
import com.king.mgt.context.ModuleDispatcher;
import com.king.mgt.util.InfoTable;
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
public abstract class UserSession extends Thread {
    private static SystemLogger log = SystemLogger.getSystemLogger(UserSession.class);
    protected Socket socket;
    protected int timeout;
    protected boolean connected = false;
    protected UserCommandSet commandSet;
    protected UserInterfaceUtility util;
    protected ContextManager context;
    protected ModuleDispatcher dispatcher;
    protected InfoTable info=InfoTable.getInstance();
    protected BufferedReader reader = null;
    protected BufferedWriter writer = null;
    protected SessionFactory factory;
    public UserSession(ContextManager context, Socket socket, int timeout) {
        this.socket = socket;
        this.timeout = timeout;
        this.context = context;
        util = UserInterfaceUtility.getInstance();
        commandSet = util.getUserCommandSet();
        factory=util.getSessionFactory();
        dispatcher=new ModuleDispatcher();
    }

    public void closeConnection() {

        if (connected) {
            try {
                connected = false;
                if (reader != null)
                    reader.close();
                if (writer != null)
                    writer.close();
                if (socket != null)
                    socket.close();
            }
            catch (IOException ex) {
                log.error(ex, ex);
            }

            factory.releaseUserSession(this);
            log.info("Connection closed.");
        }
    }
    public void run() {
        try {
            connected=true;
            socket.setSoTimeout(timeout);
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(socket.
                getInputStream())));
            writer = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(socket.
                getOutputStream())));
            // call session handling logic.
            this.service();

        } catch (IOException ex) {
            log.error(ex, ex);
        }
        catch(Exception ex)
        {
            log.error(ex,ex);
        }
        catch(Throwable t)
        {
            log.fatal(t,t);
        }
        finally
        {
            closeConnection();
        }

    }

    public abstract void service();
}
