package com.king.mgt.connection;

import java.net.ServerSocket;
import java.net.Socket;

import com.king.framework.SystemLogger;
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
public class UserCommandListener extends Thread{
    private static SystemLogger log=SystemLogger.getSystemLogger(UserCommandListener.class);
    private boolean keepRunning=false;
    private int port=-1;
    private int timeout=5*60*1000; // 5 mins
    private UserInterfaceUtility utility=UserInterfaceUtility.getInstance();
    private SessionFactory factory=null;
    public UserCommandListener() {

    }
    public void run()
    {
        ServerSocket listenSocket=null;
        try {
            listenSocket = new ServerSocket(port);
            listenSocket.setReuseAddress(true);
            log.trace("Listening on {}",port);
        }catch(Exception ex)
        {
            log.fatal("Create listening port failed.", ex);
            return;
        }
        while (keepRunning) {
            try {
                Socket socket = listenSocket.accept();
                UserSession session = factory.allocateUserSession(socket);
                if (session != null) {
                    session.start();
                }
                else {
                    if (socket!=null)
                        socket.close();
                }
            } catch (Exception ex) {
                log.error(ex,ex);
            }

        }
    }
    public void start()
    {
        this.keepRunning=true;
        this.init();
        super.start();
    }
    public void init()
    {
        try {
            String sPort = utility.getProperty("port");
            if (sPort != null) {
                port = Integer.parseInt(sPort);
            }
            factory=utility.getSessionFactory();
        } catch (NumberFormatException ex) {
            log.error(ex,ex);
        }
    }
}
