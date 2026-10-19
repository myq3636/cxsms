package com.king.mgt.connection;

import java.net.InetSocketAddress;
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
            listenSocket = new ServerSocket();
            listenSocket.setReuseAddress(true);
            listenSocket.bind(new InetSocketAddress(port));
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
        this.init();
        if (port <= 0) {
            log.fatal("Invalid user command listening port. port={}", port);
            return;
        }
        if (factory == null) {
            log.fatal("User command listener can not start because SessionFactory is null.");
            return;
        }
        this.keepRunning=true;
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
            log.info("User command listener init. port={}, sessionFactoryReady={}", port, factory != null);
        } catch (NumberFormatException ex) {
            log.error(ex,ex);
        }
    }
}
