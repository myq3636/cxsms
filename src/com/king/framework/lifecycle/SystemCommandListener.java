package com.king.framework.lifecycle;


import java.net.Socket;
import java.io.IOException;
import java.net.ServerSocket;
import java.io.BufferedOutputStream;
import java.util.concurrent.atomic.*;
import java.util.concurrent.LinkedBlockingQueue;





import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.cmd.SystemResponse;
import com.king.framework.lifecycle.event.*;
import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.message.gmms.*;

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
public class SystemCommandListener {
	protected static SystemLogger log = SystemLogger
	.getSystemLogger(SystemCommandListener.class);
    private static final int SYSTEM_LEVEL = 0;
    private static final int APP_LEVEL = 1;
    protected static int port;
    private int timeout = 10*1000;
    Socket socket = null;
    ServerSocket server = null;
    private static boolean isRunning = false;
    protected boolean isEnableSysMgt = false;
    protected SystemSession systemSession =  null; // system client
    private AtomicBoolean isSysAlow = new AtomicBoolean(true);
    private LinkedBlockingQueue<SystemCommandSessionInfo> appQueue = new
        LinkedBlockingQueue<SystemCommandSessionInfo> ();
    private LinkedBlockingQueue<SystemCommandSessionInfo> sysQueue = new
        LinkedBlockingQueue<SystemCommandSessionInfo> ();


    private LifecycleSupport lifecycleSupport;
    public SystemCommandListener(int port) {
        this.port = port;
    }
    
    public void service() {
    	if(isRunning){
    		log.warn("Command listener already bind on port:{}",port);
    		return;
    	}
        GmmsUtility util = GmmsUtility.getInstance();
        timeout = Integer.parseInt(util.getCommonProperty(
            "CommandListener.timeout", "10")) * 1000;
        lifecycleSupport=util.getLifecycleSupport();
        isEnableSysMgt = util.isSystemManageEnable();
        try {
            //startup two thread to handle app command and system command
            Thread thread_sys = new ProcessSystemCommand(SYSTEM_LEVEL,sysQueue);
            thread_sys.start();
            Thread thread_app = new ProcessSystemCommand(APP_LEVEL,appQueue);
            thread_app.start();
            if(isEnableSysMgt){
            	SystemSessionFactory sysFactory = SystemSessionFactory.getInstance();
    			systemSession = sysFactory.getSystemSessionForFunction();
            }
            server = new ServerSocket(port);
            server.setReuseAddress(true);
            while (true) {
                try {
                    socket = server.accept();
                    socket.setSoTimeout(timeout);

                    SystemCommandSession session = new SystemCommandSession(
                        socket, isSysAlow, sysQueue, appQueue);
                    session.start();
                    
                }
                catch (java.net.SocketTimeoutException ex) {
                    log.warn("Command Receiver timeout.");
                }
                catch (Exception ex) {
                    log.error(ex, ex);
                }
      		  isRunning=true;
            }
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
        finally {
            try {
                if (server != null) {
                    server.close();
                    server = null;
                }
            }
            catch (IOException e) {
                log.error(e.getMessage());
            }
        }

    }
    /**
     * stop service
     */
    public void stopService() {
    	 try {
             if (server != null) {
                 server.close();
                 server = null;
             }
         }
         catch (IOException e) {
             log.error(e.getMessage());
         }
    }
    class ProcessSystemCommand extends Thread{

        private SystemCommandSessionInfo info = null;
        private int type;
        private boolean running;
        BufferedOutputStream out = null;
        private LinkedBlockingQueue<SystemCommandSessionInfo> queue = null;

        public ProcessSystemCommand(int type,LinkedBlockingQueue<SystemCommandSessionInfo>queue){
            //type=0 system level; type=1 app level
            this.type = type;
            this.queue = queue;
            this.running = true;
            super.setName("ProcessSysCommandThread_" + type);
        }

        private boolean isShutdown(Event event){
            return (event.getEventType() == Event.TYPE_SHUTDOWN);

        }
        public void run(){
            while(running){
                try {
                    log.info("ProcessSystemCommand Thread is runing! type is:{}",type);
                    info = queue.take();
                    if(info != null){
                        if(isShutdown(info.getEvent())){
                            int group = (info.getSystemCommand().getSeqId())/1000;
                            log.info("the command is shutdown! get wirter and group :{}", group);
                            MessageBackupWriter.getInstance().initialize(group);
                        }
                        int rtnval=lifecycleSupport.notify(info.getEvent());
                        log.info("notify result:{}",rtnval);
                        SystemResponse resp = info.getSystemCommand().makeResponse(rtnval, null);
                        out = new BufferedOutputStream(info.getSocket().getOutputStream());
                        out.write(resp.getBytes());
                        out.flush();
                    }
                }
                catch (Exception ex) {
                    log.info("ProcessSystemCommand error!{}", ex);
                }
                finally {
                    try {
                        if(out != null)
                            out.close();
                        if(info.getSocket() != null){
                            info.getSocket().close();
                        }
                        out = null;
                        if(type == SYSTEM_LEVEL){
                            isSysAlow.set(true);
                        }

                    }
                    catch (Exception ex) {
                        log.info("ProcessSystemCommand error!{}", ex);
                    }
                    if(isShutdown(info.getEvent())){
                    	if(isEnableSysMgt){
                        	systemSession.moduleStop();
                        }
                        log.info("handle over shutdown command exit!");
                        MessageBackupWriter.getInstance().close();
                        System.exit(0);
                    }


                }

            }
        }

    }

	public static int getPort() {
		return port;
	}

	public static void setPort(int port) {
		SystemCommandListener.port = port;
	}

	public static boolean isRunning() {
		return isRunning;
	}

	public static void setRunning(boolean isRunning) {
		SystemCommandListener.isRunning = isRunning;
	}
}
