package com.king.framework.lifecycle;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;




import com.king.framework.lifecycle.cmd.SystemCommand;
import com.king.framework.lifecycle.cmd.SystemCommandSet;
import com.king.framework.lifecycle.cmd.SystemResponse;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.protocol.tcp.ByteBuffer;

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
public class SystemCommandSession extends Thread{

    private static Logger log = LogManager.getLogger(SystemCommandSession.class);

    private Socket socket;
    private BufferedInputStream in = null;
    private BufferedOutputStream out = null;
    private SystemCommandSet cmdSet;
    private AtomicBoolean isSysAlow;
    private Queue sysQueue;
    private Queue appQueue;

    public SystemCommandSession(Socket socket, AtomicBoolean flag, Queue sys,
                                Queue app) {
        this.socket = socket;
        cmdSet = new SystemCommandSet();
        isSysAlow = flag;
        sysQueue = sys;
        appQueue = app;

    }

    private SystemCommand parseCommand(ByteBuffer buffer) {

        SystemCommand cmd = cmdSet.parseCommand(buffer);
        if (cmd == null) {
            log.info("get command null!");
            return null;
        }

        return cmd;

    }


    public void run(){
        byte[] buf = new byte[2048];

        try {
            in = new BufferedInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());
            int len = in.read(buf);
            if(len < 0){
                log.info("connection reset!");
                closeSession();
                return;
            }
            ByteBuffer byteBuf = new ByteBuffer(buf, 0, len);

            SystemCommand cmd = parseCommand(byteBuf);

            if (cmd == null) {
                log.info("parse command error!");
                closeSession();
                return;
            }
            Event event = cmd.getEvent();

            if (event == null) {
                log.warn("Unknown Command.");
                closeSession();
                return;
            }

            if(!cmd.isSysLevel()){
            	if (log.isInfoEnabled()) {
            		log.info("Event type={} command put into app queue!",event.getEventType());
            	}
                appQueue.offer(new SystemCommandSessionInfo(socket,cmd,event));
            }
            else{
                //if can handle cmd then set flag = false;
                if(isSysAlow.compareAndSet(true,false)){
                	 log.info("system queue is free put into queue! Event type=" + event.getEventType());
                    sysQueue.offer(new SystemCommandSessionInfo(socket,cmd,event));
                }
                else{
                    log.info("system queue is busy return wait!");
                    SystemResponse resp = cmd.makeResponse(3,null);
                    out.write(resp.getBytes());
                    out.flush();
                    closeSession();
                }
            }

        }
        catch (java.net.SocketTimeoutException ex) {
            log.warn("Command Receiver timeout.");
            closeSession();
        }

        catch (Exception ex) {
            log.error(ex, ex);
            closeSession();
        }
    }

    private void closeSession() {
        try {
            if (in != null)
                in.close();
            if (out != null)
                out.close();
            if (socket != null)
                socket.close();
        }
        catch (Exception ex) {
            log.error(ex, ex);
        }

    }


}
