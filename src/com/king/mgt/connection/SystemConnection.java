package com.king.mgt.connection;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemResponse;

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
public class SystemConnection
    extends Thread {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemConnection.class);
    private SystemCommand cmd;
    private Socket socket;
    private int timeout;

    public SystemConnection(SystemCommand cmd, int timeout) {
        this.cmd = cmd;
        this.timeout = timeout;
    }

    public void run() {
        this.sendAndReceive();
    }

    private boolean connect(String ip, int port) {
        try {
            log.info("Connecting to {}:{}",ip,port);
            InetAddress address = InetAddress.getByName(ip);
            socket = new Socket(address, port);
            return true;
        }
        catch(java.net.ConnectException ex)
        {
            log.error(ex,ex);
        }
        catch (IOException ex) {
            log.error(ex, ex);
        }
        return false;
    }

    public void sendAndReceive() {
        SystemResponse response = new SystemResponse();
        if (!connect(cmd.getIp(), cmd.getPort())) {
            response.setResult(SystemResponse.COMMUNICATION_ERROR);
            cmd.setResponse(response);
            return;
        }
        BufferedOutputStream out = null;
        BufferedInputStream in = null;

        try {
            socket.setSoTimeout(timeout);
            out = new BufferedOutputStream(socket.getOutputStream());
            in = new BufferedInputStream(socket.getInputStream());
                    
            out.write(cmd.getByteArray());//reconstruct a new command for UI and send to A2P modules
            out.flush();
            // send
            log.info("Sending cmd...{}",cmd.toString());
            // receive
            log.info("Receiving Response. (SeqID:{})",cmd.getSeqId());
            byte[] readBuffer = new byte[2048];
            int len = in.read(readBuffer, 0, readBuffer.length);
            if (len < 0) {
                log.info("Receiving Failed. (SeqID:{})",cmd.getSeqId());
                response.setResult(SystemResponse.COMMUNICATION_ERROR);
            }
            else if (!response.parse(readBuffer, 0, len)) {
                log.info("Parsing Response faild. (SeqID:{})",cmd.getSeqId());
                response.setResult(SystemResponse.FAILED);
            }
            log.info("Succesful received response (SeqID:{}){}",cmd.getSeqId(),response);
        }
        catch (SocketTimeoutException ex) {
            log.warn("System Connection timeout. (SeqID:"+cmd.getSeqId()+")", ex);
            response.setResult(SystemResponse.COMMUNICATION_ERROR);

        }
        catch (IOException ex) {
            log.error("Exception when sending cmd. (SeqID:"+cmd.getSeqId()+")", ex);
            response.setResult(SystemResponse.COMMUNICATION_ERROR);
        }
        finally {
            try {
                cmd.setResponse(response);
                if (in!=null) in.close();
                if (out!=null) out.close();
                if (socket!=null) socket.close();
            }
            catch (IOException ex) {
                log.error(ex, ex);
            }

        }

    }
}
