package com.king.framework.lifecycle;

import java.net.Socket;

import com.king.framework.lifecycle.cmd.*;
import com.king.framework.lifecycle.event.Event;

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
class SystemCommandSessionInfo {

    Socket socket;
    SystemCommand cmd;
    Event event;

    public SystemCommandSessionInfo(Socket socket, SystemCommand cmd,Event event) {
        this.socket = socket;
        this.cmd = cmd;
        this.event = event;
    }

    public Event getEvent() {
        return event;
    }

    public SystemCommand getSystemCommand(){
        return cmd;
    }

    public Socket getSocket() {
        return socket;
    }

}
