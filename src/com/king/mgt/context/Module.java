package com.king.mgt.context;

import com.king.framework.SystemLogger;

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
public class Module {
    private String name;
    private String ip;
    private int port;

    public Module(String name, String ip, int port) {
        this.name=name;
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public String getName() {
        return name;
    }

    public int getPort() {
        return port;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof Module)) return false;
        Module module=(Module)o;
        return this.name.equalsIgnoreCase(module.getName());
    }
    public String toString()
    {
        return name+"("+ip+":"+port+")";
    }
}
