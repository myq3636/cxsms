package com.king.mgt.util;

import java.util.HashMap;


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
public class InfoTable {
    public static final int OK=0;
    public static final int SYNTAX_ERROR=1;
    public static final int INTERNAL_ERROR=2;
    public static final int NOT_SUPPORT=3;
    public static final int CONNECTION_LIMIT=4;
    public static final int AUTH_FAILED=5;
    public static final int ARGS_ERROR=6;
    public static final int SYSTEM_RESPONSE_ERROR=7;

    public static final int BLACKLIST_RELOAD_FTP_FAILED=100;
    public static final int HELP_INFO=200;

    public static final int ALERT_MAIL_SUBJECT=900;

    private HashMap<Integer, String> infoTable=new HashMap<Integer, String>();
    private UserInterfaceUtility util;
    private static InfoTable instance=new InfoTable();
    private InfoTable() {
        this.add(SYNTAX_ERROR, "Syntax Error.");
        this.add(OK, "OK");
        this.add(INTERNAL_ERROR, "System internal error.");
        this.add(NOT_SUPPORT, "Command not support yet.");
        this.add(CONNECTION_LIMIT, "Max connection limit reached.");
        this.add(AUTH_FAILED, "Authentication Failed.");
        this.add(ARGS_ERROR, "Syntax Error.");
        this.add(SYSTEM_RESPONSE_ERROR, "Execution Error.");
        this.add(BLACKLIST_RELOAD_FTP_FAILED, "FTP error when downloading blacklist.cfg");
        this.add(HELP_INFO, "list: Show all available modules of A2P that covered by current management interface\n"
                 + "quit: Close the current user session.\n"
                 + "startup: start modules\n"
                 + "shutdown: stop modules\n"
                 + "restart: stop and start modules\n"
                 + "switchdb: switchdb [master|slave]\n"
                 + "switchredis: switchredis [master|slave]\n"
                 + "switchdns: switchdns [master|slave]\n"
                 + "version: query GW version\n"
                 + "help: List all supported commands with a simple description");

        util=UserInterfaceUtility.getInstance();
        String ip=util.getProperty("Host");
        this.add(ALERT_MAIL_SUBJECT, "[Alert] Management Interface Alert Mail ("+ip+")");
    }
    public static InfoTable getInstance()
    {
        return instance;
    }
    public void add(int code, String info)
    {
        infoTable.put(code,info);
    }
    public String get(int code)
    {
        return infoTable.get(code);
    }
}
