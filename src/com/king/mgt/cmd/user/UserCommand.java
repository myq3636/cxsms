package com.king.mgt.cmd.user;

import java.util.*;

import com.king.mgt.context.*;
import com.king.mgt.processor.*;

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
public abstract class UserCommand {

    // for commands not need to notify, such as Help or update cm.cfg
    public static final int Notify_None = 0;
    // for context related comamnd, such as Shutdown.
    public static final int Notify_Current = 1;
    // for all module notification.
    public static final int Notify_All = 2;
    // for notifing particular modules.
    public static final int Notify_Fix = 3;

    // Command ID. Identify the command type,
    // such as blacklist, help, shutdown, etc.
    protected int cmdId;

    // command subtype.
    // for example, blacklist command will have 4 subtypes:
    // add, remove, file and reload.
    protected int type;

    // Args list
    protected List args;

    protected ContextManager context;

    protected int errorCode = 0;

    private boolean isQuite = false;

    //response default ok;
    private StringBuffer resp = null;

    protected int notifyType = -1;

    protected String cmdLine;
    protected int GroupId ;

    protected CommandProcessor processor = null;

    public UserCommand(int notifyType) {
        this.notifyType = notifyType;
        args = new ArrayList<String>();
        resp = new StringBuffer();
        processor = new CommandProcessor(this);
    }

    public int getNotifyType() {
        return notifyType;
    }

    public int getType() {
        return type;
    }

    
    public void setType(int type) {
		this.type = type;
	}

	public void setGroupId(int id){
        this.GroupId = id;
    }

    public int getGroupId(){
        return this.GroupId;
    }

    public void setErrorCode(int code) {
        this.errorCode = code;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setIsQuit(boolean flag){
        this.isQuite = flag;
    }

    public boolean isQuit(){
        return isQuite;
    }

    public void append(String resp){
        this.resp.append(resp);
    }

    public String getResp(){
        return this.resp.toString();
    }

    public void setContext(ContextManager context) {
        this.context = context;
    }

    public void setCmdLine(String cmdLine) {
        this.cmdLine = cmdLine;
    }

    public ContextManager getContext() {
        return context;
    }

    public List getArgs() {
        return this.args;
    }

    public String getCmdLine() {
        return cmdLine;
    }

    public abstract boolean parseArgs(String[] args);

    public abstract boolean process();


}
