package com.king.framework.lifecycle.cmd;

import java.io.UnsupportedEncodingException;
import java.util.*;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.*;
import com.king.gmms.protocol.tcp.ByteBuffer;
import com.king.gmms.protocol.tcp.NotEnoughDataInByteBufferException;

/**
 * <p>Title: SystemCommand</p>
 * Base class of Module received system command
 * <p>Description: </p>
 * Parsing the received byte flow to system command structure; build the Event;
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public abstract class SystemCommand {
	private SystemLogger log = SystemLogger.getSystemLogger(SystemCommand.class);
    protected int seqId = -1;
    protected short cmdId = 0;
    protected List args;
    protected boolean isSysLevel = false;

    public SystemCommand() {
        super();
    }
    public abstract SystemResponse makeResponse(int result, List args);
    public abstract Event getEvent();
    public boolean parseArgs(ByteBuffer buf){
    	try{
    		String argStr = buf.removeCString(buf.length());
        	String[] argArr = argStr.trim().split(" ");
        	if(argArr.length>0){
        		args = Arrays.asList(argArr);
        	}
    	}catch (NotEnoughDataInByteBufferException ex) {
            log.error(ex,ex);
            return false;
        }catch (UnsupportedEncodingException e) {
        	log.error(e,e);
        	return false;
    	}
        return true;
    }
    public short getCmdId() {
        return cmdId;
    }

    public boolean isSysLevel(){
        return isSysLevel;
    }

    public int getSeqId() {
        return seqId;
    }

    public List getArgs() {
        return args;
    }

    public void setArgs(List args) {
        this.args = args;
    }

    public void setCmdId(short cmdId) {
        this.cmdId = cmdId;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }
}
