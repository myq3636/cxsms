package com.king.mgt.cmd.system;


import com.king.mgt.cmd.user.*;
import com.king.mgt.context.*;
import com.king.mgt.util.*;

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
public abstract class SystemCommand {
    protected int length=0;
    protected int seqId=-1;
    protected short cmdId=0;
    protected UserCommand cmd;
    protected ByteBuffer body;
    protected ByteBuffer head;
    protected UserInterfaceUtility util=UserInterfaceUtility.getInstance();
    protected Module module;
    protected SystemResponse response=null;

    public abstract boolean genBody();

    public SystemCommand(UserCommand cmd, short cmdId) {

        this.cmd=cmd;
        this.cmdId=cmdId;
        this.seqId=util.genCommandSeqID(cmd.getGroupId());
        body=new ByteBuffer();
        head=new ByteBuffer();
    }
    public byte[] getByteArray()
    {
    	body=new ByteBuffer();
        head=new ByteBuffer();
    	this.genBody();
        this.genHead();
        return head.getBuffer();
    }

    public boolean genHead()
    {
        this.length=4+4+2+body.length();
        head.appendInt(length);
        head.appendShort(cmdId);
        head.appendInt(seqId);
        if(body.length() > 0){
            head.appendBytes(body.getBuffer());
        }
        return true;
    }
    public String getIp() {
        return module.getIp();
    }

    public int getPort() {
        return module.getPort();
    }

    public int getSeqId() {
        return seqId;
    }

    public short getCmdId() {
        return cmdId;
    }

    public UserCommand getCmd() {
        return cmd;
    }

    public ByteBuffer getBody() {
        return body;
    }

    public void setModule(Module module)
    {
        this.module=module;
    }

    public Module getModule()
    {
        return module;
    }

    public SystemResponse getResponse() {
        return response;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public void setResponse(SystemResponse response) {
        this.response = response;
    }
    public String toString()
    {
        String s="\n========= System Command("+cmdId+") =========\n";
        s+="Total len: "+this.length+"\n";
        s+="Cmd ID: "+this.cmdId+"\n";
        s+="Seq ID: "+this.seqId+"\n";
        if(this.body.length() > 0){
            s += "Body: " + this.body;
        }
        return s;
    }
}
