package com.king.framework.lifecycle.cmd;

import java.util.List;

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
public abstract class SystemResponse {
    protected int seqId=-1;
    protected byte result=-1;
    protected ByteBuffer body;
    protected ByteBuffer head;
    protected List args;
    public SystemResponse(SystemCommand req, int result, List args) {
        this.seqId=req.getSeqId();
        this.result=mapResultCode(result);
        this.args=args;
        head=new ByteBuffer();
        body=new ByteBuffer();
        this.genBody();
        this.genHead();
    }
    public void genHead()
    {
        head.appendInt(body.length()+4+4+1);
        head.appendInt(seqId);
        head.appendByte(result);
        if (body.length()>0)
        {
            head.appendBytes(body.getBuffer());
        }
    }

    public ByteBuffer getByteBuffer()
    {
        return head;
    }
    public byte[] getBytes()
    {
        return head.getBuffer();
    }

    public abstract void genBody();
    public abstract byte mapResultCode(int code);

}
