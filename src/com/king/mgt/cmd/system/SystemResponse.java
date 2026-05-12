package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.util.ByteBuffer;
import com.king.mgt.util.NotEnoughDataInByteBufferException;


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
public class SystemResponse {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemResponse.class);
    public static final byte SUCCESS=0;
    public static final byte FAILED=1;
    public static final byte COMMUNICATION_ERROR=2;
    public static final byte BUSY = 3;
    protected int totalLen;
    protected int seqId=-1;
    protected byte result=0;
    public SystemResponse() {
    }

    public boolean isSuccess()
    {
        return result==SUCCESS;
    }

    public boolean isBusy(){
        return result==BUSY;
    }

    public String getErrorText()
    {
        switch(result)
        {
            case SUCCESS:
                return "Success";
            case FAILED:
                return "Execution Failed";
            case COMMUNICATION_ERROR:
                return "Communication Error";
            case BUSY:
                return "a same command handling";

        }
        return null;
    }
    public boolean parse(byte[] response, int offset, int length)
    {
        ByteBuffer buf=new ByteBuffer(response, offset, length);
        totalLen = 0;
        try {
            totalLen = buf.readInt();
            if (totalLen!=buf.length()){
                log.error("Total length error. Length: {}, expected: {}",totalLen,buf.length());
                return false;
            }
            // remove totalLength.
            buf.removeInt();
            seqId=buf.removeInt();
            result=buf.removeByte();
            return true;

        }
        catch (NotEnoughDataInByteBufferException ex) {
            log.error(ex,ex);
            return false;
        }

    }

    public byte getResult() {
        return result;
    }

    public int getSeqId() {
        return seqId;
    }

    public void setResult(byte result) {
        this.result = result;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }
    public String toString()
    {
        String s="\n========= System Response =========\n";
        s+="Total len: "+this.totalLen+"\n";
        s+="Seq ID: "+this.seqId+"\n";
        s+="Result: "+this.result;
        return s;
    }
}
