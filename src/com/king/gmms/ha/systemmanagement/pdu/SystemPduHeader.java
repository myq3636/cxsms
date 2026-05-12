package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownCommandIdException;
public class SystemPduHeader {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemPduHeader.class);
	protected long totalLength = 0;
    protected int commandId = 0;
    private int sequenceNumber = 1;
    private String confFileVersion = null;
    
    public SystemPduHeader(){
    	A2PCustomerManager cst = GmmsUtility.getInstance().getCustomerManager();
    	confFileVersion = cst.getConfFileVersion();
    }
    
    public SystemPduHeader(SystemPduHeader pdu){
    	this.totalLength = pdu.totalLength;
    	this.commandId = pdu.commandId;
    	this.sequenceNumber = pdu.sequenceNumber;
    	this.confFileVersion = pdu.confFileVersion;
    }
    
    public static SystemPduHeader parseHeader(TcpByteBuffer buffer) throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException, UnknownCommandIdException {
        if(buffer == null) {
            log.error("TcpByteBuffer is null in parseHeader method.");
            throw new NotEnoughDataInByteBufferException("TcpByteBuffer is null in parseHeader method.");
        }

        SystemPduHeader theHeader = new SystemPduHeader();

        theHeader.totalLength = buffer.remove4BytesAsLong();
        theHeader.commandId = buffer.remove1ByteAsInt();
        theHeader.sequenceNumber = buffer.removeInt();
        theHeader.confFileVersion = buffer.removeString(32);
        boolean found = false;

        for (int i = 0; i < SystemPdu.commandIdList.length; i++) {
            if (theHeader.commandId == SystemPdu.commandIdList[i]) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new UnknownCommandIdException(theHeader.commandId);
        }

        return theHeader;

    }


    public void setCommandId(int newCommandId) {
        commandId = newCommandId;
    }


    public void setTotalLength(long newTotalLength) {
        totalLength = newTotalLength;
    }

    public TcpByteBuffer toByteBuffer() {

        TcpByteBuffer buffer = new TcpByteBuffer();
        try {
            buffer.appendLongAs4Bytes(totalLength);
            buffer.appendIntAs1Byte(commandId);
            buffer.appendInt(sequenceNumber);
            buffer.appendString(confFileVersion);
        } catch (Exception e) {
            log.error(e, e);
        }
        return buffer;
    }


	public long getTotalLength() {
		return totalLength;
	}


	public int getCommandId() {
		return commandId;
	}


	public int getSequenceNumber() {
		return sequenceNumber;
	}


	public void setSequenceNumber(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}


	public String getConfFileVersion() {
		return confFileVersion;
	}


	public void setConfFileVersion(String confFileVersion) {
		this.confFileVersion = confFileVersion;
	}
}
