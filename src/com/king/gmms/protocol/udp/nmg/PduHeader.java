/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.udp.nmg.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.udp.nmg.exception.UnknownCommandIdException;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class PduHeader {
	private static SystemLogger log = SystemLogger.getSystemLogger(PduHeader.class);

	protected long totalLength = 0;
	protected int commandId = 0;

	private int version = Pdu.VERSION_2_0;

	public PduHeader() {
		this(Pdu.VERSION_2_0);
	}
	
	 public PduHeader(int version){
	        setVersion(version);
	    }

	    public int getCommandId() {
	        return commandId;
	    }

	    public long getTotalLength() {
	        return totalLength;
	    }

	    public static PduHeader parseHeader(UdpByteBuffer buffer, int version) throws
	            NotEnoughDataInByteBufferException, UnknownCommandIdException {
	        if(buffer == null) {
	            log.error("TcpByteBuffer is null in parseHeader method.");
	            throw new NotEnoughDataInByteBufferException("TcpByteBuffer is null in parseHeader method.");
	        }

	        PduHeader theHeader = new PduHeader(version);

	        if (theHeader.version == Pdu.VERSION_1_0){
	            theHeader.totalLength = buffer.removeBytesAsInt(2);
	            theHeader.commandId = buffer.remove1ByteAsInt();
	        }else{
	            theHeader.totalLength = buffer.remove4BytesAsLong();
	            theHeader.commandId = buffer.remove1ByteAsInt();
	        }

	        boolean found = false;

	        for (int i = 0; i < Pdu.commandIdList.length; i++) {
	            if (theHeader.commandId == Pdu.commandIdList[i]) {
	                found = true;
	                break;
	            }
	        }
	        if (!found) {
	        	if(log.isInfoEnabled()){
					log.info("PduHeader commandId={}, version={}" ,theHeader.commandId,theHeader.getVersion());
	        	}
	            throw new UnknownCommandIdException(theHeader.commandId);
	        }

	        return theHeader;

	    }

	    public static PduHeader parseHeader(UdpByteBuffer buffer) throws
	            NotEnoughDataInByteBufferException, UnknownCommandIdException {
	        return parseHeader(buffer, Pdu.VERSION_2_0);
	    }


	    public void setCommandId(int newCommandId) {
	        commandId = newCommandId;
	    }


	    public void setTotalLength(long newTotalLength) {
	        totalLength = newTotalLength;
	    }

	    public UdpByteBuffer toByteBuffer() {

	    	UdpByteBuffer buffer = new UdpByteBuffer();
	        try {
	            if (version == Pdu.VERSION_1_0){
	                buffer.appendIntAs2Byte((int)totalLength);
	                buffer.appendIntAs1Byte(commandId);
	            }else{
	                buffer.appendLongAs4Bytes(totalLength);
	                buffer.appendIntAs1Byte(commandId);
	            }

	        } catch (Exception e) {
	            log.error(e, e);
	        }
	        return buffer;
	    }

	    public void setVersion(int version){
	    	this.version = version;
	    }

	    public int getVersion(){
	        return version;
	    }

}
