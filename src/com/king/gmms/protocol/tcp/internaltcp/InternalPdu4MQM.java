package com.king.gmms.protocol.tcp.internaltcp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.ByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class InternalPdu4MQM
    extends Pdu {
    private ByteBuffer content = null;
    
    public InternalPdu4MQM() {
    }

    public InternalPdu4MQM(int commandId) {
        if (header == null) {
            header = new PduHeader(commandId);
        }else{
        	header.setCommandId(commandId);
        }
    }

    public void parsePduCommand(TcpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {
        if (buffer == null) {
            return;
        }
        byte[] bytes = buffer.removeBytes(buffer.getBuffer().length).getBuffer();
        content = new ByteBuffer(); 
        content.setBuffer(bytes);
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
    	GmmsMessage msg = null;
    	ObjectInputStream ois = null;
    	try{
    		ByteArrayInputStream bais = new ByteArrayInputStream(this.content.getBuffer());  
            ois = new ObjectInputStream(bais);
            msg = (GmmsMessage)ois.readObject();
    	}catch(IOException e1){
    		log.error("convertToMsg IOException:{}",e1.getMessage());
    	}catch(ClassNotFoundException e2){
    		log.error("convertToMsg ClassNotFoundException:{}",e2.getMessage());
    	}finally{
    		if(ois!=null){
    			try{
    				ois.close();
    			}catch(Exception e){
    	    		log.error("ObjectInputStream close error:{}",e.getMessage());
    			}
    		}
    	}
        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	TcpByteBuffer buffer = null;
    	if(content!=null){
            buffer = new TcpByteBuffer(content.getBuffer());
    	}
        return buffer;
    }

    public String toString() {
        return new StringBuffer("InternalPdu4MQM:")
            .append("content:").append(content)
            .toString();
    }


    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	try{
    		ByteBuffer byteBuffer = msg.serialize();
    		this.content = byteBuffer;
    		header.setTotalLength(content.length()+PDU_HEADER_SIZE);
    	}catch(ClassNotFoundException e1){
    		log.error("convertFromMsg err:",e1);
    	}catch(Exception e2){
    		log.error("convertFromMsg err:",e2);
    	}
    }

	@Override
	public boolean isRequest() {
		// TODO Auto-generated method stub
		return false;
	}
}
