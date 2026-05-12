package com.king.message.gmms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;

/**
 * <p>Title: MessageBase</p>
 * <p>Description: Basic message structure</p>
 * <p>Copyright: Copyright (c) 2001-2010</p>
 * <p>Company: King.Inc</p>
 *
 * @version 6.1
 * @author: Jesse Duan
 */
public abstract class MessageBase
        extends com.king.db.TracableData implements Serializable {
	private static final long  serialVersionUid = 1L;
    //basic message type
    public static final String AIC_MSG_TYPE_TEXT = "text";
    public static final String AIC_MSG_TYPE_MMS = "mms";
    public static final String AIC_MSG_TYPE_BINARY = "binary";
    public static final String AIC_MSG_TYPE_WAP = "wap";

    protected String gmmsMsgType = AIC_MSG_TYPE_TEXT;
    protected String messageType = null;
    protected int messageSize = 0;
    protected String protocolVersion = null;
    protected String senderAddress = null;
    protected String[] recipientAddresses = null;
    protected String recipientAddress = null;
    
    /**
     * for 1.5way/ott to keep original value, CDR required
     */
    protected String originalSenderAddr = null;
    
    /**
     * for 1.5way/ott to keep original value, CDR required
     */
    protected String originalRecipientAddr = null;
    
    protected Date timeStamp;
    protected Date expiryDate;
    
    /**
     * GMT time
     */
    protected Date scheduleDeliveryTime;
    
    protected boolean deliveryReport = false;
//    protected boolean readReply = false;
    protected int priority = -1;
//    protected String subject = null;
    protected String contentType = null;
    protected String inContentType = null;
    protected String textContent = null;
    protected String inTextContent = null;
    protected int statusCode = GmmsStatus.UNASSIGNED.getCode();
    protected String statusText = GmmsStatus.UNASSIGNED.getText();
    
//    protected int statusCode = GmmsStatus.SUCCESS.getCode();
//    protected String statusText = GmmsStatus.SUCCESS.getText();
    protected byte[] mimeMultiPartData = null;
    protected String gmmsServerID = null;
    protected String senderAddrType = null;
    protected String recipientAddrType = null;
    protected String senderAddrTon = null;   
	protected String recipientAddrTon  = null;
//    protected String billingId = null;
    protected Date dateIn = null;
    protected int splitStatus = 0; //added by Sean ,the status of the split message.
    // added by Jesse for milter function 2005-2-17
    protected byte[] milterActionCode = new byte[2];

    // added by Jesse for A2P6.0 2005-3-18
    protected int oA2P = 0;
    protected int rA2P = 0;
    protected int currentA2P = 0;
    protected int actionCode = -1;

    public int getSplitStatus() {
        return this.splitStatus;
    }

    public void setSplitStatus(int splitStatus) {
        this.splitStatus = splitStatus;
    }

    public int getActionCode() {
        return this.actionCode;
    }

    public void setActionCode(int code) {
        this.actionCode = code;
    }

    public int getMilterActionCode() {
        return bytes2int(milterActionCode);
    }

    public void setMilterActionCode(int code) {
        this.milterActionCode = this.int2bytes(code);
    }

    public void setMilterActionCode(int index,boolean flag){
        byte milter;
        if (index<=0 || index>milterActionCode.length*8){
            return;
        }
        int newIndex = milterActionCode.length - (index-1)/8 -1;
        milter = milterActionCode[newIndex];
        int iValue = flag?1:0;
        index = (index%8==0?8:index%8);

        byte newByte;
        if (flag){//or
            newByte = (byte)((byte)iValue<<(index-1));
            milterActionCode[newIndex] =  (byte)(newByte | milter);
        }else{//and
            newByte = (byte)(0xff - (int)Math.pow(2,index-1));
            milterActionCode[newIndex] =  (byte)(newByte & milter);
        }
    }

    public boolean getMilterActionCode(int index){
        boolean result = false;
        byte milter;
        if (index <= 0 || index > milterActionCode.length*8) {
            return false;
        }
        int newIndex = milterActionCode.length - (index-1) / 8 - 1;
        milter = milterActionCode[newIndex];
        index = (index%8==0?8:index%8);

        result= ((milter>>(index-1))&0x01)==1;
        return result;
    }

    public int getCurrentA2P() {
        return this.currentA2P;
    }

    public void setCurrentA2P(int cA2P) {
        this.currentA2P = cA2P;
    }

    public int getOA2P() {
        return this.oA2P;
    }

    public void setOA2P(int a2p) {
        this.oA2P = a2p;
    }

    public int getRA2P() {
        return this.rA2P;
    }

    public void setRA2P(int A2P) {
        this.rA2P = A2P;
    }

    public String getGmmsMsgType() {
        return this.gmmsMsgType;
    }

    public void setGmmsMsgType(String gmmsMsgType) {
        this.gmmsMsgType = gmmsMsgType;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String msgType) {
        this.messageType = msgType;
    }

    public int getMessageSize() {
        return this.messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public String getProtocolVersion() {
        return this.protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public String getSenderAddress() {
        return this.senderAddress;
    }

    public void setSenderAddress(String senderAddress) {
        this.senderAddress = senderAddress;
    }

    public String[] getRecipientAddresses() {
        return this.recipientAddresses;
    }

    public void setRecipientAddresses(String[] rcipientAddresses) {
        this.recipientAddresses = rcipientAddresses;
    }

    public String getRecipientAddress() {
        return this.recipientAddress;
    }

    public void setRecipientAddress(String rcipientAddress) {
        this.recipientAddress = rcipientAddress;
    }

    public Date getTimeStamp() {
        return this.timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Date getExpiryDate() {
        return this.expiryDate;
    }

    public void setExpiryDate(Date expiryDate) {
        this.expiryDate = expiryDate;
    }

    public boolean getDeliveryReport() {
        return this.deliveryReport;
    }

    public void setDeliveryReport(boolean deliveryReport) {
        this.deliveryReport = deliveryReport;
    }

//    public boolean getReadReply() {
//        return this.readReply;
//    }
//
//    public void setReadReply(boolean readReply) {
//        this.readReply = readReply;
//    }

    public int getPriority() {
        return this.priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

//    public String getSubject() {
//        return this.subject;
//    }
//
//    public void setSubject(String subject) {
//        this.subject = subject;
//    }

    public String getContentType() {
        return this.contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getTextContent() {
        return this.textContent;
    }

    public void setTextContent(String text) {
        if (text == null)
            this.textContent = "";
        else
            this.textContent = text;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusText() {
        return this.statusText;
    }

    public void setStatusText(String statusText) {
        this.statusText = statusText;
    }

    public byte[] getMimeMultiPartData() {
        return mimeMultiPartData;
    }

    public void setMimeMultiPartData(byte[] mimeMultiPartData) {
        this.mimeMultiPartData = mimeMultiPartData;
    }

    public String getGmmsServerID() {
        return gmmsServerID;
    }

    public void setGmmsServerID(String gmmsServerID) {
        this.gmmsServerID = gmmsServerID;
    }

    public String getSenderAddrType() {
        return this.senderAddrType;
    }

    public void setSenderAddrType(String type) {
        this.senderAddrType = type;
    }

    public String getRecipientAddrType() {
        return this.recipientAddrType;
    }

    public void setRecipientAddrType(String type) {
        this.recipientAddrType = type;
    }

    public String getRecipientAddrTon() {
		return recipientAddrTon;
	}

	public void setRecipientAddrTon(String recipientAddrTon) {
		this.recipientAddrTon = recipientAddrTon;
	}

	public String getSenderAddrTon() {
		return senderAddrTon;
	}

	public void setSenderAddrTon(String senderAddrTon) {
		this.senderAddrTon = senderAddrTon;
	}
    
//    public void setBillingId(String id) {
//        this.billingId = id;
//    }
//
//    public String getBillingId() {
//        return this.billingId;
//    }

    // Unitlity Functions. Available for the sub-classes
    protected GmmsStatus getStatus() {
        return GmmsStatus.getStatus(statusCode);
    }

    protected void setStatus(GmmsStatus status) {
        statusCode = status.getCode();
        statusText = status.getText();
    }

    public void setDateIn(Date dateIn) {
        this.dateIn = dateIn;
    }

    public Date getDateIn() {
        return this.dateIn;
    }

    protected void serializeMimeParts(MimeMultipart mimeMultiPart) throws
                                                                   MessagingException, IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mimeMultiPart.writeTo(out);
        mimeMultiPartData = out.toByteArray();
        this.contentType = mimeMultiPart.getContentType();
    }

    protected MimeMultipart parseMimeParts() throws MessagingException {
        ByteArrayDataSource ds = null;
        if(contentType != null && contentType.length() > 0) {
            ds = new ByteArrayDataSource(mimeMultiPartData, this.contentType);
        }
        else {
            ds = new ByteArrayDataSource(mimeMultiPartData);
            this.contentType = ds.getContentType();
        }
        MimeMultipart mimeParts = new MimeMultipart(ds);
        return mimeParts;
    }

    public MessageBase() {
    }

    /**
     * constructor
     *
     * @param oneMsg MessageBase
     */
    public MessageBase(MessageBase oneMsg) {
        this.rowId = oneMsg.rowId;
        this.messageType = oneMsg.messageType;
        this.gmmsMsgType = oneMsg.gmmsMsgType;
        this.textContent = oneMsg.textContent;
        this.inTextContent = oneMsg.inTextContent;
        this.contentType = oneMsg.contentType;
        this.inContentType = oneMsg.inContentType;
        this.deliveryReport = oneMsg.deliveryReport;
        this.expiryDate = oneMsg.expiryDate;
        this.messageSize = oneMsg.messageSize;
//        this.mimeMultiPartData = oneMsg.mimeMultiPartData;
        if (oneMsg.getMimeMultiPartData()!=null) {
        	int mimeLen = oneMsg.getMimeMultiPartData().length;
        	byte[] mimeData = new byte[mimeLen];
            System.arraycopy(oneMsg.getMimeMultiPartData(), 0, mimeData, 0, mimeLen);
            this.setMimeMultiPartData(mimeData);
        }
        this.priority = oneMsg.priority;
        this.protocolVersion = oneMsg.protocolVersion;
//        this.readReply = oneMsg.readReply;
        this.recipientAddress = oneMsg.recipientAddress;
        this.recipientAddresses = oneMsg.recipientAddresses;
        this.senderAddress = oneMsg.senderAddress;
        this.originalSenderAddr = oneMsg.originalSenderAddr;
        this.originalRecipientAddr = oneMsg.originalRecipientAddr;
        this.statusCode = oneMsg.statusCode;
        this.statusText = oneMsg.statusText;
//        this.subject = oneMsg.subject;
        if(oneMsg.timeStamp != null) {
            this.timeStamp = oneMsg.timeStamp;
        }
        else {
            this.timeStamp = new Date();
        }
        this.senderAddrType = oneMsg.senderAddrType;
        this.recipientAddrType = oneMsg.recipientAddrType;
        this.senderAddrTon = oneMsg.senderAddrTon;
        this.recipientAddrTon = oneMsg.recipientAddrTon;
//        this.billingId = oneMsg.billingId;
        this.dateIn = oneMsg.dateIn;
        this.milterActionCode = oneMsg.milterActionCode;
        this.oA2P = oneMsg.oA2P;
        this.rA2P = oneMsg.rA2P;
        this.actionCode = oneMsg.actionCode;
        this.currentA2P = oneMsg.currentA2P;
        this.splitStatus = oneMsg.splitStatus;
        this.scheduleDeliveryTime = oneMsg.scheduleDeliveryTime;
    }

    public String toString() {
        return new StringBuilder().append("  senderAddress:").append(this.senderAddress)
                .append(" recipientAddress:").append(this.recipientAddress)
                .append(" originalSenderAddr:").append(this.originalSenderAddr)
                .append(" originalRecipientAddr:").append(this.originalRecipientAddr)
                .append(" messageType:").append(this.messageType)
                .append(" gmmsMsgType:").append(this.gmmsMsgType)
                .append(" statusCode:").append(statusCode)
                .append(" statusText:").append(statusText)
                .append(" protocolVersion:").append(protocolVersion)
                .append(" rowId :").append(rowId).toString();
    }

    int bytes2int(byte[] b) {
        int mask = 0xff;
        int temp = 0;
        int res = 0;
        for (int i = 0; i < b.length; i++) {
            res <<= 8;
            temp = b[i] & mask;
            res |= temp;
        }
        return res;
    }

    byte[] int2bytes(int num) {
        byte[] b = new byte[milterActionCode.length];
        for (int i = 0; i < milterActionCode.length; i++) {
            b[i] = (byte) (num >>> ((milterActionCode.length - i-1) * 8));
        }
        return b;
    }

	public void setAntiSpam(boolean flag) {
		this.setMilterActionCode(1, flag);
	}

	public void setT2I(boolean flag) {
		this.setMilterActionCode(2, flag);
	}

	public void setTextMMS(boolean flag) {
		this.setMilterActionCode(3, flag);
	}

	public void setNotice(boolean flag) {
		this.setMilterActionCode(4, flag);
	}

	public void setAddForeword(boolean flag) {
		this.setMilterActionCode(5, flag);
	}

	public void setKeywordFilter(boolean flag) {
		this.setMilterActionCode(6, flag);
	}

	public void setOttContentAddOaddr(boolean flag) {
		this.setMilterActionCode(7, flag);
	}

	public void setGsm7bit(boolean flag) {
		this.setMilterActionCode(8, flag);
	}
	
	public void setContentSignature(boolean flag) {
		this.setMilterActionCode(9, flag);
	}
	
	public void setCheckDuplicate(boolean flag) {
		this.setMilterActionCode(10, flag);
	}
	
	public void setContentTemplateParamter(boolean flag) {
		this.setMilterActionCode(11, flag);
	}
	
	public void setDRFailedReroute(boolean flag) {
		this.setMilterActionCode(12, flag);
	}
	
	public void setFakeDR(boolean flag) {
		this.setMilterActionCode(13, flag);
	}
	
	public void setRetryFlag(boolean flag) {
		this.setMilterActionCode(14, flag);
	}

	public boolean hasAntiSpam() {
		return this.getMilterActionCode(1);
	}

	public boolean hasT2I() {
		return this.getMilterActionCode(2);
	}

	public boolean hasTextMMS() {
		return this.getMilterActionCode(3);
	}

	public boolean hasNotice() {
		return this.getMilterActionCode(4);
	}

	public boolean hasAddForeword() {
		return this.getMilterActionCode(5);
	}

	public boolean hasKeywordFilter() {
		return this.getMilterActionCode(6);
	}

	public boolean hasOttContentAddOaddr() {
		return this.getMilterActionCode(7);
	}

	public boolean isGsm7bit() {
		return this.getMilterActionCode(8);
	}
	
	public boolean hasContentSignature() {
		return this.getMilterActionCode(9);
	}
	
	public boolean hasCheckDuplicate() {
		return this.getMilterActionCode(10);
	}
	
	public boolean hasContentTemplateParamter() {
		return this.getMilterActionCode(11);
	}
	
	public boolean hasDRFailedReroute() {
		return this.getMilterActionCode(12);
	}
	
	public boolean hasFakeSendDR() {
		return this.getMilterActionCode(13);
	}
	
	public boolean hasRetryFlag() {
		return this.getMilterActionCode(14);
	}

	public String getOriginalSenderAddr() {
		return originalSenderAddr;
	}

	public void setOriginalSenderAddr(String originalSenderAddr) {
		this.originalSenderAddr = originalSenderAddr;
	}

	public String getOriginalRecipientAddr() {
		return originalRecipientAddr;
	}

	public void setOriginalRecipientAddr(String originalRecipientAddr) {
		this.originalRecipientAddr = originalRecipientAddr;
	}
	
	

	public String getInTextContent() {
		return inTextContent;
	}

	public void setInTextContent(String inTextContent) {
		this.inTextContent = inTextContent;
	}	

	public String getInContentType() {
		return inContentType;
	}

	public void setInContentType(String inContentType) {
		this.inContentType = inContentType;
	}

	public static void main (String[] args){
//        GmmsMessage msg = new GmmsMessage();
//        msg.setMilterActionCode(128);
//        msg.setMilterActionCode(5,true);
//        System.out.println(msg.getMilterActionCode());
//
//        GmmsMessage msg2 = new GmmsMessage();
//        msg2.setMilterActionCode(1023);
//        System.out.println(msg2.getMilterActionCode(12));
//        System.out.println(msg2.getMilterActionCode(11));
//        System.out.println(msg2.getMilterActionCode(10));
//        System.out.println(msg2.getMilterActionCode(9));
//        System.out.println(msg2.getMilterActionCode(8));
    }

	public Date getScheduleDeliveryTime() {
		return scheduleDeliveryTime;
	}

	public void setScheduleDeliveryTime(Date scheduleDeliveryTime) {
		this.scheduleDeliveryTime = scheduleDeliveryTime;
	}
}
