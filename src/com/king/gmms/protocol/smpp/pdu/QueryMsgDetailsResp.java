package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;

import java.io.UnsupportedEncodingException;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 *
 * @version 1.0
 * @author: Jesse Duan
 */

public class QueryMsgDetailsResp extends Response {
    private String serviceType = Data.DFLT_SRVTYPE;
    private Address sourceAddr = new Address();
    private DestAddressList destAddresses = new DestAddressList();
    private byte protocolId = Data.DFLT_PROTOCOLID;
    private byte priorityFlag = Data.DFLT_PRIORITY_FLAG;
    private String scheduleDeliveryTime = Data.DFLT_SCHEDULE;
    private String validityPeriod = Data.DFLT_VALIDITY;
    private byte registeredDelivery = Data.DFLT_REG_DELIVERY;
    private byte dataCoding = Data.DFLT_DATA_CODING;
    private short smLength = Data.DFLT_MSG_LEN;
    private ShortMessage shortMessage = new ShortMessage(version.lengthShortMessage);
    private String msgId = Data.DFLT_MSGID;
    private String finalDate = Data.DFLT_DATE;
    private byte messageState = Data.DFLT_MSG_STATE;
    private byte errorCode = Data.DFLT_ERR;

    public QueryMsgDetailsResp() {
        super(Data.QUERY_MSG_DETAILS_RESP);
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setServiceType(buffer.removeCString());
        sourceAddr.setData(buffer);
        destAddresses.setData(buffer);
        setProtocolId(buffer.removeByte());
        setPriorityFlag(buffer.removeByte());
        setScheduleDeliveryTime(buffer.removeCString());
        setValidityPeriod(buffer.removeCString());
        setRegisteredDelivery(buffer.removeByte());
        setDataCoding(buffer.removeByte());
        setSmLength(decodeUnsigned(buffer.removeByte()));
        shortMessage.setData(buffer.removeBuffer(getSmLength()));
        setMsgId(buffer.removeCString());
        setFinalDate(buffer.removeCString());
        setMessageState(buffer.removeByte());
        setErrorCode(buffer.removeByte());
    }

    public SmppByteBuffer getBody() throws ValueNotSetException {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(getServiceType());
        buffer.appendBuffer(getSourceAddr().getData());
        buffer.appendBuffer(destAddresses.getData());
        buffer.appendByte(getProtocolId());
        buffer.appendByte(getPriorityFlag());
        buffer.appendCString(getScheduleDeliveryTime());
        buffer.appendCString(getValidityPeriod());
        buffer.appendByte(getRegisteredDelivery());
        buffer.appendByte(getDataCoding());
        buffer.appendByte(encodeUnsigned(getSmLength()));
        buffer.appendBuffer(shortMessage.getData());
        buffer.appendCString(getMsgId());
        buffer.appendCString(getFinalDate());
        buffer.appendByte(getMessageState());
        buffer.appendByte(getErrorCode());

        return buffer;

    }

    public void setServiceType(String value) throws WrongLengthOfStringException {
        checkString(value, Data.SM_SRVTYPE_LEN);
        serviceType = value;
    }

    public void setProtocolId(byte value) throws WrongLengthOfStringException {
        protocolId = value;
    }

    public void setPriorityFlag(byte value) throws WrongLengthOfStringException {
        priorityFlag = value;
    }

    public void setScheduleDeliveryTime(String value) throws WrongDateFormatException {
        checkDate(value);
        scheduleDeliveryTime = value;
    }

    public void setValidityPeriod(String value) throws WrongDateFormatException {
        checkDate(value);
        validityPeriod = value;
    }

    public void setRegisteredDelivery(byte value) {
        registeredDelivery = value;
    }

    public void setDataCoding(byte value) {
        dataCoding = value;
    }

    // setSmLength() is private as it's set to length of the message
    private void setSmLength(short value) {
        smLength = value;
    }

    public void setMsgId(String value) {
        msgId = value;
    }

    public void setFinalDate(String value) throws WrongDateFormatException {
        checkDate(value);
        finalDate = value;
    }

    public void setMessageState(byte value) {
        messageState = value;
    }

    public void setErrorCode(byte value) {
        errorCode = value;
    }


    public String getServiceType() {
        return serviceType;
    }

    public String getScheduleDeliveryTime() {
        return scheduleDeliveryTime;
    }

    public String getValidityPeriod() {
        return validityPeriod;
    }

    public String getShortMessage() {
        return shortMessage.getMessage();
    }

    public String getShortMessage(String encoding) throws
                                                   UnsupportedEncodingException {
        return shortMessage.getMessage(encoding);
    }

    public Address getSourceAddr() {
        return sourceAddr;
    }

    public short getNumberOfDests() {
        return (short) destAddresses.getCount();
    }

    public DestinationAddress getDestAddress(int i) {
        return (DestinationAddress) destAddresses.getValue(i);
    }

    public byte getProtocolId() {
        return protocolId;
    }

    public byte getPriorityFlag() {
        return priorityFlag;
    }

    public byte getRegisteredDelivery() {
        return registeredDelivery;
    }

    public byte getDataCoding() {
        return dataCoding;
    }

    public short getSmLength() {
        return smLength;
    }

    public String getMsgId() {
        return msgId;
    }

    public String getFinalDate() {
        return finalDate;
    }

    public byte getMessageState() {
        return messageState;
    }

    public byte getErrorCode() {
        return errorCode;
    }


    public void setSourceAddr(Address value) {
        sourceAddr = value;
    }

    public void setSourceAddr(String address) throws
                                              WrongLengthOfStringException {
        setSourceAddr(new Address(address));
    }

    public void setSourceAddr(byte ton, byte npi, String address) throws
                                                                  WrongLengthOfStringException {
        setSourceAddr(new Address(ton, npi, address));
    }

    public void addDestAddress(DestinationAddress destAddr) throws TooManyValuesException {
        destAddresses.addValue(destAddr);
    }


    private class DestAddressList extends ByteDataList {
        public DestAddressList() {
            super(Data.SM_MAX_CNT_DEST_ADDR, 1);
        }

        public ByteData createValue() {
            return new DestinationAddress();
        }

        public String debugString() {
            return "(dest_addr_list: " + super.debugString() + ")";
        }

    }

    public String debugString() {
        String dbgs = "(queryMsgDetailsResp: ";
        dbgs += super.debugString();
        dbgs += getSourceAddr().debugString();
        dbgs += destAddresses.debugString();
        dbgs += " ";
        dbgs += shortMessage.debugString();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }

}
