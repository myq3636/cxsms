/*
 * DeliveryReceipt.java
 *
 * Created on January 10, 2003, 5:01 PM
 */

package com.king.gmms.protocol.smpp;

import java.util.Date;

/**
 * @author mike
 */
public class DeliveryReceipt extends com.king.db.TracableData {
    String sourceSysId = null;
    String destSysId = null;
    String msgID = null;
    String sub = null;
    String dlvrd = null;
    Date submitDate = null;
    Date doneDate = null;
    String stat = null;
    String err = null;
    String text = null;


    /**
     * Creates a new instance of DeliveryReceipt
     */
    public DeliveryReceipt() {
    }

    public String getSourceSysId() {
        return sourceSysId;
    }

    public void setSourceSysId(String sourceSysId) {
        this.sourceSysId = sourceSysId;
    }

    public String getDestSysId() {
        return destSysId;
    }

    public void setDestSysId(String destSysId) {
        this.destSysId = destSysId;
    }

    public String getMsgID() {
        return msgID;
    }

    public void setMsgID(String msgID) {
        this.msgID = msgID;
    }

    public String getSub() {
        return sub;
    }

    public void setSub(String sub) {
        this.sub = sub;
    }

    public String getDlvrd() {
        return dlvrd;
    }

    public void setDlvrd(String dlvrd) {
        this.dlvrd = dlvrd;
    }

    public String getStat() {
        return stat;
    }

    public void setStat(String stat) {
        this.stat = stat;
    }

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getSubmitDate() {
        return submitDate;
    }

    public void setSubmitDate(Date submitDate) {
        this.submitDate = submitDate;
    }

    public Date getDoneDate() {
        return doneDate;
    }

    public void setDoneDate(Date doneDate) {
        this.doneDate = doneDate;
    }

    public String toDebugString() {
        return "[SrcSysID: " + sourceSysId + " DestSysID: " + destSysId + " MsgId: " + msgID + "]";
    }
}
