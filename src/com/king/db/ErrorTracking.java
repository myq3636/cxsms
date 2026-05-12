/* Static Model */
package com.king.db;

import java.sql.Timestamp;

public class ErrorTracking extends TracableData
{
    private String mfromTableName;
    private int mrecordID;
    private Timestamp mreRunTime;
    private ErrorCode merrorCode;
    private String mreferenceCode;
    private boolean mcorrected = false;

    public ErrorTracking() {
    }

    public final String getFromTableName(){
            return mfromTableName;
    }
    public final void setFromTableName(String the_mfromTableName){
            this.mfromTableName = the_mfromTableName;
    }

    public final int getRecordID(){
            return mrecordID;
    }
    public final void setRecordID(int the_mrecordID){
            this.mrecordID = the_mrecordID;
    }

    public final ErrorCode getErrorCode(){
            return merrorCode;
    }
    public final void setErrorCode(ErrorCode the_merrorCode){
            this.merrorCode = the_merrorCode;
    }

    public final String getReferenceCode(){
            return mreferenceCode;
    }
    public final void setReferenceCode(String the_mreferenceCode){
            this.mreferenceCode = the_mreferenceCode;
    }

    public final Timestamp getReRunTime(){
            return mreRunTime;
    }
    public final void setReRunTime(Timestamp the_mreRunTime){
            this.mreRunTime = the_mreRunTime;
    }

    public final boolean isCorrected(){
            return mcorrected;
    }
    public final void corrected(){
            this.mcorrected = true;
    }


}
/* END CLASS DEFINITION ErrorCDR */
