/* Static Model */
package com.king.db;

import java.util.Date;

public class Tracing
{
    public Tracing()
    { 
    }
    public final String getProcedureName()
    {
            return mprocedureName;
    }
    public final void setProcedureName(String the_mprocedureName)
    {
            this.mprocedureName = the_mprocedureName;
    }
    public final String getTableName()
    {
            return mtableName;
    }
    public final void setTableName(String the_mtableName)
    {
            this.mtableName = the_mtableName;
    }    
    public final int getStartOnRecord()
    {
            return mstartOnRecord;
    }
    public final void setStartOnRecord(int the_mstartOnRecord)
    {
            this.mstartOnRecord = the_mstartOnRecord;
    }
    public final int getFinishOnRecord()
    {
            return mendOnRecord;
    }
    public final void setFinishOnRecord(int the_mendOnRecord)
    {
            this.mendOnRecord = the_mendOnRecord;
    }
    public final int getCount()
    {
            return mcount;
    }
    public final void setCount(int the_mcount)
    {
            this.mcount = the_mcount;
    }
    public final void increase(){
        this.mcount++;
    }
    
    public final Date getStartTime() { return startTime; }
    public final void setStartTime(Date time) { startTime = time; }
    
    public final Date getFinishTime() { return finishTime; }
    public final void setFinishTime(Date time) { finishTime = time; }
    
    public final int [] getRecordIDs() { return recordIDs; }
    public final void setRecordIDs(int [] ids) { recordIDs = ids; }
    
    private String mprocedureName = null;
    private String mtableName = null;
    private int mstartOnRecord = 0;
    private int mendOnRecord = 0;
    private int mcount = 0;
    private Date startTime = null;
    private Date finishTime = null;
    private int [] recordIDs = null;

}
/* END CLASS DEFINITION RecordTrace */
