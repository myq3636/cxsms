/*
 * Data.java
 *
 * Created on August 28, 2002, 4:58 PM
 */

package com.king.db;

/**
 *
 * @author  sally
 * @version
 */
public abstract class TracableData extends Data{

    protected int rowId = -1;

    /** Creates new Data */
    public TracableData() {}

    public TracableData(TracableData data){
      this.setRowId(data.getRowId());
    }
    /** Creates new Data */
    public TracableData(int rowId) {
        this.rowId = rowId;
    }

    public int getRowId(){
        return this.rowId;
    }

    public void setRowId(int rowId){
        this.rowId = rowId;
    }

    public String toString(){
      String result = "cdr: " + getRowId();
      return result;
    }
}
