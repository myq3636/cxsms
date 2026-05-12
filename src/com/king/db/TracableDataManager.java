/* Static Model */
package com.king.db;

import java.sql.ResultSet;
import java.util.*;

public abstract class TracableDataManager
    extends DataManager {
  private TracingManager tm = null;
  private ErrorTrackingManager etm = null;
  private List errorList = null;


  public abstract ArrayList getToBeProcessed(String procName) throws
      DataManagerException;

  /**
   * setDone() is actually indicating input data are processed, and should be traced by the tracing manager.
   */

  public void setDone(ArrayList array_list, String procName, String tableName) throws
      DataManagerException {
    setDone(array_list, procName, tableName, null);
  }

  public void setDone(ArrayList array_list, String procName, String tableName,
                      Date startTime) throws DataManagerException {
    Tracing trace = null;

    if ( (array_list == null) || (array_list.size() == 0)) {
      trace = new Tracing();
      trace.setProcedureName(procName);
      trace.setTableName(tableName);
      trace.setStartTime(startTime);
      trace.setFinishTime(new Date());
    }
    else {
      int[] ids = new int[array_list.size()];
      Iterator it = array_list.iterator();
      int i = 0;
      while (it.hasNext()) {
        TracableData data = (TracableData) it.next();
        if (trace == null) {
          trace = new Tracing();
          trace.setProcedureName(procName);
          trace.setTableName(tableName);
          trace.setStartTime(startTime);
          trace.setFinishTime(new Date());
          trace.setStartOnRecord(data.getRowId());
          trace.setFinishOnRecord(data.getRowId());
        }
        else if (data.getRowId() > trace.getFinishOnRecord()) {
          trace.setFinishOnRecord(data.getRowId());
        }
        else if (data.getRowId() < trace.getStartOnRecord()) {
          trace.setStartOnRecord(data.getRowId());
        }
        trace.increase();
        ids[i++] = data.getRowId();
      }
      trace.setRecordIDs(ids);
    }
    try {
      this.getTracingManager().setDone(trace);
    }
    catch (Exception ex) {
      throw new DataManagerException(ex.getMessage());
    }
    return;
  }

  public void setDone(int startRecord, int finishRecord, String procName,
        String tableName, Date startTime) throws DataManagerException {

      Tracing trace = null;
      trace = new Tracing();
      trace.setProcedureName(procName);
      trace.setTableName(tableName);
      trace.setStartTime(startTime);
      trace.setFinishTime(new Date());
      trace.setStartOnRecord(startRecord);
      trace.setFinishOnRecord(finishRecord);

      try {
          this.getTracingManager().setDone(trace);
      }
      catch (Exception ex) {
          throw new DataManagerException(ex.getMessage());
      }
  }




  protected ResultSet selectToBeProcessed(String procName, String tableName) throws
      DataManagerException {
    try {
      Tracing tr = this.getTracingManager().getLastRecord(procName, tableName);
      String strSQL = "SELECT * FROM " + tableName + " WHERE"
          + " ID > " + tr.getFinishOnRecord() + " ORDER BY ID";
      return doSelect(strSQL);
    }
    catch (DataControlException ex) {
      throw new DataManagerException(ex.getMessage());
    }
  }

  public TracingManager getTracingManager() throws DataManagerException {
    if (tm == null) {
      try {
        tm = (TracingManager) DataControl.getDataManager(
            "com.king.db.TracingManager");
      }
      catch (Exception ex) {
        throw new DataManagerException(ex.getMessage());
      }
    }
    return tm;
  }

  protected ErrorTrackingManager getErrorTrackingManager() throws
      DataManagerException {
    if (etm == null) {
      try {
        etm = (ErrorTrackingManager) DataControl.getDataManager(
            "com.king.db.ErrorTrackingManager");
      }
      catch (Exception ex) {
        throw new DataManagerException(ex.getMessage());
      }
    }
    return etm;
  }


  public void addError(ErrorTracking ecdr) throws DataManagerException {
    getErrorTrackingManager().add(ecdr);
  }

  public abstract String getTableName();

}
