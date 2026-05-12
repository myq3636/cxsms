/* Static Model */
package com.king.db;


import java.sql.*;
import java.util.*;
import java.nio.*;
import java.io.*;


public class TracingManager extends DataManager
{
    private PreparedStatement insertStmt = null;

    public Tracing getLastRecord(String procedureName, String tableName) throws DataControlException {
        try{
            String strSQL = "SELECT * FROM tracing WHERE"
                          + " ProcedureName = '" + procedureName + "'"
                          + " AND TableName = '" + tableName + "'"
                          + " ORDER BY FinishOnRecord DESC LIMIT 1";

            ResultSet rs = doSelect(strSQL);
            Tracing trace = new Tracing();
            trace.setProcedureName(procedureName);
            trace.setTableName(tableName);
            if (rs.next()){
                trace.setStartTime(rs.getDate("StartTime"));
                trace.setFinishTime(rs.getDate("FinishTime"));
                trace.setStartOnRecord(rs.getInt("StartOnRecord"));
                trace.setFinishOnRecord(rs.getInt("FinishOnRecord"));
                byte [] bytes = rs.getBytes("RecordIDs");
                if (bytes!=null && bytes.length>0){
                    int [] newAry = new int[bytes.length/4];
                    ByteBuffer bb = ByteBuffer.allocate(bytes.length).put(bytes);
                    bb.rewind();
                    for (int i=0; i<newAry.length; i++) newAry[i] = bb.getInt();
                    trace.setRecordIDs(newAry);
                }
                trace.setCount(rs.getInt("Count"));
            }
            return trace;
        } catch (Exception e){
            throw new DataControlException(e.getMessage());
        }
    }

    public ArrayList getAll() throws DataManagerException{
        return null;
    }

    public void update(Data data) throws DataManagerException{
    }

    public void remove(Data data)  throws DataManagerException{
    }

    public void add(Data data)  throws DataManagerException {

    }

    public void add(ArrayList data) throws DataManagerException{
    }

    public final void setDone(Tracing trace) throws DataControlException {
        try{
            if (insertStmt == null){
                String strSQL = "INSERT INTO tracing SET " +
                                "ProcedureName = ?, " +
                                "TableName = ?, " +
                                "StartOnRecord = ?, " +
                                "FinishOnRecord = ?, " +
                                "StartTime = ?, " +
                                "FinishTime = ?, " +
                                "RecordIDs = ?, " +
                                "Count = ?";
                insertStmt = this.getCon().prepareStatement(strSQL);
            }

            insertStmt.setString(1, trace.getProcedureName());
            insertStmt.setString(2, trace.getTableName());
            insertStmt.setInt(3, trace.getStartOnRecord());
            insertStmt.setInt(4, trace.getFinishOnRecord());
            if (trace.getStartTime()==null) insertStmt.setNull(5, 0);
            else insertStmt.setTimestamp(5, new Timestamp(trace.getStartTime().getTime()));
            if (trace.getFinishTime()==null) insertStmt.setNull(6, 0);
            else insertStmt.setTimestamp(6, new Timestamp(trace.getFinishTime().getTime()));
            int [] ids = trace.getRecordIDs();
            if (ids==null) insertStmt.setNull(7, 0);
            else{
                ByteBuffer bb = ByteBuffer.allocate(ids.length*4);
                for (int i=0; i<ids.length; i++) bb.putInt(ids[i]);
                bb.rewind();
                insertStmt.setBinaryStream(7, new ByteArrayInputStream(bb.array()), bb.capacity());
            }
            insertStmt.setInt(8, trace.getCount());
            insertStmt.executeUpdate();
        } catch (Exception e){
            throw new DataControlException(e.getMessage());
        }
    }

    public void setDone(int startOnRecord, int finishOnRecord, String procName,
                          String tableName, java.util.Date startTime) throws
          DataManagerException {
        Tracing trace = null;

        int[] ids = new int[2];
        ids[0] = 0;
        ids[1] = 1;

        trace = new Tracing();
        trace.setProcedureName(procName);
        trace.setTableName(tableName);
        trace.setStartTime(startTime);
        trace.setFinishTime(new java.util.Date());
        trace.setStartOnRecord(startOnRecord);
        trace.setFinishOnRecord(finishOnRecord);
        trace.setCount(finishOnRecord - startOnRecord);
        trace.setRecordIDs(ids);

        try {
          setDone(trace);
        }
        catch (Exception ex) {
          throw new DataManagerException(ex.getMessage());
        }
        return;
      }

    public Class getAssociatedClass() {
        return Tracing.class;
    }


}
/* END CLASS DEFINITION RecordTracing */
