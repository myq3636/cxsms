/* Static Model */
package com.king.db;

import java.sql.*;
import java.util.Date;
import java.util.*;

import com.king.db.*;
import com.king.framework.SystemLogger;

import java.util.logging.*;
public class ErrorTrackingManager extends DataManager {


    private static final String TABLE_NAME = "errortracking";
    protected static SystemLogger log = SystemLogger
	.getSystemLogger(ErrorTrackingManager.class);
    PreparedStatement insertStmt = null;
    String preparedInsertStr = "INSERT INTO " + TABLE_NAME
        + " (RecordID, FromTableName, ErrorCode, ErrorDescription, ReferenceCode, Corrected)"
        + " VALUES(?, ?, ?, ?, ?, ?) ";


    public ArrayList getAll() throws DataManagerException {
        return getAll(null);
    }

    public ResultSet getErrorNum(String tablename) throws Exception{
      String strSql = null;
      try{
        strSql = " select count(*) errornum from errortracking where FromTableName ="+ makeSqlStr(tablename);
        return doSelect(strSql);
      }catch (Exception e) {
      log.warn("<<<<<<<<< checksum error in errortracking");
      throw new Exception(e);
    }

    }
    public ArrayList getAll(String fromTableName) throws DataManagerException {
        String sqlStr = "";
        try{
            if (fromTableName == null){
                sqlStr = "SELECT * FROM " + TABLE_NAME + " WHERE"
                      + " Corrected = 0"
                      + " ORDER BY DateIn DESC";
            }else{
                sqlStr = "SELECT * FROM " + TABLE_NAME + " WHERE"
                      + " Corrected = 0 AND FromTableName = " + makeSqlStr(fromTableName)
                      + " ORDER BY DateIn DESC";
            }

            ResultSet rs = doSelect(sqlStr);
            ArrayList al = new ArrayList();
            if (getRowCount(rs)>0){
                rs.next();
                do{
                    ErrorTracking et = new ErrorTracking();
                    et.setFromTableName(rs.getString("FromTableName"));
                    et.setRecordID(rs.getInt("RecordID"));
                    et.setErrorCode(ErrorCode.parse(rs.getString("ErrorCode")));
                   // et.setReRunTime(rs.getTimestamp("ReRunTime"));
                    al.add(et);
                    rs.next();
                }while(!rs.isAfterLast());
            }
            return al;
        } catch(Exception e){
            throw new DataManagerException(e.getMessage());
        }
    }

    public final ArrayList getErrorByCode(String errorCode) throws DataManagerException{
        if(errorCode== null)  return null;
        try{
            String strSql = null;
            if(errorCode.indexOf(",") != -1){  // have more than two error code needed to handle
              StringTokenizer er = new StringTokenizer(errorCode,",");
              while(er.hasMoreTokens()){
                if (strSql == null)
                  strSql = " ErrorCode=" + makeSqlStr(er.nextToken());
                else
                  strSql += " or ErrorCode= " + makeSqlStr(er.nextToken());
              }
            }else // only one errorcode
              strSql = " ErrorCode=" + makeSqlStr(errorCode);

            ResultSet rs = doSelect("SELECT * FROM " + TABLE_NAME + " WHERE"
                          + " Corrected = 0" + " AND ( "
                          + strSql
                          + " ) AND FixedTime is null "
                          + " ORDER BY DateIn DESC");
            ArrayList al = new ArrayList();
            if (getRowCount(rs)>0){
                rs.next();
                do{
                    ErrorTracking et = new ErrorTracking();
                    et.setFromTableName(rs.getString("FromTableName"));
                    et.setRecordID(rs.getInt("RecordID"));
                    et.setErrorCode(ErrorCode.parse(rs.getString("ErrorCode")));
                    et.setReferenceCode(rs.getString("ReferenceCode"));
                    //et.setReRunTime(rs.getTimestamp("ReRunTime"));
                    al.add(et);
                    rs.next();
                }while(!rs.isAfterLast());
            }
            return al;
        } catch(Exception e){
            throw new DataManagerException(e.getMessage());
        }
    }

    public final int[] getErrorIds(String sqlStr) throws DataManagerException{
        if (sqlStr == null || sqlStr == "") return null;

        int[] ids = null;
        try{
            ResultSet rs = doSelect(sqlStr);
            int count = this.getRowCount(rs);
            if (count>0){
                ids = new int[count];
                while (rs.next()){
                    ids[--count] = rs.getInt("recordid");
                }
            }

            return ids;

        }catch (Exception e){
            throw new DataManagerException(e.getMessage());
        }
    }

    public void add(Data data) throws DataManagerException{
        try{
            if (insertStmt == null)
                insertStmt = this.getCon().prepareStatement(preparedInsertStr);

            ErrorTracking et = (ErrorTracking)data;
            insertStmt.setInt(1, et.getRecordID());
            insertStmt.setString(2, et.getFromTableName());
            insertStmt.setString(3, et.getErrorCode().getName());
            insertStmt.setString(4, et.getErrorCode().getDescription());
            insertStmt.setString(5, et.getReferenceCode());
            insertStmt.setInt(6, et.isCorrected()? 1 : 0);
            insertStmt.executeUpdate();

        } catch(Exception e){
            throw new DataManagerException(e.toString());
        }
    }

    public void addError(TracableData td, ErrorCode ec, String tableName,
                         String refCode)throws SQLException,DataManagerException{
         ErrorTracking er = new ErrorTracking();
         er.setFromTableName(tableName);
         er.setRecordID(td.getRowId());
         er.setErrorCode(ec);
         er.setReferenceCode(refCode);
         addError(er);
     }

     public void addError(ResultSet td, ErrorCode ec, String tableName,
           String refCode)throws SQLException,DataManagerException{
         ErrorTracking er = new ErrorTracking();
         er.setFromTableName(tableName);
         er.setRecordID(td.getInt("id"));
         er.setErrorCode(ec);
         er.setReferenceCode(refCode);
         addError(er);
     }


     public void fixError(TracableData td, String tableName) throws SQLException{
        Date fixedTime = new Date();
        Timestamp ftime = new Timestamp(fixedTime.getTime());
        String strSql = " update " + this.TABLE_NAME + " set FixedTime =" +
            "'" + ftime + "', Corrected = 1, datein=datein " +
            " where RecordId = " + td.getRowId()+ " and FromTableName = " +
            makeSqlStr(tableName);
        try{
          doUpdate(strSql);
        }catch(Exception e){
          throw new SQLException(e.getMessage());
        }
     }

     public void clearFixedError(String tableName) throws SQLException{
         String strSql = null;
         if ("rawcdrs".equalsIgnoreCase(tableName)){
             strSql = "Delete from " + this.TABLE_NAME +
                 " using " + this.TABLE_NAME + ",checkedcdrs " +
                 " where " + this.TABLE_NAME + ".recordid = checkedcdrs.id";
         }else{

         }

         if (strSql != null){
             try{
                 this.doDelete(strSql);
             }catch(Exception e){
                 throw new SQLException(e.getMessage());
             }
         }
     }

     public void addError(ErrorTracking er) throws DataManagerException{
         try{
             this.add(er);
         }catch(Exception e){
             throw new DataManagerException(e.getMessage());
         }
     }
    public void update(Data data) throws DataManagerException{

        /***********************************
        *         To be implemented        *
        ************************************/
    }

    public void remove(Data data) throws DataManagerException{

    }


}
/* END CLASS DEFINITION ErrorCdrManager */
