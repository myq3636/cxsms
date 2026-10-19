package com.king.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.king.framework.SystemLogger;

public class DBLockConnection extends DataManager{
	
	protected static SystemLogger logger = SystemLogger.getSystemLogger(DBLockConnection.class); 
	private final String get_lock_SQL = "SELECT GET_LOCK('lock1',1)";
	private final String rel_lock_SQL = "SELECT RELEASE_LOCK('lock1')";
	private Connection lockConnection = null;

	public DBLockConnection(DbObject dbo) {
		super(dbo);
	}

	public DBLockConnection() {
	}

	@Override
	public void add(Data data) throws DataManagerException {
		// TODO Auto-generated method stub		
	}

	public boolean getLock() throws Exception {
		// H-13: fail-closed —— 任何错误一律视为"未获得锁"(不 NPE、不误判持锁)，
		// 避免 HA 单例选举时两节点同时自认持有 lock1 而脑裂。
		lockConnection = currentConnection();
		try (Statement st = lockConnection.createStatement();
				ResultSet rs = st.executeQuery(get_lock_SQL)) {
			return rs.next() && rs.getInt(1) == 1;
		} catch (Exception e) {
			logger.warn("GET_LOCK failed -> treat as not acquired", e);
			return false;
		}
	}

	public boolean relLock() {
		try {
			doSelect(lockConnection, rel_lock_SQL);
		} catch (Exception e) {
			logger.info("release mysql lock error!");
		}finally {			
			closeSession();
			lockConnection = null;
		}	
		return true;
	}
	
	 private ResultSet doSelect(Connection connection, String strSQL) throws DataControlException {
	        Statement stmt = null;
	        ResultSet rs = null;
            if(connection ==null){
            	return null;
            }
	        try {	            
	            stmt = connection.createStatement();
	            rs = stmt.executeQuery(strSQL);
//	            zeroFailureCount();
	        } catch (Exception e) {
	            logger.trace("SQL Statement that cause error: {}", strSQL);
//	            exceptionHandler(e);
	            //throw new DataControlException("Database query error: " +
	                logger.trace(e.getMessage());
	        }
	        return rs;
	    }

}
