package com.king.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

import com.king.framework.SystemLogger;

public class DBLockConnection extends DataManager{
	
	protected static SystemLogger logger = SystemLogger.getSystemLogger(DBLockConnection.class); 
	private final String get_lock_SQL = "SELECT GET_LOCK('lock1',1)";
	private final String rel_lock_SQL = "SELECT RELEASE_LOCK('lock1')";
	private Session son = null;

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
		son = getSession();
		ResultSet rs = doSelect(son, get_lock_SQL);
		int lock = -1;
		while (rs.next()) {
			lock = rs.getInt(1);
		}
		return lock == 1;
	}

	public boolean relLock() {
		try {
			doSelect(son, rel_lock_SQL);
		} catch (Exception e) {
			logger.info("release mysql lock error!");
			if(son!=null){
				son.close();
			}
		}finally {			
			if(son!=null){
				son.close();
			}
		}	
		return true;
	}
	
	 private Session getSession() throws DataControlException {	
		 Session se = null;
		 SessionFactory factory = null;
	        try {
	        	if(DatabaseStatus.MASTER_USED.equals(dataControl.getUsedDatabaseStatus())){
	        		factory = this.getMasterSessionFactory();	        		
	            }else if(dataControl.getCanHandover() && DatabaseStatus.SLAVE_USED.equals(dataControl.getUsedDatabaseStatus())){
	            	factory = this.getSlaveSessionFactory();	            	
	            }
	        	if(factory!=null){
	    			se = factory.openSession();
	    		}
	        	if(se == null){
	        		logger.error("Can't get DB sessions with master and slave!");
	        	}            
	        } catch (Exception e) {
	            logger.trace("get session error!");
	        }
	        return se;
	    }
	 private ResultSet doSelect(Session se, String strSQL) throws DataControlException {
	        Statement stmt = null;
	        ResultSet rs = null;
            if(se ==null){
            	return null;
            }
	        try {	            
	            Connection conn = se.connection();
	            stmt = conn.createStatement();
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
