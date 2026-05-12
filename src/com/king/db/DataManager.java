package com.king.db;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import com.king.framework.SystemLogger;

public abstract class DataManager {
    protected SessionFactory[] factories = null;
    private ThreadLocal session = new ThreadLocal();
    protected static SystemLogger logger = SystemLogger.getSystemLogger(DataManager.class); 
    private static ThreadLocal<java.util.List<Session>> allSessionsForThread = new ThreadLocal<java.util.List<Session>>() {
        protected java.util.List<Session> initialValue() {
            return new java.util.LinkedList<Session>();
        }
    };
    private Object mutex = new Object();
    protected DataControl dataControl = DataControl.getInstance();
    protected Statement stmt;
    private String dsName = null;

    public static void closeAllSessions() {
        try {
            java.util.List<Session> sessions = allSessionsForThread.get();
            if (sessions != null && !sessions.isEmpty()) {
                for (Session s : sessions) {
                    try { if (s != null && s.isOpen()) s.close(); } catch (Exception e) {}
                }
                sessions.clear();
            }
        } catch (Exception ex) {
            logger.warn("Error cleaning up thread local sessions", ex);
        }
    }

    /**
     * Its sub-class must have one nullary constructor.
     */
    public DataManager() {
    }

    public void initDataManager() throws DataControlException,
        DataManagerException{
    }

    /**
     * Legacy support
     * This constructor does nothing
     * @param dbo DbObject
     */
    public DataManager(DbObject dbo) {
        // Do nothing
    }

    /**************************************************************************
     *                       Hibernate supporting functions                   *
     **************************************************************************/

    /**
     * To be implement for subclasses: to return associated entity class <br>
     * For the consideration of easy migration, a default implementation is
     * provided. However, for any data manager that is using Hibernate
     * infrastructure should override this function to return corresponding
     * class.
     */
    public Class getAssociatedClass() {
        return null;
    }

    
    void setSessionFactory(SessionFactory[] factory) {
        this.factories = factory;
    }
    /**
     * Try to get master session, if failed get slave's
     * @return
     */
    protected SessionFactory getMasterSessionFactory(){
    	if(factories[0]==null){
    		try{
				factories[0] = dataControl.getMasterSessionFactory(dsName);
			}catch(Exception e){
				logger.warn("Can't create session to Master DB!");
			}
    	}
    	if(factories[0]!=null){
    		return factories[0];
    	}else if(factories[0]==null && factories[1]!=null){
    		dataControl.setUsedDatabaseStatus(DatabaseStatus.SLAVE_USED);
			DatabaseStatus.updateDBStatus2File(DatabaseStatus.SLAVE_USED);
    		return factories[1];
    	}
		return null;
    }
    /**
     * Try to get slave session, if failed get slave's
     * @return
     */
    protected SessionFactory getSlaveSessionFactory(){
    	if(factories[1]==null){
    		try{
				factories[1] = dataControl.getSlaveSessionFactory(dsName);
			}catch(Exception e){
				logger.warn("Can't create session to Slave DB!");
			}
    	}
    	if(factories[1]!=null){
    		return factories[1];
    	}else if(factories[1]==null && factories[0]!=null){
    		dataControl.setUsedDatabaseStatus(DatabaseStatus.MASTER_USED);
			DatabaseStatus.updateDBStatus2File(DatabaseStatus.MASTER_USED);
    		return factories[0];
    	}
		return null;
    }
    /**
     * Get an available session, create a new one if none is available
     * @return Session
     */
    public Session currentSession(){
        Session s = (Session) session.get();
        // Open a new Session, if this Thread has none yet
        SessionFactory factory = null;
        if (s == null) {
        	if(DatabaseStatus.MASTER_USED.equals(dataControl.getUsedDatabaseStatus())){
        		factory = this.getMasterSessionFactory();
        		logger.debug("getMasterSessionFactory");
            }else if(dataControl.getCanHandover() && DatabaseStatus.SLAVE_USED.equals(dataControl.getUsedDatabaseStatus())){
            	factory = this.getSlaveSessionFactory();
            	logger.debug("getSlaveSessionFactory");
            }
        	if(factory!=null){
    			s = factory.openSession();
    		}
        	if(s == null){
        		logger.error("Can't get DB sessions with master and slave!");
        	}
            s.setCacheMode(CacheMode.NORMAL);
            session.set(s);
            if (s != null) {
                allSessionsForThread.get().add(s);
            }
        }
        if (s.isDirty()) {
            s.flush();
        }

        return s;
    }

    /**
     * Close current session
     */
    public void closeSession() {
        try {
        	/*if (stmt!=null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
            Session s = (Session) session.get();
            session.set(null);
            if (s != null) {
                s.close();
                allSessionsForThread.get().remove(s);
            }
        } catch (HibernateException ex) {
//            exceptionHandler(ex);
            ex.printStackTrace();
        }
    }


    /**************************************************************************
     *                       Legacy Supporting Functions                      *
     **************************************************************************/

    /** Excutes an Insert statement
     * @param strSQL An SQL Insert Statement as a String
     * @throws DataControlException
     * @return Returns a row count of the Insert statement
     */
    public int doInsert(String strSQL) throws DataControlException {
        return doUpdate(strSQL);
    }

    /** Excutes an Update statement
     * @param strSQL
     * @throws DataControlException
     * @return
     */
    protected int doUpdate(String strSQL) throws DataControlException {
        Statement stmt = null;
        try {
            Session sess = currentSession();
            Connection conn = sess.connection();
            stmt = conn.createStatement();
            int line = stmt.executeUpdate(strSQL);
//            zeroFailureCount();
            return line;
        } catch (Exception e) {
            logger.trace("SQL Statement that cause error: {}", strSQL);
//            exceptionHandler(e);
            throw new DataControlException("Database connection error: " +
                                           e.getMessage());
        } finally {
            if (stmt != null) {
                try { stmt.close(); } catch (SQLException ex) {}
            }
        }
    }

    /** Excutes an Delete statement
     * @param strSQL An SQL Insert Statement as a String
     * @throws DataControlException
     * @return Returns a row count of the Insert statement
     */
    public int doDelete(String strSQL) throws DataControlException {
        return doUpdate(strSQL);
    }


    protected Statement getStmt() throws DataControlException {
        try {
            Session sess = currentSession();
            Connection conn = sess.connection();
            stmt = conn.createStatement();
//            zeroFailureCount();
        } catch (Exception e) {
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage());
        } finally {
            return stmt;
        }
    }

    protected Statement getBatchUpdateStmt() throws DataControlException {
        return getStmt();
    }

    protected boolean isSupportBatchUpdate() throws Exception {
        boolean br = false;
        try {
            Session sess = currentSession();
            Connection conn = sess.connection();
            DatabaseMetaData dmd = conn.getMetaData();
            br = dmd.supportsBatchUpdates();
        } catch (Exception e) {
//            exceptionHandler(e);
            throw new Exception(e);
        } finally {
            closeSession();
            return br;
        }
    }

    protected int[] doBatchUpdate() throws DataControlException {
        try {
            return stmt.executeBatch();
        } catch (SQLException e) {
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage());
        }
    }

    protected int[] batchUpdate() throws DataControlException {
        try {
            return stmt.executeBatch();
        } catch (SQLException e) {
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage());
        }
    }


    /** Executes a Select statement
     * @param strSQL a SQL Select Statement as a String
     * @throws DataControlException
     * @return
     */
    protected ResultSet doSelect(String strSQL) throws DataControlException {
        Statement stmt = null;
        ResultSet rs = null;

        try {
            Session sess = currentSession();
            Connection conn = sess.connection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(strSQL);
//            zeroFailureCount();
        } catch (Exception e) {
            logger.trace("SQL Statement that cause error: {}", strSQL);
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage());
        }
        return rs;
    }

    /** Parses a String as a SQL variable
     * @param strSQL A SQL string to be parsed
     * @return
     */
    protected String makeSqlStr(String strSQL) {
        if (strSQL == null) {
            return "NULL";
        }
        strSQL = strSQL.replaceAll("'", "''");
        return "'" + strSQL + "'";

    }

    protected String makeSqlStr(int i) {
        return Integer.toString(i);
    }

    protected String makeSqlStr(long l) {
        return Long.toString(l);
    }

    protected String makeSqlStr(short s) {
        return Integer.toString(s);
    }

    protected String makeSqlStr(java.util.Date d) {
        return formatMySqlDate(d);
    }

    protected String formatMySqlDate(java.util.Date date) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return "'" + formatter.format(date) + "'";
    }

    /** Calculates the row count of a specific ResultSet
     * @param rs A ResultSet
     * @return
     */
    public int getRowCount(ResultSet rs) {
        int rowCount, crrRow;
        try {
            crrRow = rs.getRow();
            rs.last();
            rowCount = rs.getRow();
            if (crrRow == 0) {
                rs.beforeFirst();
            } else {
                rs.absolute(crrRow);
            }
//            zeroFailureCount();
        } catch (Exception e) {
//            exceptionHandler(e);
            rowCount = 0;
        }
        return rowCount;
    }

    /**
     * Creates a new table for the given month. If table already exist,
     * then rename the old table
     * @param date Calendar
     * @throws DataManagerException
     * @return String Created Table Name
     */
    public String createMonthlyTable(Calendar date, String prefix) throws
            DataManagerException {
        String tableName = null;
        SimpleDateFormat bakTableSurfix = new SimpleDateFormat(
                "'_bk'_yyMMdd_kkmmssSSS");

        tableName = getMonthlyTableName(date, prefix);

        try {
            if (doesTableExist(tableName)) {
                java.util.Date currentTime = new java.util.Date();
                // FOR DEBBUG ONLY
                if ("true".equalsIgnoreCase((String) System.getProperty("DEBUG"))) {
                    doUpdate("DROP TABLE " + tableName);
                } else {
                    doUpdate("ALTER TABLE " + tableName + " RENAME TO " +
                             tableName + bakTableSurfix.format(currentTime));
                }
            }
            doUpdate("CREATE TABLE " + tableName + " (" + getTableDef() + ")");
//            zeroFailureCount();
            return tableName;
        } catch (Exception e) {
//            exceptionHandler(e);
            throw new DataManagerException(e);
        }

    }

    /**
     * getTableDef is to be override by implementing classes
     * @return String
     */
    protected String getTableDef() {
        return null;
    }

    /**
     *
     * @param date Calendar
     * @param prefix String
     * @return String
     */
    protected static String getMonthlyTableName(Calendar date, String prefix) {
        SimpleDateFormat tableFormat = new SimpleDateFormat("'" + prefix +
                "'_MM_yyyy");
        return tableFormat.format(date.getTime());
    }

    /**
     *
     * @param tableName String
     * @return boolean
     */
    public boolean doesTableExist(String tableName) {
        String sqlStr = "SHOW TABLES LIKE '" + tableName + "'";
        ResultSet rs = null;
        try {
            rs = doSelect(sqlStr);
//            zeroFailureCount();
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
//            exceptionHandler(e);
            return false;
        }
        finally{
            if(rs != null)
            {
                try{
                    rs.close();
                    rs = null;
                }catch(SQLException e)
                {
                    logger.warn("SQL Resultset close cause error! ");
                }
            }
        }
    }


    /** returns a database connection
     * @throws DataManagerException
     * @return returns a database connection
     */
    protected Connection getCon() throws DataManagerException {
        try {
            Session sess = currentSession();
            Connection connection = sess.connection();
//            zeroFailureCount();
            return connection;
        } catch (Exception ex) {
//            exceptionHandler(ex);
            throw new DataManagerException(ex.getMessage());
        }
    }

    public String isNullStmt(String columnName) {
        if (columnName == null)
            return "''";
        String stmt = " (" + columnName + " is null or " + columnName + "='') ";
        return stmt;
    }

    /** Abstract method add()
     * adds a Data object into the database
     * @param data
     * @throws
     */
    public abstract void add(Data data) throws DataManagerException;

    public void closeIdelFactory(DatabaseStatus dbStatus) {
        try{
        	 if(DatabaseStatus.MASTER_USED.equals(dbStatus) && factories[1] != null && !factories[1].isClosed()){
        		logger.trace("factories[1].isClosed()={}",factories[1].isClosed());
             	this.closeSession();
             	factories[1].close();
             	factories[1]=null;
             	logger.info("Close Slave DB session factory when dbStatus is {}",dbStatus);
             }else if(DatabaseStatus.SLAVE_USED.equals(dbStatus) && factories[0] != null && !factories[0].isClosed()){
            	 logger.trace("factories[0].isClosed()={}",factories[0].isClosed());
            	 this.closeSession();
            	 factories[0].close();
            	 factories[0]=null;
            	 logger.info("Close Master DB session factory when dbStatus is {}",dbStatus);
             }
        } catch (Exception e) {
            logger.warn("close DB factory error when switch dbstatus to {}",dbStatus,e);
        }
    }

	public String getDsName() {
		return dsName;
	}

	public void setDsName(String dsName) {
		this.dsName = dsName;
	}

	public DataControl getDataControl() {
		return dataControl;
	}

	public void setDataControl(DataControl dataControl) {
		this.dataControl = dataControl;
	}
	
	
}
