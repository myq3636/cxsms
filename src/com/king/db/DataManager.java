package com.king.db;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.king.framework.SystemLogger;

public abstract class DataManager {
    private ThreadLocal connectionHolder = new ThreadLocal();
    private ThreadLocal activeDsNameHolder = new ThreadLocal();
    protected static SystemLogger logger = SystemLogger.getSystemLogger(DataManager.class); 
    private static ThreadLocal<java.util.List<Connection>> allConnectionsForThread = new ThreadLocal<java.util.List<Connection>>() {
        protected java.util.List<Connection> initialValue() {
            return new java.util.LinkedList<Connection>();
        }
    };
    protected DataControl dataControl = DataControl.getInstance();
    protected Statement stmt;
    private String dsName = null;

    public static void closeAllSessions() {
        try {
            java.util.List<Connection> connections = allConnectionsForThread.get();
            if (connections != null && !connections.isEmpty()) {
                for (Connection connection : connections) {
                    try {
                        if (connection != null && !connection.isClosed()) {
                            connection.close();
                        }
                    } catch (Exception e) {}
                }
                connections.clear();
            }
        } catch (Exception ex) {
            logger.warn("Error cleaning up thread local DB connections", ex);
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
     *                       JDBC supporting functions                        *
     **************************************************************************/

    /**
     * To be implement for subclasses: to return associated entity class <br>
     * For the consideration of easy migration, a default implementation is
     * provided for legacy data managers.
     */
    public Class getAssociatedClass() {
        return null;
    }

    /**
     * Get an available JDBC connection, create a new one if none is available.
     */
    public Connection currentConnection() throws DataControlException{
        Connection connection = (Connection) connectionHolder.get();
        try {
            if (connection != null && !connection.isClosed()) {
                return connection;
            }
        } catch (SQLException e) {
            logger.warn("Check JDBC connection failed. dsName={}", dsName, e);
        }
        connection = openConnectionByStatus();
        if (connection == null) {
            logger.error("Can't get DB connections with master and slave. dsName={}, usedDbStatus={}, canHandover={}",
                    dsName, dataControl.getUsedDatabaseStatus(), dataControl.getCanHandover());
            throw new DataControlException("Can't get DB connection. dsName=" + dsName
                    + ", usedDbStatus=" + dataControl.getUsedDatabaseStatus()
                    + ", canHandover=" + dataControl.getCanHandover());
        }
        connectionHolder.set(connection);
        allConnectionsForThread.get().add(connection);
        return connection;
    }

    private Connection openConnectionByStatus() throws DataControlException {
        if(DatabaseStatus.MASTER_USED.equals(dataControl.getUsedDatabaseStatus())){
            try {
                logger.debug("getMasterConnection");
                activeDsNameHolder.set(dsName);
                return dataControl.getMasterConnection(dsName);
            } catch (Exception e) {
                logger.warn("Can't create connection to Master DB. dsName={}", dsName, e);
                if (dataControl.getCanHandover()) {
                    try {
                        activeDsNameHolder.set("backup" + dsName);
                        Connection connection = dataControl.getSlaveConnection(dsName);
                        dataControl.setUsedDatabaseStatus(DatabaseStatus.SLAVE_USED);
                        DatabaseStatus.updateDBStatus2File(DatabaseStatus.SLAVE_USED);
                        return connection;
                    } catch (Exception slaveException) {
                        logger.warn("Can't create connection to Slave DB. dsName={}", dsName, slaveException);
                    }
                }
            }
        } else if(dataControl.getCanHandover() && DatabaseStatus.SLAVE_USED.equals(dataControl.getUsedDatabaseStatus())){
            try {
                logger.debug("getSlaveConnection");
                activeDsNameHolder.set("backup" + dsName);
                return dataControl.getSlaveConnection(dsName);
            } catch (Exception e) {
                logger.warn("Can't create connection to Slave DB. dsName={}", dsName, e);
                try {
                    activeDsNameHolder.set(dsName);
                    Connection connection = dataControl.getMasterConnection(dsName);
                    dataControl.setUsedDatabaseStatus(DatabaseStatus.MASTER_USED);
                    DatabaseStatus.updateDBStatus2File(DatabaseStatus.MASTER_USED);
                    return connection;
                } catch (Exception masterException) {
                    logger.warn("Can't create connection to Master DB. dsName={}", dsName, masterException);
                }
            }
        }
        return null;
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
            Connection connection = (Connection) connectionHolder.get();
            String activeDsName = (String) activeDsNameHolder.get();
            connectionHolder.set(null);
            activeDsNameHolder.set(null);
            if (connection != null) {
                DataControl.releaseConnection(activeDsName == null ? resolveActiveDsName() : activeDsName, connection);
                allConnectionsForThread.get().remove(connection);
            }
        } catch (Exception ex) {
//            exceptionHandler(ex);
            logger.warn("Close JDBC connection exception. dsName={}", dsName, ex);
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
            Connection conn = currentConnection();
            stmt = conn.createStatement();
            int line = stmt.executeUpdate(strSQL);
//            zeroFailureCount();
            return line;
        } catch (Exception e) {
            logger.trace("SQL Statement that cause error: {}", strSQL);
//            exceptionHandler(e);
            throw new DataControlException("Database connection error: " +
                                           e.getMessage(), e);
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
            Connection conn = currentConnection();
            stmt = conn.createStatement();
//            zeroFailureCount();
        } catch (Exception e) {
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage(), e);
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
            Connection conn = currentConnection();
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
                                           e.getMessage(), e);
        }
    }

    protected int[] batchUpdate() throws DataControlException {
        try {
            return stmt.executeBatch();
        } catch (SQLException e) {
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage(), e);
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
            Connection conn = currentConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(strSQL);
//            zeroFailureCount();
        } catch (Exception e) {
            logger.trace("SQL Statement that cause error: {}", strSQL);
//            exceptionHandler(e);
            throw new DataControlException("Database query error: " +
                                           e.getMessage(), e);
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
            Connection connection = currentConnection();
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
        closeSession();
        logger.info("Close idle JDBC connection when dbStatus is {}",dbStatus);
    }

    private String resolveActiveDsName() {
        if (dataControl.getCanHandover() && DatabaseStatus.SLAVE_USED.equals(dataControl.getUsedDatabaseStatus())) {
            return "backup" + dsName;
        }
        return dsName;
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
