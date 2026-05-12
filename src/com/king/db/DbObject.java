package com.king.db;


import java.util.ArrayList;
import java.util.Properties;
import java.lang.reflect.*;

import com.king.framework.SystemLogger;

public class DbObject {
    private String driver;
    private String username;
    private String password;
    private String url;
    private int maxPoolSize = 20;
    private int minPoolSize = 5;
    private long timeout = 1800;
    private int maxStatement = 50;
    private String hibernateDialect;
    private ArrayList dataManagers = new ArrayList();
    private static SystemLogger log = SystemLogger.getSystemLogger(DbObject.class); 

    /**
     * Default construtor
     */
    public DbObject() {
        super();
    }

    /**
     * Constructs a DSConfig according to the configuration in properties file
     * @param prop Properties
     * @param dsName String
     */
    public DbObject(Properties prop, String dsName) throws ConfigurationException{
        this(prop, dsName, false);
    }

    public DbObject(Properties prop, String dsName, boolean isBackup) throws ConfigurationException{
//        super();
        String dsPrefix = "DS_" + dsName + "_";
        this.driver = prop.getProperty(dsPrefix + "Driver");
        if (this.driver == null)
            throw new ConfigurationException("Mandatory field \""+ dsPrefix + "Driver\"" + " is missing.");

        this.url = prop.getProperty(dsPrefix + "URL");
        if (this.url == null)
            throw new ConfigurationException("Mandatory field \""+ dsPrefix + "URL\"" + " is missing.");

        this.username = prop.getProperty(dsPrefix + "Login");
        if (this.username == null)
            throw new ConfigurationException("Mandatory field \""+ dsPrefix + "Login\"" + " is missing.");

        this.password = prop.getProperty(dsPrefix + "Password", "");

        this.hibernateDialect = prop.getProperty(dsPrefix + "Dialect");
        if (this.hibernateDialect == null)
            throw new ConfigurationException("Mandatory field \""+ dsPrefix + "Dialect\"" + " is missing.");

        this.maxPoolSize = Integer.parseInt(prop.getProperty(dsPrefix + "MaxPoolSize", "20"));
        this.minPoolSize = Integer.parseInt(prop.getProperty(dsPrefix + "MinPoolSize", "5"));
        this.timeout = Long.parseLong(prop.getProperty(dsPrefix + "Timeout", "1800"));
        this.maxStatement = Integer.parseInt(prop.getProperty(dsPrefix + "MaxStatement", "50"));
        if (!isBackup) {
            readDataManagers(prop, dsPrefix);
        }
    }

    public void readDataManagers(Properties prop, String dsPrefix)  throws ConfigurationException{
        String dmNamesStr = prop.getProperty(dsPrefix+ "DataManagers");
        if (dmNamesStr == null)
            return;
        String [] dmNames = dmNamesStr.split(",");
        String dmName;
        DataManager dataManager;
        Class cl = null;
        log.trace("dmNames.length={};dmNames={}",dmNames.length,dmNames);
        for (int i=0; i<dmNames.length; i++){
            dmName = dmNames[i].trim();
            try {
                cl = Class.forName(dmName);
                dataManager = (DataManager) cl.newInstance();
            }
            catch (ClassNotFoundException ex) {
                throw new ConfigurationException("Invalid DataManager class name: \"" + dmName + "\".");
            }
            catch (Exception ex) {
                /************ Legacy DataManager Support **********/
                /* In case there is no default constructor, then  *
                 * create the DataManager with a dummy constructor*/
                DbObject dummy = new DbObject();
                Class argClass = DbObject.class;
                Class[] intArgsClass = new Class[] {argClass};
                Object[] intArgs = new Object[] {dummy};
                try{
                    Constructor initCons = cl.getConstructor(intArgsClass);
                    dataManager = (DataManager) initCons.newInstance(intArgs);
                } catch (Exception ex1){
                    throw new ConfigurationException("Unable to create an instance of DataManager \"" + dmName + "\".", ex1);
                }
            }
            dataManagers.add(dataManager);
        }
    }


    public DataManager [] getDataManagers(){
        if (dataManagers.size()<=0)
            return null;
        return (DataManager[]) dataManagers.toArray(new DataManager[dataManagers.size()]);
    }

    public void setDataManagers(DataManager[] dataManagers){
        for (int i=0; i<dataManagers.length; i++){
            this.dataManagers.add(dataManagers[i]);
        }
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void setMaxStatement(int maxStatement) {
        this.maxStatement = maxStatement;
    }

    public void setHibernateDialect(String hibernateDialect) {
        this.hibernateDialect = hibernateDialect;
    }

    public String getDriver() {
        return driver;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public long getTimeout() {
        return timeout;
    }

    public int getMaxStatement() {
        return maxStatement;
    }

    public String getHibernateDialect() {
        return hibernateDialect;
    }
}
