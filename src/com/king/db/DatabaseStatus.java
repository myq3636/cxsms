package com.king.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import com.king.framework.SystemLogger;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public enum DatabaseStatus {
    MASTER_UP,
    MASTER_DOWN,
    SLAVE_UP,
    SLAVE_DOWN,
    MASTER_USED,
    SLAVE_USED;
    protected static SystemLogger log = SystemLogger.getSystemLogger(DatabaseStatus.class);
    public static DatabaseStatus get(String status) {
        if (status == null || status.length() <= 0) {
            return MASTER_UP;
        }
        if (status.equalsIgnoreCase("master_up")
            || status.equalsIgnoreCase("0")) {
            return MASTER_UP;
        }
        if (status.equalsIgnoreCase("master_down")
            || status.equalsIgnoreCase("1")) {
            return MASTER_DOWN;
        }
        if (status.equalsIgnoreCase("slave_up")
                || status.equalsIgnoreCase("2")) {
                return SLAVE_UP;
        }
        if (status.equalsIgnoreCase("slave_down")
            || status.equalsIgnoreCase("3")) {
            return SLAVE_DOWN;
        }
        if (status.equalsIgnoreCase("master_used")
                || status.equalsIgnoreCase("4")) {
            return MASTER_USED;
        }
        if (status.equalsIgnoreCase("slave_used")
                || status.equalsIgnoreCase("5")) {
            return SLAVE_USED;
        }
        //for changeDB pdu
        if (status.equalsIgnoreCase(DBHAConstants.SWITCH2MASTER)) {
            return MASTER_USED;
        }
        if (status.equalsIgnoreCase(DBHAConstants.SWITCH2SLAVE)) {
            return SLAVE_USED;
        }
        return MASTER_UP;
    }

    public static String get(DatabaseStatus status) {
        switch (status) {
            case MASTER_UP:
                return "MASTER_UP";
            case MASTER_DOWN:
                return "MASTER_DOWN";
            case SLAVE_UP:
                return "SLAVE_UP";
            case SLAVE_DOWN:
                return "SLAVE_DOWN";
            case MASTER_USED:
                return "MASTER_USED";
            case SLAVE_USED:
                return "SLAVE_USED";
            default:
                return "MASTER_UP";
        }
    }
    /**
     * read db status from configuration file
     */
    public static Properties readDBStatus(){
    	Properties dbprop = new Properties();
        String a2phome = System.getProperty("a2p_home");
        if(a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit(-1);
        }
        if(!a2phome.endsWith("/")) {
            a2phome = a2phome + "/";
            System.setProperty("a2p_home", a2phome);
        }
        InputStream in = null;
        try{
        	File dbStFile=new File(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);    
     		if(!dbStFile.exists())    
     		{    
     			log.warn("Missed {}, create it by default.",DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
     	        dbStFile.createNewFile();   
     	        DatabaseStatus.updateDBStatus2File(DatabaseStatus.MASTER_USED);
     	        DatabaseStatus.updateDBStatus2File(DatabaseStatus.MASTER_UP);
     	        DatabaseStatus.updateDBStatus2File(DatabaseStatus.SLAVE_UP);
     		}
     		in = new FileInputStream(dbStFile);
			dbprop.load(in);
			return dbprop;
        }catch(Exception e){
        	if(in!=null){
				try {
					in.close();
				} catch (IOException ex) {
					log.info("close Input stream in error!");
				}
			}
        	System.out.println("Get Exception when to read file:" + e);
        }finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					log.info("close Input stream in error!");
				}
			}			
		}    
        return dbprop;
	}
    /**
     * update db status to configuration file
     * @param status
     */
    public static void updateDBStatus2File(DatabaseStatus status){
		String a2phome = System.getProperty("a2p_home");
        if(a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit(-1);
        }
        if(!a2phome.endsWith("/")) {
            a2phome = a2phome + "/";
            System.setProperty("a2p_home", a2phome);
        }
        Properties dbprop = new Properties();
        OutputStream fos = null;
        InputStream in = null;
		try{
			in = new FileInputStream(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
	        dbprop.load(in);
			fos = new FileOutputStream(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
			switch(status){
				case MASTER_UP:
					dbprop.setProperty(DBHAConstants.MASTER_KEY, DBHAConstants.DB_STATUS_UP);
					break;
				case MASTER_DOWN:
					dbprop.setProperty(DBHAConstants.MASTER_KEY, DBHAConstants.DB_STATUS_DOWN);
					break;
				case SLAVE_UP:
					dbprop.setProperty(DBHAConstants.SLAVE_KEY, DBHAConstants.DB_STATUS_UP);
					break;
				case SLAVE_DOWN:
					dbprop.setProperty(DBHAConstants.SLAVE_KEY, DBHAConstants.DB_STATUS_DOWN);
					break;
				case MASTER_USED:
					dbprop.setProperty(DBHAConstants.USED_KEY,DBHAConstants.MASTER_KEY);
					break;
				case SLAVE_USED:
					dbprop.setProperty(DBHAConstants.USED_KEY,DBHAConstants.SLAVE_KEY);
					break;
				default:break;
			}
			dbprop.store(fos, "Update "+status+" to file.");
		}catch(Exception e){
			if(in!=null){
				try {
					in.close();
				} catch (IOException ex) {
					log.info("close Input stream in error!");
				}
			}
			if(fos!=null){
				try {
					fos.close();
				} catch (IOException ex) {
					log.info("close output stream fos error!");
				}
			}
        	log.info("Write dbstatus to "+DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH+"failed!"+e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					log.info("close Input stream in error!");
				}
			}
			if(fos!=null){
				try {
					fos.close();
				} catch (IOException e) {
					log.info("close output stream fos error!");
				}
			}
		}
	}
}
