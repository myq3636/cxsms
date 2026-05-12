package com.king.gmms.ha;

/**
 * <p>Title: HATools</p>
 *
 * <p>Description:HA Tools classs, read file with lock, write file with lock or read file without lock  </p>
 *
 * <p>Copyright: Copyright (c) 2007</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */

import java.nio.channels.FileLock;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Properties;

import com.king.db.DBHAConstants;
import com.king.db.DatabaseStatus;

public class HATools {

    /**
     * write file with lock
     * @param fi RandomAccessFile
     * @param fc FileChannel
     * @param seed String
     */
    public static void writeFile(RandomAccessFile fi, FileChannel fc,
                                 String seed) {
        FileLock fileLock = null;
        try {
            synchronized (fi) {
                fileLock = fc.tryLock();
                while (fileLock == null || !fileLock.isValid()) {
                    fileLock = fc.tryLock();
                    Thread.currentThread().sleep(10L);
                }
                fi.write(seed.getBytes());
//                fi.seek(0);
            }
        }
        catch (Exception e) {
            System.out.println("Get Exception when to write file:" + e);
        }
        finally {
            if (fileLock != null) {
                try {
                    fileLock.release();
                }
                catch (Exception e) {
                    System.out.println(
                        "Get Exception when to release file lock:" + e);
                }
            }
        }
    }

    /**
     * read file with lock
     * @param fi RandomAccessFile
     * @param fc FileChannel
     * @return String
     */
    public static String readFileWithLock(RandomAccessFile fi, FileChannel fc) {
        FileLock fileLock = null;
        try {
            synchronized (fi) {
                byte[] temp = new byte[ (int) fi.length()];
                fileLock = fc.tryLock();
                while (fileLock == null || !fileLock.isValid()) {
                    fileLock = fc.tryLock();
                    Thread.currentThread().sleep(10L);
                }
                fi.read(temp);
                fi.seek(0);
                if (temp != null && temp.length > 0) {
                    return new String(temp);
                }
            }
        }
        catch (Exception e) {
            System.out.println("Get Exception when to read file:" + e);
        }
        finally {
            if (fileLock != null) {
                try {
                    fileLock.release();
                }
                catch (Exception e) {
                    System.out.println(
                        "Get Exception when to release file lock:" + e);
                }
            }
        }
        return null;
    }

    /**
     * read file without lock
     * @param fi RandomAccessFile
     * @return String
     */
    public static String readFileWithoutLock(RandomAccessFile fi) {
        try {
            synchronized (fi) {
                byte[] temp = new byte[ (int) fi.length()];
                fi.read(temp);
                fi.seek(0);
                if (temp != null && temp.length > 0) {
                    return new String(temp);
                }
            }
        }
        catch (Exception e) {
            System.out.println("Get Exception when to read file:" + e);
        }
        return null;
    }
    /**
     * read db status from configuration file
     */
    public static Properties readDBStatus(){
    	Properties dbprop = new Properties();
        String a2phome = System.getProperty("a2p_home");
        InputStream in = null;
        if(a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit(-1);
        }
        if(!a2phome.endsWith("/")) {
            a2phome = a2phome + "/";
            System.setProperty("a2p_home", a2phome);
        }
        try{
        	in = new FileInputStream(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
			dbprop.load(in);
			return dbprop;
        }catch(Exception e){
        	if(in!=null){
				try {
					in.close();
				} catch (IOException ex) {
					System.out.println("close Input stream in error of readDBStatus() function of HATools class file.");
				}
			}
        	System.out.println("Get Exception when to read file:" + e);
        }finally{
        	if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					System.out.println("close Input stream in error of readDBStatus() function of HATools class file.");
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
        InputStream in = null;
        OutputStream fos = null;
		try{
			in = new FileInputStream(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
	        dbprop.load(in);
			fos = new FileOutputStream(a2phome + DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH);
			System.out.println("Update db status to:"+status);
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
					System.out.println("close Input stream error in updateDBStatus2File() function of HATools class file.");
				}
			}
			if(fos!=null){
				try {
					fos.close();
				} catch (IOException ex) {
					System.out.println("close output stream fos error in updateDBStatus2File() function of HATools class file.");
				}
			}
        	System.out.println("Write dbstatus to "+DBHAConstants.DBSTATUS_FILE_RELATIVE_PATH+"failed!"+e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					System.out.println("close Input stream in error in updateDBStatus2File() function of HATools class file.");
				}
			}
			if(fos!=null){
				try {
					fos.close();
				} catch (IOException e) {
					System.out.println("close output stream fos error in updateDBStatus2File() function of HATools class file.");
				}
			}
		}
	}
}
