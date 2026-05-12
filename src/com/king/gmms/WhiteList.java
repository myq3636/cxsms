package com.king.gmms;

import java.util.ArrayList;
import java.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.domain.A2PCustomerConfig;
import com.king.gmms.domain.CustomerConfigurationException;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.tcp.ByteBuffer;
import com.king.message.gmms.GmmsMessage;

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
public class WhiteList implements LifecycleListener{
    private ArrayList<String> whiteList;
    LifecycleSupport lifecycle;
    private static SystemLogger log = SystemLogger
	.getSystemLogger(WhiteList.class);
    private String confFileVersion = null;

    public WhiteList() {
    	whiteList = new ArrayList<String> ();
        lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
        lifecycle.addListener(Event.TYPE_WHITELIST_RELOAD, this, 1);
    }

    public void setLifecycleSupport(LifecycleSupport lifecycle) {
        this.lifecycle = lifecycle;
    }

    public void addWhitelist(String num) {
    	whiteList.add(num);
    }

    public boolean allowWhenRouting(GmmsMessage msg) {
        return (whiteList.contains(msg.getRecipientAddress()));
    }
   /**
    *
    * @param path String
    * @return int
    */
   public int loadWhitelist(String path) {
       try {
           int rtnval=0;
	   	   if (log.isDebugEnabled()) {
	            log.debug("Loading Whitelist from file {} ...",path);
	   	   }
           File file = new File(path);
           if(!file.exists()){
        	   return 0;
           }
           if(!compareConfBlocks(file)){
        	   return 0;
           }
           FileInputStream fin = new FileInputStream(file);
           BufferedReader reader = new BufferedReader(new InputStreamReader(
               fin));

           // clear the old blacklist after successfully read the configuration file.
           ArrayList<String> tempWhiteList=new ArrayList<String>();
           String line=null;
           while ( (line=reader.readLine())  != null) {
               line=line.trim();
               if (line.startsWith("#") || line.length()==0){
                   continue;
               }
               String[] args = line.split(";");
               for(int i=0; i<args.length; i++){
            	   if(args[i]!=null && args[i].length()!=0){
            		   tempWhiteList.add(args[i]);
            	   }            	   
               }
               
           }           
           this.whiteList = tempWhiteList;
           return rtnval;
       }
       catch (FileNotFoundException ex) {
           log.error(ex, ex);
           return 1;
       }
       catch (Exception ex) {
           log.error(ex, ex);
           return 1;
       }
   }

   public int OnEvent(Event event) {
   	   if (log.isDebugEnabled()) {
   	       log.debug("Event Received. Type: {}", event.getEventType());
   	   }
       GmmsUtility util = GmmsUtility.getInstance();
       if (event.getEventType() == Event.TYPE_WHITELIST_RELOAD) {
           return loadWhitelist(util.getWhitelistFilePath());
       }
       return 0;
   }

   public int testNotify() {
       EventFactory factory = EventFactory.getInstance();
       return this.lifecycle.notify(factory.newEvent(Event.
    		   TYPE_WHITELIST_RELOAD));
   }

   
   
   private boolean compareConfBlocks(File file) throws CustomerConfigurationException {
		String md5Sum = this.getFileCheckSum(file);
		if (md5Sum == null || md5Sum.equals(this.confFileVersion)) {
			log.warn("No need to load whitelist.cfg or md5Sum error. confFileVersion={}, md5Sum={}",new Object[] { confFileVersion, md5Sum });
			return false;
		} else {
			if (log.isInfoEnabled()) {
				log.info("whitelist.cfg change, confFileVersion={}, md5Sum={}",new Object[] { confFileVersion, md5Sum });
			}
			this.confFileVersion = md5Sum;
		}
		return true;
	}
   
   private String getFileCheckSum(File file) {
		String md5Sum = null;
		FileInputStream fin = null;
		BufferedReader reader = null;
		try {
			fin = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fin));
			ByteBuffer buffer = new ByteBuffer();
			String line = null;
			while ((line = reader.readLine()) != null) {
				buffer.appendString(line);
			}
			md5Sum = HttpUtils.encrypt(buffer.getBuffer(), "MD5");
			reader.close();
			fin.close();
		} catch (IOException ex) {
			log.error(ex, ex);
		}
		return md5Sum;
	}

   public static void main(String[] args) {
       WhiteList bl = new WhiteList();
       bl.setLifecycleSupport(new LifecycleSupport());
       int rtnval = bl.loadWhitelist(args[0]);
       System.out.println(rtnval);
       
   }
}
