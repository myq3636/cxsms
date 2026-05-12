package com.king.gmms.routing;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Observable;

import org.xbill.DNS.Lookup;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.SimpleResolver;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.SwitchDNSEvent;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;
import com.king.mgt.cmd.user.UserCommandSwitchDNS;

public class ADSServerMonitor implements LifecycleListener,Runnable {

	private SystemLogger log = SystemLogger.getSystemLogger(ADSServerMonitor.class);
	private static Resolver resolver;
	private boolean usingSlave = false;// false:using master, true:using slave
	private boolean isASY;
	private String dnsTestNumber = "8831109999";
	private int failedCount = 0;
	private int FAILED_MAX_COUNT = 3;
	private int dnsTestPeriod = 30000;
	private String[] dnsAddresses = new String[2];
	private int[] dnsPortes = new int[2];
	private int dnsTimeOut = 20;
	private static ADSServerMonitor monitor = null;
	volatile boolean running = false;
	private MNPSolution mnp;
	private RandomAccessFile fi = null;
	private FileChannel fc = null;
	private Object mutex = new Object();
	private int dnsServerTotalNumber=0;
	private LifecycleSupport lifecycle;
	
	//singleton mode
	private ADSServerMonitor() {
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		try {
			initDNSStatus();
			if (gmmsUtility.getCommonProperty("DNSAddress") == null) {
				throw new UnknownHostException(
						"No definition of DNS in property file");
			}
			String dnsAddress = gmmsUtility.getCommonProperty("DNSAddress");
			String dNSMaxFailedLimit = gmmsUtility
					.getCommonProperty("DNSMaxFailedLimit");
			String dNSTestPeriod = gmmsUtility
					.getCommonProperty("DNSTestPeriod");
			String dNSTestNumber = gmmsUtility
					.getCommonProperty("DNSTestNumber");
			if (!"".equals(dNSTestNumber) && null != dNSTestNumber) {
				dnsTestNumber = dNSTestNumber.trim();
			}
			if (!"".equals(dNSMaxFailedLimit) && null != dNSMaxFailedLimit) {
				try{
					FAILED_MAX_COUNT = Integer.parseInt(dNSMaxFailedLimit);
				}catch(Exception e){
					log.error("DNSMaxFailedLimit is an invalid number:"+dNSMaxFailedLimit);
				}
			}
			if (!"".equals(dNSTestPeriod) && null != dNSTestPeriod) {
				try{
					dnsTestPeriod = Integer.parseInt(dNSTestPeriod);
				}catch(Exception e){
					log.error("DNSTestPeriod is an invalid number:"+dnsTestPeriod);
				}
			}
			
			String[] dnsAddressePort = dnsAddress.split(",");
			dnsServerTotalNumber = dnsAddressePort.length>2 ? 2 : dnsAddressePort.length; //only support max 2 servers
			for (int i = 0; i < dnsServerTotalNumber; i++) {
				String[] dnsInfo = dnsAddressePort[i].trim().split(":");
				dnsAddresses[i] = dnsInfo[0].trim();
				try{
					dnsPortes[i] = Integer.parseInt(dnsInfo[1].trim());
				}catch(Exception e){
					log.error("dnsPort "+i+" is an invalid number:"+dnsPortes[i]+",use default port 53.");
					dnsPortes[i] = 53;
				}
			}
			try{
				dnsTimeOut = Integer.parseInt(gmmsUtility.getCommonProperty(
						"DNSTimeOut", "20"));
			}catch(Exception e){
				log.error("DNSTimeOut is an invalid number:"+dnsTimeOut);
			}
			initResolver();
			lifecycle = gmmsUtility.getLifecycleSupport();
            lifecycle.addListener(Event.TYPE_SWITCHDNS, this, 0);
		} catch (Exception ex) {
			log.error("Exception in initialize() of DNSClient", ex);
		}
	}
	
	
	/**
	 * read dns status from file
	 */
	private void initDNSStatus() {
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		String filePath = System.getProperty("a2p_home", "/usr/local/a2p")
				+ "/ha/";
		try {
			File file = new File(filePath);
			if (!file.exists()) {
				file.mkdir();
			}
			file = new File(filePath + "/" + "DNSStatus");
			if (!file.exists()) {
				file.createNewFile();
				fi = new RandomAccessFile(file, "rw");
				fc = fi.getChannel();
				this.writeFile(fi, fc, "M");
			} else {
				fi = new RandomAccessFile(file, "rw");
				fc = fi.getChannel();
				String flag = this.readFileWithoutLock(fi);
                if (flag.startsWith("S")) {
        	    	usingSlave=true;
                }
			}
		} catch (Exception e) {
			log.warn("Get exception when to initial DNS status file:" + e);
			System.exit(0);
		}
	}
	
	
	/**
	 * init resolver
	 */
	private void initResolver() {
		if (resolver == null) {
			switchResolver();
		}
	}
	
	/**
	 * switch dns for resolver
	 */
	private void switchResolver() {
		try {
			int dnsIdx = 0;
			if (usingSlave) {
				dnsIdx = 1;
			}
			resolver = new SimpleResolver(dnsAddresses[dnsIdx], isASY);
			resolver.setPort(dnsPortes[dnsIdx]);
			resolver.setTimeout(dnsTimeOut);
			Lookup.setDefaultResolver(resolver);
			mnp = new MNPSolution();
		} catch (Exception e) {
			log.info("switchResolver error:", e);
		}
	}
	
	/**
	 * clear DNS status
	 */
	private void clearStatus() {
		try {
			fi.seek(0);
		} catch (Exception e) {
			log.error("fi seek error:", e);
		}
		synchronized (mutex) {
			this.writeFile(fi, fc, "M");
		}
	}
	
	 /**
     * write file with lock
     * @param fi RandomAccessFile
     * @param fc FileChannel
     * @param seed String
     */
    public void writeFile(RandomAccessFile fi, FileChannel fc,
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
     * read file without lock
     * @param fi RandomAccessFile
     * @return String
     */
    public String readFileWithoutLock(RandomAccessFile fi) {
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
	 * Translate a number (12345) to a string 5.4.3.2.1.e164.gprs. or
	 * 5.4.3.2.1.e164.local.
	 * 
	 * @param number
	 *            String
	 * @param suffix
	 *            String
	 * @return StringBuffer
	 * @throws Exception
	 */
	private StringBuilder numberToName(String number, String suffix)
			throws Exception {
		StringBuilder e164 = new StringBuilder(100);
		for (int i = number.length() - 1; i >= 0; i--) {
			e164.append(number.charAt(i)).append('.');
		}
		e164.append("e164.");
		e164.append(suffix);
		e164.append(".");
		return e164;
	}
    
	/**
	 * deal with query status code
	 * 
	 * @param resultCode
	 */
	public void dealResultCode(int resultCode) {
		if (resultCode == Lookup.TRY_AGAIN) {
			failedCount++;
			if (failedCount < FAILED_MAX_COUNT) {
				return;
			} else if (failedCount == FAILED_MAX_COUNT) {
				usingSlave = !usingSlave;
				switchResolver();
				sendAlertMail();
				failedCount = 0;
				this.writeStatus();// write dns status to file
			}
		} else {
			failedCount = 0;
		}
	}
	
	/**
	 * send Alert Mail when switch dns
	 */
	private void sendAlertMail(){
		int dnsIdx = 0;
		if (usingSlave) {
			dnsIdx = 1;
		}
		MailSender.getInstance().sendAlertMail("A2P alert mail from " +
                ModuleURI.self().getModule() + " in " +
                ModuleURI.self().getAddress(), "Switch DNS query to ADS server with IP: "+dnsAddresses[dnsIdx]+" ,port: "+dnsPortes[dnsIdx],null);
	}
	
	/**
	 * write DNS status
	 */
	private void writeStatus() {
		try {
			fi.seek(0);
		} catch (Exception e) {
			log.error("fi seek error:", e);
		}
		synchronized (mutex) {
			if (usingSlave) {
				this.writeFile(fi, fc, "S");
			} else {
				this.writeFile(fi, fc, "M");
			}
		}
	}
	
	@Override
	public int OnEvent(Event event) {
		if (event.getEventType() == Event.TYPE_SWITCHDNS) {
        	if(dnsServerTotalNumber<2){
        		log.warn("Needn't switch dns because of DNSServer's total number is"+dnsServerTotalNumber);
        		return 0;
        	}
        	if(event.getEventSubType()==UserCommandSwitchDNS.TYPE_MASTER){
        		if(this.usingSlave==false){
        			log.warn("Needn't switch to master dns because GMD has already used master!");
        			return 1;
        		}else{
        			this.usingSlave=false;
        		}
        	}else if(event.getEventSubType()==UserCommandSwitchDNS.TYPE_SLAVE){
        		if(this.usingSlave==true){
        			log.warn("Needn't switch to slave dns because GMD has already used slave!");
        			return 1;
        		}else{
        			this.usingSlave=true;
        		}
        	}
			switchResolver();
			this.writeStatus();// write dns status to file
        }
        return 1;
	}

	@Override
	public void run() {
		log.info("ADSServerMonitor is running!");
		while (running) {
			try {
				Thread.sleep(dnsTestPeriod);
				HashMap dnsResp = mnp.query(numberToName(dnsTestNumber,
						"local").toString(), "NAPTR");
				Iterator it = dnsResp.keySet().iterator();
				int resultCode = (Integer) it.next();
				dealResultCode(resultCode);
				// TODO:remove unused code
				String dnsIP = dnsAddresses[0];
				int dnsPort = dnsPortes[0];
				if (usingSlave) {
					dnsIP = dnsAddresses[1];
					dnsPort = dnsPortes[1];
				}
				log.trace("ADSServerMonitor queried to "+  dnsIP + ":" + dnsPort+", result code=" + resultCode);
			} catch (Exception e) {
				log.error(e, e);
			}
		}
		log.info("ADSServerMonitor thread stop!");
	}
	
	public static synchronized ADSServerMonitor getInstance() {
		if (monitor == null)
			monitor = new ADSServerMonitor();
		return monitor;
	}

	public void start() {
		if (running) {
			log.info("ADSServerMonitor thread has already started!");
			return;
		}
		running = true;
		Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
				"ADSServerMonitor");
		if(dnsServerTotalNumber>1){
			monitor.start();
			log.info("ADSServerMonitor thread start!");
		}else{
			log.info("ADSServerMonitor thread needn't start,because of DNSServerTotalNumber="+dnsServerTotalNumber);
		}
	}

	public void stop() {
		running = false;
		if(dnsServerTotalNumber>1){
			clearStatus();
			log.info("ADSServerMonitor thread stop!");
		}
	}
	
	/**
	 * invoked by DNSClient
	 * @return
	 */
	public boolean getDNSStatus(){
		return this.usingSlave;
	}
	
	public String getDNSServerAddress(){
		if(usingSlave){
			return dnsAddresses[1];
		}else{
			return dnsAddresses[0];
		}
	}
	
	public int getDNSServerPort(){
		if(usingSlave){
			return dnsPortes[1];
		}else{
			return dnsPortes[0];
		}
	}
	
}
