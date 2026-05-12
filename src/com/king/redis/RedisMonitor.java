package com.king.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Observable;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.king.db.DBHAConstants;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;


/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2005
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class RedisMonitor extends Observable implements Runnable,
		LifecycleListener {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(RedisMonitor.class);
	private static volatile boolean running = false;

	private static final String MASTER = "M";
	private static final String SLAVE = "S";
	protected static JedisPoolConfig config = new JedisPoolConfig();	
	private static RedisMonitor instance = new RedisMonitor();
	private static volatile boolean initialed = false;
	private RandomAccessFile fi = null;
	private FileChannel fc = null;
	private static String masterOrSlave = "M";
	private long interval = 30000L;
	private int slaveCount = 0;
	private int masterCount = 0;
	Properties properties = null;
	private Object mutex = new Object();

	private RedisMonitor() {
	}

	private String master_host;
	private String slave_host;
	private int master_port;
	private int slave_port;
	private int MASTER_CONSECUTIVE_NUM_TO_ALERT=0;
	private int SLAVE_CONSECUTIVE_NUM_TO_ALERT=0;
    private static JedisPool master_pool;
    private static JedisPool slave_pool;
    private int redis_error_Connection_count;
    private boolean isAuth;
    private String password;
	/**
	 * max number of alert mail to send
	 */
	private int MAX_ALERT_MAIL_NUM;

	/**
	 * default max number of alert mail to send
	 */
	private static final int DEFAULT_ALERT_MAIL_NUM = 1;

	private void init() {
		InputStream fileInputStream = null;
		try {
			// String sep = System.getProperty("file.separator");
			GmmsUtility gmmsUtility = GmmsUtility.getInstance();
			String fileName = gmmsUtility.getRedisFile();
			master_host = gmmsUtility
					.getCommonProperty("Redis_master_host");
			slave_host = gmmsUtility.getCommonProperty("Redis_slave_host");
			master_port = Integer.parseInt(gmmsUtility
					.getCommonProperty("Redis_master_port"));
			slave_port = Integer.parseInt(gmmsUtility
					.getCommonProperty("Redis_slave_port"));
			//config.setMaxActive(10);
			//config.setMaxIdle(5);	
			isAuth = Boolean.parseBoolean(gmmsUtility.getCommonProperty("Redis_isAuth","false"));
			password = gmmsUtility.getCommonProperty("Redis_password",null);
			try {
				interval = Integer.parseInt(gmmsUtility
						.getCommonProperty("RedisMonitor_interval_time")) * 1000;
			} catch (Exception e) {
				interval = 30000;
			}

			try {
				redis_error_Connection_count = Integer.parseInt(gmmsUtility
						.getCommonProperty("RedisMonitor_error_connection_count","3"));
			} catch (Exception e) {
				redis_error_Connection_count = 3;
			}
			if(redis_error_Connection_count==0){
				redis_error_Connection_count = 3;
			}
			
			/*fi = new RandomAccessFile(new File(fileName), "rw");
			fc = fi.getChannel();*/
			File file=new File(fileName);  
			synchronized (file) {
				if(!file.exists()){    
				    try {    
				        file.createNewFile(); 
				        Properties ps = new Properties();				
						fileInputStream = new FileInputStream(fileName);
						ps.load(fileInputStream);
					    OutputStream fos = new FileOutputStream(fileName);
						ps.setProperty(DBHAConstants.USED_KEY, MASTER);
						ps.store(fos, "Update "+DBHAConstants.USED_KEY+" to file.");
				    } catch (IOException e) {    
				       log.error("RedisStatus file is not exist! so create it!");  
				    } 
				    
				} 
			}
				try {
					properties = new Properties();				
					fileInputStream = new FileInputStream(fileName);
					properties.load(fileInputStream);
					masterOrSlave = properties.getProperty("Used");
				} catch (Exception e) {
					log.error("Redis Monitor init redis status error!");
					masterOrSlave = MASTER;
							
				}
			

			try {
				MAX_ALERT_MAIL_NUM = Integer.parseInt(GmmsUtility.getInstance()
						.getCommonProperty("RedisMonitor_MaxAlertMailNum",
								DEFAULT_ALERT_MAIL_NUM + ""));
			} catch (NumberFormatException e) {
				// use default constant
				MAX_ALERT_MAIL_NUM = DEFAULT_ALERT_MAIL_NUM;

				log.trace(
						"RedisMonitorMaxAlertMailNumber use default value:{}",
						DEFAULT_ALERT_MAIL_NUM);

			}
		} catch (Exception e) {
			log.error("Redis config init error!");
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e1) {
				}
			}
		}
		initialed = true;
	}

	public static RedisMonitor getInstance() {
		if (!instance.initialed) {
			synchronized (instance) {
				if (!instance.initialed) {
					instance.init();
				}
			}
		}
		return instance;
	}

	public void run() {	
		try{
			while (running) {
				if(log.isInfoEnabled()){
					log.info("RedisMonitor start monitor! and current redis status is:{}",
								masterOrSlave);
				}
				try {
					Thread.sleep(interval);
					JedisPool masterPool = getMasterPool(masterOrSlave);
					if(redisHeartbeat(masterPool)){
						Jedis jedis = getJedis(masterPool);
						if(jedis!=null){
							String role = getRedisInfo(jedis);
							if (!MASTER.equalsIgnoreCase(role.substring(
									role.indexOf("role:") + 5, role
											.indexOf("role:") + 6))) {
								jedis.slaveofNoOne();								
							}
						}
						masterPool.returnResource(jedis);
						masterCount = 0;
						MASTER_CONSECUTIVE_NUM_TO_ALERT=0;
					}else{
						masterCount++;
					}
					JedisPool slavePool = getSlavePool(masterOrSlave);					
					if(redisHeartbeat(slavePool)){
						Jedis jedis = getJedis(slavePool);
						if(jedis!=null){
							String role = getRedisInfo(jedis);
							if(masterCount==0){
								if (MASTER.equalsIgnoreCase(role.substring(
										role.indexOf("role:") + 5, role
												.indexOf("role:") + 6))) {
									if(redisHeartbeat(getMasterPool(masterOrSlave))){
										if(MASTER.equalsIgnoreCase(masterOrSlave)){
											jedis.slaveof(master_host, master_port);
										}else{
											jedis.slaveof(slave_host, slave_port);
										}
										
									}
									
								}
							}							
							
							slavePool.returnResource(jedis);
							slaveCount = 0;
							SLAVE_CONSECUTIVE_NUM_TO_ALERT=0;
						}else{
							slaveCount++;
						}
						
					}else{
						slaveCount++;
					}					
					if (masterCount >=redis_error_Connection_count) {
						log.error("master redis is down! ");
						if(SLAVE.equalsIgnoreCase(masterOrSlave)){							
							sendRedisAlertMail(slave_host, slave_port,masterCount,MASTER);
						}else{							
							sendRedisAlertMail(master_host, master_port,masterCount,MASTER);
						}
						JedisPool slaveToMaster = getSlavePool(masterOrSlave);											
							Jedis jedis = getJedis(slaveToMaster);
							if(jedis!=null){
								String role = getRedisInfo(jedis);
								if (!MASTER.equalsIgnoreCase(role.substring(
										role.indexOf("role:") + 5, role
												.indexOf("role:") + 6))) {
									jedis.slaveofNoOne();
								   }
								//jedis.disconnect();
								slaveToMaster.returnResource(jedis);
								MASTER_CONSECUTIVE_NUM_TO_ALERT=0;
								if(SLAVE.equalsIgnoreCase(masterOrSlave)){
									writeFile(MASTER);
									masterOrSlave = MASTER;	
								}else{
									writeFile(SLAVE);
									masterOrSlave = SLAVE;									
								}
								notifyRedisStatus(masterOrSlave);
						       }							
						// initialed = true;
					}
					if (slaveCount >= redis_error_Connection_count) {
						if (SLAVE.equalsIgnoreCase(masterOrSlave)) {
							sendRedisAlertMail(master_host, master_port,slaveCount,SLAVE);
						} else {
							sendRedisAlertMail(slave_host, slave_port,slaveCount,SLAVE);
						}
					}
				} catch (Exception e) {
					log.warn("Get Exception when to check redis monitor file: {}"
							,e);
				}
			}	
		}finally{
			stopMonitor();
		}
	}

	public synchronized void startMonitor() {
		running = true;
		if(isAuth){
			master_pool = new JedisPool(config,master_host,master_port,Protocol.DEFAULT_TIMEOUT,password);
			slave_pool = new JedisPool(config,slave_host,slave_port,Protocol.DEFAULT_TIMEOUT,password);
		}else{
			master_pool = new JedisPool(config,master_host,master_port);
			slave_pool = new JedisPool(config,slave_host,slave_port);
		}

		try {
			Jedis tempJedis = master_pool.getResource();
			try {
				java.util.List<String> policy = tempJedis.configGet("maxmemory-policy");
				if (policy != null && policy.size() > 1) {
					if (!"noeviction".equals(policy.get(1))) {
						log.error("Phase 1 Configuration Alert: Redis maxmemory-policy is currently set to [" + policy.get(1) + "], NOT 'noeviction'. Please alter this setting to prevent unintentional message eviction in stream mode!");
					}
				}
			} finally {
				if (tempJedis != null) {
					master_pool.returnResource(tempJedis);
				}
			}
		} catch (Exception e) {
			log.error("Failed to check maxmemory-policy for master pool during RedisMonitor start", e);
		}

		Thread thread = new Thread(this);
		thread.start();
	}

	public synchronized void stopMonitor() {
		running = false;
	}

	public void writeFile(String flag) {
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
		
		try {
			File file = new File(a2phome + DBHAConstants.REDISSTATUS_FILE_RELATIVE_PATH);
			if(!file.exists()){
				file.createNewFile();
			}
				dbprop.load(new FileInputStream(file));				
				OutputStream fos = new FileOutputStream(file);
				dbprop.setProperty(DBHAConstants.USED_KEY, flag);
				dbprop.store(fos, "Update "+DBHAConstants.USED_KEY+" to file.");			
		} catch (Exception e) {
			log.warn("Get Exception when to write file:{}", e);
		} 
	}

	public void notifyRedisStatus(String status) {
		synchronized (mutex) {
			setChanged();
			notifyObservers(status);
		}
	}

	@Override
	public int OnEvent(Event event) {
		// TODO Auto-generated method stub
		return 0;
	}
	 
	private JedisPool getMasterPool(String m){
		JedisPool pool = null;
		if(MASTER.equalsIgnoreCase(m)){			
			pool = master_pool;
		}else{
			pool = slave_pool;
		}	
		return pool;
	}
    private JedisPool getSlavePool(String m){
    	JedisPool pool = null;
		if(MASTER.equalsIgnoreCase(m)){
			pool = slave_pool;
		}else{
			pool = master_pool;
		}
		return pool;
	}
	private boolean redisHeartbeat(JedisPool pool) {			
		boolean result = false;
		//log.debug("pool is: "+pool);
		try {
			if (pool != null) {
				Jedis redis = getJedis(pool);
				if (redis != null) {
					result = (redis.isConnected()&&"PONG".equals(redis.ping()));
					//redis.disconnect();
					pool.returnResource(redis);
				}
			}
		} catch (Exception e) {
		   log.error("redis check Heartbeat connection error");
		}
		
		return result;
	}
	private Jedis getJedis(JedisPool pool) {
		//log.debug("get jedis from pool is: "+pool);
		if (pool != null) {
			Jedis redis = null;
			try {
				redis = pool.getResource();
				
				return redis;
			} catch (Exception e) {
				log.error("connection redis server error!");
			}
		}
		return null;
	}

	private String getRedisInfo(Jedis redis){
		String info = null;
		try {
			info = redis.info();			
		} catch (Exception e) {
			log.error("connection redis server error!");
		}
		return info;
	}
	public static String getMasterOrSlave() {
		return masterOrSlave;
	}

	public static void setMasterOrSlave(String masterOrSlave) {
		RedisMonitor.masterOrSlave = masterOrSlave;
	}

	private void sendRedisAlertMail(String host, int port, int count,String mOS) {
		// make alert mail content and subject
		StringBuilder mailTextBuffer = new StringBuilder(100);
		String mailSubject = null;
		String hostAddress = ModuleURI.self().getAddress();

		mailTextBuffer.append(host).append(":").append(port).append(
				" redis is down, please help to start it!");
		mailSubject = "A2P alert mail from " + hostAddress
				+ " for redisMonitor!";

		if(MASTER.equalsIgnoreCase(mOS)){
			if (count % redis_error_Connection_count != 0 || MASTER_CONSECUTIVE_NUM_TO_ALERT >= MAX_ALERT_MAIL_NUM) {
				return;
			}
			MASTER_CONSECUTIVE_NUM_TO_ALERT++;
			mailTextBuffer.append(", AlertMailNumId=").append(
					MASTER_CONSECUTIVE_NUM_TO_ALERT).append(".");
		}else{
			if (count % redis_error_Connection_count != 0 || SLAVE_CONSECUTIVE_NUM_TO_ALERT >= MAX_ALERT_MAIL_NUM) {
				return;
			}
			SLAVE_CONSECUTIVE_NUM_TO_ALERT++;
			mailTextBuffer.append(", AlertMailNumId=").append(
					SLAVE_CONSECUTIVE_NUM_TO_ALERT).append(".");
		}	
		String mailText = mailTextBuffer.toString();
		if(log.isDebugEnabled()){
			log.debug("mailText is: {}" , mailText);
		}
		MailSender.getInstance().sendAlertMail(mailSubject, mailText, null);
		return;
	}
	public boolean switchRedis(String s){
		JedisPool master = getMasterPool(s);											
		JedisPool slave = getSlavePool(s);
		Jedis sjedis = getJedis(slave);
		Jedis jedis = getJedis(master);
		if(jedis!=null){
			String role = getRedisInfo(jedis);
			if (!MASTER.equalsIgnoreCase(role.substring(
					role.indexOf("role:") + 5, role
							.indexOf("role:") + 6))) {
				jedis.slaveofNoOne();
			   }
			//jedis.disconnect();		
				if(redisHeartbeat(slave)){
					if(MASTER.equalsIgnoreCase(s)){
						sjedis.slaveof(master_host, master_port);
					}else{
						sjedis.slaveof(slave_host, slave_port);
					}									
			}
			master.returnResource(jedis);			
			if(MASTER.equalsIgnoreCase(s)){
				writeFile(MASTER);
				masterOrSlave = MASTER;	
			}else{
				writeFile(SLAVE);
				masterOrSlave = SLAVE;									
			}
			notifyRedisStatus(s);
			if(log.isInfoEnabled()){
				log.info("redis switch to {} success!",s);
			}
			
			return true;
	       }else{
	    	   log.warn("redis switch to {} error!",s);
	       }
		return false;
	}
}
