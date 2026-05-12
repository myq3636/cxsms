package com.king.gmms;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;
import com.king.framework.SystemLogger;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import java.util.concurrent.Semaphore;

public class GuavaCache {
	private SystemLogger log = SystemLogger
			.getSystemLogger(GuavaCache.class);
	private ExecutorService es = Executors.newSingleThreadExecutor();
	private RemovalListener<String, GmmsMessage> async = RemovalListeners.asynchronous(new MyRemovalListener(), es);
	private BufferTimeoutInterface listener; 
	private long timeout=60*1000;
	private int bufferCapacity=100;
	private Semaphore permit;
	public GuavaCache(long timeout, int bufferCapacity, BufferTimeoutInterface listener) {
		log.debug("timeout={},bufferCapacity={},listener={}", timeout, bufferCapacity, listener);
		this.timeout = timeout;
		this.bufferCapacity = bufferCapacity;
		this.listener = listener;
		this.permit = new Semaphore(bufferCapacity);
	}
	private class MyRemovalListener implements RemovalListener<String, GmmsMessage> {  
        @Override  
        public void onRemoval(RemovalNotification<String, GmmsMessage> notification) {  
            // 无论因为什么原因被移出缓存（过期、被覆盖、主动删除），都要释放一个信号以供新数据进入
            permit.release();

            String cause = String.valueOf(notification.getCause());
            String key = String.valueOf(notification.getKey());
            // 如果是因为过期清理，触发回调业务去补发或设为异常状态
            if("EXPIRED".equals(cause)) {
            	listener.timeout(key, notification.getValue());             	
            }
        }  
    }
	  public Cache<String, GmmsMessage> cacheConnection = CacheBuilder.newBuilder()  
	      //设置cache的初始大小为20000，要合理设置该值  
	      .initialCapacity(bufferCapacity)  
	      .maximumSize(bufferCapacity)
	      //设置并发数为5，即同一时间最多只能有5个线程往cache执行写入操作  
	      .concurrencyLevel(100)  
	      //设置cache中的数据在600秒没有被读写将自动删除  
	      .expireAfterWrite(timeout, TimeUnit.MILLISECONDS) 
	      //设置监听，当出现自动删除时的回调
	      .removalListener(async)
	      //构建cache实例  
	      .build();  
	  
	  public GmmsMessage get(String key)  {
	    try {	    	
	    	GmmsMessage var = cacheConnection.getIfPresent(key);	    	
	    	cacheConnection.invalidate(key);
	        return var;
	    } catch (Exception e) {	      
	      return null;
	    }
	  }
	     
	   public boolean put(String key, GmmsMessage value) {
		 try {
		     // 循环尝试获取许可，如果是满状态，强制 Guava 保养清理以踢掉过期对象来腾出空间
		     // 这比 BufferMonitor 里面的 sleep 更高效，实现了完全一致的背压 (Backpressure) 流控
		     while (!permit.tryAcquire(50, TimeUnit.MILLISECONDS)) {
		         cacheConnection.cleanUp();
		     }
		 } catch (InterruptedException e) {
		     return false;
		 }
	     cacheConnection.put(key, value);
	     return true;
	   }
	   
	   public void remove(String key) {
		     cacheConnection.invalidate(key);
	 }
	   
	 public void shutdown() {
		     cacheConnection.cleanUp();
		     es.shutdown();
	 }
	   
	   
	   
	public BufferTimeoutInterface getListener() {
		return listener;
	}

	public void setListener(BufferTimeoutInterface listener) {
		this.listener = listener;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getBufferCapacity() {
		return bufferCapacity;
	}

	public void setBufferCapacity(int bufferCapacity) {
		this.bufferCapacity = bufferCapacity;
	}

	public static void main(String[] args) throws Exception {
		
		/*
		 * GuavaCache cache = new GuavaCache(60000,10000); //GmmsMessage msg = new
		 * GmmsMessage(); cache.put("1", "3"); // Thread.sleep(61000); cache.put("2",
		 * "3");
		 */
		  long time =System.currentTimeMillis(); 
		  //System.out.println("get 1 cost:" +(System.currentTimeMillis()-time)+ cache.get("1"));
		  //Thread.sleep(61000);
		  //cache.shutdown();
		  //cache.putCache("2", "3"); System.out.println("put 2 cost:"+(System.currentTimeMillis()-time)); cache.shutdown();
		 
	   }
	 
	}

