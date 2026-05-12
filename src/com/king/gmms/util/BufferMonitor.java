package com.king.gmms.util;

import com.king.message.gmms.GmmsMessage;
import com.king.framework.SystemLogger;
import com.king.gmms.threadpool.impl.BufferMonitorManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BufferMonitor {
    private static SystemLogger log = SystemLogger.getSystemLogger(BufferMonitor.class);
    
    // 使用 volatile 保证替换时的内存可见性
    protected volatile Map<Object, GmmsMessage> oneBuffer;
    protected volatile Map<Object, GmmsMessage> twoBuffer;
    protected volatile Map<Object, GmmsMessage> threeBuffer;
    protected volatile Map<Object, GmmsMessage> fourBuffer;
    
    // 【优化：替补回收槽】不加 volatile，因为它仅限内部定时器在后台单线程自己轮转倒腾
    private Map<Object, GmmsMessage> reserveBuffer;

    // 【优化：O(1) 极速容量护盾】代替昂贵的 ConcurrentHashMap 跨段 size 统计
    private AtomicInteger totalItemCount = new AtomicInteger(0);

    private BufferTimeoutInterface listener;
    private long timeout;
    private long waitTime;
    private int bufferCapacity;
    protected AtomicBoolean isAlow = new AtomicBoolean(true);
    private Object waitObj = new Object();

    protected UUID uuid = UUID.randomUUID();
    protected String bufferName;
    protected BufferMonitorManager bufferMonitorManager;

    public BufferMonitor() {
        this(1000);
    }

    public BufferMonitor(int bufferCapacity) {
        if (bufferCapacity < 0) {
            throw new IllegalArgumentException("Capacity must be bigger than 0!");
        }
        this.bufferCapacity = bufferCapacity;

        // 一次性初始化所有的 5 个环形复用槽！系统启动后永不再发生 New 动作！
        this.oneBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
        this.twoBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
        this.threeBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
        this.fourBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
        this.reserveBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
        
        setTimeout(90, SECONDS);
        setWaitTime(50, MILLISECONDS);
        bufferMonitorManager = BufferMonitorManager.getInstance();
    }

    private void tryWait() {
        try {
            synchronized (waitObj) {
                waitObj.wait();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
        }
    }

    private void notifySpaceAvailable() {
        if (bufferCapacity > 0 && totalItemCount.get() < bufferCapacity) {
            synchronized (waitObj) {
                waitObj.notifyAll();
            }
        }
    }

    public void setListener(BufferTimeoutInterface listener) {
        this.listener = listener;
    }

    // 极限入库
    public boolean put(Object key, GmmsMessage gmmsMessage) {
        if (key == null || gmmsMessage == null) return false;

        // 【优化】极速水位探测，只获取整型探测 O(1)，无惧一秒内数万次的探查
        if (bufferCapacity > 0 && totalItemCount.get() >= bufferCapacity) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug(gmmsMessage, "start Put a message to {}, its size:{}, Message type:{}, Message key:{}, maxCap:{}", 
                        bufferName, totalItemCount.get(), gmmsMessage.getMessageType(), key, bufferCapacity);
                }
                // 使用 waitObj 代替 Thread.sleep(50)，当有空间释放时立即唤醒，大幅提升排队效率
                synchronized (waitObj) {
                    if (totalItemCount.get() >= bufferCapacity) {
                        waitObj.wait(50);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn(gmmsMessage, e, e);
            }
            if (bufferCapacity > 0 && totalItemCount.get() >= bufferCapacity) {
                return false; 
            }
        }

        try {
            // put 方法如果返回 null 代表是一条新数据加入，自增一
            if (oneBuffer.put(key, gmmsMessage) == null) {
                totalItemCount.incrementAndGet();
            }
        } catch (Exception e) {
            log.warn(e, e);
        } 
        return true;
    }

    // 极限提取
    public GmmsMessage remove(Object key) {
        if (!isAlow.get()) {
            tryWait();
            return null;
        }
        GmmsMessage msg = null;

        try {
            // 从哪层找到踢出都记得让大水位器减一，时刻保证纯精准
            msg = oneBuffer.remove(key);
            if (msg != null) {
                totalItemCount.decrementAndGet();
                notifySpaceAvailable();
                return msg;
            }

            msg = twoBuffer.remove(key);
            if (msg != null) {
                totalItemCount.decrementAndGet();
                notifySpaceAvailable();
                return msg;
            }

            msg = threeBuffer.remove(key);
            if (msg != null) {
                totalItemCount.decrementAndGet();
                notifySpaceAvailable();
                return msg;
            }
            
        } catch (Exception e) {
            log.warn(e, e);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug(msg, "Remove message from {}", bufferName);
            }
        }
        return msg;
    }

    public GmmsMessage get(Object key) {
        if (!isAlow.get()) return null;
        
        GmmsMessage msg = oneBuffer.get(key);
        if (msg != null) return msg;

        msg = twoBuffer.get(key);
        if (msg != null) return msg;

        msg = threeBuffer.get(key);
        return msg;
    }

    // 后台清理员：纯 0 垃圾架构（0 GC）
    public void checkTimeoutProcess() {
        Map<Object, GmmsMessage> timedOutBuffer;

        try {
            timedOutBuffer = fourBuffer; 
            
            fourBuffer = threeBuffer;
            threeBuffer = twoBuffer;
            twoBuffer = oneBuffer;
            
            // 【神级优化】: 直接让干净的替补席顶上即可！全程不再创建任何对象
            oneBuffer = reserveBuffer;
            
        } catch (Exception e) {
            log.warn(e, e);
            timedOutBuffer = new ConcurrentHashMap<>();
        } 

        int expiredCount = 0;
        for (Map.Entry<Object, GmmsMessage> entry : timedOutBuffer.entrySet()) {
            expiredCount++;
            if (log.isInfoEnabled()) {
                log.info(entry.getValue(), "timeout in {}, listener is {}", bufferName, listener.getClass().getSimpleName());
            }
            listener.timeout(entry.getKey(), entry.getValue());
        }
        
        // 集中一次性把死去的尸体全部在总容量器上扣除
        if (expiredCount > 0) {
            totalItemCount.addAndGet(-expiredCount);
            notifySpaceAvailable();
        }

        timedOutBuffer.clear(); // 把过期队列彻底冲洗干净
        
        // 洗干净后的遗体，充当下一次的替补席（形成宇宙级自给自足闭环）
        reserveBuffer = timedOutBuffer; 
    }

    public synchronized void startMonitor(String bufferName) {
        this.bufferName = bufferName;
        bufferMonitorManager.register(this);
    }

    public synchronized void stopMonitor() {
        bufferMonitorManager.deregister(this);
    }

    public void sendbackBuffer() {
        backupBuffer(oneBuffer);
        backupBuffer(twoBuffer);
        backupBuffer(threeBuffer);
        backupBuffer(fourBuffer);
    }

    private void backupBuffer(Map<Object, GmmsMessage> buffer) {
        for (Map.Entry<Object, GmmsMessage> entry : buffer.entrySet()) {
            if (log.isInfoEnabled()) {
                log.info(entry.getValue(), "{} backup message!", bufferName);
            }
            listener.timeout(entry.getKey(), entry.getValue());
        }
    }

    public Set<GmmsMessage> getAll() {
        if (!isAlow.get()) return new HashSet<>();

        HashSet<GmmsMessage> result = new HashSet<GmmsMessage>();
        result.addAll(oneBuffer.values());
        result.addAll(twoBuffer.values());
        result.addAll(threeBuffer.values());
        result.addAll(fourBuffer.values());
        return result;
    }

    public void clear() {
        oneBuffer.clear();
        twoBuffer.clear();
        threeBuffer.clear();
        fourBuffer.clear();
        // 清理总容量计数器
        totalItemCount.set(0); 
        notifySpaceAvailable();
    }

    public int size() {
        // 使用计数器，消除昂贵的 map map 内部长度收集
        return Math.max(0, totalItemCount.get()); 
    }

    public boolean isFull() {
        if (bufferCapacity <= 0) return false;
        return totalItemCount.get() >= bufferCapacity;
    }

    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeout, MILLISECONDS);
    }

    public void setTimeout(long timeout, TimeUnit timeUnit) {
        if (timeout <= 0)  throw new IllegalArgumentException("Timeout > 0, but now it's:" + timeout);
        this.timeout = MILLISECONDS.convert(timeout, timeUnit);
    }

    public long getWaitTime(TimeUnit timeUnit) {
        return timeUnit.convert(waitTime, MILLISECONDS);
    }

    public void setWaitTime(long waitTime, TimeUnit timeUnit) {
        if (waitTime < 0) throw new IllegalArgumentException("WaitTime >=0, but now it's:" + waitTime);
        this.waitTime = MILLISECONDS.convert(waitTime, timeUnit);
    }
    
    public String toString() {
        return "BufferMonitor [bufferName=" + bufferName + ", currentTotalSize=" + size() + "]";
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getBufferName() {
        return bufferName;
    }

    public void setBufferName(String bufferName) {
        this.bufferName = bufferName;
    }
}
