package com.king.framework.lifecycle;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.concurrent.atomic.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.A2PThreadGroup;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.GmmsUtility;

/**
 * Lifecycle logic implmentation class.
 * Reposible for add listener, notify listeners.
 * @author not attributable
 * @version 1.0
 */
public class LifecycleSupport {
    private static Logger log = LogManager.getLogger(LifecycleSupport.class);
    public static final int MODULE_STOP_TIMEOUT=10000;
    private static final String Notify_TIMEOUT = "LifecycleSupport.NotifyTimeOut";
    private int notifyTimeOut = 10000;
    private EventFactory factory=EventFactory.getInstance();
//    private final List waitingThreads=new LinkedList();
    // H-22: 移除共享 notifyNum —— 改为每级独立 CountDownLatch(见 notifyLevelAndWait)，
    // 避免跨优先级计数泄漏破坏关停屏障。

    public LifecycleSupport() {
        notifyTimeOut = Integer.parseInt(GmmsUtility.getInstance().
                                         getCommonProperty(Notify_TIMEOUT, "10")) *
            1000;
    }
    /**
     * add one lifecycle listener.
     *
     * @param listener LifecycleListener
     * @param priority int -- the order of stopping this listener.
     */

    public void addListener(int eventType, LifecycleListener listener, int priority)
    {
        factory.addListener(eventType,listener,priority);
    }

    /**
     * notify a event
     * @param event Event
     */
    public int notify(Event event)
    {
        switch (event.getEventType())
        {
        case Event.TYPE_INVAILED:
        {
            return -1;
        }
        case Event.TYPE_BLACKLIST_RELOAD:
        case Event.TYPE_WHITELIST_RELOAD:
        case Event.TYPE_ROUTINFO_RELOAD:
        case Event.TYPE_ANTISPAM_RELOAD:
        //case Event.TYPE_PHONEPREFIX_RELOAD:
        case Event.TYPE_CONTENT_TEMPLATE_RELOAD:
        case Event.TYPE_GMMS_CONFIG_RELOAD:
        {
            return notifyAllSync(event);

        }
        case Event.TYPE_SHUTDOWN:
        {
            return notifyStop(event);
        }
        
	    case Event.TYPE_SWITCHDNS:
	    {
	        return notifySwitchDNS(event);
	    }
        default:
        {
            notifyAll(event);
            return 0;
        }
        }
    }
    private int syncNotify(Event event, LifecycleListener listener)
    {
        return listener.OnEvent(event);
    }

    /**
     * notity stop event.
     * listeners will noticed by order(priority). In same priority, the
     * notification will be asynchronized. Only after the higher order
     * notification is finished, the next level notification will start.
     * @param event Event
     */
    private int notifySwitchDNS(Event event) {
        List[] listeners=event.getListeners();
        for (int i=0;i<listeners.length;i++)
        {
        	if(listeners[i] == null){
            	break;
            }
            log.info("listeners level "+i+"size is:"+listeners[i].size());
            notifyLevelAndWait(event, listeners[i], notifyTimeOut);
            log.info("level:"+i+",notify over! timeout is: "+notifyTimeOut);
        }
        return 0;
    }

    
    
    private int notifyAllSync(Event event) {
        int rtnval = 0;
        List[] listeners = event.getListeners();
        for (int i = 0; i < listeners.length; i++) {
            if (listeners[i]==null) continue;
            Iterator itListener = listeners[i].listIterator();
            while (itListener.hasNext()) {
                LifecycleListener listener = (LifecycleListener) itListener.
                    next();
                int temp=syncNotify(event, listener);
                rtnval=(temp==0 ? rtnval : temp);
            }
        }
        return rtnval;
    }
    /**
     * notify all listeners asynchronized.
     * @param event Event
     */
    private void notifyAll(Event event)
    {
        int rtnval=0;
        List[] listeners=event.getListeners();
        if(listeners==null){
        	log.warn("Empty listeners for this event {}.",event.getEventType());
        	return;
        }
        for (int i=0; i<listeners.length; i++)
        {
        	if(listeners[i]==null){
        		continue;
        	}
            Iterator itListener = listeners[i].listIterator();
            while (itListener.hasNext()) {
                LifecycleListener listener = (LifecycleListener) itListener.
                                             next();
                notifyAsync(event, listener);
            }
        }
    }

    /**
     * notity stop event.
     * listeners will noticed by order(priority). In same priority, the
     * notification will be asynchronized. Only after the higher order
     * notification is finished, the next level notification will start.
     * @param event Event
     */
    private int notifyStop(Event event) {
        List[] listeners=event.getListeners();
        for (int i=0;i<listeners.length;i++)
        {
            if(listeners[i] == null) {
            	continue;
            }
            log.info("listeners level {} size is:{}",i,listeners[i].size());
            notifyLevelAndWait(event, listeners[i], notifyTimeOut);
            log.info("level:{} notify over! timeout is: {}",i,notifyTimeOut);

        }
        return 0;
    }
    /**
     * asynchronized notify
     * @param event Event
     * @param listener LifecycleListener
     */
    public void notifyAsync(final Event event, final LifecycleListener listener) {
        // H-22: fire-and-forget 异步通知(用于非屏障的 notifyAll)，守护线程 + 吞异常不影响调用方。
        Thread notifyThread=new Thread(A2PThreadGroup.getInstance(),"Notify Thread"){
            public void run()
            {
                try {
                    listener.OnEvent(event);
                } catch (Throwable t) {
                    log.warn("Lifecycle listener OnEvent error.", t);
                }
            }
        };
        notifyThread.setDaemon(true);
        notifyThread.start();
    }

    /**
     * H-22: 通知本级所有 listener 并等待完成(或超时)。
     * 每级独立 CountDownLatch，countDown 放 finally —— listener 抛异常也能归零(不再每级干等满 timeout)，
     * 且不会被其它级别的残留线程污染计数(消除跨级屏障破坏)。线程为守护线程。
     * @param event 事件
     * @param levelListeners 本优先级的 listener 列表
     * @param timeout 本级等待超时(ms)
     */
    private void notifyLevelAndWait(final Event event, List levelListeners, int timeout)
    {
        if (levelListeners == null || levelListeners.isEmpty()) {
            return;
        }
        final java.util.concurrent.CountDownLatch latch =
                new java.util.concurrent.CountDownLatch(levelListeners.size());
        Iterator itLevel = levelListeners.listIterator();
        while (itLevel.hasNext())
        {
            final LifecycleListener listener=(LifecycleListener)itLevel.next();
            Thread notifyThread=new Thread(A2PThreadGroup.getInstance(),"Notify Thread"){
                public void run()
                {
                    try {
                        listener.OnEvent(event);
                    } catch (Throwable t) {
                        log.warn("Lifecycle listener OnEvent error.", t);
                    } finally {
                        latch.countDown();
                    }
                }
            };
            notifyThread.setDaemon(true);
            notifyThread.start();
        }
        try
        {
            boolean done = latch.await(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!done)
            {
                log.warn("Lifecycle notify level timed out after {} ms, remaining listeners: {}",
                        timeout, latch.getCount());
            }
        }
        catch (InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            log.warn("Notify Thread interrupted.");
        }
    }

}
