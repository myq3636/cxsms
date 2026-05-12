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
    private final AtomicInteger notifyNum = new AtomicInteger(0);

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
            notifyNum.set(listeners[i].size());
            log.info("listeners level "+i+"size is:"+listeners[i].size());
            Iterator itLevel = listeners[i].listIterator();

            while (itLevel.hasNext())
            {
                LifecycleListener listener=(LifecycleListener)itLevel.next();
                singleNotify(event,listener);
            }
            waitNotify(notifyTimeOut);
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
                singleNotify(event, listener);
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
            notifyNum.set(listeners[i].size());
            log.info("listeners level {} size is:{}",i,listeners[i].size());
            Iterator itLevel = listeners[i].listIterator();

            while (itLevel.hasNext())
            {
                LifecycleListener listener=(LifecycleListener)itLevel.next();
                singleNotify(event,listener);
            }
            waitNotify(notifyTimeOut);
            log.info("level:{} notify over! timeout is: {}",i,notifyTimeOut);

        }
        return 0;
    }
    /**
     * asynchronized notify
     * @param event Event
     * @param listener LifecycleListener
     */
    public void singleNotify(final Event event, final LifecycleListener listener) {

        Thread notifyThread=new Thread(A2PThreadGroup.getInstance(),"Notify Thread"){
            public void run()
            {
                listener.OnEvent(event);
                notifyNum.getAndDecrement();
            }
        };
        notifyThread.start();
    }
    /**
     * waiting finish of notification in current level
     * @param timeout int
     */
    private void waitNotify(int timeout)
    {

        int interval=1000;
        int timeCost=0;
        while (timeCost<timeout && notifyNum.get()>0)
        {
            timeCost+=interval;
            try
            {
                Thread.sleep(interval);
            }
            catch (InterruptedException ex)
            {
                log.warn("Notify Thread interrupted.");
            }
        }
//        if (waitingThreads.size()>0)
//        {
//            Iterator itWait=waitingThreads.listIterator();
//            while (itWait.hasNext())
//            {
//                Thread waitTh=(Thread)itWait.next();
//                waitTh.interrupt();
//            }
//        }
    }

}
/**
 * Notify Thread
 * @author not attributable
 * @version 1.0
 */
class NotifyThread extends Thread
{
    private Event event;
    private LifecycleListener listener;
    public NotifyThread(Event event,LifecycleListener listener, List waitThreads)
    {
        this.event=event;
        this.listener=listener;
    }
    public void run()
    {
        listener.OnEvent(event);
    }

}
