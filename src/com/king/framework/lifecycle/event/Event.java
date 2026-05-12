package com.king.framework.lifecycle.event;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.cmd.SystemCommand;

/**
 * Lifecycle event
 *
 * @author not attributable
 * @version 1.0
 */
public abstract class Event {
    //event type
    public static final int TYPE_INVAILED = -1;
    public static final int TYPE_BLACKLIST_RELOAD=1;
    public static final int TYPE_SHUTDOWN = 2;
    
    public static final int TYPE_ROUTINFO_RELOAD = 3;
    public static final int TYPE_ANTISPAM_RELOAD = 4;
    public static final int TYPE_CONTENT_TEMPLATE_RELOAD = 5;
    public static final int TYPE_SWITCHDB=6;
    public static final int TYPE_SWITCHREDIS=7;
    public static final int TYPE_CUSTOMER_RELOAD = 8;
    public static final int TYPE_SWITCHDNS = 9;//---
    public static final int TYPE_PHONEPREFIX_RELOAD = 10;//---
    public static final int TYPE_WHITELIST_RELOAD=11;
    public static final int TYPE_RECEIPIENT_RULE_RELOAD=13;
    public static final int TYPE_SENDER_WHITELIST_RELOAD=14;
    public static final int TYPE_SENDER_BLACKLIST_RELOAD=15;
    public static final int TYPE_CONTENT_WHITELIST_RELOAD=16;
    public static final int TYPE_CONTENT_BLACKLIST_RELOAD=17;
    public static final int TYPE_RECIPIENT_BLACKLIST_RELOAD=18;
    public static final int TYPE_VENDOR_CONTENT_REPLACE__RELOAD=19;
    public static final int TYPE_SYSTEM_VENDOR_REPLACE_RELOAD=20;
    
    // event sub type
    public static final int SUBTYPE_STOP_FILE=1;
    public static final int SUBTYPE_STOP_DB=2;
   
    public static final int SUBTYPE_SWITCH_MASTER=1;
    public static final int SUBTYPE_SWITCH_SLAVE=2;
    
    public static final int SUBTYPE_MONITORSTART_SIMPLE=1;
    public static final int SUBTYPE_MONITORSTART_DETAIL=2;
    public static final int SUBTYPE_CUSTOMER_LOAD=1;
    public static final int SUBTYPE_CUSTOMER_ACTIVE=2;
    
    public static final int SUBTYPE_SWITCHDNS_MASTER=1;
    public static final int SUBTYPE_SWITCHDNS_SLAVE=2;
    
    
    
    private int eventType = TYPE_INVAILED;
    //Event specific value.
    private int eventSubType = 0;
    protected int stopLevels=10;
    protected Object[] args=null;
    private List<LifecycleListener> []listeners=new List[stopLevels];

    public Event(int eventType) {
        this.eventType = eventType;
        for (int i = 0; i < listeners.length; i++) {
            listeners[i] = new LinkedList();
        }
    }
    public void setLifecycleListener(List<LifecycleListener>[] listeners)
    {
        this.listeners=listeners;
    }
    public void setArgs(Object []args)
    {
        this.args=args;
    }
    public Object[] getArgs()
    {
        return args;
    }

    /**
     * get the account of listeners registered.
     * @return int
     */
    public int getListenerSize()
    {
        int size=0;
        for (int i=0; i<listeners.length; i++)
        {
            size+=listeners[i].size();
        }
        return size;
    }
    public List[] getListeners()
    {
        return listeners;
    }

    /**
     * Debug method.
     * print all listeners
     */
    public void printListeners()
    {
        for (int i=0; i<listeners.length; i++)
        {
            Iterator itListener = listeners[i].listIterator();
            if (itListener.hasNext())
            {
                System.out.println("----------------------------  Listeners Level "+i+" -----------------------");
            }
            while (itListener.hasNext()) {
                LifecycleListener listener = (LifecycleListener) itListener.
                                             next();
                System.out.println(listener.getClass().getName());
            }
        }
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println();

    }
    public int getEventType() {
        return eventType;
    }

    public void setEventType(int type) {
        eventType = type;
    }
    public void setEventSubType(int subType)
    {
        this.eventSubType=subType;
    }
    public int getEventSubType()
    {
        return eventSubType;
    }
    /**
     * event subtype commonly is the arguments following command. eg.
     * GEMD> stop file  -- stop service and write back msg queue to file
     * @param args String[]
     */

    public abstract boolean parseArgs(SystemCommand cmd);
}
