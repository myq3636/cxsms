package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.LifecycleListener;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class EventFactory {
    
    private static EventFactory instance = new EventFactory();
    private int stopLevels = 10;
    private HashMap<Integer, List<LifecycleListener>[]> listenerMap = new HashMap<Integer, List<LifecycleListener>[]>();
    private EventFactory() {
        super();
        init();
    }
    public void init()
    {
        listenerMap.put(Event.TYPE_BLACKLIST_RELOAD, new List[stopLevels]);
        listenerMap.put(Event.TYPE_SHUTDOWN,new List[stopLevels]);
        listenerMap.put(Event.TYPE_ROUTINFO_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_CUSTOMER_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_ANTISPAM_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_CONTENT_TEMPLATE_RELOAD, new List[stopLevels]);
        listenerMap.put(Event.TYPE_SWITCHDB,new List[stopLevels]);
        listenerMap.put(Event.TYPE_SWITCHREDIS,new List[stopLevels]);
        listenerMap.put(Event.TYPE_SWITCHDNS,new List[stopLevels]);
        listenerMap.put(Event.TYPE_PHONEPREFIX_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_WHITELIST_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_RECEIPIENT_RULE_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_SENDER_WHITELIST_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_SENDER_BLACKLIST_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_RECIPIENT_BLACKLIST_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_CONTENT_WHITELIST_RELOAD,new List[stopLevels]);
        listenerMap.put(Event.TYPE_CONTENT_BLACKLIST_RELOAD,new List[stopLevels]);
    }
    public static EventFactory getInstance()
    {
        return instance;
    }
    public Event newEvent(int eventType) {
    	if (eventType==Event.TYPE_CUSTOMER_RELOAD)
        {
            Event event=new ReloadCustomerEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
    	if (eventType==Event.TYPE_CONTENT_TEMPLATE_RELOAD)
        {
            Event event=new ReloadContentTplEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
    	if(eventType == Event.TYPE_SWITCHDB){
             Event event = new SwitchDBEvent();
             event.setLifecycleListener(listenerMap.get(eventType));
             return event;
        }
        if(eventType == Event.TYPE_SWITCHREDIS){
             Event event = new SwitchRedisEvent();
             event.setLifecycleListener(listenerMap.get(eventType));
             return event;
        }
        if (eventType==Event.TYPE_BLACKLIST_RELOAD)
        {
            Event event=new ReloadBlacklistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
        
        if(eventType == Event.TYPE_ROUTINFO_RELOAD){
        	Event event = new ReloadRoutingInfoEvent();
        	event.setLifecycleListener(listenerMap.get(eventType));
        	return event;
        }
        
        if(eventType == Event.TYPE_ANTISPAM_RELOAD){
        	Event event = new ReloadAntiSpamEvent();
        	event.setLifecycleListener(listenerMap.get(eventType));
        	return event;
        }
        
        if(eventType == Event.TYPE_SHUTDOWN){
            Event event = new ShutdownEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
        if(eventType == Event.TYPE_SWITCHDNS){
            Event event = new SwitchDNSEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
        if(eventType == Event.TYPE_WHITELIST_RELOAD){
            Event event = new ReloadWhitelistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }
        if (eventType==Event.TYPE_PHONEPREFIX_RELOAD)
        {
            Event event=new ReloadPhonePrefixEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 
        if (eventType==Event.TYPE_RECEIPIENT_RULE_RELOAD)
        {
            Event event=new ReloadRecipientRuleEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 

        
        if (eventType==Event.TYPE_SENDER_WHITELIST_RELOAD)
        {
            Event event=new ReloadSenderWhitelistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 
        if (eventType==Event.TYPE_SENDER_BLACKLIST_RELOAD)
        {
            Event event=new ReloadSenderBlacklistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 
        if (eventType==Event.TYPE_CONTENT_WHITELIST_RELOAD)
        {
            Event event=new ReloadContentWhitelistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 
        if (eventType==Event.TYPE_CONTENT_BLACKLIST_RELOAD)
        {
            Event event=new ReloadContentBlacklistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        }; 
        if (eventType==Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD)
        {
            Event event=new ReloadVendorContentReplacementEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        };
        if (eventType==Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD)
        {
            Event event=new ReloadVendorReplacementEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        };
        if (eventType==Event.TYPE_RECIPIENT_BLACKLIST_RELOAD)
        {
            Event event=new ReloadRecipientBlacklistEvent();
            event.setLifecycleListener(listenerMap.get(eventType));
            return event;
        };
        
        return null;
    }

    public int getStopLevels()
    {
        return this.stopLevels;
    }

    public void addListener(int eventType, LifecycleListener listener, int p) {
        List[] eventListener = listenerMap.get(eventType);

        if (eventListener == null)
            return;

        if (p > this.stopLevels) {
            p = stopLevels; ;
        }
        if (p < 0) {
            p = 0;
        }
        if (eventListener[p]==null)
            eventListener[p]=new ArrayList();
        eventListener[p].add(listener);

    }

}
