package com.king.framework.lifecycle.event;


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
public class ReloadSenderBlacklistEvent
    extends Event {

    public ReloadSenderBlacklistEvent() {
        super(Event.TYPE_SENDER_BLACKLIST_RELOAD);
    }

}
