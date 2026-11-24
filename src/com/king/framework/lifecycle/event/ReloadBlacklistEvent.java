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
public class ReloadBlacklistEvent
    extends Event {

    public ReloadBlacklistEvent() {
        super(Event.TYPE_BLACKLIST_RELOAD);
    }

}
