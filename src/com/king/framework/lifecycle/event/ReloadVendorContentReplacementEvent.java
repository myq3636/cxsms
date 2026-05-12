package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

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
public class ReloadVendorContentReplacementEvent
    extends Event {

    public ReloadVendorContentReplacementEvent() {
        super(Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD);
    }

    public boolean parseArgs(SystemCommand cmd) {
        return true;
    }
}
