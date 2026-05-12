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
public class ShutdownEvent
    extends Event {


    public ShutdownEvent(){
        super(Event.TYPE_SHUTDOWN);
    }
    /**
     * event subtype commonly is the arguments following command.
     *
     * @param cmd String[] / // public void parseSubType(String []args) // {
     *   // if (args.length<2) // { // this.eventSubType=0; // } // else if
     *   (this.eventType==Event.STOP) // { //
     *   if(args[1].equalsIgnoreCase("file")) // { //
     *   this.eventSubType=Event.SUBTYPE_STOP_FILE; // } // else // { //
     *   this.eventSubType=Event.SUBTYPE_STOP_DB; // } // } // }
     * @return boolean
     */
    public boolean parseArgs(SystemCommand cmd) {
        return false;
    }
}
