package com.king.framework.lifecycle.cmd;

import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.protocol.tcp.ByteBuffer;
import com.king.gmms.protocol.tcp.NotEnoughDataInByteBufferException;

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
public class SystemCommandSet {
	private SystemLogger log = SystemLogger
	.getSystemLogger(SystemCommandSet.class);
    public SystemCommandSet() {
    }

    public SystemCommand parseCommand(ByteBuffer cmd) {
        // reload
        int len = 0;
        try {
            len = cmd.removeInt();
            if (len != cmd.length() + 4) {
                return null;
            }
            short cmdId = cmd.removeShort();
            int seqId = cmd.removeInt();
            log.trace("CMDID: {}",cmdId);
            log.trace("seqId: {}",seqId);
            SystemCommand rtnval = null;
            ArrayList args = new ArrayList();
            if (cmdId == SystemCommandBlacklist.ID) {
                rtnval = new SystemCommandBlacklist();
            }else if(cmdId == SystemCommandCustomer.ID){
            	rtnval = new SystemCommandCustomer();
            }else if(cmdId == SystemCommandRoutingInfo.ID){
            	rtnval = new SystemCommandRoutingInfo();
            }else if(cmdId == SystemCommandAntiSpam.ID){
                rtnval = new SystemCommandAntiSpam();
            }else if(cmdId == SystemCommandStop.ID){
                rtnval = new SystemCommandStop();
            }else if(cmdId == SystemCommandContentTpl.ID){
                rtnval = new SystemCommandContentTpl();
            }else if(cmdId == SystemCommandSwitchDB.ID){
                rtnval = new SystemCommandSwitchDB();
            }else if(cmdId == SystemCommandSwitchRedis.ID){
                rtnval = new SystemCommandSwitchRedis();
            }
            else if(cmdId == SystemCommandSwitchDNS.ID){
                rtnval = new SystemCommandSwitchDNS();
                byte type=cmd.readByte();
                ArrayList list = new ArrayList<String>();
                if(type==Event.SUBTYPE_SWITCHDNS_MASTER){
                	list.add("Master");
                }else if(type==Event.SUBTYPE_SWITCHDNS_SLAVE){
                	list.add("Slave");
                }
                rtnval.setArgs(list);
            }else if(cmdId == SystemCommandPhonePrefix.ID){
            	rtnval = new SystemCommandPhonePrefix();
            }else if(cmdId == SystemCommandWhitelist.ID){
            	rtnval = new SystemCommandWhitelist();
            }else if(cmdId == SystemCommandRecipientAddressRule.ID){
            	rtnval = new SystemCommandRecipientAddressRule();
            }else if(cmdId == SystemCommandSenderBlacklist.ID){
            	rtnval = new SystemCommandSenderBlacklist();
            }else if(cmdId == SystemCommandSenderWhitelist.ID){
            	rtnval = new SystemCommandSenderWhitelist();
            }else if(cmdId == SystemCommandContentWhitelist.ID){
            	rtnval = new SystemCommandContentWhitelist();
            }else if(cmdId == SystemCommandContentBlacklist.ID){
            	rtnval = new SystemCommandContentBlacklist();
            }else if(cmdId == SystemCommandRecipientBlacklist.ID){
            	rtnval = new SystemCommandRecipientBlacklist();
            }else if(cmdId == SystemCommandVendorContentReplacement.ID){
            	rtnval = new SystemCommandVendorContentReplacement();
            }else if(cmdId == SystemCommandVendorReplacement.ID){
            	rtnval = new SystemCommandVendorReplacement();
            }
            if(rtnval != null){
                rtnval.setCmdId(cmdId);
                rtnval.setSeqId(seqId);
            }
            int currentlen = cmd.length();
            if(currentlen>0){
            	rtnval.parseArgs(cmd);
            }
            return rtnval;
        }catch (NotEnoughDataInByteBufferException ex) {
            log.error(ex,ex);
        }
        return null;

    }

}
