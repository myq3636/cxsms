/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.routing.nmg;

import java.net.UnknownHostException;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.protocol.udp.nmg.CommandNumberApplication;
import com.king.gmms.protocol.udp.nmg.CommandNumberQuery;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class NMGClient {
    private static SystemLogger log = SystemLogger.getSystemLogger(NMGClient.class);
    private NMGResolver resolver;
    /**
     * Constructor
     */
    public NMGClient() {
        GmmsUtility gmmsUtility = GmmsUtility.getInstance();
        try {
            if(gmmsUtility.getCommonProperty("NMGAddress") == null 
            		|| gmmsUtility.getCommonProperty("NMGPort") == null) {
                throw new UnknownHostException("No definition of NMG in property file");
            }
            
            String nmgAddr = gmmsUtility.getCommonProperty("NMGAddress").trim();
            int port = Integer.parseInt(gmmsUtility.getCommonProperty("NMGPort").trim());
            resolver = new NMGResolver(nmgAddr, port);
        }
        catch(Exception ex) {
            log.error("Exception in initialize() of NMGClient", ex);
        }
    }

    /**
     * get ack result of number_application and number_query
     * @param msgs
     * @param pduList
     */
    public void getAsyQueryResult(List<GmmsMessage> msgs, List<Pdu> pduList) {
        try {
        	// get respond pdus
    		resolver.getAsyResults(msgs, pduList);
        }
        catch (Exception ex) {
            log.warn(ex, ex);
        }
    }

    /**
     * asy number query
     * @param msg
     * @return 0: asyn <br>
     *         -1: error
     */
    public int asyQueryRop(GmmsMessage msg) {
        try {
        	CommandNumberQuery cmdQuery = new CommandNumberQuery();
    		cmdQuery.convertFromMsg(msg);
    		resolver.asySend(cmdQuery, msg);
        }
        catch (Exception e) {
            log.error(e, e);
            return -1;
        }
        return 0;
    }
    
    /**
     * asy number application
     * @param msg
     * @return 0: asyn <br>
     *         -1: error
     */
    public int asyApplySenderNumber(GmmsMessage msg) {
    	try {
    		CommandNumberApplication cmdApplication = new CommandNumberApplication();
    		cmdApplication.convertFromMsg(msg);
    		resolver.asySend(cmdApplication, msg);
        }
        catch (Exception e) {
            log.error(e, e);
            return -1;
        }
        return 0;
    }
}
