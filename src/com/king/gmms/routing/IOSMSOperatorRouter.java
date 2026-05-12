package com.king.gmms.routing;

//import org.apache.log4j.Logger;



import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import com.king.db.*;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.*;
import com.king.gmms.routing.RouteResponse;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 *
 */
public class IOSMSOperatorRouter extends OperatorRouter {
    //Logger to write log
    private static SystemLogger log = SystemLogger.getSystemLogger(IOSMSOperatorRouter.class);
    private DNSClient dnsClient;


    /**
     * Constructor
     *
     */
    public IOSMSOperatorRouter(boolean isASY) {
        dnsClient = new DNSClient(isASY);
    }

    /**
     * To get operators' information and set into GmmsMessage's own field
     *
     * @param msg GmmsMessage
     * @return boolean
     */

    /**
     * return: 1 route ok, 0 asy query, -1 query error
     */
    public RouteResponse iosmsRouteToOperator(GmmsMessage msg) {
        /*int oOperator = getOoperator(msg);
        if (oOperator < 0) {
        	if(log.isInfoEnabled()){
				log.info(msg, "Failed to query oOperator:{}" , oOperator);
        	}
            return RouteResponse.RouteFailed;
        }

        if( oOperator == 0) return RouteResponse.ASYQueryOP;*/
    	msg.setOoperator(msg.getOSsID());
        int rOperator = getRoperator(msg);
        if (rOperator < 0) {
        	if(log.isInfoEnabled()){
				log.info(msg, "Failed to query rOperator:{}" , rOperator);
        	}
            return RouteResponse.RouteFailed;
        }
        if(rOperator == 0 ) return RouteResponse.ASYQueryOP;
        return RouteResponse.RouteOK;
    }

    public int getOoperatorSYN(GmmsMessage msg) {
        int oOperator = msg.getOoperator();
        if (oOperator > 0) {
            return oOperator;
        }
        if (gmmsUtility.getCustomerManager().isOperator(msg.getOSsID())) {
            msg.setOoperator(msg.getOSsID());
            return msg.getOoperator();
        }
        if (!gmmsUtility.getCustomerManager().isHub(msg.getOSsID()) &&
            !gmmsUtility.getCustomerManager().isChannel(msg.getOSsID())) {
            msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            return msg.getOoperator();
        }
        try {
            if (!gmmsUtility.getCustomerManager().getCustomerBySSID(msg.
                getOSsID()).isParseOoperator()) {
                msg.setOoperator(msg.getOSsID());
                return msg.getOoperator();
            }
            oOperator = getOperatorByDNS(dnsClient.queryMncMcc(msg.getSenderAddress(),
                super.defaultSuffix), msg);
        }
        catch (com.king.db.DataManagerException exp) {
            msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            log.error(msg, exp, exp);
        }
        if (oOperator > 0)
            msg.setOoperator(oOperator);
        else {
            if (oOperator == -4 || oOperator == -3 || oOperator == -2) {
                msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            }
            else if (oOperator == -1) {
                msg.setStatus(GmmsStatus.SERVER_ERROR);
            }
            else {
                msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
                log.warn(msg, "Unknow Error when query DNS by sender address.");
            }
        }
        return msg.getOoperator();

    }


    public int getRoperatorSYN(GmmsMessage msg) {
        int rOperator = msg.getRoperator();
        if (rOperator > 0)
            return rOperator;
        String recipientadd = msg.getRecipientAddress();
        try {

            String suffix = super.getDNSSuffix(msg.getOSsID(), recipientadd);
            rOperator = getOperatorByDNS(dnsClient.queryMncMcc(recipientadd, suffix),
                                         msg);
        }
        catch (com.king.db.DataManagerException exp) {
            log.error(msg, exp, exp);
            msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
        }
        if (rOperator > 0)
            msg.setRoperator(rOperator);
        else {
            if (rOperator == -4 || rOperator == -3 || rOperator == -2) {
                msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
            }
            else if (rOperator == -1) {
                msg.setStatus(GmmsStatus.SERVER_ERROR);
            }
            else {
                msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
                log.warn(msg,
                         "Unknow Error when query DNS by recipient address.");
            }
        }
        return msg.getRoperator();

    }




    public int getOoperator(GmmsMessage msg) {
        int oOperator = msg.getOoperator();
        if(oOperator > 0){
            return oOperator;
        }
        if(gmmsUtility.getCustomerManager().isOperator(msg.getOSsID())) {
            msg.setOoperator(msg.getOSsID());
            return msg.getOoperator();
        }
        if(!gmmsUtility.getCustomerManager().isHub(msg.getOSsID())&&!gmmsUtility.getCustomerManager().isChannel(msg.getOSsID())){
            msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            return msg.getOoperator();
        }
        try{
            if (!gmmsUtility.getCustomerManager().getCustomerBySSID(msg.
                getOSsID()).isParseOoperator()) {
                msg.setOoperator(msg.getOSsID());
                return msg.getOoperator();
            }
            oOperator = getOperatorByDNS(dnsClient.asyQueryMncMcc(msg.getSenderAddress(),
                super.defaultSuffix, msg), msg);
        }
        catch(com.king.db.DataManagerException exp){
            msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            log.error(msg,exp,exp);
        }
        if(oOperator > 0) msg.setOoperator(oOperator);
        else{
            if(oOperator==-4||oOperator==-3||oOperator==-2){
                msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
            }
            else if(oOperator==-1){
                msg.setStatus(GmmsStatus.SERVER_ERROR);
            }else if(oOperator == 0){
                return 0;
            }
            else{
                msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
                log.warn(msg,"Unknow Error when query DNS by sender address.");
            }
        }
        return msg.getOoperator();
    }


    public List<Map> getDnsQueryResult() {

        List msgs = new ArrayList();
        List<String[]> res = dnsClient.getAsyQueryResult(msgs);

        List<Map> msg2res = getOperatorByAsyDNS(msgs, res);
        return msg2res;
    }

    private List<Map> getOperatorByAsyDNS(List msgs, List < String[] > res) {
        Object[] temp = msgs.toArray();
        List<Map> list = new ArrayList<Map> ();
        GmmsMessage msg = null;
        boolean isOop = true;
        int op = 0;
        for (int i = 0; i < res.size(); i++) {
            String[] mncmcc = res.get(i);
            msg = (GmmsMessage) (temp[i]);
            if (msg.getOoperator() > 0)
                isOop = false;
            try {
                op = getOperatorByDNS(mncmcc, msg);
            }
            catch (com.king.db.DataManagerException exp) {
                log.error(msg, exp, exp);
                if (isOop)
                    msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
                else
                    msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
            }
            if (op < 0) {
                if (op == -4 || op == -3 || op == -2) {
                    msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
                }
                else if (op == -1) {
                    msg.setStatus(GmmsStatus.SERVER_ERROR);
                }
                else {
                    msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
                    log.warn(msg,
                             "Unknow Error when query DNS by recipient address.");
                }
            } //end of if
            HashMap map = new HashMap();
            map.put(new Integer(op), msg);
            list.add(map);
            temp[i] = null;
        } //end of for
        res.clear();
        msgs.clear();
        return list;
    }


    public int getRoperator(GmmsMessage msg) {
        int rOperator = msg.getRoperator();
        if(rOperator > 0) return rOperator;
        
        //get ROP by message keyword routing
        A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
        Map<String, Integer> map = ctm.getKeywordMap(msg.getOSsID());  
        if(map != null && map.size()>0){
        	String content = msg.getTextContent().toLowerCase();
            Iterator<String> it = map.keySet().iterator(); 
            String keyword = null;
            while(it.hasNext()){
            	keyword = (String) it.next();
            	if(content.startsWith(keyword)){
            		rOperator = map.get(keyword);
            		msg.setRoperator(rOperator);
            		return rOperator;
            	}
            }
        }
                
        //get ROP by query DNS
        String recipientadd = msg.getRecipientAddress() ;
        try{
           
        	//String suffix = super.getDNSSuffix(msg.getOSsID(),recipientadd);
            //rOperator = getOperatorByDNS(dnsClient.asyQueryMncMcc(recipientadd,suffix,msg), msg);
        	rOperator = ctm.getSsidByRecPrefixAndLen(recipientadd, msg.getOSsID());
        }
        catch(Exception exp){
            log.error(msg,exp,exp);
            msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
        }
        if(rOperator > 0) msg.setRoperator(rOperator);
        else{
            if(rOperator==-4||rOperator==-3||rOperator==-2){
                msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
            }
            else if(rOperator==-1){
                msg.setStatus(GmmsStatus.SERVER_ERROR);
            }else if(rOperator == 0){
                return 0;
            }
            else{
                msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
                log.warn(msg,"Unknow Error when query DNS by recipient address.");
            }
        }
        return msg.getRoperator();
    }

	public DNSClient getDnsClient() {
		return dnsClient;
	}

	public void setDnsClient(DNSClient dnsClient) {
		this.dnsClient = dnsClient;
	}
    
    
}
