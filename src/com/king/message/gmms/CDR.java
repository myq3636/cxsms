package com.king.message.gmms;

import java.util.Date;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;

/**
 * <p>Title: CDR</p>
 * <p>Description: Structure to define CDR</p>
 * <p>Copyright: Copyright (c) 2001-2010</p>
 * <p>Company: King.Inc</p>
 *
 * @version 6.1
 * @author: Jesse Duan
 */
public class CDR
        extends GmmsMessage {
    //CDR type
    public static final String TYPE_IN_SUBMIT = "IN_SUBMIT";
    /** for in csm  */
    public static final String TYPE_IN_SUBMIT_PARTIAL = "IN_SUBMIT_PARTIAL";
    public static final String TYPE_OUT_SUBMIT = "OUT_SUBMIT";
    public static final String TYPE_IN_DELI_REP = "IN_DELI_REP";
    public static final String TYPE_OUT_DELI_REP = "OUT_DELI_REP";
    /* Added by Bill, August, July 2004 */
    public static final String TYPE_OUT_READ_REPLY_REP = "OUT_READ_REPLY_REP";
    public static final String TYPE_IN_READ_REPLY_REP = "IN_READ_REPLY_REP";
    public static final String TYPE_IN_DELI = "IN_DELI";
    /** for in csm  */
    public static final String TYPE_IN_DELI_PARTIAL = "IN_DELI_PARTIAL";
    public static final String TYPE_OUT_DELI = "OUT_DELI";

    public static final String TYPE_IN_SUBMIT_REQ = "IN_SUBMIT_REQ";
    public static final String TYPE_IN_SUBMIT_RES = "IN_SUBMIT_RES";
    public static final String TYPE_OUT_SUBMIT_REQ = "OUT_SUBMIT_REQ";
    public static final String TYPE_OUT_SUBMIT_RES = "OUT_SUBMIT_RES";

    public static final String TYPE_IN_DELIVERY_REQ = "IN_DELIVERY_REQ";
    public static final String TYPE_IN_DELIVERY_RES = "IN_DELIVERY_RES";
    public static final String TYPE_OUT_DELIVERY_REQ = "OUT_DELIVERY_REQ";
    public static final String TYPE_OUT_DELIVERY_RES = "OUT_DELIVERY_RES";

    public static final String TYPE_IN_DELI_REP_REQ = "IN_DELI_REP_REQ";
    public static final String TYPE_IN_DELI_REP_RES = "IN_DELI_REP_RES";
    public static final String TYPE_OUT_DELI_REP_REQ = "OUT_DELI_REP_REQ";
    public static final String TYPE_OUT_DELI_REP_RES = "OUT_DELI_REP_RES";

    private String cdrType = null;

    /**
     * constructor
     *
     * @param msg GmmsMessage
     */
    public CDR(GmmsMessage msg) {
        super(msg);
		if (serviceTypeID == 0) {
			try {
				A2PCustomerInfo customerInfo = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(this.oSsID);
				if ("Vendor".equalsIgnoreCase(customerInfo.getRole())) {
					serviceTypeID = 3;
				}
			} catch (Exception e) {
				// TODO: handle exception
			}			
		}
    }

    public void setCdrType(String type) {
        this.cdrType = type;
    }

    public String getCdrType() {
        return cdrType;
    }
}
