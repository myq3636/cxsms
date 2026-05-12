package com.king.rest.util;

/**
 * GmmsStatus.java
 * Created on April 2, 2004
 *
 * @author mike
 */
public class GmmsStatus
    extends Object implements Comparable {
    private int code = -1;
    private String text = "";

    //gmms status
    public static final GmmsStatus UNASSIGNED = new GmmsStatus( -1,
        "Unassigned");
    public static final GmmsStatus INDELIVERY = new GmmsStatus( -2,
        "InDelivery");
    public static final GmmsStatus SUCCESS = new GmmsStatus(0, "Success");
    public static final GmmsStatus PARTIAL_SUCCESS = new GmmsStatus(105,
        "Partial Success");
    public static final GmmsStatus CLIENT_ERROR = new GmmsStatus(2000,
        "Client Error");
    public static final GmmsStatus OPER_UNAVAILABLE = new GmmsStatus(2010,
        "Operation Not Available");
    public static final GmmsStatus INVALID_MSG_FIELD = new GmmsStatus(2100,
        "Invalid Message Field");
    public static final GmmsStatus SENDER_ADDR_ERROR = new GmmsStatus(2110,
        "Sender Address Error");
    public static final GmmsStatus RECIPIENT_ADDR_ERROR = new GmmsStatus(2120,
        "Recipient Address Error");

    public static final GmmsStatus INVALID_MSG_FORMAT = new GmmsStatus(2130,
        "Invalid Message Format");
    
    public static final GmmsStatus INSUBMIT_RESP_FAILED = new GmmsStatus(2140,
    		"Send In_Submit_Resp failed");

    public static final GmmsStatus POLICY_DENIED = new GmmsStatus(2200,
        "Policy Denied");
    public static final GmmsStatus SPAMED = new GmmsStatus(2210,
        "Spamed");
    public static final GmmsStatus BinaryFilter = new GmmsStatus(2220,
        "Binary Filter");

    /* Added by Bill after group discussion, May, 17, 2005 */
    public static final GmmsStatus UNDELIVERED = new GmmsStatus(2300,
        "Undelivered for Submit");

    public static final GmmsStatus COMMUNICATION_ERROR = new GmmsStatus(2500,
        "Communication Error");
    public static final GmmsStatus Throttled = new GmmsStatus(2600,
        "Throttling Control");

    public static final GmmsStatus SERVER_ERROR = new GmmsStatus(3000,
        "Server Error");

    public static final GmmsStatus SERVICE_ERROR = new GmmsStatus(4000,
        "Service Error");

    public static final GmmsStatus AUTHENTICATION_ERROR = new GmmsStatus(5000,
        "Authentication Error");

    public static final GmmsStatus UNKNOWN_ERROR = new GmmsStatus(9000,
        "Unknown Error for Submit");

    // added by Jesse 2005-2-16.
    public static final GmmsStatus FAIL_QUERY_DELIVERREPORT =
        new GmmsStatus(9005, "Fail to query delivery report.");

    // add by Jesse Duan 2004-10.
    public static final GmmsStatus DELIVERED = new GmmsStatus(10000,
        "Delivered");

    public static final GmmsStatus ENROUTE = new GmmsStatus(10105, "Enroute");
    public static final GmmsStatus EXPIRED = new GmmsStatus(10200, "Expired");
    public static final GmmsStatus DELETED = new GmmsStatus(10300, "Deleted");
    public static final GmmsStatus UNDELIVERABLE = new GmmsStatus(10400,
        "Undeliverable for Delivery Report");
    public static final GmmsStatus REJECTED = new GmmsStatus(10500, "Rejected");
    public static final GmmsStatus MSG_NOT_FOUND = new GmmsStatus(10600,
        "Message Not Found");

    public static final GmmsStatus UNKNOWN = new GmmsStatus(10900,
        "Unknown Error");
    // added by Jesse 2004-2-21
    public static final GmmsStatus REJECTED_BYANTISPAM = new GmmsStatus(10700,
        "Rejected by Anti-Spam");
    public static final GmmsStatus FAIL_SENDOUT_DELIVERYREPORT = new GmmsStatus(
        11005, "Fail to send out delivery report");

    public static final GmmsStatus RETRIEVED = new GmmsStatus(12000,
        "Retrieved");

    public static final GmmsStatus FAIL_QUERY_READREPLY = new GmmsStatus(12005,
        "Fail to query read reply");

    public static final GmmsStatus READ = new GmmsStatus(13000, "Read");
    public static final GmmsStatus DELETED_WITHOUT_BEING_READ = new GmmsStatus(
        13100, "Deleted without being read");

    // added by Jesse 2004-2-21
    public static final GmmsStatus FAIL_SENDOUT_READREPLY = new GmmsStatus(
        13105, "Fail to send out read reply");

    public static final GmmsStatus RETURNREAD = new GmmsStatus(13500,
        "ReturnedRead");
    
    //add by kevin for REST
    
    public static final GmmsStatus NOTEXIST=new GmmsStatus(11000,"NOTEXIST");

    public static GmmsStatus getStatus(int code) {
        switch (code) {
            case -1:
                return UNASSIGNED;
            case -2:
                return INDELIVERY;
            case 0:
                return SUCCESS;
            case 105:
                return PARTIAL_SUCCESS;
            case 2000:
                return CLIENT_ERROR;
            case 2010:
                return OPER_UNAVAILABLE;
            case 2100:
                return INVALID_MSG_FIELD;
            case 2110:
                return SENDER_ADDR_ERROR;
            case 2120:
                return RECIPIENT_ADDR_ERROR;
            case 2130:
                return INVALID_MSG_FORMAT;
            case 2200:
                return POLICY_DENIED;
            case 2210:
                return SPAMED;
            case 2220:
                return BinaryFilter;
            case 2300:
                return UNDELIVERED;
            case 2500:
                return COMMUNICATION_ERROR;
            case 2600:
                return Throttled;
            case 3000:
                return SERVER_ERROR;
            case 4000:
                return SERVICE_ERROR;
            case 5000:
                return AUTHENTICATION_ERROR;
            case 9000:
                return UNKNOWN_ERROR;
            case 9005:
                return FAIL_QUERY_DELIVERREPORT;
            case 10000:
                return DELIVERED;
            case 10105:
                return ENROUTE;
            case 10200:
                return EXPIRED;
            case 10300:
                return DELETED;
            case 10400:
                return UNDELIVERABLE;
            case 10500:
                return REJECTED;
            case 10600:
                return MSG_NOT_FOUND;
            case 10700:
                return REJECTED_BYANTISPAM;
            case 10900:
                return UNKNOWN;
            case 11000:
            	 return NOTEXIST;  //add by kevin for REST
            case 11005:
                return FAIL_SENDOUT_DELIVERYREPORT;
            case 12000:
                return RETRIEVED;
            case 12005:
                return FAIL_QUERY_READREPLY;
            case 13000:
                return READ;
            case 13100:
                return DELETED_WITHOUT_BEING_READ;
            case 13105:
                return FAIL_SENDOUT_READREPLY;
            case 13500:
                return RETURNREAD;
            default:
                return UNKNOWN_ERROR; //code: 9000
        }
    }

    // Default constructor
    public GmmsStatus() {
    }

    public GmmsStatus(int code, String text) {
        this.code = code;
        this.text = text;
    }

    public boolean isItFinal() {
        if ( (this.code % 10) == 0) {
            return true;
        }
        else {
            return false;
        }
    }

    public int getCode() {
        return code;
    }

    protected void setCode(int code) {
        this.code = code;
    }

    public String getText() {
        return this.text;
    }

    protected void setText(String text) {
        this.text = text;
    }

    /**
     * compareTo
     *
     * @param status Object
     * @return int
     */
    public int compareTo(Object status) {
        int cp = ( (GmmsStatus) status).getCode();
        if (code == cp) {
            return 0;
        }
        else if (code > cp) {
            return 1;
        }
        else {
            return -1;
        }
    }

    public String toString() {
        return code + "/" + text;
    }

    public boolean equals(Object obj) {
        if (obj instanceof GmmsStatus) {
            GmmsStatus status = (GmmsStatus) obj;
            return status.getCode() == this.getCode();
        }
        else {
            return false;
        }
    }
    
    //add by Will for JDK hashCode rule
    public int hashCode(){
    	return this.code;
    }
}
