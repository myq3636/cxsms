package com.king.message.gmms;

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
    public static final GmmsStatus SENDER_ERROR_BY_BL = new GmmsStatus(2111,
            "Sender Reject By BL");
    public static final GmmsStatus SENDER_ERROR_BY_WL = new GmmsStatus(2112,
            "Sender Reject By WL");
    public static final GmmsStatus RECIPIENT_ADDR_ERROR = new GmmsStatus(2120,
        "Recipient Address Error");
    public static final GmmsStatus RECIPIENT_ERROR_BY_BL = new GmmsStatus(2121,
            "Recipient Reject By BL");
    public static final GmmsStatus RECIPIENT_ERROR_BY_MAX_COUNT = new GmmsStatus(2122,
            "Recipient Reject By MAX COUNT IN 24H");

    public static final GmmsStatus INVALID_MSG_FORMAT = new GmmsStatus(2130,
        "Invalid Message Format");
    
    public static final GmmsStatus INSUBMIT_RESP_FAILED = new GmmsStatus(2140,
    		"Send In_Submit_Resp failed");
    
    public static final GmmsStatus INVALID_SERVICETYPEID = new GmmsStatus(2150,
    		"Invalid ServiceTypeID");
    
    public static final GmmsStatus DUPLICATE_MSG = new GmmsStatus(2160,
	        "Duplicate Message");
    
    public static final GmmsStatus INVALID_SCHEDULED_TIME = new GmmsStatus(2170, 
    		"Invalid Scheduled Delivery Time");

    public static final GmmsStatus POLICY_DENIED = new GmmsStatus(2200,
        "Policy Denied");
    public static final GmmsStatus SPAMED = new GmmsStatus(2210,
        "Spamed");
    public static final GmmsStatus SPAMED_CTBL = new GmmsStatus(2201,
            "Spamed by Content_BL");
    public static final GmmsStatus SPAMED_CTWL = new GmmsStatus(2202,
            "Spamed by Content_WL");
    public static final GmmsStatus BinaryFilter = new GmmsStatus(2220,
        "Binary Filter");

    public static final GmmsStatus TEMPLATE_FAIL = new GmmsStatus(2240,
            "Replace Template Failed");
    
    public static final GmmsStatus MSG_SIZE_OVERLENGTH = new GmmsStatus(2250,
            "Msg Size Over Length");
    public static final GmmsStatus MSG_RECIPIET_RULE_ERROR = new GmmsStatus(2260,
            "Msg Recipient length or rule error");
    
    /* Added by Bill after group discussion, May, 17, 2005 */
    public static final GmmsStatus UNDELIVERED = new GmmsStatus(2300,
        "Undelivered for Submit");

    public static final GmmsStatus COMMUNICATION_ERROR = new GmmsStatus(2500,
        "Communication Error");
    
    public static final GmmsStatus DRFAILEDTOREROUTE_ERROR = new GmmsStatus(2501,
            "DR failed for reroute Error");
    public static final GmmsStatus Throttled = new GmmsStatus(2600,
        "Throttling Control");
    public static final GmmsStatus SUBMIT_RESP_ERROR = new GmmsStatus(2700,
            "Submit Response Not Received");
    public static final GmmsStatus SERVER_ERROR = new GmmsStatus(3000,
        "Server Error");

    public static final GmmsStatus SERVICE_ERROR = new GmmsStatus(4000,
        "Service Error");

    public static final GmmsStatus INSUFFICIENT_BALANCE_CODE = new GmmsStatus(5100,
        "Insufficient balance");
    
    public static final GmmsStatus AUTHENTICATION_ERROR = new GmmsStatus(5000,
            "Authentication Error");
    
    public static final GmmsStatus SUBMIT_NOTPAID = new GmmsStatus(8000,
            "Submit Not Paid");

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
    public static final GmmsStatus REJECTED_BYNC = new GmmsStatus(10800,
            "Rejected by NotPaid");
    public static final GmmsStatus REJECTED_SDBL = new GmmsStatus(10801,
            "Rejected by Sender_BL");
    public static final GmmsStatus REJECTED_SDWL = new GmmsStatus(10802,
            "Rejected by Sender_WL");
    public static final GmmsStatus REJECTED_CTBL = new GmmsStatus(10803,
            "Rejected by Content_BL");
    public static final GmmsStatus REJECTED_CTWL = new GmmsStatus(10804,
            "Rejected by Content_WL");
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
    
    public static final GmmsStatus ACCEPT = new GmmsStatus(10001,
            "Accepted");
    public static final GmmsStatus UNDELIVERY_MSG_FORMAT_ERROR = new GmmsStatus(10401,
            "Undelivery by msg format error");
    public static final GmmsStatus UNDELIVERY_RECIPIENT_ERROR = new GmmsStatus(10402,
            "Undelivery by recipient error");
    public static final GmmsStatus UNDELIVERY_DAILY_LIMIT = new GmmsStatus(10403,
            "Undelivery by daily limit control");
    public static final GmmsStatus UNDELIVERY_SENDER_ERROR = new GmmsStatus(10404,
            "Undelivery by sender error");
    public static final GmmsStatus UNDELIVERY_NETWORK_ERROR = new GmmsStatus(10405,
            "Undelivery by network error");
    public static final GmmsStatus UNDELIVERY_THROTTLING_CONTROL = new GmmsStatus(10406,
            "Undelivery by throttling control");
    public static final GmmsStatus REJECT_RECIPENT_BL = new GmmsStatus(10502,
            "Reject by Recipent BL");
    public static final GmmsStatus REJECT_TEMPLATE_ERROR = new GmmsStatus(10503,
            "Reject by template fail");

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
            case 2111:
                return SENDER_ERROR_BY_BL;
            case 2112:
                return SENDER_ERROR_BY_WL;
            case 2120:
                return RECIPIENT_ADDR_ERROR;
            case 2121:
                return RECIPIENT_ERROR_BY_BL;
            case 2122:
                return RECIPIENT_ERROR_BY_MAX_COUNT;
            case 2130:
                return INVALID_MSG_FORMAT;
            case 2140:
            	return INSUBMIT_RESP_FAILED;
            case 2150:
                return INVALID_SERVICETYPEID;
            case 2160:
                return DUPLICATE_MSG;
            case 2170:
            	return INVALID_SCHEDULED_TIME;
            case 2200:
                return POLICY_DENIED;
            case 2201:
                return SPAMED_CTBL;
            case 2202:
                return SPAMED_CTWL;
            case 2240:
                return TEMPLATE_FAIL;
            case 2250:
                return MSG_SIZE_OVERLENGTH;
            case 2260:
                return MSG_RECIPIET_RULE_ERROR;
            case 2210:
                return SPAMED;
            case 2220:
                return BinaryFilter;
            case 2300:
                return UNDELIVERED;
            case 2500:
                return COMMUNICATION_ERROR;
            case 2501:
                return DRFAILEDTOREROUTE_ERROR;
            case 2600:
                return Throttled;
            case 2700:
                return SUBMIT_RESP_ERROR;
            case 3000:
                return SERVER_ERROR;
            case 4000:
                return SERVICE_ERROR;
            case 5000:
                return AUTHENTICATION_ERROR;
            case 5100:
                return INSUFFICIENT_BALANCE_CODE;
            case 8000:
                return SUBMIT_NOTPAID;                
            case 9000:
                return UNKNOWN_ERROR;
            case 9005:
                return FAIL_QUERY_DELIVERREPORT;
            case 10000:
                return DELIVERED;
            case 10001:
                return ACCEPT;
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
            case 10800:
                return REJECTED_BYNC;
            case 10801:
                return REJECTED_SDBL;
            case 10802:
                return REJECTED_SDWL;
            case 10803:
                return REJECTED_CTBL;
            case 10804:
                return REJECTED_CTWL;
            case 10900:
                return UNKNOWN;
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
            case 10401:
                return UNDELIVERY_MSG_FORMAT_ERROR;
            case 10402:
                return UNDELIVERY_RECIPIENT_ERROR;
            case 10403:
                return UNDELIVERY_DAILY_LIMIT;
            case 10404:
                return UNDELIVERY_SENDER_ERROR;
            case 10405:
                return UNDELIVERY_NETWORK_ERROR;
            case 10406:
                return UNDELIVERY_THROTTLING_CONTROL;
            case 10502:
                return REJECT_RECIPENT_BL;
            case 10503:
                return REJECT_TEMPLATE_ERROR;            
            default:
                return UNKNOWN_ERROR; //code: 9000
        }
    }
    
	/*
	 * public static GmmsStatus SubmitConvertErrorDRStatus(String code) { switch
	 * (code) { case "Invalid Message Field": return UNDELIVERY_MSG_FORMAT_ERROR;
	 * case 2110: return UNDELIVERY_SENDER_ERROR; case 2120: return
	 * UNDELIVERY_RECIPIENT_ERROR; case 2122: return UNDELIVERY_DAILY_LIMIT; case
	 * 2130: return UNDELIVERY_MSG_FORMAT_ERROR; case 2200: return
	 * REJECTED_BYANTISPAM; case 2201: return REJECTED_CTBL; case 2202: return
	 * REJECTED_CTWL; case 2240: return REJECT_TEMPLATE_ERROR; case 2250: return
	 * UNDELIVERY_MSG_FORMAT_ERROR; case 2260: return UNDELIVERY_RECIPIENT_ERROR;
	 * case 2210: return REJECTED_BYANTISPAM; case 2220: return REJECTED_BYANTISPAM;
	 * case 2300: return UNKNOWN; case 2500: return UNDELIVERY_NETWORK_ERROR; case
	 * 2501: return UNDELIVERY_NETWORK_ERROR; case 2600: return
	 * UNDELIVERY_THROTTLING_CONTROL; case 3000: return UNKNOWN; case 4000: return
	 * UNKNOWN; case 5000: return REJECT_INSUFFICIENT_BALANCE; case 5100: return
	 * REJECT_INSUFFICIENT_BALANCE; case 9000: return UNKNOWN; case 9005: return
	 * UNKNOWN; default: return UNDELIVERABLE; //code: 10400 } }
	 */
    
    public static GmmsStatus SubmitConvertErrorDRStatus(String desc) {
        switch (desc) {                       
            case "Invalid Message Field":
                return UNDELIVERY_MSG_FORMAT_ERROR;
            case "Sender Address Error":
                return UNDELIVERY_SENDER_ERROR;            
            case "Recipient Address Error":
                return UNDELIVERY_RECIPIENT_ERROR;            
            case "Recipient Reject By MAX COUNT IN 24H":
                return UNDELIVERY_DAILY_LIMIT;
            case "Invalid Message Format":
                return UNDELIVERY_MSG_FORMAT_ERROR;            
            case "Policy Denied":
                return REJECTED_BYANTISPAM;
            case "Spamed by Content_BL":
                return REJECTED_CTBL;
            case "Spamed by Content_WL":
                return REJECTED_CTWL;
            case "Replace Template Failed":
                return REJECT_TEMPLATE_ERROR;
            case "Msg Size Over Length":
                return UNDELIVERY_MSG_FORMAT_ERROR;
            case "Msg Recipient length or rule error":
                return UNDELIVERY_RECIPIENT_ERROR;
            case "Spamed":
                return REJECTED_BYANTISPAM;
            case "Binary Filter":
                return REJECTED_BYANTISPAM;
            case "Undelivered for Submit":
                return UNKNOWN;
            case "Communication Error":
                return UNDELIVERY_NETWORK_ERROR;
            case "DR failed for reroute Error":
                return UNDELIVERY_NETWORK_ERROR;
            case "Throttling Control":
                return UNDELIVERY_THROTTLING_CONTROL;            
            case "Server Error":
                return UNKNOWN;
            case "Service Error":
                return UNKNOWN;
            case "Recipient Reject By BL":
                return REJECT_RECIPENT_BL;            
            case "Unknown Error for Submit":
                return UNKNOWN;
            case "Fail to query delivery report.":
                return UNKNOWN; 
            case "Submit Not Paid":
                return REJECTED_BYNC;
            default:
                return null; //code: 10400
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
    /*public int hashCode(){
    	return this.code;
    }*/
}
