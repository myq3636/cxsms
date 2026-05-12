package com.king.message.gmms;

import java_cup.internal_error;

/**
 * GmmsStatus.java
 * Created on April 2, 2004
 *
 * @author mike
 */
public class BillingStatus
    extends Object implements Comparable {
    private int code = -1;
    private int billingCode= -1;
    private String text = "";

    //gmms status    
    public static final BillingStatus SUCCESS = new BillingStatus(0, 0, "Success");
    public static final BillingStatus PARTIAL_SUCCESS = new BillingStatus(105,
        -1, "Partial Success");
    public static final BillingStatus CLIENT_ERROR = new BillingStatus(2000,
        -1, "Client Error");
    public static final BillingStatus OPER_UNAVAILABLE = new BillingStatus(2010,
        -1, "Operation Not Available");
    public static final BillingStatus INVALID_MSG_FIELD = new BillingStatus(2100,
        -1, "Invalid Message Field");
    public static final BillingStatus SENDER_ADDR_ERROR = new BillingStatus(2110,
        -1, "Sender Address Error");
    public static final BillingStatus RECIPIENT_ADDR_ERROR = new BillingStatus(2120,
        -1, "Recipient Address Error");

    public static final BillingStatus INVALID_MSG_FORMAT = new BillingStatus(2130,
        -1, "Invalid Message Format");
    
    public static final BillingStatus INSUBMIT_RESP_FAILED = new BillingStatus(2140,
    		-1, "Send In_Submit_Resp failed");
    
    public static final BillingStatus INVALID_SERVICETYPEID = new BillingStatus(2150,
    		-1, "Invalid ServiceTypeID");
    
    public static final BillingStatus DUPLICATE_MSG = new BillingStatus(2160,
	        -1, "Duplicate Message");
    
    public static final BillingStatus INVALID_SCHEDULED_TIME = new BillingStatus(2170, 
    		-1, "Invalid Scheduled Delivery Time");

    public static final BillingStatus POLICY_DENIED = new BillingStatus(2200,
        -1, "Policy Denied");
    public static final BillingStatus SPAMED = new BillingStatus(2210,
        -1, "Spamed");
    public static final BillingStatus BinaryFilter = new BillingStatus(2220,
        -1, "Binary Filter");

    /* Added by Bill after group discussion, May, 17, 2005 */
    public static final BillingStatus UNDELIVERED = new BillingStatus(2300,
        -1, "Undelivered for Submit");

    public static final BillingStatus TEMPLATE_FAIL = new BillingStatus(2240,
           -1, "Replace Template Failed");
    
    public static final BillingStatus MSG_SIZE_OVERLENGTH = new BillingStatus(2250,
          -1,  "Msg Size Over Length");
    
    public static final BillingStatus COMMUNICATION_ERROR = new BillingStatus(2500,
        -1, "Communication Error");
    
    public static final BillingStatus Throttled = new BillingStatus(2600,
        -1, "Throttling Control");
    public static final BillingStatus SUBMIT_RESP_ERROR = new BillingStatus(2700,
            -1, "Submit Response Not Received");
    public static final BillingStatus SERVER_ERROR = new BillingStatus(3000,
        -1, "Server Error");

    public static final BillingStatus SERVICE_ERROR = new BillingStatus(4000,
        -1, "Service Error");

    public static final BillingStatus AUTHENTICATION_ERROR = new BillingStatus(5000,
        -1, "Authentication Error");
    
    public static final BillingStatus INSUFFICIENT_BALANCE_CODE = new BillingStatus(5100,
            -1, "Insufficient balance");
    public static final BillingStatus SUBMIT_NOTPAID = new BillingStatus(8000,
    		-1, "Submit Not Paid");
    public static final BillingStatus UNKNOWN_ERROR = new BillingStatus(9000,
        -1, "Unknown Error for Submit");

    // added by Jesse 2005-2-16.
    public static final BillingStatus FAIL_QUERY_DELIVERREPORT =
        new BillingStatus(9005, -1, "Fail to query delivery report.");

    // add by Jesse Duan 2004-10.
    public static final BillingStatus DELIVERED = new BillingStatus(10000,
        2, "Delivered");

    public static final BillingStatus ENROUTE = new BillingStatus(10105, 1, "Enroute");
    public static final BillingStatus EXPIRED = new BillingStatus(10200, 3, "Expired");
    public static final BillingStatus DELETED = new BillingStatus(10300, 4, "Deleted");
    public static final BillingStatus UNDELIVERABLE = new BillingStatus(10400,
        5, "Undeliverable for Delivery Report");
    public static final BillingStatus REJECTED = new BillingStatus(10500, 8, "Rejected");
    public static final BillingStatus MSG_NOT_FOUND = new BillingStatus(10600,
        1, "Message Not Found");

    public static final BillingStatus UNKNOWN = new BillingStatus(10900,
        7, "Unknown Error");
    // added by Jesse 2004-2-21
    public static final BillingStatus REJECTED_BYANTISPAM = new BillingStatus(10700,
        8, "Rejected by Anti-Spam");
    public static final BillingStatus FAIL_SENDOUT_DELIVERYREPORT = new BillingStatus(
        11005, 5, "Fail to send out delivery report");

    public static final BillingStatus RETRIEVED = new BillingStatus(12000,
        8, "Retrieved");

    public static final BillingStatus FAIL_QUERY_READREPLY = new BillingStatus(12005,
        4, "Fail to query read reply");

    public static final BillingStatus READ = new BillingStatus(13000, 4, "Read");
    public static final BillingStatus DELETED_WITHOUT_BEING_READ = new BillingStatus(
        13100, 4, "Deleted without being read");

    // added by Jesse 2004-2-21
    public static final BillingStatus FAIL_SENDOUT_READREPLY = new BillingStatus(
        13105, 4, "Fail to send out read reply");

    public static final BillingStatus RETURNREAD = new BillingStatus(13500,
        4, "ReturnedRead");
    
    public static final BillingStatus ACCEPT = new BillingStatus(10001,
            6, "Accepted");
    public static final BillingStatus UNDELIVERY_MSG_FORMAT_ERROR = new BillingStatus(10401,5,
            "Undelivery by msg format error");
    public static final BillingStatus UNDELIVERY_RECIPIENT_ERROR = new BillingStatus(10402,5,
            "Undelivery by recipient error");
    public static final BillingStatus UNDELIVERY_DAILY_LIMIT = new BillingStatus(10403,5,
            "Undelivery by daily limit control");
    public static final BillingStatus UNDELIVERY_SENDER_ERROR = new BillingStatus(10404,5,
            "Undelivery by sender error");
    public static final BillingStatus UNDELIVERY_NETWORK_ERROR = new BillingStatus(10405,5,
            "Undelivery by network error");
    public static final BillingStatus UNDELIVERY_THROTTLING_CONTROL = new BillingStatus(10406,5,
            "Undelivery by throttling control");
    public static final BillingStatus REJECT_RECIPENT_BL = new BillingStatus(10502,8,
            "Reject by Recipent BL");
    public static final BillingStatus REJECT_TEMPLATE_ERROR = new BillingStatus(10503,8,
            "Reject by template fail");

    public static BillingStatus getStatus(int code) {
        switch (code) {
            case -1:
            case -2:
            case 0:
                return SUCCESS;
            case 105:
                return SUCCESS;
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
            case 2210:
                return SPAMED;
            case 2220:
                return BinaryFilter;
            case 2240:
                return TEMPLATE_FAIL;
            case 2250:
                return MSG_SIZE_OVERLENGTH;
            case 2300:
                return UNDELIVERED;
            case 2500:
                return COMMUNICATION_ERROR;
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
    
    public static BillingStatus getStatus(String text) {
        switch (text) {
            case "Success":
            case "Unassigned":
            case "InDelivery":
            case "Partial Success":
                return SUCCESS;
            case "Client Error":
                return CLIENT_ERROR;
            case "Operation Not Available":
                return OPER_UNAVAILABLE;
            case "Invalid Message Field":
                return INVALID_MSG_FIELD;
            case "Sender Address Error":
                return SENDER_ADDR_ERROR;
            case "Recipient Address Error":
                return RECIPIENT_ADDR_ERROR;
            case "Invalid Message Format":
                return INVALID_MSG_FORMAT;
            case "Send In_Submit_Resp failed":
            	return INSUBMIT_RESP_FAILED;
            case "Invalid ServiceTypeID":
                return INVALID_SERVICETYPEID;
            case "Duplicate Message":
                return DUPLICATE_MSG;
            case "Invalid Scheduled Delivery Time":
            	return INVALID_SCHEDULED_TIME;
            case "Policy Denied":
                return POLICY_DENIED;
            case "Spamed":
                return SPAMED;
            case "Binary Filter":
                return BinaryFilter;
            case "Replace Template Failed":
                return TEMPLATE_FAIL;
            case "Msg Size Over Length":
                return MSG_SIZE_OVERLENGTH;
            case "Undelivered for Submit":
                return UNDELIVERED;
            case "Communication Error":
                return COMMUNICATION_ERROR;
            case "Throttling Control":
                return Throttled;
            case "Submit Response Not Received":
                return SUBMIT_RESP_ERROR;                
            case "Server Error":
                return SERVER_ERROR;
            case "Service Error":
                return SERVICE_ERROR;
            case "Authentication Error":
                return AUTHENTICATION_ERROR;
            case "Insufficient balance":
            	return INSUFFICIENT_BALANCE_CODE;
            case "Unknown Error for Submit":
                return UNKNOWN_ERROR;
            case "Fail to query delivery report.":
                return FAIL_QUERY_DELIVERREPORT;
            case "Delivered":
                return DELIVERED;
            case "Accepted":
                return ACCEPT;
            case "Enroute":
                return ENROUTE;
            case "Expired":
                return EXPIRED;
            case "Deleted":
                return DELETED;
            case "Undeliverable for Delivery Report":
                return UNDELIVERABLE;
            case "Rejected":
                return REJECTED;
            case "Message Not Found":
                return MSG_NOT_FOUND;
            case "Rejected by Anti-Spam":
                return REJECTED_BYANTISPAM;
            case  "Unknown Error":
                return UNKNOWN;
            case "Fail to send out delivery report":
                return FAIL_SENDOUT_DELIVERYREPORT;
            case "Retrieved":
                return RETRIEVED;
            case "Fail to query read reply":
                return FAIL_QUERY_READREPLY;
            case "Read":
                return READ;
            case "Deleted without being read":
                return DELETED_WITHOUT_BEING_READ;
            case "Fail to send out read reply":
                return FAIL_SENDOUT_READREPLY;
            case "ReturnedRead":
                return RETURNREAD;
            case "Undelivery by msg format error":
                return UNDELIVERY_MSG_FORMAT_ERROR;
            case "Undelivery by recipient error":
                return UNDELIVERY_RECIPIENT_ERROR;
            case "Undelivery by daily limit control":
                return UNDELIVERY_DAILY_LIMIT;
            case "Undelivery by sender error":
                return UNDELIVERY_SENDER_ERROR;
            case "Undelivery by network error":
                return UNDELIVERY_NETWORK_ERROR;
            case "Undelivery by throttling control":
                return UNDELIVERY_THROTTLING_CONTROL;
            case "Reject by Recipent BL":
                return REJECT_RECIPENT_BL;
            case "Reject by template fail":
                return REJECT_TEMPLATE_ERROR;
            case "Submit Not Paid":
                return SUBMIT_NOTPAID;
            default:
                return UNKNOWN_ERROR; //code: 9000
        }
    }    

    // Default constructor
    public BillingStatus() {
    }

    public BillingStatus(int code, int billingCode, String text) {
        this.code = code;
        this.text = text;
        this.billingCode = billingCode;
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
        int cp = ( (BillingStatus) status).getCode();
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
        if (obj instanceof BillingStatus) {
            BillingStatus status = (BillingStatus) obj;
            return status.getCode() == this.getCode();
        }
        else {
            return false;
        }
    }
    
    public int getBillingCode() {
		return billingCode;
	}

	public void setBillingCode(int billingCode) {
		this.billingCode = billingCode;
	}

	//add by Will for JDK hashCode rule
    public int hashCode(){
    	return this.code;
    }
}
