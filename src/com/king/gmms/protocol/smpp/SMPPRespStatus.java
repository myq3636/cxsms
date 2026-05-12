package com.king.gmms.protocol.smpp;

import com.king.gmms.protocol.smpp.util.Data;
import com.king.message.gmms.GmmsStatus;

/**
 * <p>Title: SMPPRespStatus </p>
 * <p>Description: mapping the smpp status code with King's code</p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 *
 * @version 6.1
 * @author: Jesse Duan
 */
public class SMPPRespStatus {
    /**
     * return gmmsMessage status by error code
     *
     * @param code int
     * @return GmmsStatus
     */
    public static GmmsStatus getGmmsStatus(int code) {
        switch(code) {
            case 0:
                return GmmsStatus.SUCCESS;
            case 0x00000003:
                return GmmsStatus.OPER_UNAVAILABLE;
            case 0x00000001:
            case 0x00000002:
            case 0x00000006:
            case 0x00000007:
            case 0x0000000C:
            case 0x00000015:
            case 0x00000040:
            case 0x00000042:
            case 0x00000043:
            case 0x00000048:
            case 0x00000049:
            case 0x00000050:
            case 0x00000051:
            case 0x00000053:
            case 0x00000054:
            case 0x00000055:
            case 0x00000061:
            case 0x00000062:
            case 0x00000063:
            case 0x000000C0:
            case 0x000000C1:
            case 0x000000C2:
            case 0x000000C3:
            case 0x000000C4:
                return GmmsStatus.INVALID_MSG_FIELD;
//            case 0x00000011:
            case 0x00000045:
            case 0x00000065:
//            case 0x000000FE:
                return GmmsStatus.UNDELIVERED;
            case 0x0000000A:
//            case 0x00000048:
//            case 0x00000049:
                return GmmsStatus.SENDER_ADDR_ERROR;
            case 0x0000000B:
            case 0x00000033:
            case 0x00000034:
            case 0x00000044:
//            case 0x00000050:
//            case 0x00000051:
                return GmmsStatus.RECIPIENT_ADDR_ERROR;
            case 0x00000014:
            case 0x00000058:
                return GmmsStatus.Throttled;
            case 0x00000067:
                return GmmsStatus.FAIL_QUERY_DELIVERREPORT;
            case 0x00000004:
            case 0x00000005:
            case 0x00000011:
            case 0x0000000D:
            case 0x00000013:
            case 0x000000FE:
                return GmmsStatus.COMMUNICATION_ERROR;
            case 0x00000008:
            case 0x00000064:
                return GmmsStatus.SERVER_ERROR;
            case 0x00000066:
                return GmmsStatus.POLICY_DENIED;
            case 0x0000000E:
            case 0x0000000F:
                return GmmsStatus.AUTHENTICATION_ERROR;
//            case 0x00000016:
//            	return GmmsStatus.INVALID_SERVICETYPEID;
//            case 0x00000017:
//            	return GmmsStatus.DUPLICATE_MSG;
            default:
                return GmmsStatus.UNKNOWN_ERROR;
        }
    }

    /**
     * return smpp status by error code
     *
     * @param gmmsCode int
     * @return SMPPRespStatus
     */
    public static int getSmppStatus(int gmmsCode) {
        switch(gmmsCode) {
        	case -1:
            case 0:
                return Data.ESME_ROK;
            case 2100:
            case 2130:
                return Data.ESME_RINVMSGLEN;
            case 2150:
            	return Data.ESME_RINVSERTYPID;
            case 2160:
            	return Data.ESME_RDUPLICATEMSG;
            case 2170:
            	return Data.ESME_RINVSCHED;
            case 2110:
                return Data.ESME_RINVSRCADR;
            case 2120:
                return Data.ESME_RINVDSTADR;
            case 2200:
            case 2210:
            case 2220:
                return Data.ESME_RX_R_APPN;
            case 2230:
            case 2600:
                return Data.ESME_RTHROTTLED;
            case 2300:
                return Data.ESME_RSUBMITFAIL;
            case 2500:
                return Data.ESME_RSYSERR;
            case 3000:
            case 4000:
                 return Data.ESME_RSYSERR;
            case 5000:
                 return Data.ESME_RINVPASWD;
            case 9005:
                 return Data.ESME_RQUERYFAIL;
            case 9000:
            default:
                return Data.ESME_RUNKNOWNERR;
        }
    }

}
