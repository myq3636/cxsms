/*
 * ErrorCode.java
 *
 * Created on September 4, 2002, 1:28 PM
 */

package com.king.db;

/**
 *
 * @author  mike
 */
public final class ErrorCode implements java.io.Serializable {

    public static ErrorCode NO_MMO = new ErrorCode("NO_MMO", 1, "No matching MMO record");
    public static ErrorCode NO_MMR = new ErrorCode("NO_MMR", 2, "No matching MMR record");
    public static ErrorCode DUPLICATE = new ErrorCode("DUPLICATE", 3, "Duplicated record");
    public static ErrorCode INCOMPLETE = new ErrorCode("INCOMPLETE", 4, "Incomplete record");
    public static ErrorCode INCOMPATIBLE = new ErrorCode("INCOMPATIBLE", 5, "Incompatible record");
    public static ErrorCode BAD_STATUS = new ErrorCode("BAD_STATUS", 6, "Bad status record");
    public static ErrorCode IRRESOLVABLE_ORG_RS_ADDRESS = new ErrorCode("IRRESOLVABLE_ORG_RS_ADDRESS", 7, "Irresolvable originator RS address");
    public static ErrorCode IRRESOLVABLE_RCPT_RS_ADDRESS = new ErrorCode("IRRESOLVABLE_RCPT_RS_ADDRESS", 8, "Irresolvable recipient RS address");
    public static ErrorCode NO_AGREEMENT_FOUND = new ErrorCode("NO_AGREEMENT_FOUND", 9, "No Bilateral Agreement found");
    public static ErrorCode NO_RATE_TIER_FOUND = new ErrorCode("NO_RATE_TIER_FOUND", 10, "No rating tier found");
    public static ErrorCode DATACOLLECT_FTPCLIENT_FAIL = new ErrorCode("DATACOLLECT_FTPCLIENT_FAIL", 1001, "FTP Client fail");
    public static ErrorCode PARSE_FAIL_NOCDRPROJ = new ErrorCode("PARSE_FAIL_NOCDRPROJ", 2001, "There is no CDRProj available");
    public static ErrorCode PARSE_FAIL_DECODE = new ErrorCode("PARSE_FAIL_DECODE", 2002, "Can't decode file");
    public static ErrorCode PARSE_FAIL_CONVERT = new ErrorCode("PARSE_FAIL_CONVERT", 2003, "Neither originator CDR, nor recipient CDR");
    public static ErrorCode IRRESOLVABLE_CUSTOMER = new ErrorCode("IRRESOLVABLE_CUSTOMER", 3001, "Unable to resolve customer");
    public static ErrorCode IRRESOLVABLE_COUNTRY = new ErrorCode("IRRESOLVABLE_COUNTRY", 3002, "Unable to resolve country");
    public static ErrorCode IRRESOLVABLE_REGION = new ErrorCode("IRRESOLVABLE_REGION", 3003, "Unable to resolve region");
    public static ErrorCode INEFFECTIVE_SERVICE_DATE = new ErrorCode("INEFFECTIVE_SERVICE_DATE", 3004, "Ineffective service date");
    public static ErrorCode CONFUSING = new ErrorCode("CONFUSING", 11, "Confusing record");

    private static ErrorCode[] ErrorCodes = new ErrorCode[]{
                                                    NO_MMO,
                                                    NO_MMR,
                                                    DUPLICATE,
                                                    INCOMPLETE,
                                                    INCOMPATIBLE,
                                                    BAD_STATUS,
                                                    IRRESOLVABLE_ORG_RS_ADDRESS,
                                                    IRRESOLVABLE_RCPT_RS_ADDRESS,
                                                    NO_AGREEMENT_FOUND,
                                                    NO_RATE_TIER_FOUND,
                                                    DATACOLLECT_FTPCLIENT_FAIL,
                                                    PARSE_FAIL_NOCDRPROJ,
                                                    PARSE_FAIL_DECODE,
                                                    PARSE_FAIL_CONVERT,
                                                    IRRESOLVABLE_CUSTOMER,
                                                    IRRESOLVABLE_COUNTRY,
                                                    IRRESOLVABLE_REGION,
                                                    INEFFECTIVE_SERVICE_DATE,
                                                    CONFUSING };
    private int mvalue = -1;
    private String mname = "";
    private String mdescription = "";

    private ErrorCode(String name,  int value, String description) {
        mname = name;
        mvalue = value;
        mdescription = description;
    }

    public String getName(){
        return mname;
    }

    public int getValue(){
        return mvalue;
    }

    public String getDescription(){
        return mdescription;
    }

    public static ErrorCode parse(String errorName){
        if (errorName== null) errorName = "";
        errorName = errorName.toUpperCase();
        for(int i =0; i< ErrorCodes.length;i++){
            if(errorName.equals(ErrorCodes[i])) return ErrorCodes[i];
        }
        return null;
    }

}
