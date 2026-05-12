/**
 * Author: frank.xue@King.com
 * Date: 2006-4-29
 * Time: 12:39:31
 * Document Version: 0.1
 */

package com.king.gmms.protocol;



import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;

public class MessageToolkit {
    public static final SimpleDateFormat SHORT_SDF_DAYFIRST = new SimpleDateFormat("ddMMyyHHmm");
    public static final SimpleDateFormat LONG_SDF_DAYFIRST = new SimpleDateFormat("ddMMyyHHmmss");
    public static final SimpleDateFormat SHORT_SDF_YEARFIRST = new SimpleDateFormat("yyMMddHHmm");
    public static final SimpleDateFormat LONG_SDF_YEARFIRST = new SimpleDateFormat("yyMMddHHmmss");

    public static String makeNumericString(String original, int digit) {
        String result;
        int length = original.length();
        if(length < digit) {
            StringBuilder zero = new StringBuilder();
            for(int i = 0; i < digit ; i++) {
                zero.append("0");
            }
            result = (zero + original);
        }
        else {
            result = new String(original);
        }
        length = result.length();
        result = result.substring(length - digit, length).toUpperCase();
        return result;
    }

    public static String makeNumericString(int original, int digit) {
        return makeNumericString(String.valueOf(original), digit);
    }

    public static String makeNumericString(boolean original) {
        return original ? "1" : "0";
    }

}
