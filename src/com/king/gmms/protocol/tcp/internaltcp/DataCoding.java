package com.king.gmms.protocol.tcp.internaltcp;

import com.king.message.gmms.GmmsMessage;

public class DataCoding {

    public static String handleDatecoding(int dataCoding) {
        String charset = null;
        switch (dataCoding) {
        case 0:
        case 1:
            charset = GmmsMessage.AIC_CS_ASCII;
            break;
        case 3:
            charset = GmmsMessage.AIC_CS_ISO8859_1;
            break;
        case 6:
            charset = GmmsMessage.AIC_CS_ISO8859_5;
            break;
        case 7:
            charset = GmmsMessage.AIC_CS_ISO8859_8;
            break;
        case 8:
            charset = GmmsMessage.AIC_CS_UCS2;
            break;
        case 10:
            charset = GmmsMessage.AIC_CS_ISO2022_JP;
            break;
        case 11:
            charset = GmmsMessage.AIC_CS_GBK;
            break;
        case 13:
            charset = GmmsMessage.AIC_CS_EUCJP;
            break;
        case 14:
            charset = GmmsMessage.AIC_CS_KSC5601;
            break;
        case 15:
            charset = GmmsMessage.AIC_CS_UTF8;
            break;
        case 16:
            charset = GmmsMessage.AIC_CS_BIG5;
            break;
        default:
            charset = GmmsMessage.AIC_CS_ASCII;
        }
        return charset;
    }

    public static int getDataCoding(String contentType) {
        if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(contentType)) {
            return 1;
        }
        if (GmmsMessage.AIC_CS_ISO8859_1.equalsIgnoreCase(contentType)) {
            return 3;
        }
        if (GmmsMessage.AIC_CS_ISO8859_5.equalsIgnoreCase(contentType)) {
            return 6;
        }
        if (GmmsMessage.AIC_CS_ISO8859_8.equalsIgnoreCase(contentType)) {
            return 7;
        }
        if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(contentType)) {
            return 8;
        }
        if (GmmsMessage.AIC_CS_ISO2022_JP.equalsIgnoreCase(contentType)) {
            return 10;
        }
        if (GmmsMessage.AIC_CS_GBK.equalsIgnoreCase(contentType)) {
            return 11;
        }
        if (GmmsMessage.AIC_CS_EUCJP.equalsIgnoreCase(contentType)) {
            return 13;
        }
        if (GmmsMessage.AIC_CS_KSC5601.equalsIgnoreCase(contentType)) {
            return 14;
        }
        if (GmmsMessage.AIC_CS_UTF8.equalsIgnoreCase(contentType)) {
            return 15;
        }
        if (GmmsMessage.AIC_CS_BIG5.equalsIgnoreCase(contentType)) {
            return 16;
        }
        return 0;
    }
}
