package com.king.gmms.protocol.smpp.version;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 1.0
 */

import com.king.gmms.protocol.smpp.pdu.Address;
import com.king.gmms.protocol.smpp.util.Data;

public class SMPPVersion33 extends SMPPVersion {

    /**
     * Maximum number of bytes in an smppv3.3 message.
     */
    public static final int SHORTMESSAGE_LENGTH_MAX = 140;
    public static final int MESSAGEID_LENGTH_MAX = 9;

    public SMPPVersion33() {
        super(0x33, "SMPP version 3.3", MESSAGEID_LENGTH_MAX, SHORTMESSAGE_LENGTH_MAX);
    }


    public boolean isSupported(int commandID) {
        // Turn off the msb, which is used to signify a response packet..
        commandID &= 0x7fffffff;

        switch(commandID) {
            case Data.BIND_TRANSCEIVER:
            case Data.DATA_SM:
            case Data.ALERT_NOTIFICATION:
                return (false);

            default:
                return (true);
        }
    }


    public int getMaxLength(int field) {
        switch(field) {
            case MESSAGE_PAYLOAD:
                return (140);

            default:
                return (Integer.MAX_VALUE);
        }
    }

    public boolean validateAddress(Address s) {
        int ton = s.getTon();
        int npi = s.getNpi();
        return ((ton >= 0 && ton <= 0xff) && (npi >= 0 && npi <= 0xff)
                && s.getAddress().length() <= 20);
    }

    public boolean validateEsmClass(int c) {
        return (c >= 0 && c <= 0xff);
    }

    public boolean validateProtocolID(int id) {
        return (id >= 0 && id <= 0xff);
    }

    public boolean validateDataCoding(int dc) {
        return (dc >= 0 && dc <= 0xff);
    }

    public boolean validateDefaultMsg(int id) {
        return (id >= 0 && id <= 0xff);
    }

    public boolean validateMessageText(String text) {
        if(text != null) {
            return (text.length() <= lengthShortMessage);
        }
        else {
            return (true);
        }
    }

    public boolean validateMessage(byte[] message) {
        if(message != null) {
            return (message.length <= lengthShortMessage);
        }
        else {
            return (true);
        }
    }

    public boolean validateServiceType(String type) {
        return (type.length() <= 5);
    }

    public boolean validateMessageId(String id) {
        try {
            // Message IDs must be valid Hex numbers in v3.3
            if(id == null || "".equals(id)) {
                return true;
            }
            long n = Long.parseLong(id, 16);
            return (id.length() <= 9);
        }
        catch(NumberFormatException x) {
            return (false);
        }
    }

    public boolean validateMessageState(int st) {
        return (st >= 0 && st <= 0xff);
    }

    public boolean validateErrorCode(int code) {
        return (code >= 0 && code <= 0xff);
    }

    public boolean validatePriorityFlag(int flag) {
        return (flag == 0 || flag == 1);
    }

    public boolean validateRegisteredDelivery(int flag) {
        return (flag == 0 || flag == 1);
    }

    public boolean validateReplaceIfPresent(int flag) {
        return (flag == 0 || flag == 1);
    }

    public boolean validateNumberOfDests(int num) {
        return (num >= 0 && num <= 255);
    }

    public boolean validateNumUnsuccessful(int num) {
        return (num >= 0 && num <= 255);
    }

    public boolean validateDistListName(String name) {
        return (name.length() <= 20);
    }

    public boolean validateSystemId(String sysId) {
        return (sysId.length() <= 15);
    }

    public boolean validatePassword(String password) {
        return (password.length() <= 8);
    }

    public boolean validateSystemType(String sysType) {
        return (sysType.length() <= 12);
    }

    public boolean validateAddressRange(String addressRange) {
        // Possibly add some checks for allowed characters??
        return (addressRange.length() <= 40);
    }

    public boolean validateParamName(String paramName) {
        return (paramName.length() <= 31);
    }

    public boolean validateParamValue(String paramValue) {
        return (paramValue.length() <= 100);
    }
}
