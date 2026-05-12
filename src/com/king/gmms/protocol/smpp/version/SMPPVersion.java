package com.king.gmms.protocol.smpp.version;

/**
 * <p>Title: SMPPVersion</p>
 * <p>Description: Class representing an SMPP protocol version. Instances of this
 * object are used by the rest of the API to determine is an SMPP message is
 * supported by a certain version of the protocol.</p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 1.0
 */

import com.king.gmms.protocol.smpp.pdu.Address;

import java.util.HashMap;

public abstract class SMPPVersion {

    public int lengthMsgId;
    public int lengthShortMessage;
//    public int lengthMsgId = 65;
//    public int lengthShortMessage = 254;

    /**
     * Constant representing the message payload mandatory parameter.
     */
    public static final int MESSAGE_PAYLOAD = 5;

    /**
     * Static SMPPVersion instance representing version SMPP v3.3.
     */
    public static final SMPPVersion V33 = new SMPPVersion33();

    /**
     * Static SMPPVersion instance representing version SMPP v3.4.
     */
    public static final SMPPVersion V34 = new SMPPVersion34();

    /**
     * Integer representing this version number. The SMPP specification states
     * integer values that represent protocol revisions. These values are used
     * mainly in the bind_* and bind response messages. Integer value 0x33
     * represents version 3.3 of the protocol, integer value 0x34 represents
     * version 3.4...it's assumed further major and minor revisions of the SMPP
     * specification will continue this numbering scheme.
     */
    private int versionID = 0;

    /**
     * Descriptive text for this protocol version. This value is used only to
     * return a representative string from toString.
     */
    private String versionString = null;

    public static HashMap hmSmppVer = new HashMap();

    /**
     * Create a new SMPPVersion object.
     *
     * @param versionID          int
     * @param versionString      String
     * @param lengthMsgId        int
     * @param lengthShortMessage int
     */
    protected SMPPVersion(int versionID, String versionString, int lengthMsgId, int lengthShortMessage) {
        this.versionID = versionID;
        this.versionString = versionString;
        this.lengthMsgId = lengthMsgId;
        this.lengthShortMessage = lengthShortMessage;
    }


    /**
     * Get an object representing the default version of the API, which is 3.4.
     *
     * @return SMPPVersion
     */
    public static final SMPPVersion getDefaultVersion() {
        return (V34);
    }

    public final SMPPVersion getVersion(int id) throws VersionException {
        if(id == V33.getVersionID()) {
            return (V33);
        }
        else if(id == V34.getVersionID()) {
            return (V34);
        }
        else {
            return (V33);
        }
    }

    /**
     * Get the integer value for this protocol version object.
     *
     * @return int
     */
    public int getVersionID() {
        return (versionID);
    }

    /**
     * Check if a version is older than this one. If <code>ver</code> is equal
     * to this version, false will be returned.
     *
     * @param ver SMPPVersion
     * @return boolean
     */
    public boolean isOlder(SMPPVersion ver) {
        return (ver.versionID < this.versionID);
    }

    /**
     * Check if a version is newer than this one. If <code>ver</code> is equal
     * to this version, false will be returned.
     *
     * @param ver SMPPVersion
     * @return boolean
     */
    public boolean isNewer(SMPPVersion ver) {
        return (ver.versionID > this.versionID);
    }

    /**
     * Test another SMPPVersion object for equality with this one.
     *
     * @param obj Object
     * @return boolean
     */
    public boolean equals(Object obj) {
        if(obj instanceof SMPPVersion) {
            return (((SMPPVersion) obj).versionID == this.versionID);
        }
        else {
            return (false);
        }
    }

    /**
     * Test <code>versionNum</code> is the numeric representation of this SMPP
     * version.
     *
     * @param versionNum int
     * @return boolean
     */
    public boolean equals(int versionNum) {
        return (versionNum == this.versionID);
    }

    /**
     * Return a descriptive string of this protocol version.
     *
     * @return String
     */
    public String toString() {
        return (versionString);
    }

    /**
     * Get the maximum allowed length for a particular field. XXX allow an
     * exception to be thrown for unidentified fields.
     *
     * @param field int
     * @return int
     */
    public abstract int getMaxLength(int field);

    /**
     * Determine if a particular command is supported by this protocol version.
     * This method takes any valid SMPP command ID (for both requests and
     * responses) and returns true or false based on whether the protocol
     * version this object represents supports that command or not.
     *
     * @param commandID the SMPP command ID to check support for.
     * @return boolean
     */
    public abstract boolean isSupported(int commandID);

    /**
     * Validate and SMPP address for this SMPP version number.
     *
     * @param s Address
     * @return boolean
     */
    public abstract boolean validateAddress(Address s);

    /**
     * Validate the ESM class mandatory parameter.
     *
     * @param c int
     * @return boolean
     */
    public abstract boolean validateEsmClass(int c);

    /**
     * Validate the Protocol ID mandatory parameter.
     *
     * @param id int
     * @return boolean
     */
    public abstract boolean validateProtocolID(int id);

    /**
     * Validate the data coding mandatory parameter.
     *
     * @param dc int
     * @return boolean
     */
    public abstract boolean validateDataCoding(int dc);

    /**
     * Validate the default message ID mandatory parameter.
     *
     * @param id int
     * @return boolean
     */
    public abstract boolean validateDefaultMsg(int id);

    /**
     * Validate the message text length.
     *
     * @param text String
     * @return boolean
     */
    public abstract boolean validateMessageText(String text);

    /** Validate the length of the message bytes.
     */
    //public abstract boolean validateMessage(byte[] message, MessageEncoding encoding);

    /**
     * Validate the service type mandatory parameter.
     *
     * @param type String
     * @return boolean
     */
    public abstract boolean validateServiceType(String type);

    /**
     * Validate the message ID mandatory parameter.
     *
     * @param id String
     * @return boolean
     */
    public abstract boolean validateMessageId(String id);

    /**
     * Validate the message state mandatory parameter. The message state and
     * message status are the same. The name of the parameter changed between
     * version 3.3 and version 3.4. The semantics, however, remain the same.
     *
     * @param st int
     * @return boolean
     */
    public final boolean validateMessageStatus(int st) {
        return (validateMessageState(st));
    }

    /**
     * Validate the message state mandatory parameter. The message state and
     * message status are the same. The name of the parameter changed between
     * version 3.3 and version 3.4. The semantics, however, remain the same.
     *
     * @param state int
     * @return boolean
     */
    public abstract boolean validateMessageState(int state);

    /**
     * Validate the error code mandatory parameter.
     *
     * @param code int
     * @return boolean
     */
    public abstract boolean validateErrorCode(int code);

    public abstract boolean validatePriorityFlag(int flag);

    public abstract boolean validateRegisteredDelivery(int flag);

    public abstract boolean validateReplaceIfPresent(int flag);

    public abstract boolean validateNumberOfDests(int num);

    public abstract boolean validateNumUnsuccessful(int num);

    public abstract boolean validateDistListName(String name);

    public abstract boolean validateSystemId(String sysId);

    public abstract boolean validatePassword(String password);

    public abstract boolean validateSystemType(String sysType);

    public abstract boolean validateAddressRange(String addressRange);

    public abstract boolean validateParamName(String paramName);

    public abstract boolean validateParamValue(String paramValue);
}
