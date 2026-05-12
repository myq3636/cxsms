package com.king.gmms.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.gmms.protocol.smpp.pdu.PDU;
import com.king.gmms.protocol.smpp.util.Data;

/**
 * Dedicated SMPP PDU logger that writes PDU packet details to a separate log file.
 * <p>
 * Usage: Call {@link #logReceived(String, String, PDU)} or {@link #logSent(String, String, PDU)}
 * from SMPP session classes. The output is written to a dedicated "smpp_pdu" logger
 * configured in log4j2.xml to write to its own file.
 * <p>
 * Log format:
 * <pre>
 * [RECV] [server] [session_name] cmd=submit_sm seq=1234 status=0x00000000 len=156 detail={...}
 * [SEND] [client] [session_name] cmd=deliver_sm_resp seq=1234 status=0x00000000 len=17
 * </pre>
 * 
 * This logger can be enabled/disabled independently via log4j2 configuration without
 * affecting the main application logs.
 */
public class SmppPduLogger {

    /**
     * A dedicated logger whose name matches the log4j2.xml configuration
     * for the PDU-specific appender.
     */
    private static final Logger pduLog = LogManager.getLogger("smpp.pdu.logger");

    private static final SmppPduLogger INSTANCE = new SmppPduLogger();

    private volatile boolean enabled = true;

    private SmppPduLogger() {}

    public static SmppPduLogger getInstance() {
        return INSTANCE;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Log a received PDU.
     *
     * @param role        "server" or "client"
     * @param sessionName name/id of the SMPP session
     * @param pdu         the received PDU
     */
    public void logReceived(String role, String sessionName, PDU pdu) {
        if (!enabled || pdu == null) {
            return;
        }
        try {
            pduLog.info("[RECV] [{}] [{}] cmd={} seq={} status=0x{} len={} detail={}",
                    role,
                    sessionName != null ? sessionName : "unknown",
                    getCommandName(pdu.getCommandId()),
                    pdu.getSequenceNumber(),
                    String.format("%08X", pdu.getCommandStatus()),
                    pdu.getCommandLength(),
                    pdu.debugString());
        } catch (Exception e) {
            // Never let PDU logging break business logic
        }
    }

    /**
     * Log a sent PDU.
     *
     * @param role        "server" or "client"
     * @param sessionName name/id of the SMPP session
     * @param pdu         the sent PDU
     */
    public void logSent(String role, String sessionName, PDU pdu) {
        if (!enabled || pdu == null) {
            return;
        }
        try {
            pduLog.info("[SEND] [{}] [{}] cmd={} seq={} status=0x{} len={} detail={}",
                    role,
                    sessionName != null ? sessionName : "unknown",
                    getCommandName(pdu.getCommandId()),
                    pdu.getSequenceNumber(),
                    String.format("%08X", pdu.getCommandStatus()),
                    pdu.getCommandLength(),
                    pdu.debugString());
        } catch (Exception e) {
            // Never let PDU logging break business logic
        }
    }

    /**
     * Translates a SMPP command ID integer to a human-readable name.
     */
    public static String getCommandName(int commandId) {
        switch (commandId) {
            case Data.BIND_TRANSMITTER:       return "bind_transmitter";
            case Data.BIND_TRANSMITTER_RESP:  return "bind_transmitter_resp";
            case Data.BIND_RECEIVER:          return "bind_receiver";
            case Data.BIND_RECEIVER_RESP:     return "bind_receiver_resp";
            case Data.BIND_TRANSCEIVER:       return "bind_transceiver";
            case Data.BIND_TRANSCEIVER_RESP:  return "bind_transceiver_resp";
            case Data.UNBIND:                 return "unbind";
            case Data.UNBIND_RESP:            return "unbind_resp";
            case Data.OUTBIND:                return "outbind";
            case Data.SUBMIT_SM:              return "submit_sm";
            case Data.SUBMIT_SM_RESP:         return "submit_sm_resp";
            case Data.DELIVER_SM:             return "deliver_sm";
            case Data.DELIVER_SM_RESP:        return "deliver_sm_resp";
            case Data.ENQUIRE_LINK:           return "enquire_link";
            case Data.ENQUIRE_LINK_RESP:      return "enquire_link_resp";
            case Data.GENERIC_NACK:           return "generic_nack";
            case Data.DATA_SM:                return "data_sm";
            case Data.DATA_SM_RESP:           return "data_sm_resp";
            case Data.QUERY_SM:               return "query_sm";
            case Data.QUERY_SM_RESP:          return "query_sm_resp";
            case Data.CANCEL_SM:              return "cancel_sm";
            case Data.CANCEL_SM_RESP:         return "cancel_sm_resp";
            case Data.REPLACE_SM:             return "replace_sm";
            case Data.REPLACE_SM_RESP:        return "replace_sm_resp";
            case Data.SUBMIT_MULTI:           return "submit_multi";
            case Data.SUBMIT_MULTI_RESP:      return "submit_multi_resp";
            case Data.ALERT_NOTIFICATION:     return "alert_notification";
            default:                          return "unknown_0x" + Integer.toHexString(commandId);
        }
    }
}
