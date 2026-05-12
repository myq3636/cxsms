package com.king.gmms.ha.systemmanagement;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2007</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public enum SystemMessageType {

    KEEP_A_LIVE(1),
    ACK(2),
    IN_BIND_REQUEST(3),
    IN_BIND_RESPONSE(4),
    OUT_BIND_REQUEST(5),
    OUT_BIND_RESPONSE(6),
    CONNECTION_STATUS_NOTIFICATION(7),
    CONNECTION_STATUS_NOTIFICATION_ACK(8),
    CONNECTION_CONFIRM(9),
    MODULE_REGISTER_REQUEST(10),
    MODULE_REGISTER_RESPONSE(11),
    MODULE_STOP_REQUEST(12),
    MODULE_STOP_RESPONSE(13),
    DB_OPERATION_REQUEST(14),
    DB_OPERATION_RESPONSE(15),
    DB_CHECK_REQUEST(16),
    DB_CHECK_RESPONSE(17),
    CHANGEDB(18);


    private int type = 1;
    private static Map<Integer, SystemMessageType> parseMap = new ConcurrentHashMap<Integer,
        SystemMessageType> (18);

    static {
        for (SystemMessageType type : SystemMessageType.values()) {
            parseMap.put(type.getType(), type);
        }
    }

    SystemMessageType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
    /**
     * is response
     * @return
     */
    public boolean isResponse(){
    	switch(type){
	    	case 2:
	    	case 4:
	    	case 6:
	    	case 8:
	    	case 9:
	    	case 11:
	    	case 13:
	    	case 15:
	    	case 17:
	    		return true;
	    	default:
	    		return false;
    	}
    }
    public static SystemMessageType parse(int value) {
        return parseMap.get(value);
    }
}
