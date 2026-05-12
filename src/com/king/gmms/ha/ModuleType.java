package com.king.gmms.ha;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum ModuleType {
	SERVER(1),
    CLIENT(2),
    ROUTER(3),
    SMG(4),
    MQM(5),
    DB(6);
	
	private int type = 1;
    private static Map<Integer, ModuleType> parseMap = new ConcurrentHashMap<Integer,ModuleType> (6);

    static {
        for (ModuleType type : ModuleType.values()) {
            parseMap.put(type.getType(), type);
        }
    }

    ModuleType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
   
    public static ModuleType parse(int value) {
        return parseMap.get(value);
    }
    /**
     * 
     * @return text type
     */
    public String toString(){
    	switch(type){
    		case 1:
    			return "Server";
    		case 2:
    			return "Client";
    		case 3:
    			return "Router";
    		case 4:
    			return "SMG";
    		case 5:
    			return "MQM";
    		case 6:
    			return "DB";
    	}
    	return null;
    }
}
