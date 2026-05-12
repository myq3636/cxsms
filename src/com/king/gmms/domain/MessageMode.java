package com.king.gmms.domain;

/**
 * Author: frank.xue@King.com
 * Date: 2006-8-23
 * Time: 11:55:45
 * Document Version: 0.1
 */
public enum MessageMode {
    STORE_FORWARD((byte) 0),
//    PROXY((byte) 1),
    DATAGRAM((byte) 2);

    private byte value;

    MessageMode(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static MessageMode parse(byte mode) {
        switch(mode) {
            case 2: {
                return DATAGRAM;
            }
//            case 1: {
//                return PROXY;
//            }
            default: {
                return STORE_FORWARD;
            }
        }
    }

    public static MessageMode parse(String mode) {
//        if("proxy".equalsIgnoreCase(mode) || "transaction".equalsIgnoreCase(mode)) {
//            return PROXY;
//        }
//        else 
    	if("datagram".equalsIgnoreCase(mode)) {
            return DATAGRAM;
        }
        else {
            return STORE_FORWARD;
        }
    }
}
