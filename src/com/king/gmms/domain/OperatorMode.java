package com.king.gmms.domain;

public enum OperatorMode {
    Common((byte) 0),
    Store((byte) 1),
    ROPBusy((byte) 2),
    ROPFailed((byte) 3);

    private byte value;

    OperatorMode(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static OperatorMode parse(byte mode) {
        switch(mode) {
            case 3: {
                return ROPFailed;
            }
            case 2: {
                return ROPBusy;
            }
            case 1: {
                return Store;
            }
            default: {
                return Common;
            }
        }
    }

    public static OperatorMode parse(String mode) {
        if("proxy".equalsIgnoreCase(mode) || "transaction".equalsIgnoreCase(mode)) {
            return Common;
        }
        else if("datagram".equalsIgnoreCase(mode)) {
            return Common;
        }
        else {
            return Common;
        }
    }
}
