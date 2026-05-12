package com.king.gmms.connectionpool;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public enum BindMode {
    Transmitter {
        public String toString() {
        return "t";
    }
},
    Receiver {
    public String toString() {
    return "r";
}
},
    Transceiver {
    public String toString() {
    return "tr";
}
} ;
    public static BindMode getBindMode(String name) {
        if (name == null || name.length() < 0) {
            return BindMode.Transceiver;
        }
        else if (name.equalsIgnoreCase("Transmitter") || name.equalsIgnoreCase("T")) {
            return BindMode.Transmitter;
        }
        else if (name.equalsIgnoreCase("Receiver")|| name.equalsIgnoreCase("R")) {
            return BindMode.Receiver;
        }
        else {
            return BindMode.Transceiver;
        }
    }
}
