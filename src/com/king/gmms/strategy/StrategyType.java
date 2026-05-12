package com.king.gmms.strategy;

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
public enum StrategyType {
    Primary,
    OriginalWay,
    Random,
    LoadBalance,
    SameIP,
    ManualSwitch,
    InternalSameSession,
    SameSession;
    

    public static StrategyType getStrategyType(String type) {
        if(null == type || "".equals(type)){
            return Random;
        }
        else if (type.equalsIgnoreCase("primary")) {
            return Primary;
        }
        else if (type.equalsIgnoreCase("random")) {
            return Random;
        }
        else if (type.equalsIgnoreCase("loadbalance")) {
            return LoadBalance;
        }
        else if (type.equalsIgnoreCase("originalway")) {
            return OriginalWay;
        }
        else if (type.equalsIgnoreCase("SameIP")) {
            return SameIP;
        }
        else if (type.equalsIgnoreCase("SameSession")) {
            return SameSession;
        }
        else if(type.equalsIgnoreCase("InternalSameSession")) {
            return InternalSameSession;
        }
        else {
            return Random;
        }
    }
}
