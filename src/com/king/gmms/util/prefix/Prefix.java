package com.king.gmms.util.prefix;

import com.king.message.gmms.GmmsMessage;

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
public abstract class Prefix {
    /**
     * iOperation: add,remove,replace
     * aim: sender number = 1, recipient number = 2
     *
     */
    protected int iAim;


    public Prefix(int aim) {
        iAim = aim;
    }

    protected void setAddress(String[] addr, String value){

        if(value == null)
            return;

        if(iAim == 1){
            addr[0] = value;
//            gmmsMsg.setSenderAddress(value);
        }
        else if(iAim == 2){
            addr[1] = value;
//            gmmsMsg.setRecipientAddress(value);
        }
    }

//    protected void setAddress(GmmsMessage gmmsMsg, String value){
//
//        if(value == null)
//            return;
//
//        if(iAim == 1){
//            gmmsMsg.setSenderAddress(value);
//        }
//        else if(iAim == 2){
//            gmmsMsg.setRecipientAddress(value);
//        }
//    }

    protected String getAddress(String[] address){
        if(iAim == 1){
            return address[0];
//            return gmmsMsg.getSenderAddress();
        }
        else if(iAim == 2){
            return address[1];
//            return gmmsMsg.getRecipientAddress();
        }
        else{
            return null;
        }

    }
    public abstract boolean parseValue(String value);

    public abstract void handle(String[] address);

    public abstract String toString();
}
