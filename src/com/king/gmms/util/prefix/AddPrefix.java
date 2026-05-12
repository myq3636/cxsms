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
public class AddPrefix extends Prefix {

    private String sAddValue = null;

    public AddPrefix(int aim){
        super(aim);
    }

    /**
     * handl
     *
     * @param gmmsMsg GmmsMessage
     * @todo Implement this prefix.Prefix method
     */
    public void handle(String[] address) {
//        if(gmmsMsg == null)
//            return;

        String addr = super.getAddress(address);

        if(addr == null)
            return;
        addr = addPrefix(addr,this.sAddValue);

        super.setAddress(address,addr);
    }

    /**
     * parseValue
     *
     * @param value String
     * @return boolean
     * @todo Implement this prefix.Prefix method
     */
    public boolean parseValue(String value) {
        if(value == null || value.equals(""))
            return false;

        boolean bResult = false;

        this.sAddValue = value.trim();

        bResult = true;
        return bResult;
    }

    private String addPrefix(String addr, String prefix) {
        if (prefix == null) {
            return addr;
        }
        if (addr != null) {
            return prefix + addr;
        }
        return addr;
    }

    public String toString() {
    	String prefix = ";";
        StringBuffer buf = new StringBuffer();
        buf.append("{iOperation: add");
        buf.append(prefix);
        buf.append("iAim:" + this.iAim);
        buf.append(prefix);
        buf.append("add value:" + this.sAddValue);
        buf.append("}");
        return buf.toString();

    }
}
