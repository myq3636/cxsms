/*
 * DeliveryReceiptParser.java
 *
 * Created on January 10, 2003, 4:53 PM
 */

package com.king.gmms.protocol.smpp;

import java.text.SimpleDateFormat;

/**
 * @author mike
 */
public class DeliveryReceiptParser {
    public static final int VENDER_DEFAULT = 0;
    public static final int VENDER_ATT = 1;
    public static final int VENDER_CMCC = 2;
    private int vender = 0;

    /**
     * Creates a new instance of DeliveryReceiptParser
     */
    public DeliveryReceiptParser(int vender) {
        if(vender > VENDER_CMCC) {
            vender = VENDER_DEFAULT;
        }
        else {
            this.vender = vender;
        }
    }

    public DeliveryReceipt parse(String receiptStr) throws InvalidDeliveryReceiptException {
        switch(vender) {
            case VENDER_ATT:
                return parseATT(receiptStr);
            case VENDER_CMCC:
                // Assume CMCC don't use String for delivery receipt
                return null;
            default:
                return null;
        }
    }

    public DeliveryReceipt parse(byte[] bytes) throws InvalidDeliveryReceiptException {
        switch(vender) {
            case VENDER_ATT:
                // Assume they don't use binary delivery receipt
                return null;
            case VENDER_CMCC:
                //return parseCMCC(bytes);
            default:
                return null;
        }
    }

    private DeliveryReceipt parseATT(String receiptStr) throws InvalidDeliveryReceiptException {
        return parseDefaultSmppReceipt(receiptStr);
    }


    private DeliveryReceipt parseDefaultSmppReceipt(String receiptStr) throws InvalidDeliveryReceiptException {
        DeliveryReceipt receipt = new DeliveryReceipt();
        try {
            receipt.setMsgID(receiptStr.substring(receiptStr.indexOf("id:") + 3, receiptStr.indexOf("sub:") - 1));
            receipt.setSub(receiptStr.substring(receiptStr.indexOf("sub:") + 4, receiptStr.indexOf("dlvrd:") - 1));
            receipt.setDlvrd(receiptStr.substring(receiptStr.indexOf("dlvrd:") + 6, receiptStr.indexOf("submit date:") - 1));

            SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmm");
            receipt.setSubmitDate(df.parse(receiptStr.substring(receiptStr.indexOf("submit date:") + 12, receiptStr.indexOf("done date:") - 1)));
            receipt.setDoneDate(df.parse(receiptStr.substring(receiptStr.indexOf("done date:") + 10, receiptStr.indexOf("stat:") - 1)));

            receipt.setStat(receiptStr.substring(receiptStr.indexOf("stat:") + 5, receiptStr.indexOf("err:") - 1));
            receipt.setErr(receiptStr.substring(receiptStr.indexOf("err:") + 4, receiptStr.indexOf("Text:") - 1));
            receipt.setText(receiptStr.substring(receiptStr.indexOf("Text:") + 5));
        }
        catch(Exception e) {
            throw new InvalidDeliveryReceiptException(e);
        }
        return receipt;
    }

    public static String formatDefault(DeliveryReceipt receipt) {
        SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmm");
        String receiptStr = "";

        receiptStr += "id:" + (receipt.getMsgID() != null ? receipt.getMsgID() : "") + " ";
        receiptStr += "sub:" + (receipt.getSub() != null ? receipt.getSub() : "") + " ";
        receiptStr += "dlvrd:" + (receipt.getDlvrd() != null ? receipt.getDlvrd() : "") + " ";
        receiptStr += "submit date:" + df.format(receipt.getSubmitDate()) + " ";
        receiptStr += "done date:" + df.format(receipt.getDoneDate()) + " ";
        receiptStr += "stat:" + (receipt.getStat() != null ? receipt.getStat() : "") + " ";
        receiptStr += "err:" + (receipt.getErr() != null ? receipt.getErr() : "") + " ";
        receiptStr += "Text:" + (receipt.getText() != null ? receipt.getText() : "");

        return receiptStr;
    }

}
