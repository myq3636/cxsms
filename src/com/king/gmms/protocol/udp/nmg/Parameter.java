/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

/**
 * @author bensonchen
 * @version 1.0.0
 */
import com.king.framework.SystemLogger;

public class Parameter {
    private static SystemLogger log = SystemLogger.getSystemLogger(Pdu.class);

    private int id = -1;
    private int length = 0;

    private String value = null;
    private byte[] byteValue = null;
    private int intValue;
    private String type = STRING_TYPE;

    public static String STRING_TYPE = "string";
    public static String BYTES_TYPE = "bytes";
    public static String INT_1BYTE_TYPE = "int1byte";
    public static String INT_2BYTE_TYPE = "int2byte";
    public static String INT_4BYTE_TYPE = "int4byte";


    public Parameter() {
    }

    public Parameter(int id, String value) {
        this.id = id;
        this.value = value;
        if (value == null ){
            length = 0;
        }
        else{
            this.length = value.getBytes().length;
        }
        this.type = STRING_TYPE;
    }

    public Parameter(int id, int value) {
        this(id, value, 2);
    }

    public Parameter(int id, int value, int length) {
        this.id = id;
        this.intValue = value;

        if(length==1){
           this.length = 1;
           this.type = INT_1BYTE_TYPE;
        }
        else if(length==2){
           this.length = 2;
           this.type = INT_2BYTE_TYPE;
        }
        else if(length==4){
           this.length = 4;
           this.type = INT_4BYTE_TYPE;
        }
        else{
            this.length = 4;
            this.type = INT_4BYTE_TYPE;
        }
    }


    public Parameter(int id, byte[] byteValue) {
        this.id = id;
        this.length = byteValue.length;
        this.byteValue = byteValue;
        this.type = BYTES_TYPE;
    }

    //Specially for PayLoad parameter
    public Parameter(int id, byte[] byteValue, int length){
        this.id = id;
        this.length = length;
        this.byteValue = byteValue;
        this.type = BYTES_TYPE;
    }

    public UdpByteBuffer toByteBuffer() {

    	UdpByteBuffer buffer = new UdpByteBuffer();

        try {
            buffer.appendIntAs1Byte(id);

            if(STRING_TYPE.equalsIgnoreCase(this.type)){
                buffer.appendIntAs2Byte(length);
                if(length!=0)
                    buffer.appendString(this.value);
            }
            else if(BYTES_TYPE.equalsIgnoreCase(this.type)){
                buffer.appendIntAs2Byte(length);
                buffer.appendBytes(new UdpByteBuffer(byteValue));
            }
            else if(INT_1BYTE_TYPE.equalsIgnoreCase(this.type)){
                buffer.appendIntAs2Byte(1);
                buffer.appendIntAs1Byte(this.intValue);
            }
            else if(INT_2BYTE_TYPE.equalsIgnoreCase(this.type)){
                buffer.appendIntAs2Byte(2);
                buffer.appendIntAs2Byte(this.intValue);
            }
            else if(INT_4BYTE_TYPE.equalsIgnoreCase(this.type)){
                buffer.appendIntAs2Byte(4);
                buffer.appendInt(this.intValue);
            }

        } catch (Exception e) {
            log.error(e, e);
        }
        return buffer;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setByteValue(byte[] byteValue) {
        this.byteValue = byteValue;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public String getValue() {
        return value;
    }

    public byte[] getByteValue() {
        return byteValue;
    }

    public int getLength() {
        return length;
    }

    public int getIntValue() {
        return intValue;
    }

    public String getType() {
        return type;
    }
}
