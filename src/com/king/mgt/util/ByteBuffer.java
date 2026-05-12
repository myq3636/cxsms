package com.king.mgt.util;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;

public class ByteBuffer {
    private byte[] buffer;

    public static final byte SZ_BYTE = 1;
    public static final byte SZ_SHORT = 2;
    public static final byte SZ_INT = 4;
    public static final byte SZ_LONG = 4;

    private static byte[] zero;
    private static SystemLogger log = SystemLogger.getSystemLogger(ByteBuffer.class);
    static {
        zero = new byte[1];
        zero[0] = 0;
    }

    public ByteBuffer() {
        buffer = null;
    }

    public ByteBuffer(byte[] buffer) {
        this.buffer = buffer;
    }
    public ByteBuffer(byte[] buffer, int index, int len)
    {
        this.buffer=new byte[len];
        System.arraycopy(buffer, index,this.buffer, 0, len);
    }
    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public int length() {
        if(buffer == null) {
            return 0;
        }
        else {
            return buffer.length;
        }
    }

    public void appendByte(byte data) {
        byte[] byteBuf = new byte[SZ_BYTE];
        byteBuf[0] = data;
        appendBytes0(byteBuf, SZ_BYTE);
    }

    public void appendBytes(byte[] data) {
        appendBytes0(data, data.length);
    }

    public void appendIntAsByte(int data) {
        byte[] byteBuf = new byte[SZ_BYTE];
        byteBuf[0] = (byte) (data & 0xff);
        appendBytes0(byteBuf, SZ_BYTE);
    }

    public void appendIntAsBytes(int data, int len) {
        byte[] byteBuf = new byte[len];
        for(int i = 0; i < len ; i++) {
            byteBuf[len - 1 - i] = (byte) (data & 0xff);
            data = data >>> 8;
        }
        appendBytes0(byteBuf, len);
    }

    public void appendShort(short data) {
        byte[] shortBuf = new byte[SZ_SHORT];
        shortBuf[1] = (byte) (data & 0xff);
        shortBuf[0] = (byte) ( (data >>> 8) & 0xff);
        appendBytes0(shortBuf, SZ_SHORT);
    }
    public void appendInt(int data) {
        byte[] intBuf = new byte[SZ_INT];
        intBuf[3] = (byte) (data & 0xff);
        intBuf[2] = (byte) ((data >>> 8) & 0xff);
        intBuf[1] = (byte) ((data >>> 16) & 0xff);
        intBuf[0] = (byte) ((data >>> 24) & 0xff);
        appendBytes0(intBuf, SZ_INT);
    }

    public void appendLong(long data) {
        byte[] longBuf = new byte[SZ_LONG];
        longBuf[3] = (byte) (data & 0xff);
        longBuf[2] = (byte) ((data >>> 8) & 0xff);
        longBuf[1] = (byte) ((data >>> 16) & 0xff);
        longBuf[0] = (byte) ((data >>> 24) & 0xff);
        appendBytes0(longBuf, SZ_LONG);
    }

    public void appendString(String string) {
        try {
            appendString(string, string.length(), "ASCII");
        }
        catch(UnsupportedEncodingException e) {
            log.debug(e,e);
            // this can't happen as we use ASCII encoding
            // whatever is in the buffer it gets interpreted as ascii
        }
    }

    public void appendString(String string, int len) {
        try {
            appendString(string, len, "ASCII");
        }
        catch(UnsupportedEncodingException e) {
                        log.debug(e,e);
            // this can't happen as we use ASCII encoding
            // whatever is in the buffer it gets interpreted as ascii
        }
    }

    private void appendBytes0(byte[] bytes, int count) {
        int len = length();
        byte[] newBuf = new byte[len + count];
        if(len > 0) {
            System.arraycopy(buffer, 0, newBuf, 0, len);
        }
        System.arraycopy(bytes, 0, newBuf, len, count);
        setBuffer(newBuf);
    }

    public void appendString(String string, int len, String encoding) throws
                                                                      UnsupportedEncodingException {
        UnsupportedEncodingException encodingException = null;
        if (len<=0) return;
        if((string != null) && (string.length() > 0)) {
            if(string.length() > len) {
                string = string.substring(0, len - 1);
            }
            byte[] stringBuf = new byte[len];
            byte[] tmpByte = null;
            try {
                if(encoding != null) {
                    tmpByte = string.getBytes(encoding);
                }
                else {
                    tmpByte = string.getBytes();
                }
                System.arraycopy(tmpByte, 0, stringBuf, 0, tmpByte.length);
            }
            catch(UnsupportedEncodingException e) {
                //                   debug.write("Unsupported encoding exception "+e);
                //                   event.write(e,null);
                            log.debug(e,e);
                encodingException = e;
            }
            if((stringBuf != null) && (stringBuf.length > 0)) {
                appendBytes0(stringBuf, len);
            }
        }else{
            byte[] stringBuf = new byte[len];
            appendBytes0(stringBuf, len);
        }
        if(encodingException != null) {
            throw encodingException;
        }
    }
    private void insertBytes0(byte[] bytes, int desIndex, int count)
    {
        int len = length();
        if (bytes==null) throw new NullPointerException("source byte array is null.");
        if (desIndex<0 || desIndex>len || count<0) throw new IndexOutOfBoundsException();
        byte[] newBuffer=new byte[len+count];
        if (desIndex>0)
            System.arraycopy(buffer,0,newBuffer,0,desIndex);
        if (count>0)
            System.arraycopy(bytes, 0, newBuffer, desIndex, count);
        if (desIndex<len)
            System.arraycopy(buffer, desIndex, newBuffer, desIndex+count, len-desIndex);
        this.setBuffer(newBuffer);
    }
    public byte removeByte() throws NotEnoughDataInByteBufferException {
        byte result = 0;
        byte[] resBuff = removeBytes(SZ_BYTE).getBuffer();
        result = resBuff[0];
        return result;
    }
    public short removeShort() throws NotEnoughDataInByteBufferException
    {
        short result=this.readShort();
        removeBytes0(SZ_SHORT);
        return result;
    }
    public int removeInt() throws NotEnoughDataInByteBufferException {
        int result = readInt();
        removeBytes0(SZ_INT);
        return result;
    }

    public int removeLong() throws NotEnoughDataInByteBufferException {
        int result = readInt();
        removeBytes0(SZ_LONG);
        return result;
    }

    public byte readByte(){
        return buffer[0];
    }

    public short readShort() throws NotEnoughDataInByteBufferException {
            short result = 0;
            int len = length();
            if(len >= SZ_SHORT) {
                result |= buffer[0] & 0xff;
                result <<= 8;
                result |= buffer[1] & 0xff;
                return result;
            }
            else {
                throw new NotEnoughDataInByteBufferException(len, 2);
            }
    }

    public int readInt() throws NotEnoughDataInByteBufferException {
        int result = 0;
        int len = length();
        if(len >= SZ_INT) {
            result |= buffer[0] & 0xff;
            result <<= 8;
            result |= buffer[1] & 0xff;
            result <<= 8;
            result |= buffer[2] & 0xff;
            result <<= 8;
            result |= buffer[3] & 0xff;
            return result;
        }
        else {
            throw new NotEnoughDataInByteBufferException(len, 4);
        }
    }

    public String removeCString(int size) throws UnsupportedEncodingException,
                                                 NotEnoughDataInByteBufferException {
        return removeCString(size, "ASCII");
    }

    public String removeCString(int size, String encoding) throws
                                                           NotEnoughDataInByteBufferException,
                                                           UnsupportedEncodingException {
        int len = length();
        if(len < size) {
            buffer = null;
            throw new NotEnoughDataInByteBufferException(len, size);
        }
        UnsupportedEncodingException encodingException = null;
        String result = null;
        int realSize = size;
        if(len > 0) {
            for(int i = 0; i < size ; i++) {
                if(buffer[i] == 0) {
                    realSize = i;
                    break;
                }
            }
            if (realSize <= 0){
                 result = "";
            }else{
                try {
                    if (encoding != null) {
                        result = new String(buffer, 0, realSize, encoding);
                    }
                    else {
                        for (int j = 0; j < buffer.length; j++) {
                        }
                        result = new String(buffer, 0, realSize);
                    }
                }
                catch (UnsupportedEncodingException e) {
                    encodingException = e;
                }
            }
            removeBytes0(size);
        }
        else {
            result = new String("");
        }
        if(encodingException != null) {
            throw encodingException;
        }
        return result;
    }

    public String removeString(int size, String encoding) throws
                                                          NotEnoughDataInByteBufferException,
                                                          UnsupportedEncodingException {
        int len = length();
        if(len < size) {
            throw new NotEnoughDataInByteBufferException(len, size);
        }
        UnsupportedEncodingException encodingException = null;
        String result = null;
        if(len > 0) {
            try {
                if(encoding != null) {
                    result = new String(buffer, 0, size, encoding);
                }
                else {
                    result = new String(buffer, 0, size);
                }
            }
            catch(UnsupportedEncodingException e) {
                            log.debug(e,e);
                encodingException = e;
            }
            removeBytes0(size);
        }
        else {
            result = "";
        }
        if(encodingException != null) {
            throw encodingException;
        }
        return result;
    }


    public ByteBuffer removeBuffer(int count) throws
                                              NotEnoughDataInByteBufferException {
        return removeBytes(count);
    }

    public ByteBuffer removeBytes(int count) throws
                                             NotEnoughDataInByteBufferException {
        ByteBuffer result = readBytes(count);
        removeBytes0(count);
        return result;
    }

    // just removes bytes from the buffer and doesnt return anything
    public void removeBytes0(int count) throws
                                        NotEnoughDataInByteBufferException {
        int len = length();
        int lefts = len - count;
        if(lefts > 0) {
            byte[] newBuf = new byte[lefts];
            System.arraycopy(buffer, count, newBuf, 0, lefts);
            setBuffer(newBuf);
        }
        else {
            setBuffer(null);
        }
    }

    public int removeBytesAsInt(int len) throws NotEnoughDataInByteBufferException {
        int result = 0;
        int temp = 0;
        if(len >= 1) {
            for(int i = 0; i < len ; i++) {
                temp = buffer[i] & 0xff;
                result = (result << 8) + temp;
            }
            removeBytes0(len);
            return result;
        }
        else {
            throw new NotEnoughDataInByteBufferException(len, 4);
        }
    }

    public ByteBuffer readBytes(int count) throws
                                           NotEnoughDataInByteBufferException {
        int len = length();
        ByteBuffer result = null;
        if(count > 0) {
            if(len >= count) {
                byte[] resBuf = new byte[count];
                System.arraycopy(buffer, 0, resBuf, 0, count);
                result = new ByteBuffer(resBuf);
                return result;
            }
            else {
                throw new NotEnoughDataInByteBufferException(len, count);
            }
        }
        else {
            return result; // just null as wanted count = 0
        }
    }

    public int readBytesAsInt(int len) throws NotEnoughDataInByteBufferException {
        int result = 0;
        int temp = 0;
        if(len >= 1) {
            for(int i = 0; i < len ; i++) {
                temp = buffer[i] & 0xff;
                result = (result << 8) + temp;
            }
            return result;
        }
        else {
            throw new NotEnoughDataInByteBufferException(len, 4);
        }
    }
    public String toString()
    {
        String s="";
        for (int i=0; i<this.buffer.length; i++)
        {
            s+=buffer[i];
        }
        return s;
    }

}

