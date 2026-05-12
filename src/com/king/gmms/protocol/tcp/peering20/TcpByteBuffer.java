package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.peering20.exception.NotEnoughDataInByteBufferException;

public class TcpByteBuffer {
    protected static final byte SZ_4 = 4;
    protected static final byte SZ_2 = 2;
    protected static final byte SZ_INT = 4;
    protected static final byte SZ_BYTE = 1;
    protected byte[] buffer = null;
    private static SystemLogger log = SystemLogger.getSystemLogger(TcpByteBuffer.class);

    public TcpByteBuffer() {
    }


    public TcpByteBuffer(byte[] newBuffer) {
        buffer = newBuffer;
    }

    public TcpByteBuffer(TcpByteBuffer bb){
        this.buffer = bb.getBuffer();
    }

    public void appendBytes(TcpByteBuffer bytes)
        throws NotEnoughDataInByteBufferException {
        appendBytes(bytes,bytes.length());
    }

    public void appendBytes(TcpByteBuffer bytes, int count)
        throws NotEnoughDataInByteBufferException {
        if (count > 0) {
            if (bytes == null) {
                throw new NotEnoughDataInByteBufferException(0, count);
            }
            if (bytes.length() < count) {
                throw new NotEnoughDataInByteBufferException(bytes.length(), count);
            }
            appendBytes0(bytes.getBuffer(), count);
        }
    }

    private void appendBytes0(byte[] bytes, int count) {
        int len = length();
        byte[] newBuf = new byte[len + count];
        if (len > 0) {
            System.arraycopy(buffer, 0, newBuf, 0, len);
        }

        System.arraycopy(bytes, 0, newBuf, len, count);
        setBuffer(newBuf);
    }


    public void appendIntAs1Byte(int value)
        throws NotEnoughDataInByteBufferException {

        byte[] buffer = new byte[1];
        buffer[0] = (byte) (value & 0xff);
        appendBytes(new TcpByteBuffer(buffer), 1);
    }

    public void appendIntAs2Byte(int value)
        throws NotEnoughDataInByteBufferException {

        byte[] buffer = new byte[2];
        buffer[0] = (byte) ((value & 0xff00) >> 8);
        buffer[1] = (byte) (value & 0x00ff);

        appendBytes(new TcpByteBuffer(buffer), 2);
    }


    public void appendLongAs4Bytes(long value)
        throws NotEnoughDataInByteBufferException {

        byte[] buffer = new byte[4];
        buffer[0] = (byte) ((value & 0xff000000) >> 24);
        buffer[1] = (byte) ((value & 0x00ff0000) >> 16);
        buffer[2] = (byte) ((value & 0x0000ff00) >> 8);
        buffer[3] = (byte) (value & 0x000000ff);

        appendBytes(new TcpByteBuffer(buffer), 4);
    }


    public void appendInt(int value) throws NotEnoughDataInByteBufferException {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) ((value & 0xff000000) >> 24);
        buffer[1] = (byte) ((value & 0x00ff0000) >> 16);
        buffer[2] = (byte) ((value & 0x0000ff00) >> 8);
        buffer[3] = (byte) (value & 0x000000ff);

        appendBytes(new TcpByteBuffer(buffer), 4);
    }

    public void appendString(String value) throws
            NotEnoughDataInByteBufferException {
        appendString(value, value.getBytes().length);
    }


    public void appendString(String value, int count) throws
            NotEnoughDataInByteBufferException {
        byte[] buf = null;

        buf = value.getBytes();

        if (buf.length < count) {
            appendBytes(new TcpByteBuffer(buf), buf.length);

            byte[] buf1 = new byte[count - buf.length];
            for (int i = 0; i < buf1.length; i++) {
                buf1[i] = 0;
            }
            appendBytes(new TcpByteBuffer(buf1), buf1.length);
        } else {
            appendBytes(new TcpByteBuffer(buf), count);
        }
    }

    public void appendString(String value, int count,String charset) throws
                                                                     NotEnoughDataInByteBufferException {
        byte[] buf = null;
        if (charset == null||"".equals(charset)){
            buf = value.getBytes();
        }else{
            try {
                buf = value.getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                log.error("Doesn't support GBK encoding!", e);
            }
        }
        if (buf.length < count) {
            appendBytes(new TcpByteBuffer(buf), buf.length);

            byte[] buf1 = new byte[count - buf.length];
            for (int i = 0; i < buf1.length; i++) {
                buf1[i] = 0;
            }
            appendBytes(new TcpByteBuffer(buf1), buf1.length);
        } else {
            appendBytes(new TcpByteBuffer(buf), count);
        }
    }


    public byte[] getBuffer() {
            return buffer;
    }


    public int length() {
        if (buffer == null) {
            return 0;
        }
        else {
            return buffer.length;
        }
    }


    public int read1ByteAsInt() throws NotEnoughDataInByteBufferException {
        int result = 0;
        int len = length();
        if (len >= SZ_BYTE) {
            result = buffer[0] & 0xff;

            return result;
        }
        else {
            throw new NotEnoughDataInByteBufferException(len, 1);
        }
    }

    public long read4BytesAsLong()
        throws NotEnoughDataInByteBufferException
        {
            long result = 0;
            int len = length();
            if (len >= SZ_4) {
                result |= buffer[0]&0xff;
                result <<= 8;
                result |= buffer[1]&0xff;
                result <<= 8;
                result |= buffer[2]&0xff;
                result <<= 8;
                result |= buffer[3]&0xff;
                return result;
            } else {
                throw new NotEnoughDataInByteBufferException(len,4);
            }
        }

    public TcpByteBuffer readBytes(int count)
        throws NotEnoughDataInByteBufferException {
        int len = length();
        TcpByteBuffer result = null;
        if (count > 0) {
            if (len >= count) {
                byte[] resBuf = new byte[count];
                System.arraycopy(buffer, 0, resBuf, 0, count);
                result = new TcpByteBuffer(resBuf);
                return result;
            }
            else {
                throw new NotEnoughDataInByteBufferException(len, count);
            }
        }
        else {
            return result;
        }
    }


    public int readInt() throws NotEnoughDataInByteBufferException {
        int result = 0;
        int len = length();
        if (len >= SZ_INT) {
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


    public int remove1ByteAsInt() throws NotEnoughDataInByteBufferException {
        int result = read1ByteAsInt();
        removeBytesOnly(SZ_BYTE);
        return result;
    }

    public int removeBytesAsInt(int len) throws NotEnoughDataInByteBufferException {
        int result = 0;
        int temp = 0;
        if (len >= 1) {
            for (int i = 0; i < len; i++) {
                temp = buffer[i] & 0xff;
                result = (result << 8) + temp;
            }
            removeBytes0(len);
            return result;
        } else {
            throw new NotEnoughDataInByteBufferException(len, 4);
        }

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


    public long remove4BytesAsLong() throws NotEnoughDataInByteBufferException {
        long result = read4BytesAsLong();
        removeBytesOnly(SZ_4);
        return result;
    }


    public TcpByteBuffer removeBytes(int count)
        throws NotEnoughDataInByteBufferException {
        TcpByteBuffer result = readBytes(count);
        removeBytesOnly(count);
        return result;
    }


    public void removeBytesOnly(int count)
        throws NotEnoughDataInByteBufferException {
        int len = length();
        int lefts = len - count;
        if (lefts > 0) {
            byte[] newBuf = new byte[lefts];
            System.arraycopy(buffer, count, newBuf, 0, lefts);
            setBuffer(newBuf);
        }
        else {
            setBuffer(null);
        }
    }


    public int removeInt() throws NotEnoughDataInByteBufferException {
        int result = readInt();
        removeBytesOnly(SZ_INT);
        return result;
    }

    public String removeString(int size) throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        return removeString(size, "ASCII");
    }

    public String removeString(int size, String encoding)
        throws NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        int len = length();
        if (len < size) {
            throw new NotEnoughDataInByteBufferException(len, size);
        }
        UnsupportedEncodingException encodingException = null;
        String result = null;
        if (len > 0) {
            try {
                if (encoding != null) {
                    result = new String(buffer, 0, size, encoding);
                }
                else {
                    result = new String(buffer, 0, size);
                }
            }
            catch (UnsupportedEncodingException e) {

                encodingException = e;
            }
            removeBytesOnly(size);
        }
        else {
            result = new String("");
        }
        if (encodingException != null) {
            throw encodingException;
        }
        return result;
    }

    public void setBuffer(byte[] newBuffer) {
            buffer = newBuffer;
    }

    public void clear(){
        buffer = null;
    }
}
