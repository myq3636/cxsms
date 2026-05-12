/*
 * Copyright (c) 1996-2001
 * Logica Mobile Networks Limited
 * All rights reserved.
 *
 * This software is distributed under Logica Open Source License Version 1.0
 * ("Licence Agreement"). You shall use it and distribute only in accordance
 * with the terms of the License Agreement.
 *
 */
package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;
import com.king.message.gmms.GmmsMessage;

import java.io.UnsupportedEncodingException;

/**
 * Provides encapsulation of message data with optional message encoding.
 * Can contain an ordinary data message or a message containing data encoded
 * in one of the Java supported encodings, including multibyte.
 * On Java encodings see <a href="http://java.sun.com/j2se/1.3/docs/guide/intl/encoding.doc.html">Supported encodings</a>
 *
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 16 Nov 2001
 */

public class ShortMessage extends ByteData {
    /**
     * Minimal size of the message in bytes. For multibyte encoded messages
     * it means size after converting to sequence of octets.
     */
    int minLength = 0;

    /**
     * Max size of the message in octets. For multibyte encoded messages
     * it means size after converting to sequence of octets.
     */
    int maxLength = 0;

    /**
     * The actual message encoded with the provided encoding.
     *
     * @see #encoding
     */
    String message = null;

    /**
     * The encoding of the message
     */
    String encoding = null;


    /**
     * The length of the message data.
     */
    int length = 0;

    /**
     * The message data after conversion to the sequence of octets,
     * i.e. the octets.
     */
    byte[] messageData = null;

    /**
     * Construct the short message with max data length -- the max count
     * of octets carried by the massege. It's not count of chars when interpreted
     * with certain encoding.
     *
     * @param maxLength the max length of the message
     */
    public ShortMessage(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * Construct the short message with mina nd max data length --
     * the min and max count of octets carried by the massege.
     * It's not count of chars when interpreted with certain encoding.
     *
     * @param minLength the min length of the message
     * @param maxLength the max length of the message
     */
    public ShortMessage(int minLength, int maxLength) {
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    /**
     * Reads data from the buffer and stores them into <code>messageData</code>.
     * The data can be later fetched using one of the <code>getMessage</code>
     * methods.
     *
     * @param buffer the buffer containing the message data; must contain
     *               exactly the data of the message (not zero terminated nor length tagged)
     * @throws PDUException
     * @throws NotEnoughDataInByteBufferException
     *
     * @throws TerminatingZeroNotFoundException
     *
     * @see #getMessage()
     * @see #getMessage(String)
     */
    public void setData(SmppByteBuffer buffer)
            throws PDUException,
                   NotEnoughDataInByteBufferException,
                   TerminatingZeroNotFoundException {
        byte[] messageData = null;
        int length = 0;
        if(buffer != null) {
            messageData = buffer.getBuffer();
            length = messageData == null ? 0 : messageData.length;
            checkString(minLength, length, maxLength);
        }
        this.message = null;
        this.messageData = messageData;
        this.length = length;
    }

    /**
     * Returns the sequence of octets generated from the message according the encoding
     * provided.
     *
     * @return the bytes generated from the message
     */
    public SmppByteBuffer getData() {
        SmppByteBuffer buffer = null;
        buffer = new SmppByteBuffer(messageData);
        return buffer;
    }


    /**
     * Sets the message a new value. Default encoding <code>Data.ENC_ASCII</code>
     * is used.
     *
     * @param message the message
     * @throws WrongLengthOfStringException thrown when the message
     *                                      too short or long
     */
    public void setMessage(String message)
            throws WrongLengthOfStringException {
        try {
            setMessage(message, Data.ENC_ASCII);
        }
        catch(UnsupportedEncodingException e) {
            // ascii always supported
        }
    }

    /**
     * Sets the message to a value with given encoding.
     *
     * @param message  the message
     * @param encoding the encoding of the message provided
     * @throws WrongLengthOfStringException thrown when the message
     *                                      too short or long
     * @throws UnsupportedEncodingException if the required encoding is not
     *                                      available for the Java Runtime system
     */
    public void setMessage(String message, String encoding)
            throws WrongLengthOfStringException,
                   UnsupportedEncodingException {
        if (message == null) {
            return;
        }
        // added for null message
        if("".equals(message)) {
            this.message = " ";
            this.length = 1;
            //modified by Amy 2007-07-26
            //this.messageData = new byte[]{0x00};
            this.messageData = new byte[]{0x20};
            this.encoding = GmmsMessage.AIC_CS_ASCII;
            return;
        }
        // handle USC2 encoding issue
        if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(encoding) && message.length() >
                                                              maxLength / 2) {
            message = message.substring(0, maxLength / 2 - 1);
        }
        else if(message.length() > maxLength) {
            message = message.substring(0, maxLength - 1); //added by chenlin
        }

        checkString(message, minLength, maxLength, encoding);
        if(message != null) {
            try {
                messageData = message.getBytes(encoding);
            }
            catch(UnsupportedEncodingException e) {
                //debug.write("encoding "+encoding+" not supported. Exception "+e);
                // event.write(e,"encoding "+encoding+" not supported");
                throw e; // re-throw
            }
            this.message = message;
            this.length = messageData.length;
            this.encoding = encoding;
        }
        else {
            this.message = null;
            this.messageData = null;
            this.encoding = encoding;
            this.length = 0;
        }
    }

    /**
     * Sets the encoding of the messasge.
     * Handy for message read from <code>ByteBuffer</code> to set the encoding ad hoc.
     *
     * @param encoding the message encoding
     * @throws UnsupportedEncodingException if the required encoding is not
     *                                      available for the Java Runtime system
     */
    public void setEncoding(String encoding)
            throws UnsupportedEncodingException {
        message = new String(messageData, encoding);
        this.encoding = encoding;
    }

    /**
     * Returns the message. If the message was read from <code>ByteBuffer</code>
     * and no explicit encoding is set, the <code>Data.ENC_ASCII</code> encoding
     * is used. Otherwise the encoding set is used.
     *
     * @return String
     */
    public String getMessage() {
        String useEncoding =
                encoding != null ? encoding : Data.ENC_ASCII;
        String theMessage = null;
        try {
            theMessage = getMessage(useEncoding);
        }
        catch(UnsupportedEncodingException e) {
            // 1. ascii always supported
            // 2. the encoding can be set only via setMessage
            //    or setEncoding => it's been already checked there
        }
        return theMessage;
    }

    /**
     * Returns the message applying the provided encoding to convert the
     * sequence of octets.
     *
     * @param encoding the required encoding of the resulting (String) message
     * @return String
     * @throws UnsupportedEncodingException if the required encoding is not
     *                                      available for the Java Runtime system
     */
    public String getMessage(String encoding)
            throws UnsupportedEncodingException {
        String message = null;
        if(messageData != null) {
            if((encoding != null) && (this.encoding != null) &&
               (encoding.equals(this.encoding))) {
                // if the required encoding is the same as current encoding
                // or if the encoding haven't been set yet
                if(this.message == null) {
                    this.message = new String(messageData, encoding);
                }
                message = this.message;
            }
            else {
                if(encoding != null) {
                    message = new String(messageData, encoding);
                }
                else {
                    message = new String(messageData);
                }
            }
        }
        return message;
    }

    /**
     * Returns the length of the message in octets.
     *
     * @return int
     */
    public int getLength() {
        return messageData.length;
    }

    /**
     * Returns the encoding of the message.
     *
     * @return String
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Returns if the encoding provided is supported by the Java Runtime system.
     *
     * @param encoding String
     * @return boolean
     */
    public static boolean encodingSuported(String encoding) {
        boolean supported = true;
        /*try {
            //CharToByteConverter.getConverter(encoding);
        }
        catch(UnsupportedEncodingException e) {
            supported = false;
        }*/
        return supported;
    }

    public String debugString() {
        String dbgs = "(sm: ";
        if(encoding != null) {
            dbgs += "enc: ";
            dbgs += encoding;
            dbgs += " ";
        }
        dbgs += "msg: ";
        dbgs += getMessage();
        dbgs += ") ";
        return dbgs;
    }
}
