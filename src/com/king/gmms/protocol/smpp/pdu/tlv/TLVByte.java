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
package com.king.gmms.protocol.smpp.pdu.tlv;

import com.king.gmms.protocol.smpp.pdu.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.smpp.pdu.ValueNotSetException;
import com.king.gmms.protocol.smpp.util.SmppByteBuffer;

/**
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 11 Jun 2001
 */

public class TLVByte extends TLV {
    private byte value = 0;

    public TLVByte() {
        super(1, 1);
    }

    public TLVByte(short p_tag) {
        super(p_tag, 1, 1);
    }

    public TLVByte(short p_tag, byte p_value) {
        super(p_tag, 1, 1);
        value = p_value;
        markValueSet();
    }

    protected void setValueData(SmppByteBuffer buffer)
            throws TLVException {
        checkLength(buffer);
        try {
            value = buffer.removeByte();
        }
        catch(NotEnoughDataInByteBufferException e) {
            // can't happen as the size is already checked by checkLength()
        }
        markValueSet();
    }

    protected SmppByteBuffer getValueData()
            throws ValueNotSetException {
        SmppByteBuffer valueBuf = new SmppByteBuffer();
        valueBuf.appendByte(getValue());
        return valueBuf;
    }

    public void setValue(byte p_value) {
        value = p_value;
        markValueSet();
    }

    public byte getValue()
            throws ValueNotSetException {
        if(hasValue()) {
            return value;
        }
        else {
            throw new ValueNotSetException();
        }
    }

    public String debugString() {
        String dbgs = "(byte: ";
        dbgs += super.debugString();
        dbgs += value;
        dbgs += ") ";
        return dbgs;
    }

}