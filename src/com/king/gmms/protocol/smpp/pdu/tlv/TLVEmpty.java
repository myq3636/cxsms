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

import com.king.gmms.protocol.smpp.pdu.ValueNotSetException;
import com.king.gmms.protocol.smpp.util.SmppByteBuffer;

/**
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 11 Jun 2001
 */

public class TLVEmpty extends TLV {
    private boolean present = false;

    public TLVEmpty() {
        super(0, 0);
    }

    public TLVEmpty(short p_tag) {
        super(p_tag, 0, 0);
    }

    public TLVEmpty(short p_tag, boolean p_present) {
        super(p_tag, 0, 0);
        present = p_present;
        markValueSet();
    }

    public SmppByteBuffer getValueData() {
        // nothing, just present or not
        return null;
    }

    public void setValueData(SmppByteBuffer buffer)
            throws WrongLengthException {
        // nothing, just set presence
        checkLength(buffer);
        setValue(true);
    }

    public void setValue(boolean p_present) {
        present = p_present;
        markValueSet();
    }

    public boolean getValue()
            throws ValueNotSetException {
        if(hasValue()) {
            return present;
        }
        else {
            throw new ValueNotSetException();
        }
    }

    public String debugString() {
        String dbgs = "(empty: ";
        dbgs += super.debugString();
        dbgs += ") ";
        return dbgs;
    }

}