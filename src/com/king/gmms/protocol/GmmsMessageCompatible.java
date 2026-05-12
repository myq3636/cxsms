/**
 * Author: frank.xue@King.com
 * Date: 2006-4-29
 * Time: 22:35:26
 * Document Version: 0.1
 */

package com.king.gmms.protocol;

import com.king.message.gmms.GmmsMessage;

import java.util.zip.DataFormatException;
public interface GmmsMessageCompatible {
    GmmsMessage toGmmsMessage();

    void assignFromGmmsMessage(GmmsMessage gmmsMessage) throws Exception;

}
