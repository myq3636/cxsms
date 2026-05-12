/**
 * Author: frank.xue@King.com
 * Date: 2006-6-1
 * Time: 11:33:24
 * Document Version: 0.1
 */

package com.king.gmms.transport.tcp;

import com.king.message.gmms.GmmsMessage;

import java.nio.ByteBuffer;
import java.util.EventListener;

public interface TcpIpListener extends EventListener {
    void received(ByteBuffer received);

    void connectionUnavailable();

    void bufferTimeout(Object key, GmmsMessage timeoutMessage);
}
