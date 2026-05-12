package com.king.gmms.connectionpool;

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
public enum ConnectionStatus {
    INITIAL,
    CONNECT,
    DISCONNECT,
    RETRY,
    RECOVER;
}
