package com.king.gmms.connectionpool.connection;

import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;

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
public interface Connection {

    public String getConnectioName();

    public ConnectionStatus getConnectionStatus();

    public boolean addSession(Session session);

    public boolean deleteSession(Session session);

    public boolean initialize(ConnectionInfo connectionInfo);

    public Session getSession();

    public Session getSession(TransactionURI uri);

    public int getSessionNum();

    public BindMode getBindMode();
}
