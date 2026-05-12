package com.king.gmms.connectionpool.connection;

import java.util.ArrayList;
import java.util.Observer;

import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.strategy.Strategy;

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
public interface ConnectionManager extends Observer{

    public Session getSession();

    public Session getSession(TransactionURI transaction);

    public Session getSession(ArrayList connectionIDList,boolean isSplit4DR);

    public boolean insertConnection(Connection connection);

    public boolean deleteConnection(Connection connection);

    public boolean closePool();

    public boolean insertSession(String connectionName,Session session);

    public boolean deleteSession(String connectionName,Session session);

    public boolean contain(String connectionName);
    
    public Connection getConnection(String connectionName);

    public int getConnectionNum();

    public int getSessionNum();

    public void setStrategy(Strategy strategy);


}
