package com.king.gmms.strategy;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.message.gmms.GmmsMessage;

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
public class IndexBalanceStrategy extends IndexStrategy{

    private int serial = 0;
    private Object mutex= new Object();
    private static SystemLogger log = SystemLogger
			.getSystemLogger(IndexBalanceStrategy.class);
    public int getNextIndex(int size){
        int result = 0;
        if(size <= 0){
            result = 0;
        }else if(size == 1){
            result = 0;
        }else{
            synchronized(mutex){
                result = (serial+1) % size;
                serial = result;
            }
        }
        return result ;
    }

}
