package com.king.gmms.strategy;

import java.util.Random;

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
public class IndexRandomStrategy extends IndexStrategy{

    private Random random = new Random();
    private Object mutex = new Object();

    public int getNextIndex (int size){
        int result = 0;
        if(size <= 0){
            result = 0;
        }
        else if(size == 1){
            result = 0;
        }
        else{
            synchronized (mutex) {
                result = random.nextInt(size);
            }
        }
        return result;
    }

}
