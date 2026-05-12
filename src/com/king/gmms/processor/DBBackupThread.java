package com.king.gmms.processor;

import com.king.gmms.GmmsUtility;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;

public class DBBackupThread extends RunnableMsgTask{
    private MessageStoreManager msm;
    
    public DBBackupThread(GmmsMessage msg) {
    	this.message = msg;
        msm = GmmsUtility.getInstance().getMessageStoreManager();
    }
    
    @Override
	public void run() {
		if(message != null){
			msm.handleMessageError(message);
		}
	}
}
