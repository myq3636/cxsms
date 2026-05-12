package com.king.mgt.framework;

import java.util.Timer;

import com.king.mgt.connection.UserCommandListener;
import com.king.mgt.processor.ContentListFileTask;
import com.king.mgt.processor.RecipientListFileTask;
import com.king.mgt.processor.SenderListFileTask;
import com.king.mgt.processor.WhiteListFileTask;
import com.king.mgt.util.UserInterfaceUtility;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2008</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class Launcher {
    public Launcher() {
        super();
    }
    public static void main(String args[])
    {
        UserInterfaceUtility util=UserInterfaceUtility.getInstance();
        util.init();
        if(args.length >= 1 && args[0].equalsIgnoreCase("true")) {
            util.setHa();
        }
        util.initUtilities();

        UserCommandListener cmdListener=new UserCommandListener();
        cmdListener.start();
        
        Timer downloadPolicyFileTimer = new Timer("WhiteListFileTimer", true);
        String timer = util.getProperty("WhiteListFileTask.Interval.InMin", "0");     
        long timeIndex = 5;
        try {
        	timeIndex = Integer.parseInt(timer.trim());
        	if (timeIndex<=0) {
        		//log.warn("the DownloadPolicyFileTask.Interval.InMin parameter value "+timer+" is invalid, use default value 60.");
				timeIndex = 0;
			}
		} catch (Exception e) {
			timeIndex = 0;
		}
        if(timeIndex>0){
        	downloadPolicyFileTimer.schedule(new WhiteListFileTask(), 1000, timeIndex*60*1000);
        }
        
        Timer downloadSenderPolicyFileTimer = new Timer("SenderListFileTimer", true);
        String senderTimer = util.getProperty("SenderListFileTask.Interval.InMin", "4");     
        long sdTimeIndex = 4;
        try {
        	sdTimeIndex = Integer.parseInt(senderTimer.trim());
        	if (sdTimeIndex<=0) {
        		//log.warn("the DownloadPolicyFileTask.Interval.InMin parameter value "+timer+" is invalid, use default value 60.");
        		sdTimeIndex = 0;
			}
		} catch (Exception e) {
			sdTimeIndex = 4;
		}
        if(sdTimeIndex>0){
        	downloadSenderPolicyFileTimer.schedule(new SenderListFileTask(), 1000, sdTimeIndex*60*1000);
        }
        
        Timer downloadContentPolicyFileTimer = new Timer("ContentListFileTimer", true);
        String contentTimer = util.getProperty("ContentListFileTask.Interval.InMin", "6");     
        long contentTimerIndex = 6;
        try {
        	contentTimerIndex = Integer.parseInt(contentTimer.trim());
        	if (contentTimerIndex<=0) {
        		//log.warn("the DownloadPolicyFileTask.Interval.InMin parameter value "+timer+" is invalid, use default value 60.");
        		contentTimerIndex = 0;
			}
		} catch (Exception e) {
			contentTimerIndex = 6;
		}
        if(contentTimerIndex>0){
        	downloadContentPolicyFileTimer.schedule(new ContentListFileTask(), 1000, contentTimerIndex*60*1000);
        }
        
        Timer downloadRecipeintPolicyFileTimer = new Timer("RecipientListFileTimer", true);
        String recipientTimer = util.getProperty("RecipientListFileTask.Interval.InMin", "360");     
        long recipientTimerIndex = 360;
        try {
        	recipientTimerIndex = Integer.parseInt(recipientTimer.trim());
        	if (recipientTimerIndex<=0) {
        		//log.warn("the DownloadPolicyFileTask.Interval.InMin parameter value "+timer+" is invalid, use default value 60.");
        		recipientTimerIndex = 0;
			}
		} catch (Exception e) {
			recipientTimerIndex = 360;
		}
        if(contentTimerIndex>0){
        	downloadRecipeintPolicyFileTimer.schedule(new RecipientListFileTask(), 1000, recipientTimerIndex*60*1000);
        }
    }
}
