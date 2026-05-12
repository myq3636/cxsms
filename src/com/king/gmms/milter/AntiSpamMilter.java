package com.king.gmms.milter;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.AntiSpamFilter;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;

public class AntiSpamMilter extends LocalMilter implements LifecycleListener  {

    private static AntiSpamMilter m_instance = null;
	private AntiSpamFilter antiSpam;
	private LifecycleSupport lifecycle;
	
    private AntiSpamMilter() {
        log = SystemLogger.getSystemLogger(AntiSpamMilter.class.getName());
        antiSpam = initAntiSpamInfo();
		if (antiSpam == null) {
			log.warn("antiSpam init error!");
		}
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_ANTISPAM_RELOAD, this, 1);
    }

    
    public synchronized static AntiSpamMilter getInstance() {
        if (m_instance == null) {
            try {
                m_instance = new AntiSpamMilter();
            }
            catch (Exception e) {
                log.error("cann't create one ContentScanMilter instance!", e);
            }
        }
        return m_instance;
    }
    
    
	/**
	 * reload antispam info
	 * @return
	 */
	private int loadAntiSpamInfo() {
		log.debug("Start to reload AntiSpam Info....");
		AntiSpamFilter tmpAntispam = this.antiSpam;
		this.antiSpam = initAntiSpamInfo();
		tmpAntispam.clearAntiSpamInfo(); //free resoures
		if(antiSpam!=null){
            	log.debug("Reload AntiSpam Info successfully!");
			return 0;
		}else{
			return 1;
		}
	} 
	
	
	public boolean checkAntiSpam(int ssid, GmmsMessage msg, boolean isIn) {
		if (this.antiSpam == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "antiSpam init error,just pass the message!");
			}
			return false;
		}

		boolean isAscii = false;

		if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(msg.getContentType())) {
			isAscii = true;
		}

		return antiSpam.checkContent(ssid, msg, isIn, isAscii);

	}
	
	private AntiSpamFilter initAntiSpamInfo() {
		String path = gmmsUtility.getAntiSpamFilePath();
		AntiSpamFilter anti = null;
		if (path != null) {
			anti = new AntiSpamFilter(path);
			anti.loadAntiSpamInfo();
		}
		return anti;
	}

	public int OnEvent(Event event) {
			log.trace("Event Received. Type: {}", event.getEventType());
		if (event.getEventType() == Event.TYPE_ANTISPAM_RELOAD) {
			return loadAntiSpamInfo();
		}else {
			return 1;
		}

	}
	
}
