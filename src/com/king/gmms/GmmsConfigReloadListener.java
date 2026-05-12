package com.king.gmms;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.event.Event;

public class GmmsConfigReloadListener implements LifecycleListener {
	private static final SystemLogger log = SystemLogger.getSystemLogger(GmmsConfigReloadListener.class);

	@Override
	public int OnEvent(Event event) {
		if (event == null || event.getEventType() != Event.TYPE_GMMS_CONFIG_RELOAD) {
			return 0;
		}
		String action = "-a";
		Object[] args = event.getArgs();
		if (args != null && args.length > 0 && args[0] != null) {
			action = String.valueOf(args[0]);
		}
		try {
			return GmmsUtility.getInstance().reloadGmmsConfig(action);
		} catch (Exception e) {
			log.error("Reload GmmsConfig failed. action={}", action, e);
			return 1;
		}
	}
}
