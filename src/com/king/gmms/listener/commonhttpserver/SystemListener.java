package com.king.gmms.listener.commonhttpserver;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

import java.io.IOException;
import java.util.Timer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;

public class SystemListener extends HttpServlet implements ServletContextListener {
	private static final long serialVersionUID = 1000001L;
	SystemLogger log = SystemLogger.getSystemLogger(SystemListener.class);
	Timer timer = new Timer();

	public void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
		//

	}

	public void contextInitialized(ServletContextEvent sce) {
		log.info("initial context....");
		String a2phome = System.getProperty("a2p_home");
		if (!a2phome.endsWith("/")) {
			a2phome = a2phome + "/";
		}
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		gmmsUtility.initUtility(a2phome + "Gmms/GmmsConfig.properties");
		gmmsUtility.initRedisClient("M");
		timer.schedule(new BillingCounterTask(gmmsUtility), 0, 5*60*1000);

	}

	public void contextDestroyed(ServletContextEvent sce) {
		log.info("destory context....");
		timer.cancel();
	}

}
