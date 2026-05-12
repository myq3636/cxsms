package com.king.mgt.connection;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;
import java.util.HashSet;
import java.util.StringTokenizer;

import com.king.framework.SystemLogger;
import com.king.mgt.context.ContextManager;
import com.king.mgt.util.InfoTable;
import com.king.mgt.util.UserInterfaceUtility;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class SessionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SessionFactory.class);
	// private String screenedIP=null;
	private HashSet<String> authIP = new HashSet<String>();
	private HashSet<String> telnetIp = new HashSet<String>();

	private UserInterfaceUtility util = UserInterfaceUtility.getInstance();
	private int cmTimeout = 30 * 1000;
	private int telnetTimeout = 10 * 60 * 1000;
	private int totalCMSession = 1;
	private int totalTelnetSession = 5;
	private int curCMSession = 0;
	private int curTelnetSession = 0;
	private HashSet<String> screenedIP = new HashSet<String>();
	private InfoTable info;

	public SessionFactory() {
		info = InfoTable.getInstance();
	}

	private boolean isScreenedIP(Socket socket) {
		String ip = socket.getInetAddress().getHostAddress();
		boolean isScreened = screenedIP.contains(ip.trim());
		if (isScreened) {
			if(log.isDebugEnabled()){
				log.debug("Address screened:{}", ip);
			}
		}
		return isScreened;
	}

	public synchronized UserSession allocateUserSession(Socket socket) {
		try {

			if (isScreenedIP(socket))
				return null;
			log.info("Connection from {}", socket.getInetAddress()
					.getHostAddress());
			ContextManager context = new ContextManager();
			OutputStreamWriter writer = new OutputStreamWriter(
					new BufferedOutputStream(socket.getOutputStream()));

			if (authCMTools(socket, context)) {
				if (curCMSession >= totalCMSession) {
					log.info("CMTools Connection out of Limit({})",
							totalCMSession);
					writer.write(info.get(InfoTable.CONNECTION_LIMIT));
					writer.flush();
					return null;
				}
				curCMSession++;
				log.info("CMTool Session. ({}/{})", curCMSession,
						totalCMSession);
				return new CMToolsSession(context, socket, cmTimeout);
			}
			if (authUserPwd(socket, context)) {

				if (curTelnetSession >= totalTelnetSession) {
					log.info("User Telnet Connection out of Limit({})",
							totalTelnetSession);
					writer.write(info.get(InfoTable.CONNECTION_LIMIT));
					writer.flush();
					return null;
				}
				curTelnetSession++;
				log.info("User Telnet Session. ({}/{})", curTelnetSession,
						totalTelnetSession);
				return new TelnetSession(context, socket, telnetTimeout);
			}

			writer.write(info.get(InfoTable.AUTH_FAILED));
			writer.flush();
			log.info("Connection Authentication Failed. ");
			return null;
		} catch (IOException ex) {
			log.error(ex, ex);
			return null;
		}

	}

	public void releaseUserSession(UserSession session) {
		if (session instanceof CMToolsSession) {
			this.curCMSession--;
		} else if (session instanceof TelnetSession) {
			this.curTelnetSession--;
		}
	}

	public boolean authCMTools(Socket socket, ContextManager context) {
		InetAddress addr = socket.getInetAddress();
		String ip = addr.getHostAddress();
		if (authIP.contains(ip)) {
			context.setCurUser("CMTools");
			context.setCurUserIP(ip);
			context.setCurUserLoginTime(new Date());
			return true;
		}
		return false;
	}

	public boolean authUserPwd(Socket socket, ContextManager context) {
		// for now, we do not support username/password authentication.
		InetAddress addr = socket.getInetAddress();
		String ip = addr.getHostAddress();
		if (telnetIp.contains(ip)) {
			context.setCurUser("Telnet");
			context.setCurUserIP(ip);
			context.setCurUserLoginTime(new Date());
			return true;
		}
		return false;

	}

	public void init() {

		cmTimeout = Integer.parseInt(util.getProperty("CMToolsTimeout", "30")) * 1000;
		telnetTimeout = Integer.parseInt(util.getProperty("CommandLineTimeout",
				"600")) * 1000;
		totalTelnetSession = Integer.parseInt(util.getProperty(
				"TotalCommandLineSession", "5"));
		totalCMSession = Integer.parseInt(util.getProperty("TotalCMSession",
				"1"));

		String address = null;
		if ((address = util.getProperty("CMToolsAddress")) != null) {
			StringTokenizer st = new StringTokenizer(address.trim(), ",");
			while (st.hasMoreTokens()) {
				authIP.add(st.nextToken().trim());
			}
		}

		String telnetAdd = null;
		if ((telnetAdd = util.getProperty("TelnetAddress")) != null) {
			StringTokenizer st = new StringTokenizer(telnetAdd.trim(), ",");
			while (st.hasMoreTokens()) {
				telnetIp.add(st.nextToken().trim());
			}
		}

		String scIPs = null;
		if ((scIPs = util.getProperty("ScreenedIPs")) != null) {
			StringTokenizer st = new StringTokenizer(scIPs.trim(), ",");
			while (st.hasMoreTokens()) {
				screenedIP.add(st.nextToken().trim());
			}
		}

	}
}
