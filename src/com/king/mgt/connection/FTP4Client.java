package com.king.mgt.connection;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;

import com.king.framework.SystemLogger;
import com.king.gmms.util.ftp.FTPClientException;
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
 * Copyright: Copyright (c) 2007
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class FTP4Client {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(FTP4Client.class);
	public static final int OVERWRITE = 1;
	public static final int SKIP = 2;

	public static final int FTP_PORT =21;
	private UserInterfaceUtility util = UserInterfaceUtility.getInstance();
	private String ip;
	private int port = FTP_PORT;
	private String username;
	private String pwd;
	private int fileExistAction = OVERWRITE;
	private FTPClient ftpClient;

	public FTP4Client() {
		initServerConf();
	}

	public FTP4Client(String ip, int port, String username, String pwd)
			throws IOException {

		this.ip = ip;
		this.username = username;
		this.pwd = pwd;

		if (port > 0)
			this.port = port;
		else
			this.port = FTP_PORT;
		this.ftpClient = new FTPClient();

	}

	public FTP4Client(String ip, String username, String pwd)
			throws IOException {
		this(ip, -1, username, pwd);
	}

	public boolean connect() throws Exception {
		log.info("Connecting FTP server (host={}, port={}).", this.ip,
				this.port);
		FTPClientConfig ftpClientConfig = new FTPClientConfig(
				FTPClientConfig.SYST_UNIX);
		ftpClientConfig.setServerTimeZoneId(TimeZone.getDefault().getID());
		if(ftpClient==null){
			ftpClient = new FTPClient();
		}
		this.ftpClient.setControlEncoding("UTF-8");
		this.ftpClient.configure(ftpClientConfig);

		boolean isLogin = false;
		try {
			if (this.port > 0) {
				this.ftpClient.connect(this.ip, this.port);
			} else {
				this.ftpClient.connect(this.ip);
			}
			int reply = this.ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				this.ftpClient.disconnect();
				log.error("login error!");
				return isLogin;
			}
			this.ftpClient.login(this.username, this.pwd);
			this.ftpClient.enterLocalPassiveMode();
			this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			log.info("login success!");
			isLogin = true;
		} catch (Exception e) {
			log.error("login error!");
		}
		this.ftpClient.setBufferSize(1024 * 2);
		this.ftpClient.setDataTimeout(30 * 1000);
		return isLogin;

	}

	private String[] parseFilePath(String filePath, String seperator) {
		if (seperator == null) {
			seperator = File.separator;
		}
		String[] rtnval = new String[2];

		if (filePath == null)
			return rtnval;

		int lastSlash = filePath.lastIndexOf(seperator);

		rtnval[0] = filePath.substring(0, lastSlash + 1);
		rtnval[1] = filePath.substring(lastSlash + 1, filePath.length());
		return rtnval;
	}

	private String[] parseFilePath(String filePath) {
		return this.parseFilePath(filePath, null);
	}

	private void download0(String remotePath, String remoteFileName,
			File localFile) throws Exception {
		if (remotePath != null && remotePath.length() > 0) {
			ftpClient.changeWorkingDirectory(remotePath);
		}				
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(localFile);
			log.info("Downloading...");
			boolean success = ftpClient.retrieveFile(remoteFileName, out);
			if (success) {
				log.info("Download successful.");
			}
		} catch (Exception ex) {
			log.info("download error!");
		} finally {
			if (out != null) {
				out.flush();
				out.close();
			}
		}
	}

	/**
	 * Download the file from remote path and store it to the path specified by
	 * argument local.
	 * 
	 * The remote path seperator is forced to "/", and use File.seperator as the
	 * local path seperator. For example, when client running on Windows, the
	 * local path format should be: "C:\\directory\\filename.ext". And, when
	 * client is running on Unix, the local path format is:
	 * "/usr/local/dir/filename"
	 * 
	 * No matter where the server is running, the remote path should be:
	 * "/usr/local/dir/filename"
	 * 
	 * 
	 * @param remote
	 *            String Specify the remote file path
	 * @param local
	 *            String Specify the local file path
	 * @param useTempFile
	 *            boolean if true, the downloading file will be stored in a temp
	 *            file.
	 * @throws IOException
	 *             when connection problems happens
	 * @throws FTPClientException
	 *             other exception situations
	 * @throws NullPointerException
	 *             when argument is null.
	 */
	public void download(String remote, String local, boolean useTempFile){
		try {
			if (remote == null || local == null) {
				return;
			}
			File tempFile = null;
			String[] filePath = parseFilePath(remote, "/");
			System.out.println();
			String remotePath = filePath[0];
			String remoteFileName = filePath[1];			
			filePath = parseFilePath(local);
			String localPath = filePath[0];
			String localFileName = filePath[1];

			if (remoteFileName.length() == 0)
				throw new FTPClientException("Missing remote file name. "
						+ remote);
			if (localFileName.length() == 0)
				throw new FTPClientException("Missing local file name. "
						+ local);

			log.info("Download remote file: {}", remote);
			log.info("Stored in local: {}", local);
			log.info("Using Temp file: {}", (useTempFile ? "yes" : "no"));

			if (localPath != null) {
				File localDir = new File(localPath);
				if (!localDir.exists()) {
					log.info(
							"local dir({}) does not exist, create it automatically.",
							localPath);
					localDir.mkdirs();
				}
			}
			// down load as temp file
			String tempFileName = local + "." + System.currentTimeMillis()
					+ ".tmp";
			log.info("Creating temp file: {}", tempFileName);
			tempFile = new File(tempFileName);
			this.download0(remotePath, remoteFileName, tempFile);

			// rename original file
			File localFile = new File(local);
			if (localFile.exists()) {
				Date curTime = new Date(System.currentTimeMillis());
				SimpleDateFormat printTime = new SimpleDateFormat(
						"yyyyMMdd_HHmmss");
				String bakFileName = local + "." + printTime.format(curTime)
						+ ".bak";
				File bakFile = new File(bakFileName);
				if (!localFile.renameTo(bakFile))
					throw new FTPClientException(
							"Rename backup file failed. Original file name: "
									+ local + ", rename target: " + bakFileName);
				localFile = new File(local);
			}

			// rename new file
			if (localFile.exists()) {
				localFile.delete();
			}
			if (!tempFile.renameTo(localFile))
				throw new FTPClientException(
						"Rename file failed. Downloaded file name: "
								+ tempFileName + ", rename target: " + local);
			log.info("Temp file is successfully renamed to {}", local);
		} catch (Exception ex) {
			log.error("download error!");
		}
	}

	public void closeServer() {
		if (null != this.ftpClient && this.ftpClient.isConnected()) {
			try {
				boolean reuslt = this.ftpClient.logout();
				if (reuslt) {
					log.info("logout sucess");
				}
			} catch (IOException e) {
				log.warn("logout error, " + e.getMessage());
			} finally {
				try {
					this.ftpClient.disconnect();
				} catch (IOException e) {
					log.warn("close connection error!");
				}
			}
		}

	}

	private void initServerConf() {

		this.setIp(util.getProperty("Ftp.host"));
		this.setUsername(util.getProperty("Ftp.username"));
		this.setPwd(util.getProperty("Ftp.password"));
		try {
			this.setPort(Integer.parseInt(util.getProperty("Ftp.port", "21")));
		} catch (NumberFormatException ex) {
			log.warn("Wrong ftp port configuration: Must be a integer. Use port 21 as default.");
			this.port = FTP_PORT;
		}
		this.ftpClient = new FTPClient();
 
	}

	public boolean downloadConfigFileOrCreate(String confKeyName) {

		if (!this.downloadConfigFile(confKeyName)) {
			String localFile = util.getProperty("Ftp." + confKeyName
					+ ".localPath");
			File file = new File(localFile);
			try {
				if (!file.exists()) {
					return file.createNewFile();
				}
			} catch (IOException e) {
				log.error(e, e);
				return false;
			}

		}
		return true;

	}

	public boolean downloadConfigFile(String confKeyName) {
		try {

			if ("no".equalsIgnoreCase(util.getProperty("Ftp." + confKeyName
					+ ".enable"))) {
				// Just return true if downloading is disabled.
				return true;
			}
			int timeout = -1;
			try {
				timeout = Integer.parseInt(util.getProperty("Ftp."
						+ confKeyName + ".timeout", "30"));
			} catch (NumberFormatException ex) {
				log.warn(
						"Wrong configuration item for {} timout: Must be a integer. Use 30 sec as default.",
						confKeyName);
				timeout = 30;

			}
			timeout *= 1000;
			ftpClient.setConnectTimeout(timeout);

			String remotePath = util.getProperty("Ftp." + confKeyName
					+ ".remotePath");
			String localPath = util.getProperty("Ftp." + confKeyName
					+ ".localPath");
			log.info("Download {} config file", confKeyName);
			this.connect();
			this.download(remotePath, localPath, true);
			return true;

		} catch (FTPClientException ex) {
			log.error(ex, ex);
			return false;
		} catch (Exception ex) {
			log.error(ex, ex);
			return false;
		} finally {
			this.closeServer();
		}

	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public String getPwd() {
		return pwd;
	}

	public String getUsername() {
		return username;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setUtil(UserInterfaceUtility util) {
		this.util = util;
	}

	public static void main(String[] args) {
		FTP4Client c = null;
		try {
			c = new FTP4Client("192.168.23.117", 21, "root", "root");
			c.connect();
			c.download("/usr/local/a2p/conf/ModuleManagement.xml",
					"D:", false);
			// c.upload("D:\\gmd-mmvd.log", "/usr/local/gmd/conf/gmd-mmvd.log");
			c.closeServer();
			System.out.println("finish");
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
