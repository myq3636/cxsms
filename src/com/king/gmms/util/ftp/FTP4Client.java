package com.king.gmms.util.ftp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.TimeZone;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2007</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class FTP4Client{
    public static final int OVERWRITE = 1;
    public static final int SKIP = 2;
    public static final int FTP_PORT =21;
    private String ip;
    private int port = FTP_PORT;
    private String username;
    private String pwd;
    private int fileExistAction = OVERWRITE;

    private FTPClient ftpClient;
    public FTP4Client() {
    }

    public FTP4Client(String ip, int port, String username, String pwd) throws
        IOException {
        this.ip = ip;
        this.username = username;
        this.pwd = pwd;

        if (port > 0) {
            this.port = port;
        }
        else {
            this.port = FTP_PORT;
        }
        this.ftpClient = new FTPClient();
    }

    public FTP4Client(String ip, String username, String pwd) throws IOException {
        this(ip, -1, username, pwd);
    }

    public boolean connect() throws IOException {
		FTPClientConfig ftpClientConfig = new FTPClientConfig(FTPClientConfig.SYST_NETWARE);  
		ftpClientConfig.setServerTimeZoneId(TimeZone.getDefault().getID());  
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
				return isLogin;
			}
			this.ftpClient.login(this.username, this.pwd);
			this.ftpClient.enterLocalPassiveMode();
			this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			isLogin = true;
		} catch (Exception e) {
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

        if (filePath == null) {
            return rtnval;
        }

        int lastSlash = filePath.lastIndexOf(seperator);

        rtnval[0] = filePath.substring(0, lastSlash + 1);
        rtnval[1] = filePath.substring(lastSlash + 1, filePath.length());
        return rtnval;
    }

    private String[] parseFilePath(String filePath) {
        return this.parseFilePath(filePath, null);
    }

    private void download0(String remotePath, String remoteFileName,
                           File localFile) throws IOException {
    	if (remotePath != null && remotePath.length() > 0) {
			ftpClient.changeWorkingDirectory(remotePath);
		}
		BufferedInputStream in = null;
		FileOutputStream out = null;		 
		try {
			out = new FileOutputStream(localFile);			
			ftpClient.retrieveFile(remoteFileName, out);  								
		} catch (Exception ex) {			
		} finally {			
			if (out != null){
				out.flush();
				out.close();
			}						
		}
    }

    public boolean upload0(String remotePath, String remoteFileName,
                           File localFile) throws IOException
    {
    	BufferedInputStream inStream = null;  
    	        boolean success = false;  
    	        try {  
    	            this.ftpClient.changeWorkingDirectory(remotePath);  
    	            inStream = new BufferedInputStream(new FileInputStream(localFile));  
    	            success = this.ftpClient.storeFile(remoteFileName, inStream);  
    	            if (success == true) {  
    	                return success;  
    	            }  
    	        } catch (Exception e) {  
    	             
    	        } finally {  
    	            if (inStream != null) {  
    	                try {  
    	                    inStream.close();  
    	                } catch (IOException e) {  
    	                    e.printStackTrace();  
    	                }  
    	            }  
    	        }  
    	        return success; 

    }

    /**
     * Download the file from remote path and store it to the path specified by
     * argument local.
     *
     * The remote path seperator is forced to "/", and use
     * File.seperator as the local path seperator. For example, when client
     * running on Windows, the local path format should be:
     * "C:\\directory\\filename.ext".
     * And, when client is running on Unix, the local path format is:
     * "/usr/local/dir/filename"
     *
     * No matter where the server is running, the remote path should be:
     * "/usr/local/dir/filename"
     *
     *
     * @param remote String
     *   Specify the remote file path
     * @param local String
     *   Specify the local file path
     * @param useTempFile boolean
     *   if true, the downloading file will be stored in a temp file.
     * @throws IOException
     *   when connection problems happens
     * @throws FTPClientException
     *   other exception situations
     * @throws NullPointerException
     *   when argument is null.
     */
    public void download(String remote, String local, boolean useTempFile) throws
        IOException, FTPClientException {
        try {
            if (remote == null || local == null) {
                return;
            }
            File tempFile = null;
            String[] filePath = parseFilePath(remote, "/");
            String remotePath = filePath[0];
            String remoteFileName = filePath[1];

            filePath = parseFilePath(local);
            String localPath = filePath[0];
            String localFileName = filePath[1];

            if (remoteFileName.length() == 0) {
                return;
            }
            if (localFileName.length() == 0) {
                return;
            }

            if (localPath != null) {
                File localDir = new File(localPath);
                if (!localDir.exists()) {
                    localDir.mkdirs();
                }
            }

            File localFile = new File(local);
            if (useTempFile) {
                String tempFileName = local + "." + System.currentTimeMillis() +
                    ".tmp";
                tempFile = new File(tempFileName);
                this.download0(remotePath, remoteFileName, tempFile);

                if (localFile.exists()) {
                    localFile.delete();
                }
                if (!tempFile.renameTo(localFile)) {
                    throw new FTPClientException(
                        "Rename file failed. Downloaded file name: " +
                        tempFileName + ", rename target: " + local);
                }
            }
            else {
                this.download0(remotePath, remoteFileName, localFile);
            }
        }
        catch (Exception ex) {            
        }
    }

    public void upload(String local, String remote) throws
        IOException, FTPClientException {
        try {
            if (remote == null || local == null) {
                throw new NullPointerException(
                    "remote path or local path is null.");
            }
            String[] filePath = parseFilePath(remote, "/");
            String remotePath = filePath[0];
            String remoteFileName = filePath[1];

            filePath = parseFilePath(local);
            String localPath = filePath[0];
            String localFileName = filePath[1];

            if (remoteFileName.length() == 0) {
                throw new FTPClientException("Missing remote file name. " +
                                             remote);
            }
            if (localFileName.length() == 0) {
                throw new FTPClientException("Missing local file name. " +
                                             local);
            }
            File localFile = new File(local);
            if (!localFile.exists()) {
                throw new FTPClientException("local file not exist.");
            }
            this.upload0(remotePath, remoteFileName, localFile);
        }
        catch (Exception ex) {
            throw new FTPClientException(ex);
        }
    }

    public void closeServer() {
		if (null != this.ftpClient && this.ftpClient.isConnected()) {  
			            try {  
			                this.ftpClient.logout();  
			            } catch (IOException e) {  
		            } finally {  
			                try {  
			                    this.ftpClient.disconnect();  
			                } catch (IOException e) {  			                   
			                }  
			            }  
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

    public static void main(String[] args) {
        FTP4Client c = null;
        try {
            c = new FTP4Client("192.168.23.117", 21, "root", "root");
            c.connect();
//            c.download("/usr/local/gmd/conf/ClusterConfigbak.properties",
//                       "D:\\ClusterConfigbak.properties", true);
            c.upload("D:\\.1382516584845.tmp", "/usr/local/a2p/conf/133.temp");
            c.closeServer();
            System.out.println("finish");
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        catch (FTPClientException ex) {
            /** @todo Handle this exception */
            ex.printStackTrace();
        }
    }
}
