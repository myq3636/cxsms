package com.king.message.gmms;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;

public class ExceptionMessageWriter {
	private SystemLogger log = SystemLogger.getSystemLogger(ExceptionMessageWriter.class);
    protected boolean isOpen;
    protected PrintWriter outStream;
    protected String currentFilePath = null;
    private String previousFilePath = null;
    protected String moduleName = null;
    protected int currentFileSize;
    protected File currentFile = null;
    protected String filePrefix = null;
    protected int maxFileSize=10*1024*1024;
    protected long maxFileTime=300*1000;//ms
    private long lastFileTime;
    private ExceptionFileMonitor monitor = null;
    private static SimpleDateFormat sdFormat = new SimpleDateFormat("yyyyMMdd.HHmmss.SSS");
    protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
    private String synSuffix= "exceptionMsg";
    
    public ExceptionMessageWriter(){
    	isOpen = false;
        monitor = new ExceptionFileMonitor();        
    }
    
    /**
     * initialize
     * @param group
     */
    public void initialize() {
    	initialize(synSuffix);
    }
    
    /**
     * initialize
     * @param group
     */
    public void initialize(String suffix) {
    	String a2phome = System.getProperty("a2p_home");
    	if(a2phome == null) {
            System.out.println("No a2p_home configured in system!");
            System.exit(-1);
        }else if (!a2phome.endsWith("/")) {
			a2phome = a2phome + "/";			
		}
    	currentFilePath = a2phome + "queue/"+suffix+"/";
        moduleName = System.getProperty("module");
        if (!createFolder(currentFilePath)) {
            return;
        }
        if(log.isDebugEnabled()){
        	log.debug("ExceptionMessageWriter initialize() moduleName is:"+moduleName);
        }
        this.filePrefix = currentFilePath + moduleName + "."+suffix+".";
        previousFilePath = currentFilePath + "link/";
        if (!createFolder(previousFilePath)) {
            return;
        }
        maxFileTime = Integer.parseInt(gmmsUtility.getCommonProperty("ExceptionMsgFileSwitchInterval", "300")) * 1000;
        maxFileSize = Integer.parseInt(gmmsUtility.getCommonProperty("ExceptionMsgFileMaxSize", "10240")) * 1024;
        if(log.isDebugEnabled()){
        	log.debug("ExceptionMessageWriter initialize() ExceptionMsgFileSwitchInterval is:"+maxFileTime+"   ExceptionMsgFileMaxSize is:"+maxFileSize+"  \n currentFilePath is:"+currentFilePath);
        }
        backupFile(currentFilePath);
        isOpen = open();
        monitor.start();
    }
    
    /**
     * createFolder
     * @param folderName
     * @return
     */
    protected boolean createFolder(String folderName) {
        File d = new File(folderName);
        if (d.exists() == false) {
            if (d.mkdirs() == false) {
                log.error("Error create folder: " + folderName);
                MailSender.getInstance().sendAlertMail("A2P alert mail from " +
                    ModuleURI.self().getAddress() + " for error message backup create folder",
                    "",
                    null);
                return false;
            }
        }
        return true;
    }
    
    /**
     * open file
     * @return
     */
    private boolean open() {
        String tempTime = sdFormat.format(new Date());
        String fileName = filePrefix + tempTime;
        currentFile = new File(fileName);
        boolean result = false;

        try {
            outStream = new PrintWriter(fileName, "UTF-8");
            currentFileSize = 0;
            lastFileTime = System.currentTimeMillis();
            result = true;
        }
        catch (Exception ex) {
            log.error("Error open new ExceptionMsg file: " + fileName, ex);
            MailSender.getInstance().sendAlertMail("A2P alert mail from " +
                ModuleURI.self().getAddress() + " for open file Exception",
                ex);
        }
        return result;
    }
    
    /**
     * backupFile
     */
    void backupFile() {
        if (currentFile == null) {
            return;
        }
        String backupFile = previousFilePath + currentFile.getName();
        File backFile = new File(backupFile);
        currentFile.renameTo(backFile);
    }
    
    /**
     * backupFile
     * @param filePathName
     */
    void backupFile(String filePathName) {
        File[] fileList;
        File dirFile = new File(filePathName);
        fileList = dirFile.listFiles();
        String fileStart = moduleName+"."+synSuffix+ ".";
        if (fileList == null || fileList.length == 0) {
            return;
        }
        for (int i = 0; i < fileList.length; i++) {
            String fileName = fileList[i].getName();
            if (fileList[i].isFile() == false
                || !fileName.startsWith(fileStart)) {
                continue;
            }
            
            currentFile = new File(fileList[i].toString());
            if(currentFile.length() > 0){
                backupFile();
            }else{
                currentFile.delete();
            }
        }
    }
    
    /**
     * 
     * @param message
     * @return
     */
    public boolean insertExceptionMessage(GmmsMessage message,String name){
        if (!isOpen) {
        	if(log.isInfoEnabled()){
        		log.info("ExceptionMsg file is not opened!");
        	}
            return false;
        }
        return write(constructMsg(message,name));
    }
    
    /**
     * 
     * @param message
     * @return
     */
    private String constructMsg(GmmsMessage msg,String name) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(name).append(",")
          .append(this.formatMySqlDate(new Date())).append(",")
          .append(msg.getMessageType()).append(",")
          .append(msg.getMsgID()).append(",");
	        if("Delivery Report".equalsIgnoreCase(msg.getMessageType())){
	        	sb.append(msg.getInMsgID()).append(",");
	        }else{
	        	sb.append(msg.getOutMsgID()).append(",");
	        }
        sb.append(msg.getSenderAddress()).append(",")
          .append(msg.getRecipientAddress()).append(",")
          .append(msg.getOSsID()).append(",")
          .append(msg.getRSsID()).append(",")
          .append(msg.getOoperator()).append(",")
          .append(msg.getRoperator()).append(",");          
	        if("Delivery Report".equalsIgnoreCase(msg.getMessageType())){
	        	sb.append(msg.getInTransID()).append(",");
	        }else{
		        sb.append(msg.getOutTransID()).append(",");
	        }
          sb.append(msg.getStatusCode());
        
        return sb.toString();
    }
    
    
    /**
     * 
     * @param content
     * @return
     */
    private synchronized boolean write(String content) {
        boolean result = false;
        try {
            if (currentFileSize > maxFileSize) {
                if (log.isInfoEnabled()) {
                    log.info("SDQWriter up to file max size");
                }
                rotate();
            }
            currentFileSize = currentFileSize + content.length();
            outStream.write(content+"\n");
            if(outStream.checkError()){
                throw new Exception("Error occur in the backup message file:" + currentFile.getName());
            }
            result = true;
        }
        catch (Exception ex) {
            log.error("Error backup message: " + content, ex);
        }
        return result;
    }
    
    /**
     * close
     */
    public void close(){
        outStream.close();
        backupFile();
    }
    
    /**
     * rotate
     */
    private void rotate() {
        close();
        open();
    }
    
    private String formatMySqlDate(java.util.Date date) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return "'" + formatter.format(date) + "'";
    }
    
    /**
     * swichFile
     */
    private synchronized void swichFile() {
        if(System.currentTimeMillis() - lastFileTime >= maxFileTime) {
        	if (log.isInfoEnabled()) {
        		log.info("ExceptoinMsgWriter up to file max time");
            }
            if (!isOpen || (currentFileSize == 0)) {
                lastFileTime = System.currentTimeMillis();
                return;
            }
            rotate();
        }
    }

    class ExceptionFileMonitor implements Runnable {
        volatile boolean running = false;
        public void run() {
            while (isOpen && running) {
                try{
                    Thread.sleep(1000L);
                    long waitTime = maxFileTime - (System.currentTimeMillis() - lastFileTime);
                    if(waitTime > 0L) {
                        Thread.sleep(waitTime);
                    }
                    swichFile();
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            if (log.isInfoEnabled()) {
            	log.info("ExceptionFileMonitor thread stop!");
            }
        }

        public void start(){
            running = true;
            Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
                                   "ExceptionFileMonitor");
            monitor.start();
            if (log.isInfoEnabled()) {
            	log.info("ExceptionFileMonitor thread start!");
            }
        }

        public void stop(){
            running = false;
        }
    }
}
