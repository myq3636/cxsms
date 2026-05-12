package com.king.gmms.mqm;

import java.util.Properties;
import java.io.IOException;
import java.io.FileInputStream;

import com.king.framework.SystemLogger;

import java.io.File;

public final class TaskConfiguration {
    private static SystemLogger log = SystemLogger.getSystemLogger(TaskConfiguration.class);
    private static TaskConfiguration taskConfiguration = new TaskConfiguration();
    private static Properties prop = null;
    private static volatile boolean initialed = false;

    private TaskConfiguration() {
    }

    /**
     * read properties from configure file
     */
    public void init() {
        FileInputStream in = null;
        try {
            String a2pHome = System.getProperty("a2p_home", "/usr/local/a2p/");
            String configFileName = a2pHome + "/conf/MQMTaskConfig.properties";
            File aFile = new File(configFileName);
            if (!aFile.exists()) {
                log.warn("MQMTask configuration file not exist, create one!");
                aFile.createNewFile();
            }
            in = new FileInputStream(configFileName);
            prop = new Properties();
            prop.load(in);
        }
        catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        finally {
            try {
                in.close();
            }
            catch (IOException e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }
    }

    /**
     * get instance
     * @return taskConfiguration
     */
    public synchronized static TaskConfiguration getInstance() {
        if (!initialed) {
            taskConfiguration.init();
            initialed = true;
        }
        return taskConfiguration;
    }

    /**
     *
     * @param propertyName String
     * @return String
     */
    public String getProperty(String propertyName) {
        String propertyValue = prop.getProperty(propertyName);
        if (propertyValue == null || propertyValue.equals("")) {
            return null;
        }
        else {
            return propertyValue.trim();
        }
    }

    /**
     *
     * @param propertyName String
     * @param defaultValue String
     * @return String
     */
    public String getProperty(String propertyName, String defaultValue) {
        String propertyValue = prop.getProperty(propertyName, defaultValue);
        return propertyValue.trim();
    }

    /**
     * is initialed
     * @return boolean
     */
    public synchronized static boolean isIniatial() {
        return initialed;
    }

    /**
     * get properties of configure file
     * @return Properties
     */
    public Properties getProperties() {
        return prop;
    }
}
