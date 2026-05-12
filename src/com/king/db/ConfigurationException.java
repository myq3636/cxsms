package com.king.db;

/**
 * <p>Title: </p>
 * ConfigurationException
 * <p>Description: </p>
 * Configuration Exception
 * <p>Copyright: Copyright (c) 2005</p>
 *
 * <p>Company: </p>
 * King Inc.
 * @version 1.0
 */
public class ConfigurationException extends Exception {
    public ConfigurationException() {
        super();
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }
}
