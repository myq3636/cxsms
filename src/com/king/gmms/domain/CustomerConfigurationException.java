package com.king.gmms.domain;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class CustomerConfigurationException
    extends Exception {
    public CustomerConfigurationException() {
        super();
    }

    public CustomerConfigurationException(String message) {
        super(message);
    }

    public CustomerConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CustomerConfigurationException(Throwable cause) {
        super(cause);
    }
}
