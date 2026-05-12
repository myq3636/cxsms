/*
 * DbException.java
 *
 * Created on August 28, 2002, 3:04 PM
 */

package com.king.db;

/**
 *
 * @author  mike
 */
public class DbException extends java.lang.Exception {
    
    /**
     * Creates a new instance of <code>DbException</code> without detail message.
     */
    public DbException() {
    }
    
    
    /**
     * Constructs an instance of <code>DbException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public DbException(String msg) {
        super(msg);
    }
}
