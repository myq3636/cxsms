/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Various helper method for thread naming.
 */
public final class ThreadHelper {
	public static final String THREAD_COUNTER_SEPERATOR = "#";
    public static final String DEFAULT_PATTERN = "#name#" + THREAD_COUNTER_SEPERATOR + "#counter#";

    private static AtomicLong threadCounter = new AtomicLong();
    
    private ThreadHelper() {
    }
    
    private static long nextThreadCounter() {
        return threadCounter.getAndIncrement();
    }

    /**
     * Creates a new thread name with the given pattern
     * <p/>
     * @param pattern the pattern
     * @param name    the name
     * @return the thread name, which is unique
     */
    public static String resolveThreadName(String name) {
        String pattern = DEFAULT_PATTERN;

        // replace tokens
        String answer = pattern.replaceFirst("#counter#", "" + nextThreadCounter());
        answer = answer.replaceFirst("#name#", name);

        return answer;
    }
    
}
