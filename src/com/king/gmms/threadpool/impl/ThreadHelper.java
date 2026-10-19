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

        // L-7: 用字面 replace 而非 replaceFirst(正则)——name 含 $ / \ 等会被当作正则替换语义导致异常或乱名。
        String answer = pattern.replace("#counter#", "" + nextThreadCounter());
        answer = answer.replace("#name#", name == null ? "" : name);

        return answer;
    }
    
}
