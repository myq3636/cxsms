/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

public class ThreadPoolProfileBuilder {
    private final ThreadPoolProfile profile;

//    public ThreadPoolProfileBuilder() {
//        this.profile = new ThreadPoolProfile();
//    }
    
    public ThreadPoolProfileBuilder(String workQueueName) {
        this.profile = new ThreadPoolProfile(workQueueName);
    }

    public ThreadPoolProfileBuilder(String id, ThreadPoolProfile origProfile) {
        this.profile = origProfile.clone();
        this.profile.setWorkQueueName(id);
    }
    
    public ThreadPoolProfileBuilder poolSize(Integer poolSize) {
        profile.setPoolSize(poolSize);
        return this;
    }

    public ThreadPoolProfileBuilder maxPoolSize(Integer maxPoolSize) {
        profile.setMaxPoolSize(maxPoolSize);
        return this;
    }

    public ThreadPoolProfileBuilder keepAliveTime(Long keepAliveTime, TimeUnit timeUnit) {
        profile.setKeepAliveTime(keepAliveTime);
        profile.setTimeUnit(timeUnit);
        return this;
    }

    public ThreadPoolProfileBuilder keepAliveTime(Long keepAliveTime) {
        profile.setKeepAliveTime(keepAliveTime);
        return this;
    }
    
    public ThreadPoolProfileBuilder maxQueueSize(Integer maxQueueSize) {
        if (maxQueueSize != null) {
            profile.setMaxQueueSize(maxQueueSize);
        }
        return this;
    }

    public ThreadPoolProfileBuilder rejectedPolicy(RejectedExecutionHandler rejectedPolicy) {
        profile.setRejectedPolicy(rejectedPolicy);
        return this;
    }
    
    public ThreadPoolProfileBuilder needSafeExit(Boolean needSafeExit) {
        if (needSafeExit != null) {
            profile.setNeedSafeExit(needSafeExit);
        }
        return this;
    }

    /**
     * Builds the new thread pool
     * 
     * @return the created thread pool
     * @throws Exception is thrown if error building the thread pool
     */
    public ThreadPoolProfile build() {
        return profile;
    }
}
