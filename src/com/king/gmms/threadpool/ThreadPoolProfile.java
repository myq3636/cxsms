/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import java.io.Serializable;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

/**
 * A profile which defines thread pool settings.
 *
 * @version 
 */
public class ThreadPoolProfile implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private String workQueueName;
    private Integer poolSize;
    private Integer maxPoolSize;
    private Long keepAliveTime;
    private TimeUnit timeUnit;
    private Integer maxQueueSize;
    private RejectedExecutionHandler rejectedPolicy;
    private Boolean needSafeExit;

    /**
     * Creates a new thread pool profile, with no id set.
     */
    public ThreadPoolProfile() {
    }

    /**
     * Creates a new thread pool profile
     *
     * @param id id of the profile
     */
    public ThreadPoolProfile(String workQueueName) {
        this.workQueueName = workQueueName;
    }

    public String getWorkQueueName() {
        return workQueueName;
    }

    public void setWorkQueueName(String workQueueName) {
        this.workQueueName = workQueueName;
    }

    /**
     * Gets the core pool size (threads to keep minimum in pool)
     *
     * @return the pool size
     */
    public Integer getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the core pool size (threads to keep minimum in pool)
     *
     * @param poolSize the pool size
     */
    public void setPoolSize(Integer poolSize) {
        this.poolSize = poolSize;
    }

    /**
     * Gets the maximum pool size
     *
     * @return the maximum pool size
     */
    public Integer getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Sets the maximum pool size
     *
     * @param maxPoolSize the max pool size
     */
    public void setMaxPoolSize(Integer maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Gets the keep alive time for inactive threads
     *
     * @return the keep alive time
     */
    public Long getKeepAliveTime() {
        return keepAliveTime;
    }

    /**
     * Sets the keep alive time for inactive threads
     *
     * @param keepAliveTime the keep alive time
     */
    public void setKeepAliveTime(Long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    /**
     * Gets the time unit used for keep alive time
     *
     * @return the time unit
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Sets the time unit used for keep alive time
     *
     * @param timeUnit the time unit
     */
    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    /**
     * Gets the maximum number of tasks in the work queue.
     * <p/>
     * Use <tt>-1</tt> or <tt>Integer.MAX_VALUE</tt> for an unbounded queue
     *
     * @return the max queue size
     */
    public Integer getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * Sets the maximum number of tasks in the work queue.
     * <p/>
     * Use <tt>-1</tt> or <tt>Integer.MAX_VALUE</tt> for an unbounded queue
     *
     * @param maxQueueSize the max queue size
     */
    public void setMaxQueueSize(Integer maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    /**
     * Gets the policy for tasks which cannot be executed by the thread pool.
     *
     * @return the policy for the handler
     */
    public RejectedExecutionHandler getRejectedPolicy() {
        return rejectedPolicy;
    }

    /**
     * Overwrites each attribute that is null with the attribute from defaultProfile 
     * 
     * @param defaultProfile profile with default values
     */
    public void addDefaults(ThreadPoolProfile defaultProfile) {
        if (defaultProfile == null) {
            return;
        }
        if (poolSize == null || poolSize <=0) {
            poolSize = defaultProfile.getPoolSize();
        }
        if (maxPoolSize == null || maxPoolSize <= 0) {
            maxPoolSize = defaultProfile.getMaxPoolSize();
        }
        if (keepAliveTime == null || keepAliveTime < 0) {
            keepAliveTime = defaultProfile.getKeepAliveTime();
        }
        if (timeUnit == null) {
            timeUnit = defaultProfile.getTimeUnit();
        }
        if (maxQueueSize == null) {
            maxQueueSize = defaultProfile.getMaxQueueSize();
        }
        if (rejectedPolicy == null) {
            rejectedPolicy = defaultProfile.getRejectedPolicy();
        }
        
        if (needSafeExit == null) {
        	needSafeExit = defaultProfile.getNeedSafeExit();
        }
    }

    @Override
    public ThreadPoolProfile clone() {
        ThreadPoolProfile cloned = new ThreadPoolProfile();
        cloned.setWorkQueueName(workQueueName);
        cloned.setKeepAliveTime(keepAliveTime);
        cloned.setMaxPoolSize(maxPoolSize);
        cloned.setMaxQueueSize(maxQueueSize);
        cloned.setPoolSize(maxPoolSize);
        cloned.setRejectedPolicy(rejectedPolicy);
        cloned.setTimeUnit(timeUnit);
        cloned.setNeedSafeExit(needSafeExit);
        return cloned;
    }

    /**
	 * @param rejectedPolicy2
	 */
    public void setRejectedPolicy(RejectedExecutionHandler rejectedPolicy) {
		this.rejectedPolicy = rejectedPolicy;
		
	}

	@Override
    public String toString() {
		return new StringBuilder().append("ThreadPoolProfile[").append(workQueueName)
		            .append(" size: ").append(poolSize)
		            .append("-").append(maxPoolSize)
		            .append(", keepAlive: ").append(keepAliveTime)
		            .append(" ").append(timeUnit)
		            .append(", maxQueue: ").append(maxQueueSize)
		            .append(", rejectedPolicy: ").append(rejectedPolicy.getClass().getSimpleName())
		            .append(", needSafeExit: ").append(needSafeExit)
		            .append("]")
		            .toString();
    }

	public Boolean getNeedSafeExit() {
		return needSafeExit;
	}

	public void setNeedSafeExit(Boolean needSafeExit) {
		this.needSafeExit = needSafeExit;
	}

}
