/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.connectionpool.session.AbstractInternalSession;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.gmms.threadpool.ThreadPoolFactory;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.MessageBackupWriter;

/**
 * @version 
 */
public class DefaultExecutorServiceManager implements ExecutorServiceManager, LifecycleListener{
	private static SystemLogger log = SystemLogger.getSystemLogger(DefaultExecutorServiceManager.class);

    private ThreadPoolFactory defaultThreadPoolFactory = new DefaultThreadPoolFactory();
    private ThreadPoolFactory expiredThreadPoolFactory = new ExpiredThreadPoolFactory();
    private MessageBackupWriter safeExitWriter = MessageBackupWriter.getInstance();
    
    /**
     * pool list
     */
    private final LinkedBlockingDeque<ExecutorService> poolList = new LinkedBlockingDeque<ExecutorService>();
    
    /**
     * give a bit of time to shutdown nicely as the thread pool is most likely in the process of being shutdown
     */
    private long shutdownAwaitTermination = 2000;
    
    private ThreadPoolProfile defaultProfile;
    
    private LifecycleSupport lifecycle;
    

    public DefaultExecutorServiceManager(ThreadPoolProfile defaultProfile, LifecycleSupport lifecycleSupport) {
        this.defaultProfile = defaultProfile;
        this.lifecycle = lifecycleSupport;
        // use lowest priority 10
		lifecycle.addListener(Event.TYPE_SHUTDOWN, this, 9);
		
    }

    @Override
    public ExecutorService newDefaultThreadPool(Object source, String name) {
        return newThreadPool(source, name, defaultProfile);
    }

    @Override
    public ScheduledExecutorService newDefaultScheduledThreadPool(Object source, String name) {
        return newScheduledThreadPool(source, name, defaultProfile);
    }

    @Override
    public ExecutorService newThreadPool(Object source, String name, ThreadPoolProfile profile) {
        profile.addDefaults(defaultProfile);
        
        ThreadFactory threadFactory = createThreadFactory(name);
        ExecutorService executorService = defaultThreadPoolFactory.newThreadPool(profile, threadFactory);
        onThreadPoolCreated(executorService, source);
        if (log.isDebugEnabled()) {
            log.debug("Create ThreadPool {} for {} with {}", new Object[]{name, source.getClass().getSimpleName(), profile});
        }

        return executorService;
    }

    @Override
    public ExecutorService newFixedThreadPool(Object source, String name, int poolSize) {
        ThreadPoolProfile profile = new ThreadPoolProfile(name);
        profile.setPoolSize(poolSize);
        profile.setMaxPoolSize(poolSize);
        profile.setKeepAliveTime(0L);
        return newThreadPool(source, name, profile);
    }

    @Override
    public ScheduledExecutorService newScheduledThreadPool(Object source, String name, ThreadPoolProfile profile) {
        profile.addDefaults(defaultProfile);
        
        ScheduledExecutorService answer = defaultThreadPoolFactory.newScheduledThreadPool(profile, createThreadFactory(name));
        onThreadPoolCreated(answer, source);

        if (log.isDebugEnabled()) {
            log.debug("Create ThreadPool(Scheduled) {} for {} with {}", new Object[]{name, source.getClass().getSimpleName(), profile});
        }
        return answer;
    }

    @Override
    public void shutdown(ExecutorService executorService) {
        boolean warned =  doShutdown(executorService, shutdownAwaitTermination, true);
        
        // log the thread pools which was forced to shutdown so it may help the user to identify a problem of his
        if (warned) {
            log.warn("Forced shutdown of ExecutorService's which has not been shutdown properly -> {}", executorService);
        }
    }

    /**
     * 
     * @param executorService
     * @param shutdownAwaitTermination
     * @return
     */
    private boolean doShutdown(ExecutorService executorService, long shutdownAwaitTermination, boolean isShutdownSingle) {
        if (executorService == null) {
            return false;
        }

        boolean warned = false;

        if (!executorService.isShutdown()) {
        	
        	if (log.isDebugEnabled()) {
        		log.debug("Start doShutdown {}", new Object[]{executorService});
        	}
        	StopWatch watch = new StopWatch();
        	
        	executorService.shutdown();
        	
        	// special process for ExpiredDynamicBlockingQueue: safe shutdown and expired task
        	if (executorService instanceof A2PThreadPoolExecutor) {
            	A2PThreadPoolExecutor a2pExecutorService = (A2PThreadPoolExecutor)executorService;
            	BlockingQueue<?> workQueue = a2pExecutorService.getQueue();
            	if (workQueue instanceof ExpiredDynamicBlockingQueue) {
            		// deregister expired task queue
            		ExpiredThreadPoolTaskQueueManager.getInstance().deregister((ExpiredDynamicBlockingQueue<?>)workQueue);
            		
            		if (isShutdownSingle) { // shutdown single pool
            			ExpiredDynamicBlockingQueue<?> expiredWorkQueue = (ExpiredDynamicBlockingQueue<?>) workQueue;
            			QueueTimeoutInterface listener = expiredWorkQueue.getListener();
            			List<Runnable> droppedTasks = a2pExecutorService.shutdownNow();
            			handleTimeoutMessage(droppedTasks, listener, a2pExecutorService);
            			
            			if (log.isDebugEnabled()) {
                    		log.debug("Expired {} is shutdown: {} and terminated: {} took: {}.",
                            new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), printDuration(watch.taken())});
                    	}
            			poolList.remove(executorService);
            			return warned;
            			
            		} else { // shutdown all pools of the module
            			// special process for those need safe shutdown, just write to db for the task
                    	boolean needSafeExit = a2pExecutorService.isNeedSafeExit();
                    	if (needSafeExit) {
                    		List<Runnable> droppedTasks = a2pExecutorService.shutdownNow();
                    		sendbackMessage(droppedTasks, a2pExecutorService);
                    		if (log.isDebugEnabled()) {
                        		log.debug("Expired {} is shutdown with safe exit: {} and terminated: {} took: {}.",
                                new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), printDuration(watch.taken())});
                        	}
                    		return warned;
                    	}
                    	// if not needSafeExit, go to next step 
            		}
            	}
            }
        	
        	// shutting down a normal thread pool is a 2 step process. First we try graceful, and if that fails, then we go more aggressively
            // and try shutting down again. In both cases we wait at most the given shutdown timeout value given
            // (total wait could then be 2 x shutdownAwaitTermination, but when we shutdown the 2nd time we are aggressive and thus
            // we ought to shutdown much faster)
            if (shutdownAwaitTermination > 0) {
                try {
                    if (!awaitTermination(executorService, shutdownAwaitTermination)) {
                        warned = true;
                        log.warn("Forcing shutdown of ExecutorService: {} due first await termination elapsed.", executorService);
                        executorService.shutdownNow();
                        // we are now shutting down aggressively, so wait to see if we can completely shutdown or not
                        if (!awaitTermination(executorService, shutdownAwaitTermination)) {
                            log.warn("Cannot completely force shutdown of ExecutorService: {} due second await termination elapsed.", executorService);
                        }
                    }
                } catch (InterruptedException e) {
                    warned = true;
                    log.warn("Forcing shutdown of ExecutorService: {} due interrupted.", executorService);
                    // we were interrupted during shutdown, so force shutdown
                    executorService.shutdownNow();
                }
            }

            // report when we are complete so the end user can see this in the log
            if (warned) {
                log.info("{} is shutdown: {} and terminated: {} took: {}.",
                        new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), printDuration(watch.taken())});
            } else {
            	if (log.isDebugEnabled()) {
            		log.debug("{} is shutdown: {} and terminated: {} took: {}.",
                    new Object[]{executorService, executorService.isShutdown(), executorService.isTerminated(), printDuration(watch.taken())});
            	}
            }
        }

        // remove reference as its shutdown
        if (isShutdownSingle) {
            poolList.remove(executorService);
        }

        return warned;
    }

    @Override
	public void shutdownAll() throws Exception {
        // shutdown all remainder executor services by looping
        Set<ExecutorService> forced = new LinkedHashSet<ExecutorService>();
        if (!poolList.isEmpty()) {
            for (ExecutorService executorService : poolList) {
                try {
                    boolean warned = doShutdown(executorService, shutdownAwaitTermination, false);
                    // remember the thread pools that was forced to shutdown (eg warned)
                    if (warned) {
                        forced.add(executorService);
                    }
                } catch (Throwable e) {
                    // only log if something goes wrong as we want to shutdown them all
                    log.warn("Error occurred during shutdown of ExecutorService: "
                            + executorService + ". This exception will be ignored.", e);
                }
            }
        }

        // log the thread pools which was forced to shutdown so it may help the user to identify a problem of his
        if (!forced.isEmpty()) {
            log.warn("Forced shutdown of {} ExecutorService's which has not been shutdown properly", forced.size());
            for (ExecutorService executorService : forced) {
                log.warn("  forced -> {}", executorService);
            }
        }
        forced.clear();

        // clear list
        poolList.clear();

    }

    /**
     * Awaits the termination of the thread pool.
     * <p/>
     * This implementation will log every 2nd second at INFO level that we are waiting, so the end user
     * can see we are not hanging in case it takes longer time to terminate the pool.
     *
     * @param executorService            the thread pool
     * @param shutdownAwaitTermination   time in millis to use as timeout
     * @return <tt>true</tt> if the pool is terminated, or <tt>false</tt> if we timed out
     * @throws InterruptedException is thrown if we are interrupted during waiting
     */
    private boolean awaitTermination(ExecutorService executorService, long shutdownAwaitTermination) throws InterruptedException {
        // log progress every 2nd second so end user is aware of we are shutting down
        StopWatch watch = new StopWatch();
        long interval = Math.min(2000, shutdownAwaitTermination);
        boolean done = false;
        while (!done && interval > 0) {
            if (executorService.awaitTermination(interval, TimeUnit.MILLISECONDS)) {
                done = true;
            } else {
            	if (log.isInfoEnabled()) {
            		log.info("Waited {} for ExecutorService: {} to terminate...", printDuration(watch.taken()), executorService);
            	}
                // recalculate interval
                interval = Math.min(2000, shutdownAwaitTermination - watch.taken());
            }
        }

        return done;
    }
    
    /**
     * Invoked when a new thread pool is created.
     * @param executorService the thread pool
     * @param source          the source to use the thread pool
     */
    private void onThreadPoolCreated(ExecutorService executorService, Object source) {
        // add to list of thread pools
    	// do this so that shutdown sequence would be: first customer(receiver/sender) pool, then internal pool
    	if (source instanceof AbstractInternalSession) {
    		poolList.addLast(executorService);
    	} else if (source instanceof SystemSession) {
    		// don't add systemsession pool to list here, it will shutdown by SystemSession.moduleStop()
    	} else {
    		poolList.addFirst(executorService);
    	}
    }

    private ThreadFactory createThreadFactory(String name) {
        ThreadFactory threadFactory = new A2PThreadFactory(name);
        return threadFactory;
    }

    /** 
	 * @param source
	 * @param name
	 * @return
	 * @see com.king.gmms.threadpool.ExecutorServiceManager#newDefaultExpiredThreadPool(java.lang.Object, java.lang.String)
	 */
    @Override
	public ExecutorService newDefaultExpiredThreadPool(Object source, String name, QueueTimeoutInterface timeoutHandler, long timeoutMillis) {
		return newExpiredThreadPool(source, name, defaultProfile, timeoutHandler, timeoutMillis);
	}
	
	public ExecutorService newExpiredThreadPool(Object source, String name, ThreadPoolProfile profile, QueueTimeoutInterface timeoutHandler, long timeoutMillis) {
        profile.addDefaults(defaultProfile);
        
        ThreadFactory threadFactory = createThreadFactory(name);
        ExecutorService executorService = expiredThreadPoolFactory.newThreadPool(profile, threadFactory, timeoutHandler, timeoutMillis);
        onThreadPoolCreated(executorService, source);
        
        if (log.isDebugEnabled()) {
            log.debug("Create ThreadPool(Expired) {} for {} with {}", new Object[]{name, source.getClass().getSimpleName(), profile});
        }

        return executorService;
    }

	private String printDuration(double uptime) {
        // Code taken from Karaf
        // https://svn.apache.org/repos/asf/karaf/trunk/shell/commands/src/main/java/org/apache/karaf/shell/commands/impl/InfoAction.java

        NumberFormat fmtI = new DecimalFormat("###,###", new DecimalFormatSymbols(Locale.ENGLISH));
        NumberFormat fmtD = new DecimalFormat("###,##0.000", new DecimalFormatSymbols(Locale.ENGLISH));

        uptime /= 1000;
        if (uptime < 60) {
            return fmtD.format(uptime) + " seconds";
        }
        uptime /= 60;
        if (uptime < 60) {
            long minutes = (long) uptime;
            String s = fmtI.format(minutes) + (minutes > 1 ? " minutes" : " minute");
            return s;
        }
        uptime /= 60;
        if (uptime < 24) {
            long hours = (long) uptime;
            long minutes = (long) ((uptime - hours) * 60);
            String s = fmtI.format(hours) + (hours > 1 ? " hours" : " hour");
            if (minutes != 0) {
                s += " " + fmtI.format(minutes) + (minutes > 1 ? " minutes" : " minute");
            }
            return s;
        }
        uptime /= 24;
        long days = (long) uptime;
        long hours = (long) ((uptime - days) * 24);
        String s = fmtI.format(days) + (days > 1 ? " days" : " day");
        if (hours != 0) {
            s += " " + fmtI.format(hours) + (hours > 1 ? " hours" : " hour");
        }
        return s;
    }
	
	/**
	 * for safe exit, write msg to db
	 * @param taskList
	 */
	private void sendbackMessage(List<Runnable> taskList, ExecutorService executorService) {
		for (Runnable task : taskList) {
			try {
				if (task instanceof RunnableMsgTask) {
					RunnableMsgTask msgTask = (RunnableMsgTask) task;
					if(log.isInfoEnabled()){
						log.info(msgTask.getMsg(), "{} backup message!", executorService);
					}
					safeExitWriter.backupMessage(msgTask.getMsg());
				} else {
					if (log.isInfoEnabled()) {
						log.info("Failed to backup message from task {}", task.toString());
					}
				}

			} catch (Exception e) {
				log.warn("Failed to backup message", e);
			}
		}
	}
	
	private void handleTimeoutMessage(List<Runnable> taskList, QueueTimeoutInterface listener, ExecutorService executorService) {
		for (Runnable task : taskList) {
			try {
				if (task instanceof RunnableMsgTask) {
					RunnableMsgTask msgTask = (RunnableMsgTask) task;
					if (log.isInfoEnabled()) {
						log.info(msgTask.getMsg(), "{} handleTimeoutMessage", executorService);
					}
					listener.timeout(msgTask.getMsg());
				} else {
					if (log.isInfoEnabled()) {
						log.info("Failed to handleTimeoutMessage {}", task.toString());
					}
				}

			} catch (Exception e) {
				log.warn("Failed to handleTimeoutMessage", e);
			}
		}
		
	}

	/** 
	 * @param event
	 * @return
	 * @see com.king.framework.lifecycle.LifecycleListener#OnEvent(com.king.framework.lifecycle.event.Event)
	 */
	@Override
	public int OnEvent(Event event) {
		if (log.isTraceEnabled()) {
			log.trace("DefaultExecutorServiceManager Event Received. Type: {}", event.getEventType());
		}
		
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			try {
				shutdownAll();
			} catch (Exception e) {
				log.error(e, e);
			}
		}
		return 0;
	}

	@Override
	public boolean updateThreadPoolProfile(ThreadPoolExecutor pool, ThreadPoolProfile profile, String name) {
		profile.addDefaults(defaultProfile);
		
		int maxPoolSize = profile.getMaxPoolSize();
		int minPoolSize = profile.getPoolSize();
		if (maxPoolSize < minPoolSize) {
            log.warn("maxPoolSize must be >= corePoolSize, was {}, set to corePoolSize {}", maxPoolSize, minPoolSize);
            profile.setMaxPoolSize(minPoolSize);
        }
		
		if (log.isDebugEnabled()) {
			log.debug("update {} with {} ", pool, profile);
		}
		
		// update current thread name
		String currenthreadName = Thread.currentThread().getName();
		if (currenthreadName.lastIndexOf(ThreadHelper.THREAD_COUNTER_SEPERATOR)>0) {
			String counterPart = currenthreadName.substring(currenthreadName.lastIndexOf(ThreadHelper.THREAD_COUNTER_SEPERATOR));
			Thread.currentThread().setName(name + counterPart);
		}
		
		pool.setCorePoolSize(profile.getPoolSize());
		pool.setMaximumPoolSize(profile.getMaxPoolSize());
		pool.setKeepAliveTime(profile.getKeepAliveTime(), profile.getTimeUnit());
		pool.setRejectedExecutionHandler(profile.getRejectedPolicy());
		
		ThreadFactory threadFactory = pool.getThreadFactory();
		if (threadFactory instanceof A2PThreadFactory) {
			((A2PThreadFactory)threadFactory).setName(name);
		}
		
		return true;
	}

}
