package com.king.gmms.util;

import java.util.*;

import com.king.framework.*;

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
public abstract class AbstractTimer {

    private static SystemLogger log = SystemLogger.getSystemLogger(AbstractTimer.class);
    private Timer timer = null;
    private long wakeupTime;
    private volatile boolean running = false;

    public AbstractTimer(long wakeupTime){
        this.wakeupTime = wakeupTime;
    }

    public void setWakeupTime(long time){
        this.wakeupTime = time;
    }

    public synchronized void startTimer() {
        if(running || wakeupTime <= 0L){
            return;
        }
        log.info("Start timer and wakeup time is {}", wakeupTime);
        timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                excute();
                //running = false;
            }
        }, wakeupTime, wakeupTime);
        running = true;
    }
    
    public synchronized void startTimer(String timerName) {
        if(running || wakeupTime <= 0L){
            return;
        }
        log.info("Start timer and wakeup time is {}", wakeupTime);
        timer = new Timer(timerName);
        timer.schedule(new TimerTask() {
            public void run() {
                excute();
                //running = false;
            }
        }, wakeupTime, wakeupTime);
        running = true;
    }

    public synchronized void restartTimer(){
        stopTimer();
        while(running){
            try{
                Thread.currentThread().sleep(10);
            }catch(Exception e){
                log.error(e,e);
            }
        }
        startTimer();
    }

    public synchronized void stopTimer() {
        if(running){
            log.info("Stop timer");
            if(timer != null){
                timer.cancel();
            }
            running = false;
        }
    }

    public abstract void excute();

    public boolean isRunning() {
        return running;
    }

}
