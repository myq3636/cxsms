package com.king.gmms.mqm;

import java.lang.reflect.Constructor;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.task.TaskTimer;

public class TaskFactory {
    private static SystemLogger log = SystemLogger.getSystemLogger(TaskFactory.class);

    /**
     * create object
     * @param args Object[]
     * @param className String
     * @return Object
     * @throws ConfigurationException
     */
    public static TaskTimer createTask(String className, Object[] args) {
         Class c = null;
         Object o = null;
         try {
             c = Class.forName("com.king.gmms.mqm.task." + className);
             if (args == null || args.length == 0) {
                 o = c.newInstance();
             }
             else {
                 Class[] intArgsClass = new Class[args.length];
                 for(int i = 0 ; i < args.length ; i++){
                     intArgsClass[i] = args[i].getClass();
                 }
                 Constructor constructor = c.getConstructor(intArgsClass);
                 o = constructor.newInstance(args);
             }
         } catch (Exception ex) {
             ex.printStackTrace();
             log.error(ex.getMessage());
         }
         if (o instanceof TaskTimer) {
             return (TaskTimer) o;
         }
         else {
             log.warn("Create TaskTimer for MQM failed, Task name is: {}",
                      className);
             return null;
         }
    }
}
