
package com.king.framework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;

public class A2PThreadGroup extends ThreadGroup{
    private static A2PThreadGroup instance = new A2PThreadGroup("Thread Group");

    private A2PThreadGroup(String name) {
        super(name);
    }

    private A2PThreadGroup(A2PThreadGroup parent, String name) {
        super(parent, name);
    }

    public static A2PThreadGroup getInstance() {
        return instance;
    }

    public void uncaughtException(Thread t, Throwable e) {
        Logger log = LogManager.getLogger(e.getStackTrace()[0].getClassName());
        log.fatal("Uncaught exception thrown from thread {}",t.getName(), e);       
        MailSender.getInstance().sendAlertMail("A2P alert mail from " +
                                               ModuleURI.self().getModule() + " in " +
                                               ModuleURI.self().getAddress() +
                                               " For ThreadGroup Uncaught Exception: " +
                                               t.getName(), e);
    }
}
