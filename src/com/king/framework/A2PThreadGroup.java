
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
        // L-6: 栈可能为空(某些 JVM 优化/重复抛出会省略栈)，避免 getStackTrace()[0] 越界掩盖原始错误。
        StackTraceElement[] st = (e == null) ? null : e.getStackTrace();
        String cls = (st != null && st.length > 0) ? st[0].getClassName() : A2PThreadGroup.class.getName();
        Logger log = LogManager.getLogger(cls);
        log.fatal("Uncaught exception thrown from thread {}",t.getName(), e);
        MailSender.getInstance().sendAlertMail("A2P alert mail from " +
                                               ModuleURI.self().getModule() + " in " +
                                               ModuleURI.self().getAddress() +
                                               " For ThreadGroup Uncaught Exception: " +
                                               t.getName(), e);
    }
}
