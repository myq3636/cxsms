package com.king.gmms;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.ModuleURI;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;

import java.io.*;
import java.util.*;

public class MailSender {
    private static SystemLogger log = SystemLogger.getSystemLogger(MailSender.class);
    private static MailSender instance = new MailSender();
    private List<InternetAddress> recpToAddresses;
    private Properties configure;
    private Map<Class, Long> previousMailTimes;
    private long alertFreq;

    private MailSender() {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(System.getProperty("a2p_home") + "Gmms/GmmsConfig.properties"));
            configure = new Properties();
            configure.put("mail.smtp.host", properties.getProperty("MailHost", "10.10.8.90"));
            configure.put("mail.smtp.port", properties.getProperty("MailPort", "25"));
            configure.put("mail.smtp.from", properties.getProperty("MailFrom", "king@133.com"));
            configure.put("mail.smtp.localhost", "A2P");
            recpToAddresses = new ArrayList<InternetAddress>();
            StringTokenizer st = new StringTokenizer(properties.getProperty("MailList"), ",");
            while(st.hasMoreTokens()) {
                recpToAddresses.add(new InternetAddress(st.nextToken()));
            }
            alertFreq = Integer.parseInt(properties.getProperty("AlertMailFrequence", "5").trim()) * 60 * 1000;
            previousMailTimes = new HashMap<Class, Long>();
        }
        catch(Exception e) {
            log.error(e, e);
            e.printStackTrace();
        }
    }

    public static MailSender getInstance() {
        return instance;
    }

    public void sendAlertMail(String subject, String text, String filename) {
        /*try {
            javax.mail.Session session = javax.mail.Session.getInstance(configure);
            MimeMessage message = new MimeMessage(session);

            message.setSentDate(new Date());
            message.setSubject(subject);
            message.setRecipients(Message.RecipientType.TO, recpToAddresses.toArray(new InternetAddress[recpToAddresses.size()]));

            MimeMultipart multi = new MimeMultipart();
            BodyPart textBodyPart = new MimeBodyPart();
            textBodyPart.setText(text);
            multi.addBodyPart(textBodyPart);
            if(filename != null) {
                FileDataSource fds = new FileDataSource(filename);
                BodyPart fileBodyPart = new MimeBodyPart();
                fileBodyPart.setDataHandler(new DataHandler(fds));
                fileBodyPart.setFileName(filename);
                multi.addBodyPart(fileBodyPart);
            }
            message.setContent(multi);
            message.saveChanges();
            Transport trans = session.getTransport("smtp");
            trans.connect();
            Transport.send(message, recpToAddresses.toArray(new InternetAddress[recpToAddresses.size()]));
            trans.close();
        }
        catch(Exception e) {
            log.error(e, e);
            e.printStackTrace();
        }*/
    }

    public synchronized void sendAlertMail(String subject, Throwable e) {
        /*if(previousMailTimes.containsKey(e.getClass())
           && System.currentTimeMillis() - previousMailTimes.get(e.getClass()) < alertFreq) {
            return;
        }
        previousMailTimes.put(e.getClass(), System.currentTimeMillis());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        MailSender.getInstance().sendAlertMail(subject, "Exception on " + ModuleURI.self() + "\n" + baos.toString(), null);*/
    }
}
