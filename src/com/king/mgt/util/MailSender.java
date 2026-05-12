package com.king.mgt.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.ModuleURI;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemResponse;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.context.ContextManager;
import com.king.mgt.context.Module;

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
public class MailSender {
    private static SystemLogger log = SystemLogger.getSystemLogger(MailSender.class);
    private static MailSender instance = new MailSender();
    private List<InternetAddress> recpToAddresses;
    private Properties configure;
    private Map<Class, Long> previousMailTimes;
    private long alertFreq;
    private UserInterfaceUtility util;
    private InfoTable info;
    private MailSender() {
        try {
            util = UserInterfaceUtility.getInstance();
            info = util.getInfoTable();
            Properties properties = util.getProperties();
            configure = new Properties();
            configure.put("mail.smtp.host",
                          properties.getProperty("MailHost", "10.10.8.90"));
            configure.put("mail.smtp.port",
                          properties.getProperty("MailPort", "25"));
            configure.put("mail.smtp.from",
                          properties.getProperty("MailFrom", "gmdadm@King.com"));
            configure.put("mail.smtp.localhost", "GMD");
            recpToAddresses = new ArrayList<InternetAddress> ();
            StringTokenizer st = new StringTokenizer(properties.getProperty(
                "MailList"), ",");
            while (st.hasMoreTokens()) {
                recpToAddresses.add(new InternetAddress(st.nextToken()));
            }
            alertFreq = Integer.parseInt(properties.getProperty(
                "AlertMailFrequence", "5").trim()) * 60 * 1000;
            previousMailTimes = new HashMap<Class, Long> ();
        }
        catch (Exception e) {
            log.error(e, e);
        }
    }

    public static MailSender getInstance() {
        return instance;
    }

    public void sendAlertMailByResponse(UserCommand cmd) {
        StringBuffer body = new StringBuffer();
        SystemCommand[] sysCmds = cmd.getSystemCommands();
        for (int i = 0; i < sysCmds.length; i++) {
            SystemResponse response=sysCmds[i].getResponse();
            if (response==null){
                response=new SystemResponse();
                response.setResult(SystemResponse.COMMUNICATION_ERROR);
            }

            if (!response.isSuccess()) {
                Module module = sysCmds[i].getModule();
                // Format:
                // ModuleName(IP:Port) --- Response Result Text
                // eg.
                // DeliveryRouter(127.0.0.1:2345)  ---  Execution Failed
                body.append(module.getName()).append("(").append(module.getIp()).
                    append(":").append(module.getPort()).append(")").append(
                    "  ---  ").append(response.getErrorText()).append("\n");
            }
        }
        if (body.length()==0)
        {// no error.
            return;
        }
        this.sendAlertMail(cmd, body.toString());
    }

    public void sendAlertMail(UserCommand cmd, int errCode) {
        this.sendAlertMail(cmd, info.get(errCode));
    }

    public void sendAlertMail(UserCommand cmd, String description) {
        String subject = info.get(InfoTable.ALERT_MAIL_SUBJECT);
        SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss zzz");
        TimeZone zone = TimeZone.getTimeZone("GMT");
        dateFormat.setTimeZone(zone);
        String time = dateFormat.format(new Date());
        ContextManager context = cmd.getContext();
        String userName = context.getCurUser();
        String ip = context.getCurUserIP();
        String cmdLine = cmd.getCmdLine();
        StringBuffer body = new StringBuffer();
        body.append("Time: ").append(time).append("\n")
            .append("User: ").append(userName).append("@").append(ip).append(
            "\n")
            .append("Commnad: ").append(cmdLine).append("\n")
            .append("\n")
            .append(description);
        this.sendAlertMail(subject, body.toString(), null);
//        this.debugAlertMail(subject, body.toString());
    }
    public void debugAlertMail(String subject, String text)
    {
        log.trace("============= Alert Mail ============");
        log.trace("Subject: {}",subject);
        log.trace(text);
    }
    public void sendAlertMail(String subject, String text, String filename) {
        try {
            javax.mail.Session session = javax.mail.Session.getInstance(
                configure);
            MimeMessage message = new MimeMessage(session);

            message.setSentDate(new Date());
            message.setSubject(subject);
            message.setRecipients(Message.RecipientType.TO,
                                  recpToAddresses.
                                  toArray(new InternetAddress[recpToAddresses.
                                          size()]));

            MimeMultipart multi = new MimeMultipart();
            BodyPart textBodyPart = new MimeBodyPart();
            textBodyPart.setText(text);
            multi.addBodyPart(textBodyPart);
            if (filename != null) {
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
            Transport.send(message,
                           recpToAddresses.toArray(new InternetAddress[recpToAddresses.
                size()]));
            trans.close();
        }
        catch (Exception e) {
            log.error(e, e);
        }
    }

    public synchronized void sendAlertMail(String subject, Exception e) {
        if (previousMailTimes.containsKey(e.getClass())
            &&
            System.currentTimeMillis() - previousMailTimes.get(e.getClass()) < alertFreq) {
            return;
        }
        previousMailTimes.put(e.getClass(), System.currentTimeMillis());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        log.error(e.getMessage());
        MailSender.getInstance().sendAlertMail(subject,
                                               "Exception on " + ModuleURI.self() +
                                               "\n" + baos.toString(), null);
    }
}
