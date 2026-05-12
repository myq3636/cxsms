
package com.king.framework;

import com.king.gmms.ha.ModuleURI;
import com.king.message.gmms.GmmsMessage;

public class MessageLogger {
    //private static Logger log = Logger.getLogger("message");
    private static String module = System.getProperty("module");
    private static int size = 2048;

//    public static void logMessageViaJMS(ModuleURI uri,GmmsMessage message){
//        log.info(new StringBuilder(size).append(module)
//                .append(" sends following message[")
//                .append(message.getMsgID())
//                .append("] to ").append(uri)
//                .append(message).append("\n\n"));
//    }
//
//    public static void logMessageViaNetwork(GmmsMessage message) {
//        log.info(new StringBuilder(size).append(module)
//                .append(" tries to transfer the message[")
//                .append(message.getMsgID())
//                .append("]. The following message will be stored in DB:")
//                .append(message).append("\n\n"));
//    }
//
//    public static void logMessageError(GmmsMessage message) {
//        log.info(new StringBuilder(size).append(module)
//                .append(" tries to handle message[")
//                .append(message.getMsgID())
//                .append("]. But error occurs:")
//                .append(message).append("\n\n"));
//    }
//
//    public static void logMessage(String info) {
//        log.info(info);
//    }
//
//    public static void logMessage(String info, GmmsMessage message) {
//        log.info(new StringBuilder().append(info).append(":").append(message).append("\n\n"));
//    }
}
