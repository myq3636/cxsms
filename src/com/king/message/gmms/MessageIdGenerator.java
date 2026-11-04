package com.king.message.gmms;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

import com.king.framework.SystemLogger;

/**
 * <p>Title: MessageIdGenerator</p>
 * <p>Description: To generate message id</p>
 * <p>Copyright: Copyright (c) 2001-2010</p>
 * <p>Company: King.Inc</p>
 *
 * @version 7.2.x
 * @author: Neal
 */
public class MessageIdGenerator {

    private static int inmsgidSn = 0;
    private static int outmsgidSn = 0;
    private static int msgidSn = 0;
    private static int cmppSn = 0;
    private volatile int hexSn = 0;
    private volatile int decSn = 0;
    private static int msgStrSn = 0;
    private static int nodeId = 0; // V4.0 Node ID for multi-node uniqueness
    private static boolean includeNodeId = false;

    /** v2 format (MessageIdFormat=v2, set up by MessageIdSetup): ids of different modules cannot be equal. */
    public static final long V2_COUNTER_SPACE = 1000000000L;
    /** hex ids: the module number in the top 4 bits, so always 8 hex digits, and a counter in the other 28 */
    private static final int V2_HEX_COUNTER_BITS = 28;
    private static final long V2_HEX_COUNTER_SPACE = 1L << V2_HEX_COUNTER_BITS;
    private static final long V2_HEX_COUNTER_PER_SECOND = 2000L;
    private static long v2HexOffset = 0L;
    private static long v2HexCounter = 0L;
    private static volatile int v2ModuleNo = 0; // 0: not enabled, the v1 format is used
    private static long v2CounterPerSecond = 5000L;
    private static long v2Offset = 0L;
    private static long v2Counter = 0L;
    private static final Object v2Mutex = new Object();
    private static java.util.function.LongSupplier clock = System::currentTimeMillis;

    private static Object mutex1 = new Object();
    private static Object mutex2 = new Object();
    private static Object mutex3 = new Object();
    private Object mutex4 = new Object();
    private Object mutex5 = new Object();
    private static Object mutex6 = new Object();
    private static Object mutex7 = new Object();
    private static Object mutex8 = new Object();

    
    private static MessageIdGenerator instance = new MessageIdGenerator();
    
    private MessageIdGenerator(){
    }

    public static MessageIdGenerator getInstance(){
    	return instance;
    }

    public static void setNodeId(int id) {
        nodeId = id;
    }

    public static int getNodeId() {
        return nodeId;
    }

    public static void setIncludeNodeId(boolean include) {
        includeNodeId = include;
    }

    public static boolean isIncludeNodeId() {
        return includeNodeId;
    }

    /** @param otherHalf Client modules: half a cycle from the Servers, which have the same module numbers */
    public static void enableV2(int moduleNo, long counterPerSecond, boolean otherHalf, long savedCounter, long savedHexCounter) {
        synchronized (v2Mutex) {
            v2CounterPerSecond = counterPerSecond;
            v2Offset = otherHalf ? V2_COUNTER_SPACE / 2 : 0L;
            v2HexOffset = otherHalf ? V2_HEX_COUNTER_SPACE / 2 : 0L;
            v2Counter = Math.max(v2Counter, savedCounter);
            v2HexCounter = Math.max(v2HexCounter, savedHexCounter);
            v2ModuleNo = moduleNo;
        }
    }

    public static long v2Counter() {
        synchronized (v2Mutex) {
            return v2Counter;
        }
    }

    public static long v2HexCounter() {
        synchronized (v2Mutex) {
            return v2HexCounter;
        }
    }

    /** How far the further of the two counters is ahead of the clock, in seconds. */
    public static long v2AheadSeconds() {
        synchronized (v2Mutex) {
            long dec = (v2Counter - v2Floor()) / v2CounterPerSecond;
            long hex = (v2HexCounter - v2HexFloor()) / V2_HEX_COUNTER_PER_SECOND;
            return Math.max(0L, Math.max(dec, hex));
        }
    }

    /** The counter never goes back and follows the clock when traffic is below counterPerSecond. */
    private static long v2Floor() {
        return clock.getAsLong() / 1000L * v2CounterPerSecond + v2Offset;
    }

    private static long v2HexFloor() {
        return clock.getAsLong() / 1000L * V2_HEX_COUNTER_PER_SECOND + v2HexOffset;
    }

    private static int nextV2HexID() {
        long counter;
        synchronized (v2Mutex) {
            v2HexCounter = Math.max(v2HexCounter + 1L, v2HexFloor());
            counter = v2HexCounter;
        }
        return (v2ModuleNo << V2_HEX_COUNTER_BITS) | (int) (counter & (V2_HEX_COUNTER_SPACE - 1L));
    }

    private static String nextV2DecID() {
        long counter;
        synchronized (v2Mutex) {
            v2Counter = Math.max(v2Counter + 1L, v2Floor());
            counter = v2Counter;
        }
        return v2ModuleNo + String.format("%09d", counter % V2_COUNTER_SPACE);
    }

    /** Different modules never share a sequence value under v2: with the module number in front of it. */
    private static int withModuleNo(int sn) {
        int moduleNo = v2ModuleNo;
        return moduleNo > 0 ? moduleNo * 10000 + sn : sn;
    }

    /**
     *
     * @param ssid int, inMsgId:ossid; outMsgId:rssid; msgid:ossid
     * @return String
     */
    private static String generateStringID(int ssid, int sn) {
    	StringBuilder builder = new StringBuilder();
    	builder.append(ssid).append("_");
    	if (includeNodeId) {
    		builder.append(nodeId).append("_");
    	}
    	builder.append(System.currentTimeMillis()).append(sn);
        return builder.toString();
    }

    public static String generateCommonInMsgID(int ssid) {
        synchronized (mutex1) {
            if(++inmsgidSn >= 9999) {
                inmsgidSn = 0;
            }
            return generateStringID(ssid, withModuleNo(inmsgidSn));
        }
    }

    public static String generateCommonOutMsgID(int ssid) {
        synchronized (mutex2) {
            if(++outmsgidSn >= 9999) {
                outmsgidSn = 0;
            }
            return generateStringID(ssid, withModuleNo(outmsgidSn));
        }
    }
    
    public static String generateCommonStringID() {
        synchronized (mutex7){
            if(++msgStrSn >= 9999) {
            	msgStrSn = 0;
            }
            int moduleNo = v2ModuleNo;
            if (moduleNo > 0) {
                return new SimpleDateFormat("MMddHHmmss").format(new Date(clock.getAsLong())) + moduleNo
                        + String.format("%04d", msgStrSn);
            }
            SimpleDateFormat df = new SimpleDateFormat("yyMMddHHmmss");
            return ("" + df.format(new Date()).substring(1) + msgStrSn);
        }
    }

    public static String generateCommonMsgID(int ssid) {
        synchronized (mutex3){
            if(++msgidSn >= 9999) {
                msgidSn = 0;
            }
            return generateStringID(ssid, withModuleNo(msgidSn));
        }
    }
    
    public static String generateCommonMsgID(int ssid , int moduleIndex) {
        if (moduleIndex > 0) {
        	moduleIndex = moduleIndex % 6;
        }
        synchronized (mutex3){
            if(++msgidSn >= 9999) {
                msgidSn = 0;
            }
            // v2: the fixed module number; the position % 6 repeats from the seventh module
            return generateStringID(ssid, v2ModuleNo > 0 ? withModuleNo(msgidSn) : (moduleIndex+1)*10000+msgidSn);
        }
    }

    /**
     * length 8, max:FFFFFFFF
     *
     * @param moduleNum int, 0:SMPP3.3 Server, 1:Dacom, 2:KTF, 3:SMPP3.3 Client
     * @return int
     */
    public int generateHexID(int moduleNum) {
        if (v2ModuleNo > 0) {
            return nextV2HexID();
        }
        if (moduleNum > 0) {
            moduleNum = moduleNum % 6;
        }
        synchronized (mutex4){
            GregorianCalendar currentDay = new GregorianCalendar();
            // dayOfWeek: [1-7]
            int dayOfWeek = currentDay.get(Calendar.DAY_OF_WEEK);

            int hourOfDay = currentDay.get(Calendar.HOUR_OF_DAY);
            int minute = currentDay.get(Calendar.MINUTE);
            int second = currentDay.get(Calendar.SECOND);
            // half_secondOfDay: [0000-A8C0]
            int half_secondOfDay = (hourOfDay * 3600 + minute * 60 + second) / 2;

            int id = dayOfWeek * 0x10000000 + half_secondOfDay * 0x1000 +
                    moduleNum * 1024;
            if(++hexSn >= 1024) {
                hexSn = 0;
            }
            id =  id + hexSn;
            return id;
        }
    }

    /**
     * length 10, for SMPP 3.4,
     * Note: do not call this mothod in other modules
     *
     * @param moduleNum int, 0:Server, 1:Client
     * @return int
     */
    public String generateDecID(int moduleNum) {
        if (v2ModuleNo > 0) {
            return nextV2DecID();
        }
        if (moduleNum > 0) {
            moduleNum = moduleNum % 6;
        }
       synchronized (mutex5){
           GregorianCalendar currentDay = new GregorianCalendar();
           int dayOfMonth = currentDay.get(Calendar.DAY_OF_MONTH);
           int hourOfDay = currentDay.get(Calendar.HOUR_OF_DAY);
           // hourOfMonth: [024-767]
           int hourOfMonth = dayOfMonth * 24 + hourOfDay;

           int minute = currentDay.get(Calendar.MINUTE);
           int second = currentDay.get(Calendar.SECOND);
           // secondOfHour: [0000-3600)
           int secondOfHour = minute * 60 + second;

           //Six digits for time and modulenum
           String s1 = String.format("%07d", hourOfMonth * 10000 + secondOfHour + 1000 * (moduleNum+4)).substring(1);
           //Four digits for sequence
            if(++decSn >= 9999) {
                decSn = 0;
            }
            String s2 = String.format("%04d", decSn);
            return s1+s2;
        }
    }
    
    /**
     * length 13, for SMPP 3.4,
     * Note: do not call this mothod in other modules
     *
     * @param moduleNum int, 0:Server, 1:Client
     * @return int
     */
    public String generateMaxDecID(int moduleNum) {
        if (moduleNum > 0) {
            moduleNum = moduleNum % 10;
        }
       synchronized (mutex5){
           GregorianCalendar currentDay = new GregorianCalendar();
           int dayOfMonth = currentDay.get(Calendar.DAY_OF_MONTH);
           int hourOfDay = currentDay.get(Calendar.HOUR_OF_DAY);
           // hourOfMonth: [024-767]
           int hourOfMonth = dayOfMonth * 24 + hourOfDay;

           int minute = currentDay.get(Calendar.MINUTE);
           int second = currentDay.get(Calendar.SECOND);
           // secondOfHour: [0000-3600)
           int secondOfHour = minute * 60 + second;

           //Six digits for time and modulenum
           //String s1 = String.format("%07d", hourOfMonth * 10000 + secondOfHour + 1000 * (moduleNum+4)).substring(1);
           String s1 = String.format("%08d", hourOfMonth * 100000 + secondOfHour + 10000 * (moduleNum));
           //Four digits for sequence
            if(++decSn >= 99999) {
                decSn = 0;
            }
            String s2 = String.format("%05d", decSn);
            return s1+s2;
        }
    }
    /**
     * length 9
     *
     * @param 0:Server, 1:Client
     * @return String
     */
    public String generateLongID() {
       synchronized (mutex8){
           GregorianCalendar currentDay = new GregorianCalendar();
           int dayOfMonth = currentDay.get(Calendar.DAY_OF_MONTH);
           int hourOfDay = currentDay.get(Calendar.HOUR_OF_DAY);
           // hourOfMonth: [024-767]
           int hourOfMonth = dayOfMonth * 24 + hourOfDay;

           int minute = currentDay.get(Calendar.MINUTE);
           int second = currentDay.get(Calendar.SECOND);
           // secondOfHour: [0000-3600)
           int secondOfHour = minute * 60 + second;

           //Five digits for time and modulenum
           String s1 = String.format("%06d", hourOfMonth * 1000 + 100 * minute + secondOfHour).substring(1);
           //Four digits for sequence
            if(++decSn >= 9999) {
                decSn = 0;
            }
            String s2 = String.format("%04d", decSn);
            return s1+s2;
        }
    }

    /**
     *
     * @param iGateway int
     * @return byte[]
     */
    public static byte[] generateCmppID(int iGateway) {
        SimpleTimeZone tz =  new SimpleTimeZone(8*60*60*1000,"CST");
        GregorianCalendar currentDay = new GregorianCalendar(tz);
        long month = currentDay.get(Calendar.MONTH) + 1;
        long day = currentDay.get(Calendar.DAY_OF_MONTH);
        long hour = currentDay.get(Calendar.HOUR_OF_DAY);
        long minute = currentDay.get(Calendar.MINUTE);
        long second = currentDay.get(Calendar.SECOND);

        long temp = 0L;
        synchronized (mutex6){
            if (cmppSn > 65534) {
                cmppSn = 0;
            }
            temp = cmppSn++;
        }
        byte[] b = new byte[8];
        for(int i = 7; i > 5 ; i--) {
            b[i] = new Long(temp & 0xff).byteValue();
            temp = temp >> 8;
        }

        temp = iGateway + (second << 22) + (minute << 28) + (hour << 34) +
               (day << 39) + (month << 44);
        for(int i = 5; i > -1 ; i--) {
            b[i] = new Long(temp & 0xff).byteValue();
            temp = temp >> 8;
        }
        return b;
    }


    public static void main(String args[]) throws Exception {
        java.io.File file = new java.io.File("e:\\system.log");
        if(!file.exists()){
        	file.createNewFile();
        }
        final java.io.PrintWriter printWriter = new java.io.PrintWriter(file);
        System.out.println(new java.util.Date());
        for (int i = 0; i <= 15; i++) {
            Thread thread = new Thread(){
            	public void run(){
            		int j = 0;
            		while(j++ < 9999){
            			try{
            				String st = MessageIdGenerator.getInstance().generateDecID(3);
            				synchronized(printWriter){
            					printWriter.write(st);
            					printWriter.write("\r\n");
            				}
            				Thread.sleep(1);
            			}catch(Exception e){
            				e.printStackTrace();
            			}
            		}
            	}
            };
            thread.start();
        }
        System.out.println(new java.util.Date());
    }

}
