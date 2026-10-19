package com.king.gmms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.message.gmms.GmmsMessage;

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
public class BlackList implements LifecycleListener{
    // H-25: 精确匹配项改 HashSet (O(1))；H-26: 全部字段 volatile —— reload 线程整体替换、
    // 消息线程无锁读，需 happens-before 保证 reload 可见。前缀匹配项无法 HashSet，保持线性扫描。
    private volatile Set<String> oNumberBlacklist;       // 发送号精确匹配(已归一化)
    private volatile List<String> rNumberBlacklist;      // 接收号前缀匹配
    private volatile Set<Integer> oopBlacklist;          // 源运营商精确匹配
    private volatile Set<Integer> ropBlacklist;          // 目标运营商精确匹配
    private volatile List<BlackListValue> oopRnumberBlacklist;   // 源运营商 + 接收号前缀
    private volatile List<BlackListValue> oNumberRopBlacklist;   // 发送号前缀 + 目标运营商
    private volatile Set<Long> oopRopBlacklist;          // (源运营商,目标运营商) 精确匹配(打包)

    LifecycleSupport lifecycle;
    private static SystemLogger log = SystemLogger
	.getSystemLogger(BlackList.class);

    public BlackList() {
        oNumberBlacklist = new HashSet<String>();
        rNumberBlacklist = new ArrayList<String>();
        oopBlacklist = new HashSet<Integer>();
        ropBlacklist = new HashSet<Integer>();
        oopRnumberBlacklist = new ArrayList<BlackListValue>();
        oNumberRopBlacklist = new ArrayList<BlackListValue>();
        oopRopBlacklist = new HashSet<Long>();
        lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
        lifecycle.addListener(Event.TYPE_BLACKLIST_RELOAD, this, 1);
    }

    public void setLifecycleSupport(LifecycleSupport lifecycle) {
        this.lifecycle = lifecycle;
    }

    // H-25: replace 而非 replaceAll(正则)，且 null→"" 统一归一化(小写、去空格)。
    private static String normalizeNumber(String s) {
        if (s == null) {
            return "";
        }
        return s.toLowerCase().replace(" ", "");
    }

    // H-25: 把 (o_op, r_op) 打包成 long 作为精确匹配 key。
    private static long packOpPair(int oOp, int rOp) {
        return (((long) oOp) << 32) | (rOp & 0xFFFFFFFFL);
    }

    public void addOopBlacklist(int o_op) {
        oopBlacklist.add(o_op);
    }

    public void addRopBlacklist(int r_op) {
        ropBlacklist.add(r_op);
    }

    public void addONumberBlacklist(String oNumber) {
        oNumberBlacklist.add(normalizeNumber(oNumber));
    }

    public void addRNumberBlacklist(String rNumber) {
        rNumberBlacklist.add(rNumber);
    }

    public void addOopRNumberBlacklist(int o_op, String rNumber) {
        oopRnumberBlacklist.add(new BlackListValue(null, rNumber, o_op, -1));
    }

    public void addONumberRopBlacklist(String oNumber, int r_op) {
        oNumberRopBlacklist.add(new BlackListValue(oNumber, null, -1, r_op));
    }

    public void addOopRopBlacklist(int o_op,int r_op){
    	oopRopBlacklist.add(packOpPair(o_op, r_op));
    }

    private boolean blockOopRop(int o_op,int r_op) {
        return oopRopBlacklist.contains(packOpPair(o_op, r_op));
    }
    private boolean blockOop(int o_op) {
        return oopBlacklist.contains(o_op);
    }

    private boolean blockRop(int r_op) {
        return ropBlacklist.contains(r_op);
    }

    private boolean blockONumber(String oNumber) {
        // H-25: 入参只归一化一次，存储项装载时已预归一化 → O(1) 精确匹配。
        return oNumberBlacklist.contains(normalizeNumber(oNumber));
    }

    private boolean blockRNumber(String rNumber) {
        if (rNumber == null) {
            return false;
        }
        for (String prefix : rNumberBlacklist) {
            if (prefix != null && rNumber.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean blockOopRNumber(int o_op, String rNumber) {
        if (rNumber == null) {
            return false;
        }
        for (BlackListValue blackList : oopRnumberBlacklist) {
            if (o_op == blackList.getOOP() &&
                rNumber.startsWith(blackList.getRNumber())) {
                return true;
            }
        }
        return false;
    }

    private boolean blockONumberRop(String oNumber, int r_op) {
        if (oNumber == null) {
            return false;
        }
        for (BlackListValue blackList : oNumberRopBlacklist) {
            if (r_op == blackList.getROP() &&
                oNumber.startsWith(blackList.getONumber())) {
                return true;
            }
        }
        return false;
    }

    public boolean allowWhenReceived(GmmsMessage msg) {
        return (!blockRNumber(msg.getRecipientAddress()) &&
                !blockOopRNumber(msg.getOoperator(), msg.getRecipientAddress()) &&
                !blockOop(msg.getOSsID()));
    }

    public boolean allowBlackList(GmmsMessage msg) {
        return (!blockONumber(msg.getSenderAddress()));
    }

    public boolean allowWhenRouting(GmmsMessage msg) {
        return (!blockOop(msg.getOoperator()) && !blockRop(msg.getRoperator()) &&
                !blockOopRNumber(msg.getOoperator(), msg.getRecipientAddress()) &&
                !blockONumberRop(msg.getSenderAddress(), msg.getRoperator())&&
                !blockOopRop(msg.getOoperator(),msg.getRoperator()));
    }
   /**
    *
    * @param path String
    * @return int
    */
   public int loadBlacklist(String path) {
       try {
           int rtnval=0;
	   	   if (log.isDebugEnabled()) {
	            log.debug("Loading Blacklist from file {} ...",path);
	   	   }
           File file = new File(path);
           // M-20: try-with-resources 确保 reader/流在任何路径都关闭，避免每次 reload 漏 FD。
           try (BufferedReader reader = new BufferedReader(new InputStreamReader(
               new FileInputStream(file)))) {

           // clear the old blacklist after successfully read the configuration file.
           Set<String> tempONumber=new HashSet<String>();
           List<String> tempRNumber=new ArrayList<String>();
           List<BlackListValue> tempONumberROp=new ArrayList<BlackListValue>();
           List<BlackListValue> tempOOpRNumber=new ArrayList<BlackListValue>();
           Set<Integer> tempOOp=new HashSet<Integer>();
           Set<Integer> tempRop=new HashSet<Integer>();
           Set<Long> tempOopRop=new HashSet<Long>();
           String line=null;
           while ( (line=reader.readLine())  != null) {
               line=line.trim();
               if (line.startsWith("#") || line.length()==0){
                   continue;
               }
               String[] args = line.split(",", 2);

               if (args.length<2)
               {
                   log.error("Wrong blacklist length for line:{}, {}",line, args.length);
                   rtnval=2;
                   continue;
               }
               String left = args[0].trim();
               String right = args[1].trim();
               if(right.startsWith(",")){
            	   left= left+",";
            	   right = right.substring(1);
               }
               try {
                   if (left.length() == 0 && right.startsWith("p")) {
                       String rNumber=right.substring(1);
                       tempRNumber.add(rNumber);
                   }
                   else if (left.startsWith("p") && right.length() == 0) {
                       String oNumber=left.substring(1);
                       tempONumber.add(normalizeNumber(oNumber));
                   }
                   else if (left.startsWith("o") && right.startsWith("p")) {
                       String rNumber=right.substring(1);
                       int o_op=Integer.parseInt(left.substring(1));
                       tempOOpRNumber.add(new BlackListValue(null,rNumber,o_op,-1));
                   }
                   else if (left.startsWith("p") && right.startsWith("o")) {
                       String oNumber=left.substring(1);
                       int r_op=Integer.parseInt(right.substring(1));
                       tempONumberROp.add(new BlackListValue(oNumber,null,-1,r_op));
                   }
                   else if (left.startsWith("o") && right.length() == 0) {
                       int o_op=Integer.parseInt(left.substring(1));
                       tempOOp.add(o_op);
                   }
                   else if (left.length() == 0 && right.startsWith("o")) {
                       int r_op=Integer.parseInt(right.substring(1));
                       tempRop.add(r_op);
                   }
                   else if (left.startsWith("o") && right.startsWith("o")) {
                       int o_op=Integer.parseInt(left.substring(1));
                       int r_op=Integer.parseInt(right.substring(1));
                       tempOopRop.add(packOpPair(o_op,r_op));
                   }
                   else
                   {
                       log.error("Wrong blacklist for line:{}",line);
                       rtnval=2;
                   }
               }
               catch (Exception ex) {
                   rtnval=2;
                   log.error("Wrong blacklist configuration for line:{}, {}",line, ex);
               }
           }
           //log.trace("Blacklist loaded.");
           this.oNumberBlacklist=tempONumber;
           this.rNumberBlacklist=tempRNumber;
           this.oNumberRopBlacklist=tempONumberROp;
           this.oopRnumberBlacklist=tempOOpRNumber;
           this.ropBlacklist = tempRop;
           this.oopRopBlacklist=tempOopRop;
           this.oopBlacklist=tempOOp;
           log.trace("Blacklist Loaded:\n"+printBlacklist());
           return rtnval;
           } // close try-with-resources (reader)
       }
       catch (FileNotFoundException ex) {
           log.error(ex, ex);
           return 1;
       }
       catch (IOException ex) {
           log.error(ex, ex);
           return 1;
       }
   }

   public int OnEvent(Event event) {
   	   if (log.isDebugEnabled()) {
   	       log.debug("Event Received. Type: {}", event.getEventType());
   	   }
       GmmsUtility util = GmmsUtility.getInstance();
       if (event.getEventType() == Event.TYPE_BLACKLIST_RELOAD) {
           return loadBlacklist(util.getBlacklistFilePath());
       }
       return 0;
   }

   public int testNotify() {
       EventFactory factory = EventFactory.getInstance();
       return this.lifecycle.notify(factory.newEvent(Event.
           TYPE_BLACKLIST_RELOAD));
   }

   private String printBlacklist() {
       StringBuilder rtnval = new StringBuilder("Prefix_ALL:\n");
       for (String prefix_all : this.oNumberBlacklist) {
           rtnval.append("Prefix(").append(prefix_all).append(
               ") --- All\n");
       }
       rtnval.append("\nALL_Prefix:\n");
       for (String all_prefix : this.rNumberBlacklist) {
           rtnval.append("All --- Prefix(").append(all_prefix).
               append(")\n");
       }
       rtnval.append("\nOP_Prefix:\n");
       for (BlackListValue op_prefix : this.oopRnumberBlacklist) {
           rtnval.append("OP(" + op_prefix.getOOP()).append(") --- Prefix(").
               append(op_prefix.getRNumber())
               .append(")\n");
       }
       rtnval.append("\nPrefix_OP:\n");
       for (BlackListValue prefix_op : this.oNumberRopBlacklist) {
           rtnval.append("Prefix(").append(prefix_op.getONumber() +
                                           ") --- OP(").append(prefix_op.
               getROP())
               .append(")\n");
       }
       rtnval.append("\nOP_All:\n");
       for (Integer op_all : this.oopBlacklist) {
           rtnval.append("OP(").append(op_all).append(") --- ALL\n");
       }
       return rtnval.toString();
   }

   public static void main(String[] args) {
      /* BlackList bl = new BlackList();
       bl.setLifecycleSupport(new LifecycleSupport());
       int rtnval = bl.loadBlacklist(args[0]);*/
       String tb = "pOSPLSG,";
       String[] s1 = tb.split(",");
       if(s1.length==1){
    	   System.out.println(s1[0]);
       }else{
    	   System.out.println(s1[1]+":2");
       }
      /* System.out.println(rtnval);
       String res = bl.printBlacklist();
       System.out.println(res);*/
   }
}
