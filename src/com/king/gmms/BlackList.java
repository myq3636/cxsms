package com.king.gmms;

import java.util.ArrayList;
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
    private ArrayList<BlackListValue> oopBlacklist;
    private ArrayList<BlackListValue> ropBlacklist;
    private ArrayList<BlackListValue> oNumberBlacklist;
    private ArrayList<BlackListValue> rNumberBlacklist;
    private ArrayList<BlackListValue> oopRnumberBlacklist;
    private ArrayList<BlackListValue> oNumberRopBlacklist;
    private ArrayList<BlackListValue> oopRopBlacklist;
    
    LifecycleSupport lifecycle;
    private static SystemLogger log = SystemLogger
	.getSystemLogger(BlackList.class);

    public BlackList() {
        oopBlacklist = new ArrayList<BlackListValue> ();
        ropBlacklist = new ArrayList<BlackListValue> ();
        oNumberBlacklist = new ArrayList<BlackListValue> ();
        rNumberBlacklist = new ArrayList<BlackListValue> ();
        oopRnumberBlacklist = new ArrayList<BlackListValue> ();
        oNumberRopBlacklist = new ArrayList<BlackListValue> ();
        oopRopBlacklist = new ArrayList<BlackListValue> ();
        lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
        lifecycle.addListener(Event.TYPE_BLACKLIST_RELOAD, this, 1);
    }

    public void setLifecycleSupport(LifecycleSupport lifecycle) {
        this.lifecycle = lifecycle;
    }

    public void addOopBlacklist(int o_op) {
        oopBlacklist.add(new BlackListValue(null, null, o_op, -1));
    }

    public void addRopBlacklist(int r_op) {
        ropBlacklist.add(new BlackListValue(null, null, -1, r_op));
    }

    public void addONumberBlacklist(String oNumber) {
        oNumberBlacklist.add(new BlackListValue(oNumber, null, -1, -1));
    }

    public void addRNumberBlacklist(String rNumber) {
        rNumberBlacklist.add(new BlackListValue(null, rNumber, -1, -1));
    }

    public void addOopRNumberBlacklist(int o_op, String rNumber) {
        oopRnumberBlacklist.add(new BlackListValue(null, rNumber, o_op, -1));
    }

    public void addONumberRopBlacklist(String oNumber, int r_op) {
        oNumberRopBlacklist.add(new BlackListValue(oNumber, null, -1, r_op));
    }

    public void addOopRopBlacklist(int o_op,int r_op){
    	oopRopBlacklist.add(new BlackListValue(null, null, o_op, r_op));
    }
    
    private boolean blockOopRop(int o_op,int r_op) {
        for (BlackListValue blackList : oopRopBlacklist) {
            if ((o_op == blackList.getOOP()) && (r_op == blackList.getROP())) {
                return true;
            }
        }
        return false;
    }
    private boolean blockOop(int o_op) {
        for (BlackListValue blackList : oopBlacklist) {
            if (o_op == blackList.getOOP()) {
                return true;
            }
        }
        return false;
    }

    private boolean blockRop(int r_op) {
        for (BlackListValue blackList : ropBlacklist) {
            if (r_op == blackList.getROP()) {
                return true;
            }
        }
        return false;
    }

    private boolean blockONumber(String oNumber) {
        for (BlackListValue blackList : oNumberBlacklist) {
        	String sender = blackList.getONumber();
        	if(sender == null){
        		sender = "";
        	}
        	if(oNumber == null){
        		oNumber = "";       		
        	}
        	oNumber = oNumber.toLowerCase();
        	oNumber = oNumber.replaceAll(" ", "");
        	sender = sender.toLowerCase();
        	sender = sender.replaceAll(" ", "");
            if (oNumber.equalsIgnoreCase(sender)) {
                return true;
            }
        }
        return false;
    }

    private boolean blockRNumber(String rNumber) {
        for (BlackListValue blackList : rNumberBlacklist) {
            if (rNumber.startsWith(blackList.getRNumber())) {
                return true;
            }
        }
        return false;
    }

    private boolean blockOopRNumber(int o_op, String rNumber) {
        for (BlackListValue blackList : oopRnumberBlacklist) {
            if (o_op == blackList.getOOP() &&
                rNumber.startsWith(blackList.getRNumber())) {
                return true;
            }
        }
        return false;
    }

    private boolean blockONumberRop(String oNumber, int r_op) {
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
           FileInputStream fin = new FileInputStream(file);
           BufferedReader reader = new BufferedReader(new InputStreamReader(
               fin));

           // clear the old blacklist after successfully read the configuration file.
           ArrayList<BlackListValue> tempONumber=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempRNumber=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempONumberROp=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempOOpRNumber=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempOOp=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempRop=new ArrayList<BlackListValue>();
           ArrayList<BlackListValue> tempOopRop=new ArrayList<BlackListValue>();
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
                       tempRNumber.add(new BlackListValue(null,rNumber,-1,-1));
                   }
                   else if (left.startsWith("p") && right.length() == 0) {
                       String oNumber=left.substring(1);
                       tempONumber.add(new BlackListValue(oNumber,null,-1,-1));
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
                       tempOOp.add(new BlackListValue(null,null,o_op,-1));
                   }
                   else if (left.length() == 0 && right.startsWith("o")) {
                       int r_op=Integer.parseInt(right.substring(1));
                       tempRop.add(new BlackListValue(null,null,-1,r_op));
                   }
                   else if (left.startsWith("o") && right.startsWith("o")) {
                       int o_op=Integer.parseInt(left.substring(1));
                       int r_op=Integer.parseInt(right.substring(1));
                       tempOopRop.add(new BlackListValue(null,null,o_op,r_op));
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
// Note: this method costs lots of time. Try to improve it.
           log.trace("Blacklist Loaded:\n"+printBlacklist());
           return rtnval;
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
       for (BlackListValue prefix_all : this.oNumberBlacklist) {
           rtnval.append("Prefix(").append(prefix_all.getONumber()).append(
               ") --- All\n");
       }
       rtnval.append("\nALL_Prefix:\n");
       for (BlackListValue all_prefix : this.rNumberBlacklist) {
           rtnval.append("All --- Prefix(").append(all_prefix.getRNumber()).
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
       for (BlackListValue op_all : this.oopBlacklist) {
           rtnval.append("OP(").append(op_all.getOOP()).append(") --- ALL\n");
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
