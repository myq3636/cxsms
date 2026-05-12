package com.king.gmms.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;

import sun.misc.Signal;

public enum SolarisSignal {
	HUP(1),
	INT(2),
	QUIT(3),
	ILL(4),
	TRAP(5),
	ABRT(6),
	EMT(7),
	FPE(8),
	KILL(9),
	BUS(10),
	SEGV(11),
	SYS(12),
	PIPE(13),
	ALRM(14),
	TERM(15);
	
	private int value = 1;
    private static Map<Integer, Signal> parseMap = new ConcurrentHashMap<Integer, Signal> (15);
    private static SystemLogger log = SystemLogger.getSystemLogger(SolarisSignal.class);

    static {
        for (SolarisSignal sig : SolarisSignal.values()) {
        	Signal signal = new Signal(sig.toString());
            parseMap.put(sig.getValue(), signal);
        }
    }

    SolarisSignal(int sig) {
        this.value = sig;
    }

    public int getValue() {
        return value;
    }

    public static Signal parse(int value) {
        return parseMap.get(value);
    }
    public static boolean needHandle(Signal sig){
    		log.info("Capture os signal {}",sig.getName());
    	int value = sig.getNumber();
    	switch(value){
	    	case 1:
	    	case 2:
	    	case 3:
	    	case 4:
	    	case 8:
	    	case 9:
	    	case 11:
	    		return false;
	    	case 5:
	    	case 6:
	    	case 7:
	    	case 10:
	    	case 12:
	    	case 13:
	    	case 14:
	    	case 15:
	    		return true;
	    	default:
	    		return false;
    	}
    }
	/**
	 *  Name             Value   Default    Event
     SIGHUP           1       Exit       Hangup (see termio(7I))
     SIGINT           2       Exit       Interrupt (see termio(7I))
     SIGQUIT          3       Core       Quit (see termio(7I))
     SIGILL           4       Core       Illegal Instruction
     SIGTRAP          5       Core       Trace or Breakpoint Trap
     SIGABRT          6       Core       Abort
     SIGEMT           7       Core       Emulation Trap
     SIGFPE           8       Core       Arithmetic Exception
     SIGKILL          9       Exit       Killed
     SIGBUS           10      Core       Bus Error
     SIGSEGV          11      Core       Segmentation Fault
     SIGSYS           12      Core       Bad System Call
     SIGPIPE          13      Exit       Broken Pipe
     SIGALRM          14      Exit       Alarm Clock
     SIGTERM          15      Exit       Terminated
	 */
}
