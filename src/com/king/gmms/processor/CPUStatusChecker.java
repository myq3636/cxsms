package com.king.gmms.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;


public class CPUStatusChecker {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(CPUStatusChecker.class);
	
	private static CPUStatusChecker instance = new CPUStatusChecker();
	private int maxUsage = 85;
	private GmmsUtility gmmsUtility = null;
	private static final String STARTCPUSTRING = "Average";
	private static final String SPLITSTRING = " ";
	private static final String COMMAND = "sar -u 2 2";
	
	private CPUStatusChecker(){
		gmmsUtility = GmmsUtility.getInstance();
		maxUsage = Integer.parseInt(gmmsUtility.getCommonProperty("MaxCPUUsage","85"));
	}
	
	public static CPUStatusChecker getInstance(){
		return instance;
	}
	
	//Get CPU usage, if usage exceed maxUsage, return true, otherwise return false;
	//The output of sar command for CPU 
	//	root@lab112 # sar -u 2 2
	//
	//	SunOS lab112 5.10 Generic_144488-06 sun4v    08/02/2012
	//
	//	15:57:10    %usr    %sys    %wio   %idle
	//	15:57:12       3       4       0      93
	//	15:57:15       4       5       0      92
	//
	//	Average        4       4       0      92
	public boolean isOverload(){
		BufferedReader in = null;
		Process p = null;
		float cpuUsage = 0;
		 try {	 			
			 p = Runtime.getRuntime().exec(COMMAND);
			 in = new BufferedReader(new InputStreamReader(p.getInputStream()));			
			 String data = "";
			 while((data = in.readLine()) != null){
				 if(data.startsWith(STARTCPUSTRING)){
					 String[] cpuList = data.split(SPLITSTRING);
					 if(cpuList != null && cpuList.length > 0){
						 String idleCPU = cpuList[cpuList.length - 1];
						 if(idleCPU != null){
							 cpuUsage = 100 - Float.parseFloat(idleCPU);
						 }
					 }
					 break;
				 }
			 }		
		 }catch(Exception ex){
			 log.error("read CPU process info error:{}",ex.getMessage());
		 } finally {
			 if (in != null) {
				 try {
					 in.close();
				 }catch (IOException ex) {
					 log.error(" I/O close error occurs!{}", ex);
				 }
			 }	
			 if (p != null){
				 try{
					 p.destroy();
				 }catch(Exception e){
				 }
			 }
		 }
		 
		 if(cpuUsage > maxUsage){
			return true;
		 }else{
			return false;
		 }
	}

}
