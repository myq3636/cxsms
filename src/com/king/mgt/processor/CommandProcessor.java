package com.king.mgt.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.util.SystemConstants;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemResponse;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.connection.SystemCommandSender;
import com.king.mgt.context.Module;
import com.king.mgt.context.ModuleDispatcher;
import com.king.mgt.util.InfoTable;
import com.king.mgt.util.StartupModuleManager;
import com.king.mgt.util.UserInterfaceUtility;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class CommandProcessor {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommandProcessor.class);
	private UserCommand cmd = null;
	protected UserInterfaceUtility util;
	protected ModuleDispatcher dispatcher;
	private String a2pPath;
	private ModuleManager moduleManager;

	public CommandProcessor(UserCommand cmd) {
		this.cmd = cmd;
		util = UserInterfaceUtility.getInstance();
		dispatcher = new ModuleDispatcher();
		a2pPath = System.getProperty("a2p_home", "/usr/local/a2p");
		moduleManager = ModuleManager.getInstance();
		log.info("a2pPath:{}", a2pPath);
	}

	public boolean handleCommandList() {
		String[] activeModules = moduleManager.getActiveModules();
		if (activeModules == null) {
			cmd.append("no active module.");
			return true;
		}
		for (String module : activeModules) {
			cmd.append(module);
			cmd.append(" ");
		}

		return true;
	}

	/**
	 * for antispam,blacklist,contentTpl,routingInfo
	 * 
	 * @return
	 */
	public boolean handleCommandActive() {
		log.trace("start handleCommandActive...");
		if (util.isEnableNotification()
				&& cmd.getNotifyType() != UserCommand.Notify_None) {
			ArrayList nameList = new ArrayList();
			;
			if (cmd.getNotifyType() == UserCommand.Notify_Fix) {
				nameList.addAll(moduleManager.getRouterModules());
			} else if (cmd.getNotifyType() == UserCommand.Notify_All) {
				String[] activeModules = moduleManager.getActiveModules();
				nameList.addAll(Arrays.asList(activeModules));
			}
			log.trace("nameList size=" + nameList.size());
			if (nameList.size() == 0) {
				nameList = null;
				return false;
			}

			setNotifyModulesByName(nameList);
			if (!dispatcher.dispatch(cmd, cmd.getContext())) {
				cmd.append("\nother modules dispatch error!");
				return false;
			}
			// send systemcommand to modules
			SystemCommandSender sender = new SystemCommandSender();
			sender.send(cmd.getSystemCommands());
			SystemCommand[] failedSysCmds = setResponses4DB();
			final int maxTry = 10;
			final int interval = 5000;
			int count = 0;
			while (failedSysCmds != null && count < maxTry) {
				try {
					Thread.sleep(interval * (1 + count++));
					sender.send(failedSysCmds);
					failedSysCmds = setResponses4DB();
				} catch (Exception ex) {
					log.error(ex.getMessage());
				}
			}
		} else {
			cmd.append("\nforbid to execute such command!");
			return false;
		}
		return true;
	}

	public boolean handleCommandHelp() {
		cmd.append(InfoTable.getInstance().get(InfoTable.HELP_INFO));
		return true;
	}

	public boolean handleCommandQuit() {

		cmd.append(cmd.getContext().getCurUserIP());
		cmd.append(" close the connection. login time:");
		cmd.append(cmd.getContext().getCurUserLoginTime().toString());
		cmd.setIsQuit(true);
		return true;

	}

	public boolean handleCommandStartup(Map modulesName) {
		if (util.isHa()) {
			cmd.append("HA mode, can not startup module manually!");
			return true;
		}
		List<ModuleConnectionInfo> moduleList = new ArrayList<ModuleConnectionInfo>();
		for (Object key : modulesName.keySet()) {
			String moduleName = (String) key;
			ModuleConnectionInfo modInfo = moduleManager
					.getConnectionInfo(moduleName);
			moduleList.add(modInfo);
		}
		Collections.sort(moduleList);
		for (ModuleConnectionInfo modInfo : moduleList) {
			String name = modInfo.getModuleName();
			cmd.append(name);
			cmd.append(" ");

			if (!checkModuleName(name)) {
				cmd.append("has not this module name!\n");
				continue;
			}
			if (isAlive(name)) {
				cmd.append("has already started!\n");
				continue;
			}

			if (startup(modInfo)) {
				cmd.append("startup ok!\n");
			} else {
				cmd.append("startup error!\n");
			}
		}
		return true;
	}

	public boolean handleCommandStop(Map modulesName, boolean isFromHAMonitor) {
		if (util.isHa() && cmd.getNotifyType() != UserCommand.Notify_All
				&& !isFromHAMonitor) {
			cmd.append("HA mode, can not stop certain modules manually!");
			return false;
		}
		return stopModules(modulesName);
	}

	private boolean stopModules(Map modulesName) {
		if (util.isEnableNotification()
				&& cmd.getNotifyType() != UserCommand.Notify_None) {

			ArrayList nameList = getNotifyModuleNames(modulesName);
			if (nameList.size() == 0) {
				nameList = null;
				return false;
			}
			setNotifyModulesByName(nameList);

			if (!dispatcher.dispatch(cmd, cmd.getContext())) {
				cmd.append("\nothers modules dispatch error!");
				return false;
			}

			// send systemcommand to modules
			SystemCommandSender sender = new SystemCommandSender();
			sender.send(cmd.getSystemCommands());

			setRespsBySystemCommandResp();
			// call script to load message to database
			callMsgbackupScript();

			return true;

		} else {
			cmd.append("\nforbid to execute stop command!");
			return false;
		}
	}

	public boolean handleCommandReStart(Map modulesName) {
		this.stopModules(modulesName);
		try {
			Thread.sleep(2000);
		} catch (Exception ex) {
		}
		ArrayList<ModuleConnectionInfo> moduleList = new ArrayList<ModuleConnectionInfo>();
		for (Object key : modulesName.keySet()) {
			String moduleName = (String) key;
			ModuleConnectionInfo modInfo = moduleManager
					.getConnectionInfo(moduleName);
			moduleList.add(modInfo);
		}
		Collections.sort(moduleList);
		for (ModuleConnectionInfo modInfo : moduleList) {
			String name = modInfo.getModuleName();
			if (moduleManager.isActiveModule(name)) {
				if (this.isAlive(name)) {
					StartupModuleManager.stop(modInfo);
				}
				StartupModuleManager.start(modInfo, "start");
				cmd.append(name);
				cmd.append(": restart ok!\n");
			}
		}
		return true;
	}
	
    private SystemCommand[] setResponses4DNS() {
        SystemCommand[] sysCmds = cmd.getSystemCommands();
        SystemCommand[] failedSysCmds = new SystemCommand[sysCmds.length];
        int failedCount = 0;
        for (int i = 0; i < sysCmds.length; i++) {
        	if(sysCmds[i]==null)continue;
            SystemResponse response = sysCmds[i].getResponse();
            if (response == null) {
                response = new SystemResponse();
                response.setResult(SystemResponse.COMMUNICATION_ERROR);
            }
            Module module = sysCmds[i].getModule();
            log.trace("Switch DNS response's module name="+module.getName()+";response="+response.getErrorText());
            if(response.isBusy()){
                cmd.append(module.getName());
                cmd.append(": a same command doing, please wait!\n");
            }
            else if (!response.isSuccess()) {
            	failedSysCmds[failedCount++] = sysCmds[i];
                cmd.append(module.getName());
                cmd.append(": switch dns failed or time out, at first please make sure the command format is 'switchdns master|slave'!\n");
            }
            else {
                //try wait 500ms
                try {
                    Thread.sleep(500);
                }
                catch (Exception ex) {

                }
                cmd.append(module.getName());
                cmd.append(": switch dns done!\n");
            }
        }
        if(failedCount==0){
        	return null;
        }
        return failedSysCmds;
    }

    
    private boolean switchDNS(Map modulesName) {
        if (util.isEnableNotification() &&
            cmd.getNotifyType() != UserCommand.Notify_None) {
    		ArrayList nameList = getNotifyDNSModuleNames(modulesName);
            if (nameList.size() == 0) {
                nameList = null;
                return false;
            }
            setNotifyModulesByName(nameList);

            if (!dispatcher.dispatch(cmd, cmd.getContext())) {
                cmd.append("\nothers modules dispatch error!");
                return false;
            }

            //send systemcommand to modules
            SystemCommandSender sender = new SystemCommandSender();
            sender.send(cmd.getSystemCommands());

            setResponses4DNS();
            //call script to load message to database

            return true;

        }
        else {
            cmd.append("\nforbid to execute stop command!");
            return false;
        }
    }

    /**
     * switch dns server
     * @param modulesName
     * @return
     */
    public boolean handleCommandSwitchDNS(Map modulesName){
    	return switchDNS(modulesName);
    }

	
    private ArrayList<String> getNotifyDNSModuleNames(Map names) {
        ArrayList<String> notifyNames = new ArrayList<String> ();

        for (Object key : names.keySet()) {
            String name = (String) key;
            if (!ModuleManager.getInstance().isActiveModule(name)) {
                cmd.append(name);
                cmd.append(": is not in module list!\n");
                continue;
            }
            if (!isAlive(name)) {
                cmd.append(name);
                cmd.append(": has already stoped!\n");
                continue;
            }
            notifyNames.add(name);
        }

        return notifyNames;
    }


	private void setRespsBySystemCommandResp() {
		SystemCommand[] sysCmds = cmd.getSystemCommands();
		for (int i = 0; i < sysCmds.length; i++) {

			SystemResponse response = sysCmds[i].getResponse();
			if (response == null) {
				response = new SystemResponse();
				response.setResult(SystemResponse.COMMUNICATION_ERROR);
			}
			Module module = sysCmds[i].getModule();
			if (response.isBusy()) {
				cmd.append(module.getName());
				cmd.append(": a same command doing, please wait!\n");
			} else if (!response.isSuccess()) {
				killModule(module.getName());
				cmd.append(module.getName());
				cmd.append(": execute time out, killed!\n");
			} else {
				// try wait 500ms
				try {
					Thread.sleep(500);
				} catch (Exception ex) {

				}
				cmd.append(module.getName());
				cmd.append(": execute over, secure shutdown!\n");
			}
		}

	}

	private ArrayList<String> getNotifyModuleNames4DB(
			List<ModuleConnectionInfo> mgts) {
		ArrayList<String> notifyNames = new ArrayList<String>();

		for (ModuleConnectionInfo minfo : mgts) {
			String name = minfo.getModuleName();
			if (!moduleManager.isActiveModule(name)) {
				cmd.append(name);
				cmd.append(": is not in module list!\n");
				continue;
			}
			if (!isAlive(name)) {
				cmd.append(name);
				cmd.append(": has already stoped!\n");
				continue;
			}
			log.trace("add module name=" + name);
			notifyNames.add(name);
		}

		return notifyNames;
	}

	/**
	 * switch db to master or slave
	 */
	public void handleSwitchDB() {
		log.trace("start handleSwitchDB...");
		List<ModuleConnectionInfo> mgts = moduleManager.getConnectionInfo4MGT();
		if (util.isEnableNotification()
				&& cmd.getNotifyType() != UserCommand.Notify_None) {
			ArrayList nameList = getNotifyModuleNames4DB(mgts);
			log.trace("nameList size=" + nameList.size());
			if (nameList.size() == 0) {
				nameList = null;
				return;
			}
			setNotifyModulesByName(nameList);

			if (!dispatcher.dispatch(cmd, cmd.getContext())) {
				cmd.append("\nother modules dispatch error!");
				return;
			}
			// send systemcommand to modules
			SystemCommandSender sender = new SystemCommandSender();
			sender.send(cmd.getSystemCommands());
			SystemCommand[] failedSysCmds = setResponses4DB();
			final int maxTry = 10;
			final int interval = 5000;
			int count = 0;
			while (failedSysCmds != null && count < maxTry) {
				try {
					Thread.sleep(interval * (1 + count++));
					sender.send(failedSysCmds);
					failedSysCmds = setResponses4DB();
				} catch (Exception ex) {

				}
			}
			return;
		} else {
			cmd.append("\nforbid to execute switch db command!");
			return;
		}
	}

	private SystemCommand[] setResponses4DB() {
		SystemCommand[] sysCmds = cmd.getSystemCommands();
		SystemCommand[] failedSysCmds = new SystemCommand[sysCmds.length];
		int successCount = 0;
		int failedCount = 0;
		for (int i = 0; i < sysCmds.length; i++) {
			if (sysCmds[i] == null)
				continue;
			SystemResponse response = sysCmds[i].getResponse();
			if (response == null) {
				log.trace("Null response!->" + i);
				response = new SystemResponse();
				response.setResult(SystemResponse.COMMUNICATION_ERROR);
			}
			Module module = sysCmds[i].getModule();
			log.trace("response's module name=" + module.getName()
					+ ";response=" + response.getErrorText());
			if (response.isBusy()) {
				cmd.append(module.getName());
				cmd.append(": a same command doing, please wait!\n");
			} else if (!response.isSuccess()) {
				failedSysCmds[failedCount++] = sysCmds[i];
				cmd.append(module.getName());
				cmd
						.append(": execute command time out, retry again,please check it!\n");
			} else {
				// try wait 500ms
				try {
					Thread.sleep(500);
				} catch (Exception ex) {

				}
				cmd.append(module.getName());
				cmd.append(": execute command done!\n");
				successCount++;
			}
		}
		if (failedCount == 0) {
			return null;
		}
		return failedSysCmds;
	}

	private boolean killModule(String procName) {
		try {
			ModuleConnectionInfo modInfo = moduleManager
					.getConnectionInfo(procName);
			StartupModuleManager.kill(modInfo, "kill");
		} catch (Exception ex) {
			log.info("Runtime error! {}", ex.getMessage());
			return false;
		}
		return true;

	}

	private void callMsgbackupScript() {
		try {
			String group = Integer.toString(cmd.getGroupId());
			String command = a2pPath + "/script/msgbackup/callbackup.sh";
			log.info("call msg backup seript:{} group is :{}", command, group);
			String[] cmd = { command, group };
			Runtime.getRuntime().exec(cmd);
		} catch (Exception ex) {
			log.info("callMsgbackupScript Runtime error!{}", ex.getMessage());
		}
	}

	private void setNotifyModulesByName(ArrayList list) {

		Module[] notifyModules = new Module[list.size()];
		for (int i = 0; i < list.size(); i++) {
			Module mod = moduleManager.getModuleByName((String) list.get(i));
			notifyModules[i] = mod;
		}

		cmd.setFixNotifyModules(notifyModules);
	}

	private ArrayList<String> getNotifyModuleNames(Map names) {
		ArrayList<String> notifyNames = new ArrayList<String>();
		List<ModuleConnectionInfo> moduleList = new ArrayList<ModuleConnectionInfo>();
		for (Object key : names.keySet()) {
			String moduleName = (String) key;
			ModuleConnectionInfo modInfo = moduleManager
					.getConnectionInfo(moduleName);
			moduleList.add(modInfo);
		}
		Collections.sort(moduleList);
		Collections.reverse(moduleList);
		for (ModuleConnectionInfo modInfo : moduleList) {
			String name = modInfo.getModuleName();
			if (!moduleManager.isActiveModule(name)) {
				cmd.append(name);
				cmd.append(": is not in module list!\n");
				continue;
			}
			if (!isAlive(name)) {
				cmd.append(name);
				cmd.append(": has already stoped!\n");
				continue;
			}

			if (moduleManager.isDirectStopModule(name)) {
				this.killModule(name);
				cmd.append(name);
				cmd.append(": direct stop ok!\n");
				continue;
			}
			notifyNames.add(name);
		}

		return notifyNames;
	}

	private boolean startup(ModuleConnectionInfo modInfo) {
		try {
			StartupModuleManager.start(modInfo, "start");
		} catch (Exception ex) {
			log.info("Runtime error! {}", ex.getMessage());
			return false;
		}
		return true;

	}

	private boolean isAlive(String procName) {
		try {
			String command = "ps -ef|grep java|grep Dmodule=" + procName
					+ "|grep -v grep|awk '{print $2}'";
			String[] cmd = { "bash", "-c", command };
			Process proc = Runtime.getRuntime().exec(cmd);
			BufferedReader in = new BufferedReader(new InputStreamReader(proc
					.getInputStream()));

			String string_Temp = in.readLine();

			if (string_Temp != null) {
				log.info("process info:{}", string_Temp);
				return true;
			} else
				return false;

		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("check isAlive error:{}", e);
			}
			return false;
		}
	}

	private boolean checkModuleName(String name) {
		if (cmd.getNotifyType() == UserCommand.Notify_All)
			return true;

		return moduleManager.isContainModule(name);

	}
	
	/**
	 * Handle query GW version command
	 * @return
	 */
	public boolean handleCommandVersion() {
		cmd.append(SystemConstants.GW_VERSION);
		return true;
	}
}
