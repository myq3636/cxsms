package com.king.mgt.cmd.user;

import java.util.HashMap;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemCommandStop;

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
public class UserCommandStop
    extends UserCommand {

    public static final String keyword = "shutdown";
    private static SystemLogger log = SystemLogger.getSystemLogger(UserCommandStop.class);
    private Map<String,Integer> modulesName = new HashMap<String,Integer>();
    private boolean isFromHAMonitor = false;

    /**
     * buildSystemCommand
     *
     * @return SystemCommand
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public UserCommandStop(){
        super(UserCommand.Notify_All);
    }

    public SystemCommand buildSystemCommand() {
        return new SystemCommandStop(this);
    }

    /**
     * parseArgs
     *
     * @param args String[]
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean parseArgs(String[] args) {

        if (args.length == 2 && args[1].equals("-a")) {
            this.notifyType = Notify_All;
            String[] activeModules = ModuleManager.getInstance().getActiveModules();
            if (activeModules == null) {
                log.info("no active module.");
                return true;
            }
            int i = 0;
            for (String module : activeModules) {
                modulesName.put(module, i++);
                log.info("Add stop module: {}", module);
            }
            return true;
        }
        else if (args.length >= 3 && args[1].equals("-ha")) {
            this.notifyType = Notify_Fix;
            this.isFromHAMonitor = true;

            for (int i = 1; i < args.length - 1; i++) {
                modulesName.put(args[i + 1].trim(),i);
                log.info("From HAMonitor, Add stop module: {}", args[i + 1].trim());
            }
            return true;
        }
        else if (args.length >= 2) {
            this.notifyType = Notify_Fix;

            for (int i = 0; i < args.length - 1; i++) {
                modulesName.put(args[i + 1].trim(),i);
                log.info("Add stop module: {}", args[i + 1].trim());
            }
            return true;
        }
        else {
            return false;
        }
    }
    /**
     * process
     *
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean process() {
        this.processor.handleCommandStop(modulesName, isFromHAMonitor);
        return true;
    }
}
