package com.king.mgt.context;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.user.UserCommand;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2008</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ModuleDispatcher {
    private static SystemLogger log = SystemLogger.getSystemLogger(ModuleDispatcher.class);

    public ModuleDispatcher() {
        super();
    }

    private SystemCommand buildSingleModuleCommnad(UserCommand cmd,
            Module module) {
        if (cmd == null || module == null)
            return null;
        SystemCommand res;
        res = cmd.buildSystemCommand();
        if(res!=null){
            log.info("build system command over cmd:{}",res.toString());
        }else{
            log.info("build system command over null cmd!");
        }
        log.info("module name is:{}",module.getName());
        res.setModule(module);
        return res;
    }

    private SystemCommand[] genForAllModules(UserCommand cmd)
    {
        if (ModuleManager.getInstance().getActiveModulesSize() == 0) {
            return null;
        }
        SystemCommand []sysCmds=new SystemCommand[ModuleManager.getInstance().getActiveModulesSize()];
        String[] modules=ModuleManager.getInstance().getActiveModules();
        int i=0;
        for (String module : modules)
        {
            sysCmds[i++]=buildSingleModuleCommnad(cmd, ModuleManager.getInstance().getModuleByName(module));
        }
        return sysCmds;
    }

    public boolean dispatch(UserCommand cmd, ContextManager context) {
        log.info("Dispatching UserCommand {}", cmd.getType());
        int notifyType = cmd.getNotifyType();
        switch (notifyType) {

            case UserCommand.Notify_All: {
                log.info("Notify Type: ALL");
                cmd.setSystemCommands(genForAllModules(cmd));
                return true;
            }

            case UserCommand.Notify_Current: {
                log.info("Notify Type: Current");
                Module cur = context.getCurrentModule();
                if (cur.equals(ModuleManager.getInstance().A2P)) {
                    cmd.setSystemCommands(genForAllModules(cmd));
                    return true;
                }
                else {
                    SystemCommand[] sysCmds = new SystemCommand[1];
                    sysCmds[0] = this.buildSingleModuleCommnad(cmd, cur);
                    cmd.setSystemCommands(sysCmds);
                    return true;
                }
            }

            case UserCommand.Notify_Fix: {
                log.info("Notify Type: Fix");
                Module[] modules = cmd.getNotifyModules();
                SystemCommand[] sysCmds = new SystemCommand[modules.length];
                for (int i = 0; i < modules.length; i++) {
                    sysCmds[i] = this.buildSingleModuleCommnad(cmd, modules[i]);
                }
                cmd.setSystemCommands(sysCmds);
                return true;

            }

            default: {
                log.error("Unknown Notify Type: {}", notifyType);
                return false;
            }
        }

    }
}
