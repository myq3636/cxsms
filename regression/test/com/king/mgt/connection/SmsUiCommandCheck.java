package com.king.mgt.connection;

import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.cmd.user.UserCommandClientShardsReload;
import com.king.mgt.cmd.user.UserCommandContentBlacklist;
import com.king.mgt.cmd.user.UserCommandContentWhitelist;
import com.king.mgt.cmd.user.UserCommandCoreShardsReload;
import com.king.mgt.cmd.user.UserCommandCustomer;
import com.king.mgt.cmd.user.UserCommandDrErrorReload;
import com.king.mgt.cmd.user.UserCommandHelp;
import com.king.mgt.cmd.user.UserCommandModulesReload;
import com.king.mgt.cmd.user.UserCommandRecipientBlacklist;
import com.king.mgt.cmd.user.UserCommandRoutingInfo;
import com.king.mgt.cmd.user.UserCommandSenderBlacklist;
import com.king.mgt.cmd.user.UserCommandSenderWhitelist;
import com.king.mgt.cmd.user.UserCommandStop;
import com.king.mgt.cmd.user.UserCommandSwitchDNS;
import com.king.mgt.cmd.user.UserCommandSwitchRedis;
import com.king.mgt.cmd.user.UserCommandWhitelist;

/**
 * The control commands the smsui commands are published as (RedisControlCommands), for telnet and CM tools
 * sessions alike: shutdown keeps its targets, switchdns its side, a reload maps to RELOAD_*, the local ones
 * (help, -g) are not published; the list timers publish RELOAD_*:-a.
 */
public class SmsUiCommandCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	static UserCommand parsed(UserCommand cmd, String line) {
		return cmd.parseArgs(line.split(" ")) ? cmd : null;
	}

	static String control(UserCommand cmd) {
		return cmd == null ? null : RedisControlCommands.toRedisControlCommand(cmd);
	}

	public static void main(String[] args) {
		check("SHUTDOWN".equals(control(parsed(new UserCommandStop(), "shutdown -a"))), "shutdown -a -> SHUTDOWN");
		String one = control(parsed(new UserCommandStop(), "shutdown CoreEngine1"));
		check("SHUTDOWN@CoreEngine1".equals(one), "shutdown CoreEngine1 -> SHUTDOWN@CoreEngine1 (was SHUTDOWN to every module): " + one);
		String two = control(parsed(new UserCommandStop(), "shutdown MultiSmppClient1 MultiSmppClient2"));
		check("SHUTDOWN@MultiSmppClient1,MultiSmppClient2".equals(two), "shutdown of two modules keeps both targets: " + two);
		String ha = control(parsed(new UserCommandStop(), "shutdown -ha CoreEngine2"));
		check("SHUTDOWN@CoreEngine2".equals(ha), "shutdown -ha CoreEngine2 -> SHUTDOWN@CoreEngine2: " + ha);
		check(parsed(new UserCommandStop(), "shutdown") == null, "shutdown without a target is refused");

		String dns = control(parsed(new UserCommandSwitchDNS(), "switchdns slave"));
		check("SWITCH_DNS:slave".equals(dns), "switchdns slave -> SWITCH_DNS:slave (was SWITCH_DNS: without the side): " + dns);
		check(parsed(new UserCommandSwitchDNS(), "switchdns other") == null, "switchdns with another side is refused");
		String redis = control(parsed(new UserCommandSwitchRedis(), "switchredis master"));
		check("SWITCH_REDIS:master".equals(redis), "switchredis master -> SWITCH_REDIS:master: " + redis);

		check("RELOAD_ROUTING:-r".equals(control(parsed(new UserCommandRoutingInfo(), "routinginfo -r"))), "routinginfo -r -> RELOAD_ROUTING:-r");
		check("RELOAD_CUSTOMER:-a".equals(control(parsed(new UserCommandCustomer(), "customer -a"))), "customer -a -> RELOAD_CUSTOMER:-a");
		UserCommand generate = parsed(new UserCommandRoutingInfo(), "routinginfo -g");
		check(generate != null && !RedisControlCommands.handles(generate), "routinginfo -g stays local (file download only)");
		UserCommand help = parsed(new UserCommandHelp(), "help");
		check(help != null && !RedisControlCommands.handles(help), "help stays local");

		// the reloads that only have a control command
		check("RELOAD_DRERROR:-a".equals(control(parsed(new UserCommandDrErrorReload(), "drError -a"))), "drError -a -> RELOAD_DRERROR:-a");
		check(parsed(new UserCommandDrErrorReload(), "drError -a CoreEngine1") == null, "drError takes no module");
		check(parsed(new UserCommandDrErrorReload(), "drError -r") == null, "drError -r is refused");
		check("RELOAD_CORE_SHARDS:-a".equals(control(parsed(new UserCommandCoreShardsReload(), "coreShards -a"))), "coreShards -a -> RELOAD_CORE_SHARDS:-a");
		String cs = control(parsed(new UserCommandCoreShardsReload(), "coreShards -a CoreEngine2"));
		check("RELOAD_CORE_SHARDS@CoreEngine2:-a".equals(cs), "coreShards -a CoreEngine2 -> RELOAD_CORE_SHARDS@CoreEngine2:-a: " + cs);
		String cl = control(parsed(new UserCommandClientShardsReload(), "clientShards -a MultiSmppClient1 MultiSmppClient2"));
		check("RELOAD_CLIENT_SHARDS@MultiSmppClient1,MultiSmppClient2:-a".equals(cl), "clientShards of two modules: " + cl);
		check("RELOAD_MODULES:-a".equals(control(parsed(new UserCommandModulesReload(), "modules -a"))), "modules -a -> RELOAD_MODULES:-a");

		// the smsui list timers (WhiteListFileTask etc.) publish their commands as TYPE_ACTIVE
		UserCommand[] timed = { new UserCommandWhitelist(), new UserCommandSenderWhitelist(), new UserCommandSenderBlacklist(),
				new UserCommandContentWhitelist(), new UserCommandContentBlacklist(), new UserCommandRecipientBlacklist() };
		String[] expected = { "RELOAD_WHITELIST:-a", "RELOAD_SENDER_WHITELIST:-a", "RELOAD_SENDER_BLACKLIST:-a",
				"RELOAD_CONTENT_WHITELIST:-a", "RELOAD_CONTENT_BLACKLIST:-a", "RELOAD_RECIPIENT_BLACKLIST:-a" };
		for (int i = 0; i < timed.length; i++) {
			timed[i].setType(UserCommandWhitelist.TYPE_ACTIVE);
			String c = control(timed[i]);
			check(expected[i].equals(c), "list timer command -> " + expected[i] + ": " + c);
		}

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
