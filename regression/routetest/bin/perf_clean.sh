#!/bin/bash
# Clean the a2p data before a performance test (docs/性能测试部署与执行手册.md):
#   perf_clean.sh [--logs]
# 1. stop the a2p modules (PERF_MODULES, default: the modules of conf/a2p_modules.conf that run)
# 2. every Redis of the a2p: state (Redis_master_*), the submit and report MQ masters (Redis_submit_masters /
#    Redis_report_masters, else Redis_submit_master_* / Redis_report_master_*): all keys but thcon, then BGREWRITEAOF
# 3. TRUNCATE of every table of the gmms database
# 4. --logs: also the module logs (emptied), rotated logs, CDR files, SDQ and exception message files
# The modules are not started again: bin/run_case.sh restarts them (or $A2P/bin/a2pStart.sh).
set -u
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
C=${REDIS_CLI:-/usr/local/redis-7.4.9/src/redis-cli}
MYSQL=${MYSQL_BIN:-/usr/local/mysql8/bin/mysql}
gp() { grep -E "^$1=" "$G" | tail -1 | cut -d= -f2- | tr -d '\r'; }
MODULES=${PERF_MODULES:-"CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2"}

echo "[$(date '+%F %T')] stop: $MODULES"
"$A2P/bin/a2pStop.sh" $MODULES > /tmp/perf_clean_stop.out 2>&1
sleep 5

export REDISCLI_AUTH=$(gp Redis_password)
redis_list() {   # host:port of state and every MQ master
	echo "$(gp Redis_master_host | tr -d ' '):$(gp Redis_master_port | tr -d ' ')"
	for k in submit report; do
		local l=$(gp Redis_${k}_masters | tr -d ' ')
		if [ -n "$l" ]; then echo "$l" | tr ',' '\n'; else echo "$(gp Redis_${k}_master_host | tr -d ' '):$(gp Redis_${k}_master_port | tr -d ' ')"; fi
	done | sort -u
}
for hp in $(redis_list); do
	h=${hp%:*}; p=${hp##*:}
	for r in 1 2 3 4; do
		$C -h $h -p $p --no-auth-warning --scan --count 10000 | grep -vx "thcon" | xargs -r -d '\n' -n 2000 $C -h $h -p $p --no-auth-warning unlink > /dev/null
		[ "$($C -h $h -p $p --no-auth-warning dbsize)" -le 1 ] && break
	done
	$C -h $h -p $p --no-auth-warning bgrewriteaof > /dev/null 2>&1
	echo "[$(date '+%F %T')] redis $hp: $($C -h $h -p $p --no-auth-warning dbsize) key(s) left (thcon kept)"
done

dbh=$(gp DS_gmms_URL | sed -E 's#.*//([^:/]+).*#\1#')
export MYSQL_PWD=$(gp DS_gmms_Password)
MY="$MYSQL -h$dbh -u$(gp DS_gmms_Login) -N gmms"
n=0
for t in $($MY -e "show tables"); do $MY -e "TRUNCATE TABLE $t" && n=$((n + 1)); done
echo "[$(date '+%F %T')] mysql: $n tables truncated"

if [ "${1:-}" = "--logs" ]; then
	L=$(readlink -f "$A2P/log"); D=$(readlink -f "$A2P/cdr"); Q=$(readlink -f "$A2P/queue")
	rm -f "$L"/*.log.[0-9]* "$L"/*_gc.log.[0-9]* "$L"/*.gz
	rm -rf "$L/previous"
	for f in "$L"/*.log; do [ -f "$f" ] && : > "$f"; done
	rm -rf "$D"/link/* "$D"/*.cdr.* "$Q"/sdq/link/* "$Q"/sdq/*.sdq.* "$Q"/exceptionMsg/*.exceptionMsg.* 2>/dev/null
	echo "[$(date '+%F %T')] logs emptied, rotated logs, CDR, SDQ and exception message files removed ($L, $D, $Q)"
fi
