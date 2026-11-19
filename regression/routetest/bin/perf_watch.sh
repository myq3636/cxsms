#!/bin/bash
# Monitoring during a performance test (docs/性能测试部署与执行手册.md):
#   perf_watch.sh OUT_PREFIX        (runs until the file OUT_PREFIX.stop exists)
# every PERF_WATCH_SEC (default 10) seconds, the time in epoch seconds first:
#   OUT_PREFIX.streams  entries in the streams of each MQ master by type and ssid (one Lua per master)
#   OUT_PREFIX.cpu      %CPU of the a2p modules, Redis, mysqld and the test tools; idle= of the host
#   OUT_PREFIX.redis    used memory (MB) and ops/s of state and every MQ master
#   OUT_PREFIX.disk     GB free on / and on the log disk
# Guard: a Redis above PERF_GUARD_REDIS_PCT (default 90) % of its maxmemory, under PERF_GUARD_ROOT_GB (default 8) GB
# free on /, or under PERF_GUARD_LOG_GB (default 20) GB free on the log disk (logs, CDRs), stops the test tool
# (pkill rt.RouteCase); the reason goes to OUT_PREFIX.guard.
set -u
OUT=$1
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
C=${REDIS_CLI:-/usr/local/redis-7.4.9/src/redis-cli}
EVERY=${PERF_WATCH_SEC:-10}
GUARD_PCT=${PERF_GUARD_REDIS_PCT:-90}
GUARD_ROOT=${PERF_GUARD_ROOT_GB:-8}
GUARD_LOG=${PERF_GUARD_LOG_GB:-20}
gp() { grep -E "^$1=" "$G" | tail -1 | cut -d= -f2- | tr -d '\r'; }
export REDISCLI_AUTH=$(gp Redis_password)
STATE="$(gp Redis_master_host | tr -d ' '):$(gp Redis_master_port | tr -d ' ')"
mq() { local l=$(gp Redis_$1_masters | tr -d ' '); [ -n "$l" ] && echo "$l" | tr ',' ' ' || echo "$(gp Redis_$1_master_host | tr -d ' '):$(gp Redis_$1_master_port | tr -d ' ')"; }
MQS="$(mq submit) $(mq report)"
LOGDISK=$(readlink -f "$A2P/log")
SUMS='local s={} for _,k in ipairs(redis.call("SMEMBERS","idx:streams")) do local f=string.match(k,"^stream:([a-z:-]+:[0-9]+):") or string.match(k,"^stream:([a-z:-]+)") or k s[f]=(s[f] or 0)+redis.call("XLEN",k) end local o={} for f,n in pairs(s) do if n>0 then table.insert(o,f.."="..n) end end return o'
rm -f "$OUT.stop" "$OUT.guard"
while [ ! -f "$OUT.stop" ]; do
	t=$(date +%s)
	line="$t"
	for hp in $MQS; do line="$line $($C -h ${hp%:*} -p ${hp##*:} --no-auth-warning eval "$SUMS" 0 2>/dev/null | tr '\n' ' ')"; done
	echo "$line" >> "$OUT.streams"
	echo "$t $(top -b -n2 -d1 -c -w 512 | awk '/^top -/{n++} n==2 && /^%Cpu/ {idle=$8} n==2 && /-Dmodule=|redis-server|mysqld|rt\.RouteCase|rt\.FlowCheck/ {c=$9; m="mysqld"; if (match($0,/-Dmodule=[A-Za-z0-9]+/)) m=substr($0,RSTART+9,RLENGTH-9); else if (match($0,/rt\.[A-Za-z]+/)) m=substr($0,RSTART,RLENGTH); else if (match($0,/:[0-9][0-9][0-9][0-9]+( |$)/)) m="redis" substr($0,RSTART,RLENGTH); gsub(/ /,"",m); printf "%s=%s ", m, c} END {printf "idle=%s", idle}')" >> "$OUT.cpu"
	for hp in $STATE $MQS; do
		h=${hp%:*}; p=${hp##*:}
		used=$($C -h $h -p $p --no-auth-warning info memory 2>/dev/null | awk -F: '/^used_memory:/{printf "%d", $2/1048576}')
		max=$($C -h $h -p $p --no-auth-warning config get maxmemory 2>/dev/null | tail -1 | awk '{printf "%d", $1/1048576}')
		ops=$($C -h $h -p $p --no-auth-warning info stats 2>/dev/null | awk -F: '/^instantaneous_ops_per_sec:/{printf "%d", $2}')
		echo "$t $p usedMB=${used:-down} maxMB=${max:-0} ops=${ops:-0}" >> "$OUT.redis"
		if [ -n "$used" ] && [ "${max:-0}" -gt 0 ] && [ $((used * 100)) -gt $((max * GUARD_PCT)) ] && [ ! -f "$OUT.guard" ]; then
			echo "$(date '+%F %T') GUARD: redis $p used $used MB > $GUARD_PCT % of $max MB, the test tool stops sending" > "$OUT.guard"
			pkill -f "rt.RouteCase"
		fi
	done
	rootfree=$(df -BG / | awk 'NR==2{print $4+0}'); logfree=$(df -BG "$LOGDISK" | awk 'NR==2{print $4+0}')
	echo "$t rootFreeGB=$rootfree logFreeGB=$logfree" >> "$OUT.disk"
	if [ "${rootfree:-99}" -lt "$GUARD_ROOT" ] && [ ! -f "$OUT.guard" ]; then
		echo "$(date '+%F %T') GUARD: / has $rootfree GB free (< $GUARD_ROOT), the test tool stops sending" > "$OUT.guard"
		pkill -f "rt.RouteCase"
	fi
	if [ "${logfree:-99}" -lt "$GUARD_LOG" ] && [ ! -f "$OUT.guard" ]; then
		echo "$(date '+%F %T') GUARD: the log disk has $logfree GB free (< $GUARD_LOG), the test tool stops sending" > "$OUT.guard"
		pkill -f "rt.RouteCase"
	fi
	sleep "$EVERY"
done
