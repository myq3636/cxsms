#!/bin/bash
# Memory samples of the a2p modules and the Redis instances for the memory leak test (docs/内存泄漏测试设计.md 2.3).
#   memwatch.sh OUT_DIR [INTERVAL_SEC] [GC_EVERY]     runs until OUT_DIR/stop exists
# OUT_DIR/jvm.csv: ts,module,pid,heap,afterGc,old,meta,rss,threads,fds (MB; afterGc only on the samples after a
# jcmd GC.run, every GC_EVERY-th sample). OUT_DIR/redis.csv: ts,port,usedMB,keys,noTtl,sampled (noTtl of sampled random keys).
set -u
OUT=$1; INTERVAL=${2:-30}; GCEVERY=${3:-2}
A2P=${A2P_HOME:-/usr/local/a2p}
MODULES=${RT_MODULES:-"CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2"}
CLI=${REDIS_CLI:-/usr/local/redis-7.4.9/src/redis-cli}
G=$A2P/Gmms/GmmsConfig.properties
RHOST=$(grep -E '^Redis_master_host=' $G | cut -d= -f2- | tr -d ' \r')
export REDISCLI_AUTH=$(grep -E '^Redis_password=' $G | cut -d= -f2- | tr -d '\r')
PORTS=$(grep -E '^Redis_(master|submit_master|report_master)_port=' $G | cut -d= -f2- | tr -d ' \r' | tr '\n' ' ')
SAMPLE='local n=0 for i=1,tonumber(ARGV[1]) do local k=redis.call("RANDOMKEY") if k and redis.call("TTL",k)==-1 then n=n+1 end end return n'
mkdir -p "$OUT"
echo "ts,module,pid,heap,afterGc,old,meta,rss,threads,fds" > "$OUT/jvm.csv"
echo "ts,port,usedMB,keys,noTtl,sampled" > "$OUT/redis.csv"
pidof_module() { ps -eo pid,args | awk -v m="-Dmodule=$1" '{for(i=2;i<=NF;i++) if ($i==m) {print $1; exit}}'; }
n=0
while [ ! -f "$OUT/stop" ]; do
	t0=$(date +%s)
	n=$((n+1))
	gc=0
	declare -A PID=()
	for m in $MODULES; do PID[$m]=$(pidof_module $m); done
	if [ $((n % GCEVERY)) -eq 0 ]; then
		gc=1
		for m in $MODULES; do [ -n "${PID[$m]}" ] && jcmd ${PID[$m]} GC.run > /dev/null 2>&1 & done
		wait
		sleep 2
	fi
	ts=$(date +%s%3N)
	for m in $MODULES; do
		pid=${PID[$m]}
		if [ -z "$pid" ] || [ ! -d /proc/$pid ]; then echo "$ts,$m,,,,,,,," >> "$OUT/jvm.csv"; continue; fi
		read heap old meta < <(jstat -gc $pid 2>/dev/null | awk 'NR==1{for(i=1;i<=NF;i++)h[$i]=i} NR==2{printf "%.1f %.1f %.1f\n", ($h["S0U"]+$h["S1U"]+$h["EU"]+$h["OU"])/1024, $h["OU"]/1024, $h["MU"]/1024}')
		after=""; [ $gc -eq 1 ] && after=$heap
		rss=$(awk '/^VmRSS/{printf "%.1f", $2/1024}' /proc/$pid/status 2>/dev/null)
		thr=$(awk '/^Threads/{print $2}' /proc/$pid/status 2>/dev/null)
		fds=$(ls /proc/$pid/fd 2>/dev/null | wc -l)
		echo "$ts,$m,$pid,$heap,$after,$old,$meta,$rss,$thr,$fds" >> "$OUT/jvm.csv"
	done
	for port in $PORTS; do
		used=$($CLI -h $RHOST -p $port --no-auth-warning info memory 2>/dev/null | awk -F: '/^used_memory:/{printf "%.1f", $2/1048576}')
		keys=$($CLI -h $RHOST -p $port --no-auth-warning dbsize 2>/dev/null)
		nottl=$($CLI -h $RHOST -p $port --no-auth-warning eval "$SAMPLE" 0 500 2>/dev/null)
		echo "$ts,$port,$used,$keys,$nottl,500" >> "$OUT/redis.csv"
	done
	left=$((INTERVAL - ($(date +%s) - t0)))
	[ $left -gt 0 ] && for i in $(seq 1 $left); do [ -f "$OUT/stop" ] && break; sleep 1; done
done
