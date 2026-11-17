#!/bin/bash
# DB samples for the DB backlog test (docs/DB积压性能测试设计.md 2.3), every INTERVAL seconds until OUT_DIR/stop exists.
#   dbwatch.sh OUT_DIR [INTERVAL_SEC]
# OUT_DIR/db.csv: ts,questions,slow,locksWaited,locksImmediate,threads,waitingLock,longestSec
# OUT_DIR/processlist.log: the statements that waited for a lock or ran 1 s or more, at each sample
set -u
OUT=$1; INTERVAL=${2:-5}
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
export MYSQL_PWD=$(grep -E '^DS_gmms_Password=' $G | cut -d= -f2- | tr -d '\r')
U=$(grep -E '^DS_gmms_Login=' $G | cut -d= -f2- | tr -d ' \r')
H=$(grep -E '^DS_gmms_URL=' $G | sed -E 's#.*mysql://([^:/]+).*#\1#')
M="${MYSQL:-/usr/local/mysql/bin/mysql} -h$H -u$U -N -B"
mkdir -p "$OUT"
echo "ts,questions,slow,locksWaited,locksImmediate,threads,waitingLock,longestSec" > "$OUT/db.csv"
while [ ! -f "$OUT/stop" ]; do
	ts=$(date +%s%3N)
	st=$($M -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Questions','Slow_queries','Table_locks_waited','Table_locks_immediate','Threads_connected')" | awk '{v[$1]=$2} END {print v["Questions"]","v["Slow_queries"]","v["Table_locks_waited"]","v["Table_locks_immediate"]","v["Threads_connected"]}')
	pl=$($M -e "SELECT TIME, STATE, LEFT(REPLACE(INFO, '\n', ' '), 300) FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Sleep' AND INFO IS NOT NULL AND ID <> CONNECTION_ID()")
	wait=$(echo "$pl" | grep -ci "lock")
	longest=$(echo "$pl" | awk -F'\t' 'NF{if ($1>m) m=$1} END {print m+0}')
	echo "$ts,$st,$wait,$longest" >> "$OUT/db.csv"
	echo "$pl" | awk -F'\t' -v ts="$(date '+%T')" 'NF && ($1>=1 || tolower($2) ~ /lock/) {print ts " " $1 "s [" $2 "] " $3}' >> "$OUT/processlist.log"
	for i in $(seq 1 $INTERVAL); do [ -f "$OUT/stop" ] && break; sleep 1; done
done
