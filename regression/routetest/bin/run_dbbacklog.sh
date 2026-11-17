#!/bin/bash
# The DB backlog test (docs/DB积压性能测试设计.md): the same traffic before (A) and after (B) N rows were put into wdq
# and rdq; DB samples, slow query log and Core thread dumps during each; then the rows are deleted and A and B compared.
#   run_dbbacklog.sh [CASE_DIR] [N]      default cases/PF001_db_backlog, 500000
# Report: CASE_DIR/report/{time}/report.txt (last line RESULT: PASS = no slowdown, FAIL = slower, with the evidence)
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
CASE=$(cd "${1:-$RT/cases/PF001_db_backlog}" && pwd)
N=${2:-500000}
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
export MYSQL_PWD=$(grep -E '^DS_gmms_Password=' $G | cut -d= -f2- | tr -d '\r')
U=$(grep -E '^DS_gmms_Login=' $G | cut -d= -f2- | tr -d ' \r')
H=$(grep -E '^DS_gmms_URL=' $G | sed -E 's#.*mysql://([^:/]+).*#\1#')
MYSQL=${MYSQL:-/usr/local/mysql/bin/mysql}
M="$MYSQL -h$H -u$U -N -B gmms"
OUT=$CASE/report/$(date '+%Y%m%d_%H%M%S'); mkdir -p "$OUT"
REP=$OUT/report.txt
SLOW_ON=$($M -e "SELECT @@slow_query_log"); LQT=$($M -e "SELECT @@long_query_time"); SLOW_FILE=$($M -e "SELECT @@slow_query_log_file")
cleanup() {
	$M -e "SET GLOBAL slow_query_log=$SLOW_ON, long_query_time=$LQT, slow_query_log_file='$SLOW_FILE'"
	"$RT/bin/dbbacklog_load.sh" --clean >> "$OUT/load.txt" 2>&1
}
trap cleanup EXIT
log() { echo "[$(date '+%F %T')] $*" | tee -a "$OUT/steps.txt"; }

phase() {   # name
	local P=$OUT/$1; mkdir -p "$P"
	local slow=/tmp/rt_slow_$1_$$.log; rm -f $slow
	$M -e "SET GLOBAL slow_query_log_file='$slow', long_query_time=0.2, slow_query_log=1"
	"$RT/bin/dbwatch.sh" "$P" 5 > /dev/null 2>&1 & local W=$!
	# thread dumps of the Cores in the middle of the measured traffic
	( sleep 270; for m in CoreEngine1 CoreEngine2 CoreEngine3; do
		pid=$(ps -eo pid,args | awk -v m="-Dmodule=$m" '{for(i=2;i<=NF;i++) if ($i==m) {print $1; exit}}')
		[ -n "$pid" ] && jcmd $pid Thread.print > "$P/threads_$m.txt" 2>&1
	  done ) & local T=$!
	log "phase $1: run_case"
	"$RT/bin/run_case.sh" "$CASE" > "$P/run.out" 2>&1
	touch "$P/stop"; wait $W; wait $T
	$M -e "SET GLOBAL slow_query_log=0"
	cp $slow "$P/slow.log" 2>/dev/null; rm -f $slow
	local last; last=$(ls -1d "$CASE"/result/*/ | tail -1)
	cp "$last/summary.txt" "$P/summary.txt"; cp "$last/soak.txt" "$P/soak.txt"
	echo "$last" > "$P/result_dir.txt"
	# slow statements: count, total and max time per statement shape (numbers and strings blanked)
	awk '/^# Query_time:/{qt=$3; getline; while ($0 ~ /^(SET timestamp|use |# )/) getline; q=$0; gsub(/[0-9]+/,"N",q); gsub(/'"'"'[^'"'"']*'"'"'/,"S",q); n[q]++; t[q]+=qt; if (qt>mx[q]) mx[q]=qt}
	     END {for (q in n) printf "%6d %9.1f %7.2f  %s\n", n[q], t[q], mx[q], substr(q,1,220)}' "$P/slow.log" 2>/dev/null | sort -k2 -nr | head -12 > "$P/slow_top.txt"
	# Core threads waiting in the DB
	for f in "$P"/threads_*.txt; do [ -f "$f" ] || continue
		echo "$(basename $f): $(grep -c 'com.mysql' $f) frames in the MySQL driver; in the SQL of: $(grep -oE 'MessageStoreManager\.(clearMessage|doDelete|insertMessageToDB|updateByInMsgId|getGmmsMessageByOutMsgID|[a-zA-Z]+)' $f | sort | uniq -c | sort -nr | head -5 | awk '{printf "%s x%s; ", $2, $1}')"
	done > "$P/threads_db.txt"
}

log "N=$N, case $CASE"
"$RT/bin/dbbacklog_load.sh" --clean > "$OUT/load.txt" 2>&1
log "rows before: wdq $($M -e 'SELECT COUNT(*) FROM wdq'), rdq $($M -e 'SELECT COUNT(*) FROM rdq')"
phase A
log "loading $N rows into wdq and rdq"
s0=$(date +%s); "$RT/bin/dbbacklog_load.sh" $N >> "$OUT/load.txt" 2>&1; log "loaded in $(( $(date +%s) - s0 )) s: $(tail -2 $OUT/load.txt | tr '\n' ' ')"
{
	echo "-- RDQ ReSendDRTask (every 10 s)"; $MYSQL -h$H -u$U -t gmms -e "EXPLAIN SELECT * FROM rdq WHERE ActionCode != 0 AND now() > NextRetryTime AND O_A2P IN (1) ORDER BY DateIn ASC limit 500"
	echo "-- WDQ ExpiredMessageTask (every 60 s)"; $MYSQL -h$H -u$U -t gmms -e "EXPLAIN SELECT * FROM wdq WHERE ActionCode != 0 AND DeliveryReport = 1 AND Split < 2 AND O_A2P IN (1) AND ExpiryDate < now() ORDER BY DateIn ASC limit 50"
	echo "-- the DELETE of every vendor DR (handleOutDeliveryReportReq)"; $MYSQL -h$H -u$U -t gmms -e "EXPLAIN SELECT * FROM wdq WHERE OutMsgID='x'"
	echo "-- time of one RDQ scan"; s=$(date +%s%3N); $M -e "SELECT COUNT(*) FROM (SELECT ID FROM rdq WHERE ActionCode != 0 AND now() > NextRetryTime AND O_A2P IN (1) ORDER BY DateIn ASC limit 500) x" > /dev/null; echo "$(( $(date +%s%3N) - s )) ms"
	echo "-- time of one WDQ scan"; s=$(date +%s%3N); $M -e "SELECT COUNT(*) FROM (SELECT ID FROM wdq WHERE ActionCode != 0 AND DeliveryReport = 1 AND Split < 2 ORDER BY DateIn ASC limit 50) x" > /dev/null; echo "$(( $(date +%s%3N) - s )) ms"
	echo "-- engines"; $M -e "SELECT TABLE_NAME, ENGINE, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='gmms' AND TABLE_NAME IN ('wdq','rdq')"
} > "$OUT/explain.txt" 2>&1
phase B
log "deleting the rows"
"$RT/bin/dbbacklog_load.sh" --clean >> "$OUT/load.txt" 2>&1
trap - EXIT; $M -e "SET GLOBAL slow_query_log=$SLOW_ON, long_query_time=$LQT, slow_query_log_file='$SLOW_FILE'"

# ---- report
val() { grep "^$2=" "$OUT/$1/soak.txt" | sed -E "s/.*$3=([0-9]+).*/\1/"; }
delta() { awk -F, -v c=$2 'NR==2{a=$c} NR>1{b=$c} END {print b-a}' "$OUT/$1/db.csv"; }
maxcol() { awk -F, -v c=$2 'NR>1 && $c>m {m=$c} END {print m+0}' "$OUT/$1/db.csv"; }
{
	echo "DB backlog test: $N rows in wdq and rdq ($(date '+%F %T'))"; echo
	printf "%-44s %12s %12s\n" "" "A (as is)" "B (+$N)"
	for k in latMt latDr latFull; do
		for f in p50 p95 p99 max; do printf "%-44s %12s %12s\n" "$k $f (ms)" "$(val A $k $f)" "$(val B $k $f)"; done
	done
	printf "%-44s %12s %12s\n" "MTs sent" "$(grep '^sent=' $OUT/A/soak.txt | cut -d= -f2)" "$(grep '^sent=' $OUT/B/soak.txt | cut -d= -f2)"
	printf "%-44s %12s %12s\n" "MTs without a DR at the end" "$(grep '^noDr=' $OUT/A/soak.txt | cut -d= -f2)" "$(grep '^noDr=' $OUT/B/soak.txt | cut -d= -f2)"
	printf "%-44s %12s %12s\n" "MySQL Table_locks_waited (during)" "$(delta A 4)" "$(delta B 4)"
	printf "%-44s %12s %12s\n" "MySQL slow statements >= 0.2 s (during)" "$(delta A 3)" "$(delta B 3)"
	printf "%-44s %12s %12s\n" "most statements waiting for a lock at once" "$(maxcol A 7)" "$(maxcol B 7)"
	printf "%-44s %12s %12s\n" "longest running statement seen (s)" "$(maxcol A 8)" "$(maxcol B 8)"
	echo; echo "slowest statement shapes in B (count, total s, max s):"; cat "$OUT/B/slow_top.txt"
	echo; echo "statements that waited for a lock or ran >= 1 s in B (first 15):"; head -15 "$OUT/B/processlist.log"
	echo; echo "Core threads in the DB at the middle of B:"; cat "$OUT/B/threads_db.txt"
	echo; echo "EXPLAIN and scan times with the rows:"; cat "$OUT/explain.txt"
	echo
	a=$(val A latFull p95); b=$(val B latFull p95); ad=$(val A latDr p95); bd=$(val B latDr p95)
	lim=$(( a * 2 > a + 300 ? a * 2 : a + 300 )); limd=$(( ad * 2 > ad + 300 ? ad * 2 : ad + 300 ))
	if [ -n "$a" ] && [ -n "$b" ] && [ "$b" -le "$lim" ] && [ "$bd" -le "$limd" ]; then
		echo "RESULT: PASS (no slowdown: p95 customer submit -> DR $a -> $b ms, vendor DR -> customer $ad -> $bd ms; limit max(2x, +300 ms))"
	else
		echo "RESULT: FAIL (slower: p95 customer submit -> DR $a -> $b ms, vendor DR -> customer $ad -> $bd ms; limit max(2x, +300 ms))"
	fi
} > "$REP"
cat "$REP"
grep -q "RESULT: PASS" "$REP"
