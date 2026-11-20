#!/bin/bash
# Step test: the highest TPS without a growing backlog (docs/性能测试部署与执行手册.md):
#   perf_step.sh CASE_DIR "3000 3500 4000" [--no-replicas]
# For each TPS: a copy of the case with soak.tps=TPS (case.id {id}T{TPS}), bin/perf_clean.sh, bin/perf_watch.sh while
# bin/run_case.sh runs it (the a2p modules restarted). --no-replicas: the Redis replicas (Redis_slave_*,
# Redis_submit_slaves, Redis_report_slaves) are stopped for the whole test and started again at the end.
# Output: $A2P/log/perf_step_{id}_{time}/ run_{TPS}.out, watch_{TPS}.*, notes.txt, report.md (rt.PerfReport).
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
CASE=$(cd "$1" && pwd)
LEVELS=$2
NOREP=false; [ "${3:-}" = "--no-replicas" ] && NOREP=true
REDIS_HOME=${REDIS_HOME:-/usr/local/redis-7.4.9}
C=$REDIS_HOME/src/redis-cli
gp() { grep -E "^$1=" "$G" | tail -1 | cut -d= -f2- | tr -d '\r'; }
export REDISCLI_AUTH=$(gp Redis_password)
id=$(grep "^case.id=" "$CASE/case.properties" | cut -d= -f2 | tr -d ' ')
OUT=$A2P/log/perf_step_${id}_$(date +%m%d_%H%M); mkdir -p "$OUT"
echo "case $CASE, levels $LEVELS, replicas stopped: $NOREP, start $(date '+%F %T')" > "$OUT/notes.txt"

replicas() {   # host:port of each replica that is not its master
	{ echo "$(gp Redis_slave_host | tr -d ' '):$(gp Redis_slave_port | tr -d ' ')"; gp Redis_submit_slaves | tr ',' '\n'; gp Redis_report_slaves | tr ',' '\n'; } \
		| tr -d ' ' | grep ":" | sort -u | while read r; do
			grep -qE "^(Redis_master_port=${r##*:}|Redis_(submit|report)_masters=.*${r})" "$G" || echo "$r"
		done
}
if $NOREP; then
	for r in $(replicas); do $C -h ${r%:*} -p ${r##*:} --no-auth-warning shutdown nosave > /dev/null 2>&1; done
	echo "replicas stopped: $(replicas | tr '\n' ' ')" >> "$OUT/notes.txt"
fi

for TPS in $LEVELS; do
	STEP="$RT/cases/$(basename "$CASE")_T$TPS"
	rm -rf "$STEP"; cp -r "$CASE" "$STEP"; rm -rf "$STEP/result"
	sed -i -E "s/^soak.tps=.*/soak.tps=$TPS/; s/^case.id=.*/case.id=${id}T$TPS/" "$STEP/case.properties"
	"$RT/bin/perf_clean.sh" >> "$OUT/notes.txt" 2>&1
	"$RT/bin/perf_watch.sh" "$OUT/watch_$TPS" > /dev/null 2>&1 &
	echo "$TPS start $(date '+%F %T')" >> "$OUT/notes.txt"
	"$RT/bin/run_case.sh" "$STEP" > "$OUT/run_$TPS.out" 2>&1
	touch "$OUT/watch_$TPS.stop"; wait
	echo "$TPS end $(date '+%F %T')" >> "$OUT/notes.txt"
	[ -f "$OUT/watch_$TPS.guard" ] && cat "$OUT/watch_$TPS.guard" >> "$OUT/notes.txt"
	# the steady window for rt.PerfReport (the step's result/ goes with the copy)
	ph=$(ls -t "$STEP"/result/*/phases.txt "$STEP"/result/*/mem/phases.txt 2>/dev/null | head -1)
	[ -n "$ph" ] && cp "$ph" "$OUT/phases_$TPS.txt"
	rm -rf "$STEP"
done

if $NOREP; then
	for r in $(replicas); do
		p=${r##*:}; f=$REDIS_HOME/conf/redis$p.conf
		[ -f "$f" ] && (cd "$REDIS_HOME" && src/redis-server "$f") || echo "no $f: replica $r not started" >> "$OUT/notes.txt"
	done
	sleep 10
	echo "replicas: $(for r in $(replicas); do echo -n "${r##*:}=$($C -h ${r%:*} -p ${r##*:} --no-auth-warning info replication | grep -oE "master_link_status:[a-z]+" | cut -d: -f2) "; done)" >> "$OUT/notes.txt"
fi
"$RT/bin/perf_clean.sh" >> "$OUT/notes.txt" 2>&1
"$A2P/bin/a2pStart.sh" ${PERF_MODULES:-CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2} > /dev/null 2>&1
echo "end $(date '+%F %T')" >> "$OUT/notes.txt"
${JAVA_BIN:-java} -cp "$RT/lib/routetest.jar" rt.PerfReport "$OUT" > "$OUT/report.md" 2>&1
echo "report: $OUT/report.md"
