#!/bin/bash
# Long performance test (docs/性能测试部署与执行手册.md):
#   perf_long.sh CASE_DIR [--no-capture] [--no-replicas]
# bin/perf_clean.sh, then bin/perf_watch.sh while the case runs: by bin/run_hightps.sh (capture on both sides, the
# capture checked against the CDRs by rt.FlowCheck, memory by memwatch/MemReport; the pcap files are deleted once read
# unless CAP_KEEP=true) or, with --no-capture, by bin/run_case.sh (a run of millions of MTs: rt.FlowCheck keeps every MT
# until the end). --no-replicas: the Redis replicas are stopped for the test and started again at the end. Output: $A2P/log/perf_long_{id}_{time}/ run.out,
# watch.*, notes.txt, report.md (rt.PerfReport); the case's result directory holds flow/ and mem_report.txt.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
CASE=$(cd "$1" && pwd)
NOCAP=false; NOREP=false
for a in "${@:2}"; do case "$a" in --no-capture) NOCAP=true ;; --no-replicas) NOREP=true ;; esac; done
G=$A2P/Gmms/GmmsConfig.properties
REDIS_HOME=${REDIS_HOME:-/usr/local/redis-7.4.9}
C=$REDIS_HOME/src/redis-cli
gp() { grep -E "^$1=" "$G" | tail -1 | cut -d= -f2- | tr -d '\r'; }
export REDISCLI_AUTH=$(gp Redis_password)
replicas() {   # host:port of each replica that is not its master (as bin/perf_step.sh)
	{ echo "$(gp Redis_slave_host | tr -d ' '):$(gp Redis_slave_port | tr -d ' ')"; gp Redis_submit_slaves | tr ',' '\n'; gp Redis_report_slaves | tr ',' '\n'; } \
		| tr -d ' ' | grep ":" | sort -u | while read r; do
			grep -qE "^(Redis_master_port=${r##*:}|Redis_(submit|report)_masters=.*${r})" "$G" || echo "$r"
		done
}
id=$(grep "^case.id=" "$CASE/case.properties" | cut -d= -f2 | tr -d ' ')
OUT=$A2P/log/perf_long_${id}_$(date +%m%d_%H%M); mkdir -p "$OUT"
echo "case $CASE, capture: $( $NOCAP && echo no || echo yes ), replicas stopped: $NOREP, start $(date '+%F %T')" > "$OUT/notes.txt"
if $NOREP; then
	for r in $(replicas); do $C -h ${r%:*} -p ${r##*:} --no-auth-warning shutdown nosave > /dev/null 2>&1; done
	echo "replicas stopped: $(replicas | tr '\n' ' ')" >> "$OUT/notes.txt"
fi
"$RT/bin/perf_clean.sh" >> "$OUT/notes.txt" 2>&1
"$RT/bin/perf_watch.sh" "$OUT/watch" > /dev/null 2>&1 &
if $NOCAP; then
	"$RT/bin/run_case.sh" "$CASE" > "$OUT/run.out" 2>&1
else
	CAP_KEEP=${CAP_KEEP:-false} "$RT/bin/run_hightps.sh" "$CASE" > "$OUT/run.out" 2>&1
fi
rc=$?
touch "$OUT/watch.stop"; wait
echo "rc=$rc end $(date '+%F %T')" >> "$OUT/notes.txt"
[ -f "$OUT/watch.guard" ] && cat "$OUT/watch.guard" >> "$OUT/notes.txt"
if $NOREP; then
	for r in $(replicas); do
		p=${r##*:}; f=$REDIS_HOME/conf/redis$p.conf
		[ -f "$f" ] && (cd "$REDIS_HOME" && src/redis-server "$f") || echo "no $f: replica $r not started" >> "$OUT/notes.txt"
	done
	sleep 30
	echo "replicas: $(for r in $(replicas); do echo -n "${r##*:}=$($C -h ${r%:*} -p ${r##*:} --no-auth-warning info replication | grep -oE "master_link_status:[a-z]+" | cut -d: -f2) "; done)" >> "$OUT/notes.txt"
fi
${JAVA_BIN:-java} -cp "$RT/lib/routetest.jar" rt.PerfReport "$OUT" > "$OUT/report.md" 2>&1
grep -h "^RESULT\|^MEMORY" "$OUT/run.out" >> "$OUT/notes.txt"
echo "report: $OUT/report.md"
exit $rc
