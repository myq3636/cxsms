#!/bin/bash
# Long performance test (docs/性能测试部署与执行手册.md):
#   perf_long.sh CASE_DIR [--no-capture]
# bin/perf_clean.sh, then bin/perf_watch.sh while the case runs: by bin/run_hightps.sh (capture on both sides, the
# capture checked against the CDRs by rt.FlowCheck, memory by memwatch/MemReport; the pcap files are deleted once read
# unless CAP_KEEP=true) or, with --no-capture, by bin/run_case.sh. Output: $A2P/log/perf_long_{id}_{time}/ run.out,
# watch.*, notes.txt, report.md (rt.PerfReport); the case's result directory holds flow/ and mem_report.txt.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
CASE=$(cd "$1" && pwd)
id=$(grep "^case.id=" "$CASE/case.properties" | cut -d= -f2 | tr -d ' ')
OUT=$A2P/log/perf_long_${id}_$(date +%m%d_%H%M); mkdir -p "$OUT"
echo "case $CASE, start $(date '+%F %T')" > "$OUT/notes.txt"
"$RT/bin/perf_clean.sh" >> "$OUT/notes.txt" 2>&1
"$RT/bin/perf_watch.sh" "$OUT/watch" > /dev/null 2>&1 &
if [ "${2:-}" = "--no-capture" ]; then
	"$RT/bin/run_case.sh" "$CASE" > "$OUT/run.out" 2>&1
else
	CAP_KEEP=${CAP_KEEP:-false} "$RT/bin/run_hightps.sh" "$CASE" > "$OUT/run.out" 2>&1
fi
rc=$?
touch "$OUT/watch.stop"; wait
echo "rc=$rc end $(date '+%F %T')" >> "$OUT/notes.txt"
[ -f "$OUT/watch.guard" ] && cat "$OUT/watch.guard" >> "$OUT/notes.txt"
${JAVA_BIN:-java} -cp "$RT/lib/routetest.jar" rt.PerfReport "$OUT" > "$OUT/report.md" 2>&1
grep -h "^RESULT\|^MEMORY" "$OUT/run.out" >> "$OUT/notes.txt"
echo "report: $OUT/report.md"
exit $rc
