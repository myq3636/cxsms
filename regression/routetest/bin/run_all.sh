#!/bin/bash
# Run every routing test case under cases/ (regression/routetest, docs/路由服务器测试说明.md), one after another.
#   run_all.sh [--no-restart] [CASE_NAME ...]
# Each case changes conf/cm.cfg and conf/routing, so the modules are restarted for each one (run_case.sh);
# --no-restart only when every case uses the same config. Without names: every directory of cases/ with a
# case.properties, in name order. The summary of the run: runs/{time}/summary.txt; exit 0 when every case passed.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
ARGS=()
NAMES=()
for a in "$@"; do
	if [ "$a" = "--no-restart" ]; then ARGS+=("$a"); else NAMES+=("$a"); fi
done
if [ ${#NAMES[@]} -eq 0 ]; then
	for d in "$RT"/cases/*/; do
		[ -f "$d/case.properties" ] && NAMES+=("$(basename "$d")")
	done
fi
RUN="$RT/runs/$(date '+%Y%m%d_%H%M%S')"
mkdir -p "$RUN"
SUM="$RUN/summary.txt"
echo "routing test run $(date '+%F %T'), ${#NAMES[@]} case(s)" | tee "$SUM"
pass=0; fail=0
for n in "${NAMES[@]}"; do
	d="$RT/cases/$n"
	if [ ! -f "$d/case.properties" ]; then
		echo "$n: no such case" | tee -a "$SUM"; fail=$((fail+1)); continue
	fi
	"$RT/bin/run_case.sh" "$d" ${ARGS[@]+"${ARGS[@]}"} > "$RUN/$n.out" 2>&1
	rc=$?
	result=$(ls -1d "$d"/result/*/ 2>/dev/null | tail -1)
	verdict=$(grep -h "^RESULT:" "$result/summary.txt" 2>/dev/null | tail -1)
	[ -z "$verdict" ] && verdict="RESULT: ERROR (see $RUN/$n.out)"
	if [ $rc -eq 0 ]; then pass=$((pass+1)); else fail=$((fail+1)); fi
	printf '%-40s %s  %s\n' "$n" "$verdict" "${result%/}" | tee -a "$SUM"
done
echo "TOTAL: ${pass} passed, ${fail} failed" | tee -a "$SUM"
echo "summary: $SUM"
[ $fail -eq 0 ]
