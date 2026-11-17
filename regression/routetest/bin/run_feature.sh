#!/bin/bash
# Run the tests of one feature (docs/路由服务器测试说明.md 6): its regression suites (features/{f}/regression.txt, with
# newsms/regression/run.sh: own Redis on 127.0.0.1:16379, test ports 17775-18099, does not touch the running a2p),
# then its message-level cases on the running a2p (features/{f}/cases.txt, run_case.sh).
#   run_feature.sh FEATURE [--no-restart]      e.g. run_feature.sh 01_路由重构
#   run_feature.sh --all [--no-restart]        every feature in features/, in name order
# Results: features/{f}/result/{time}/ (summary.txt, regression/{suite}.txt and .log, cases.txt); exit 0 = all passed.
# Checks of the regression (features/{f}/baseline.txt, "suite pass known" per line): every suite of regression.txt ran
# and is OK, has at least the baseline's PASS checks and no more KNOWN than the baseline. Without a baseline, a run where
# every suite is OK writes it (baseline.txt) for the next runs. The last line of summary.txt: RESULT: PASS / FAIL.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
SRC="$RT/newsms"
REDIS_DIR=${REDIS_DIR:-/usr/local/redis-7.4.9/src}
JAVA_HOME=${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")}
export JAVA_HOME
FEATS=(); ARGS=()
for a in "$@"; do
	case "$a" in
		--no-restart) ARGS+=("$a") ;;
		--all) for d in "$RT"/features/*/; do [ -f "$d/regression.txt" ] && FEATS+=("$(basename "$d")"); done ;;
		*) FEATS+=("$a") ;;
	esac
done
[ ${#FEATS[@]} -eq 0 ] && { echo "usage: $0 FEATURE|--all [--no-restart]"; ls "$RT/features"; exit 2; }
bad=0
for f in "${FEATS[@]}"; do
	F="$RT/features/$f"
	[ -f "$F/regression.txt" ] || { echo "$f: no such feature"; bad=$((bad+1)); continue; }
	OUT="$F/result/$(date '+%Y%m%d_%H%M%S')"
	mkdir -p "$OUT/regression"
	SUM="$OUT/summary.txt"
	fbad=0
	echo "feature $f, $(date '+%F %T')" | tee "$SUM"
	suites=$(grep -v '^\s*#' "$F/regression.txt" | tr '\n' ' ')
	if [ -n "${suites// /}" ]; then
		echo "regression: $suites" | tee -a "$SUM"
		( cd "$SRC" && REDIS_SERVER="$REDIS_DIR/redis-server" REDIS_CLI="$REDIS_DIR/redis-cli" nice regression/run.sh $suites ) > "$OUT/regression/run.out" 2>&1
		rc=$?
		for s in $suites; do
			cp "$SRC/regression/.work/results/$s.txt" "$OUT/regression/" 2>/dev/null
			cp "$SRC/regression/.work/results/logs/$s.log" "$OUT/regression/" 2>/dev/null
		done
		grep -E '^[A-Za-z0-9]+ +(OK|FAILED)|^TOTAL|^FAIL ' "$OUT/regression/run.out" | tee -a "$SUM"
		[ $rc -eq 0 ] || fbad=$((fbad+1))
		# every suite ran, OK, and not fewer checks than the baseline
		grep -E '^[A-Za-z0-9]+ +(OK|FAILED) ' "$OUT/regression/run.out" | awk '{p=$3; k=$5; sub("pass=","",p); sub("known=","",k); print $1, $2, p, k}' > "$OUT/regression/suites.got"
		for s in $suites; do
			line=$(grep "^$s " "$OUT/regression/suites.got")
			if [ -z "$line" ]; then echo "CHECK FAIL $s: did not run" | tee -a "$SUM"; fbad=$((fbad+1)); continue; fi
			set -- $line
			[ "$2" = OK ] || { echo "CHECK FAIL $s: $2" | tee -a "$SUM"; fbad=$((fbad+1)); }
			if [ -f "$F/baseline.txt" ]; then
				base=$(grep "^$s " "$F/baseline.txt")
				if [ -z "$base" ]; then echo "CHECK NOTE $s: not in baseline.txt (pass=$3 known=$4)" | tee -a "$SUM"; continue; fi
				set -- $base $3 $4
				[ "$4" -ge "$2" ] || { echo "CHECK FAIL $1: pass=$4, baseline $2 (fewer checks)" | tee -a "$SUM"; fbad=$((fbad+1)); }
				[ "$5" -le "$3" ] || { echo "CHECK FAIL $1: known=$5, baseline $3 (more known defects)" | tee -a "$SUM"; fbad=$((fbad+1)); }
			fi
		done
		if [ ! -f "$F/baseline.txt" ] && [ $fbad -eq 0 ]; then
			{ echo "# suite pass known: baseline of $f, written by run_feature.sh from $OUT $(date '+%F %T')"; awk '{print $1, $3, $4}' "$OUT/regression/suites.got"; } > "$F/baseline.txt"
			echo "baseline written: $F/baseline.txt" | tee -a "$SUM"
		elif [ -f "$F/baseline.txt" ]; then
			echo "checked against $F/baseline.txt" | tee -a "$SUM"
		fi
	fi
	cases=$(grep -v '^\s*#' "$F/cases.txt" 2>/dev/null | tr '\n' ' ')
	if [ -n "${cases// /}" ]; then
		echo "message-level cases: $cases" | tee -a "$SUM"
		"$RT/bin/run_all.sh" ${ARGS[@]+"${ARGS[@]}"} $cases > "$OUT/cases.txt" 2>&1 || fbad=$((fbad+1))
		grep -E 'RESULT:|^TOTAL' "$OUT/cases.txt" | tee -a "$SUM"
	fi
	echo "result: $OUT" | tee -a "$SUM"
	if [ $fbad -eq 0 ]; then echo "RESULT: PASS" | tee -a "$SUM"; else echo "RESULT: FAIL ($fbad problem(s))" | tee -a "$SUM"; bad=$((bad+1)); fi
done
[ $bad -eq 0 ]
