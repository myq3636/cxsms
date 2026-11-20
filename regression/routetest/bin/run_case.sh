#!/bin/bash
# Run one routing test case on this machine's a2p (docs/路由服务器测试说明.md 4, 5).
#   run_case.sh CASE_DIR [--no-restart]
# 1. config: conf/cm.cfg = base/cm.cfg + CASE_DIR/cm.extra.cfg; conf/routing/RT*_routingInfo.cfg = CASE_DIR/routing/;
#    CASE_DIR/conf/* replace, CASE_DIR/conf.append/* are appended to, CASE_DIR/conf.sed/* are sed scripts run on the files of conf/ (put back when the case ends,
#    also the ones phase2/conf/ replaces)
# 2. restart the a2p modules (or, with --no-restart, keep them running)
# 3. rt.RouteCase plays the vendors and the customers and checks every MT; result in CASE_DIR/result/{time}/
#    (a case with soak.durationSec: steady traffic, bin/memwatch.sh samples, rt.MemReport judges the memory)
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
CASE=$(cd "$1" && pwd)
RESTART=true
[ "${2:-}" = "--no-restart" ] && RESTART=false
MODULES=${RT_MODULES:-"CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2"}
JAVA=${JAVA_BIN:-java}
SAVE="$RT/.confsave"

# conf/ files the case changes: saved first (a missing one is marked), put back on exit
restore() {
	[ -f "$SAVE/GmmsConfig.properties.gmms" ] && cp -p "$SAVE/GmmsConfig.properties.gmms" "$A2P/Gmms/GmmsConfig.properties" && rm -f "$SAVE/GmmsConfig.properties.gmms"
	[ -d "$SAVE" ] || return
	for f in "$SAVE"/*; do
		[ -e "$f" ] || continue
		n=$(basename "$f")
		case "$n" in
			*.missing) rm -f "$A2P/conf/${n%.missing}" ;;
			*) cp -p "$f" "$A2P/conf/$n" ;;
		esac
	done
	rm -rf "$SAVE"
	echo "[$(date '+%F %T')] conf files put back"
}
restore   # left over by a case that was killed
mkdir -p "$SAVE"
for d in conf conf.append conf.sed phase2/conf; do
	[ -d "$CASE/$d" ] || continue
	for f in "$CASE/$d"/*; do
		n=$(basename "$f")
		[ -e "$SAVE/$n" ] || [ -e "$SAVE/$n.missing" ] && continue
		if [ -e "$A2P/conf/$n" ]; then cp -p "$A2P/conf/$n" "$SAVE/$n"; else touch "$SAVE/$n.missing"; fi
	done
done
# Gmms/GmmsConfig.properties: CASE_DIR/gmms.append/ and phase2/gmms.append/ add lines (a later key wins)
if [ -f "$CASE/gmms.append/GmmsConfig.properties" ] || [ -f "$CASE/phase2/gmms.append/GmmsConfig.properties" ]; then
	cp -p "$A2P/Gmms/GmmsConfig.properties" "$SAVE/GmmsConfig.properties.gmms"
fi
trap restore EXIT

echo "[$(date '+%F %T')] case $CASE"
cat "$RT/base/cm.cfg" > "$A2P/conf/cm.cfg"
[ -f "$CASE/cm.extra.cfg" ] && cat "$CASE/cm.extra.cfg" >> "$A2P/conf/cm.cfg"
rm -f "$A2P"/conf/routing/RT*_routingInfo.cfg
cp "$CASE"/routing/*_routingInfo.cfg "$A2P/conf/routing/"
[ -d "$CASE/conf" ] && for f in "$CASE"/conf/*; do cp "$f" "$A2P/conf/"; done
[ -d "$CASE/conf.append" ] && for f in "$CASE"/conf.append/*; do cat "$f" >> "$A2P/conf/$(basename "$f")"; done
[ -d "$CASE/conf.sed" ] && for f in "$CASE"/conf.sed/*; do sed -i -f "$f" "$A2P/conf/$(basename "$f")"; done
[ -f "$CASE/gmms.append/GmmsConfig.properties" ] && { echo; cat "$CASE/gmms.append/GmmsConfig.properties"; } >> "$A2P/Gmms/GmmsConfig.properties"
echo "config: cm.cfg $(grep -c '^\[' "$A2P/conf/cm.cfg") blocks, routing files: $(ls "$CASE"/routing | tr '\n' ' ')" \
	"$( [ -d "$CASE/conf" ] && echo "conf: $(ls "$CASE/conf" | tr '\n' ' ')")$( [ -d "$CASE/conf.append" ] && echo "conf.append: $(ls "$CASE/conf.append" | tr '\n' ' ')")"

RT_SINCE=$(($(date +%s) * 1000))
export RT_SINCE
if $RESTART; then
	echo "[$(date '+%F %T')] restart: $MODULES"
	"$A2P/bin/a2pStop.sh" $MODULES > "$RT/log_stop.out" 2>&1
	sleep 3
	"$A2P/bin/a2pStart.sh" $MODULES > "$RT/log_start.out" 2>&1
	for i in $(seq 1 90); do
		ss -lnt | grep -q ":16272 " && break
		sleep 2
	done
	ss -lnt | grep -q ":16272 " || { echo "MultiSmppServer1 is not listening on 16272"; tail -20 "$RT/log_start.out"; exit 2; }
	sleep 10   # Core and Client finish starting after the Server port
fi

# the memory leak test (docs/内存泄漏测试设计.md): bin/memwatch.sh samples while the tool sends
SOAK=false
grep -q '^soak.durationSec' "$CASE/case.properties" && ! grep -q '^soak.memwatch=false' "$CASE/case.properties" && SOAK=true
if $SOAK; then
	RT_MEMDIR="$RT/.memwatch"; export RT_MEMDIR
	rm -rf "$RT_MEMDIR"; mkdir -p "$RT_MEMDIR"
	every=$(grep '^soak.sampleSec=' "$CASE/case.properties" | cut -d= -f2); every=${every:-30}
	"$RT/bin/memwatch.sh" "$RT_MEMDIR" "$every" 2 > "$RT_MEMDIR/memwatch.out" 2>&1 &
	MW=$!
fi

echo "[$(date '+%F %T')] run"
# RT_JAVA_OPTS: options of the test tool JVM (e.g. -Xss256k for the many-connection case: a thread per connection)
"$JAVA" ${RT_JAVA_OPTS:-} -cp "$RT/lib/routetest.jar" rt.RouteCase "$CASE" "$RT/env.properties"
rc=$?
if $SOAK; then
	touch "$RT_MEMDIR/stop"; wait $MW
	last=$(ls -1d "$CASE"/result/*/ 2>/dev/null | tail -1)
	if [ -n "$last" ]; then
		mv "$RT_MEMDIR" "$last/mem"
		"$JAVA" -cp "$RT/lib/routetest.jar" rt.MemReport "$last" "$CASE"
		[ $? -eq 0 ] || rc=1
	fi
fi
last=$(ls -1d "$CASE"/result/*/ 2>/dev/null | tail -1)
if [ -n "$last" ]; then
	mkdir -p "$last/config/routing" "$last/config/conf"
	cp "$A2P/conf/cm.cfg" "$last/config/cm.cfg"
	cp "$A2P"/conf/routing/RT*_routingInfo.cfg "$last/config/routing/"
	for d in conf conf.append conf.sed phase2/conf; do
		[ -d "$CASE/$d" ] && for f in "$CASE/$d"/*; do cp "$A2P/conf/$(basename "$f")" "$last/config/conf/" 2>/dev/null; done
	done
	[ -f "$SAVE/GmmsConfig.properties.gmms" ] && cp "$A2P/Gmms/GmmsConfig.properties" "$last/config/GmmsConfig.properties"
	grep -a -h -E "\[ROUTING\]|routing file .* not loaded|Reload routingInfo completed" "$A2P"/log/CoreEngine1.log 2>/dev/null | tail -40 > "$last/config/routing_load.log"
fi
exit $rc
