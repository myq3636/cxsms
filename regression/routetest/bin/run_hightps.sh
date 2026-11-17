#!/bin/bash
# The high-TPS test (docs/高TPS一致性和内存测试设计.md):
#   run_hightps.sh CASE_DIR
# 1. tcpdump on lo: the Server ports (customer side) and the vendor port of the tool (Client side), the packets with
#    data (and SYN, FIN, RST), files of 500 MB in $A2P/log/pcap_{case}_{time}/ (kept; CAP_KEEP=false: deleted once read)
# 2. rt.FlowCheck follows the files: decodes the SMPP PDUs, counts them per MT
# 3. bin/run_case.sh: the soak traffic of the case, bin/memwatch.sh and rt.MemReport (memory)
# 4. tcpdump stopped (its "dropped by kernel" count is kept); FlowCheck reads the CDRs and compares, the result goes to
#    the case's result/{time}/flow/. The capture is complete when FlowCheck finds no TCP gap (tcpdump's "dropped by
#    kernel" is only reported: it also counts the packets the filter would drop)
#   FLOWCHECK_EVENTS=file: FlowCheck also writes every decoded PDU there (only for a short trial run)
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
CASE=$(cd "$1" && pwd)
JAVA=${JAVA_BIN:-java}
P="$CASE/case.properties"
prop() { grep "^$1=" "$P" | tail -1 | cut -d= -f2- | tr -d ' '; }
caseid=$(prop case.id)
tps=$(prop soak.tps); dur=$(prop soak.durationSec)
max=$(( tps * dur * 11 / 10 + 10000 ))
sports=$(grep -h "^server.port=" "$RT/env.properties" | cut -d= -f2 | tr -d ' ')
for x in $(grep -h -E '^customer\.[a-z0-9]+\.port=' "$P" | cut -d= -f2 | tr -d ' '); do sports="$sports,$x"; done
vport=$(grep -h "^vendor.port=" "$RT/env.properties" | cut -d= -f2 | tr -d ' ')
RT_SOAK_TAG=T$(date +%H%M); export RT_SOAK_TAG                    # markers {case}T{hhmm}-S{n}: not mixed up with an earlier run
CAP=${CAP_DIR:-$A2P/log/pcap_${caseid}_$(date +%Y%m%d_%H%M%S)}
OUT="$RT/.flowcheck"
rm -rf "$CAP" "$OUT"; mkdir -p "$CAP" "$OUT"
cports=""
for x in ${sports//,/ }; do cports="$cports${cports:+ or }tcp port $x"; done
data="((tcp[tcpflags] & (tcp-syn|tcp-fin|tcp-rst) != 0) or (ip[2:2] - ((ip[0]&0xf)<<2) - ((tcp[12]&0xf0)>>2) != 0))"
since=$(( $(date +%s) * 1000 - 60000 ))

# the DB rows an earlier run of this case left (DRs that failed after its customers had gone: RDQ, resent by MQM)
G=$A2P/Gmms/GmmsConfig.properties
MYSQL=${MYSQL_BIN:-/usr/local/mysql8/bin/mysql}
dbh=$(grep -E '^DS_gmms_URL=' $G | sed -E 's#.*//([^:/]+).*#\1#')
for t in rdq wdq rmq smq; do
	n=$(MYSQL_PWD=$(grep -E '^DS_gmms_Password=' $G | cut -d= -f2-) "$MYSQL" -h"$dbh" -u"$(grep -E '^DS_gmms_Login=' $G | cut -d= -f2-)" -N gmms \
		-e "DELETE FROM $t WHERE TextContent LIKE '$caseid%'; SELECT ROW_COUNT()" 2>&1 | tail -1)
	echo "[$(date '+%F %T')] $t: $n rows of earlier $caseid runs deleted"
done

# two tcpdumps, each with its own buffer: the customer side (cap_cust.pcap*) and the vendor side (cap_vend.pcap*)
echo "[$(date '+%F %T')] capture: lo, customer side ($cports), vendor side (tcp port $vport), packets with data; $CAP; MTs up to $max"
tcpdump -i lo -nn -s 0 -B 1048576 -C 500 -Z root -w "$CAP/cap_cust.pcap" "($cports) and $data" > "$OUT/tcpdump_cust.out" 2>&1 &
TD1=$!
tcpdump -i lo -nn -s 0 -B 1048576 -C 500 -Z root -w "$CAP/cap_vend.pcap" "(tcp port $vport) and $data" > "$OUT/tcpdump_vend.out" 2>&1 &
TD2=$!
sleep 2
for t in $TD1 $TD2; do kill -0 $t 2>/dev/null || { echo "tcpdump did not start:"; cat "$OUT"/tcpdump_*.out; exit 2; }; done
"$JAVA" ${FLOWCHECK_JAVA_OPTS:--Xms4g -Xmx8g} -Dflowcheck.keep=${CAP_KEEP:-true} -cp "$RT/lib/routetest.jar" rt.FlowCheck "$CAP" "$CAP/stop" "$caseid$RT_SOAK_TAG-S" "$max" "$sports" "$vport" \
	"$A2P/cdr,$A2P/cdr/link" "$since" "$OUT" ${FLOWCHECK_EVENTS:-} > "$OUT/flowcheck.out" 2>&1 &
FC=$!

"$RT/bin/run_case.sh" "$CASE"
rc=$?

kill -INT $TD1 $TD2; wait $TD1 $TD2
for s in cust vend; do echo "[$(date '+%F %T')] capture $s stopped: $(grep -E 'captured|dropped' "$OUT/tcpdump_$s.out" | tr '\n' ' ')"; done
touch "$CAP/stop"
wait $FC
fcrc=$?
last=$(ls -1d "$CASE"/result/*/ 2>/dev/null | tail -1)
if [ -n "$last" ]; then
	mkdir -p "$last/flow"
	cp -p "$OUT"/* "$last/flow/" 2>/dev/null
	echo "$CAP" > "$last/flow/pcap_dir.txt"
	echo "[$(date '+%F %T')] flow check: $last/flow/ (pcap files: $CAP)"
	grep -h "^RESULT" "$last/flow/report.txt" 2>/dev/null
fi
[ $fcrc -eq 0 ] || rc=1
exit $rc
