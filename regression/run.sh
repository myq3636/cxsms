#!/bin/bash
# Long-term regression: run.sh [suite ...]   (no argument: every suite in suites.txt)
#   JAVA_HOME     JDK 17 (default: java on PATH)
#   REDIS_SERVER  redis-server to start when nothing answers on 127.0.0.1:16379 (default: redis-server on PATH)
#   REDIS_CLI     redis-cli (default: redis-cli on PATH)
#   REG_WORK      build and run directory, kept out of git (default: regression/.work)
# Exit code 0 when no check failed. Checks in known-defects.txt print KNOWN and do not fail the run.
ROOT=$(cd "$(dirname "$0")/.." && pwd); REG=$ROOT/regression
W=${REG_WORK:-$REG/.work}; OUT=$W/results; rm -rf "$OUT"; mkdir -p "$OUT"
if [ -n "$JAVA_HOME" ]; then J=$JAVA_HOME/bin; else J=$(dirname "$(readlink -f "$(command -v javac)")"); fi
RS=${REDIS_SERVER:-redis-server}; RC=${REDIS_CLI:-redis-cli}
timeout 5 "$RC" -p 16379 ping >/dev/null 2>&1 || { "$RS" --port 16379 --bind 127.0.0.1 --daemonize yes --dir "$W" --save "" --appendonly no --logfile "$W/redis.log"; sleep 1; }

rm -rf "$W/out" "$W/testall"; mkdir -p "$W/out" "$W/testall"
find "$ROOT/src" -name '*.java' > "$W/srcs.txt"
"$J/javac" --release 17 -nowarn -Xmaxerrs 1000 -encoding UTF-8 -cp "$ROOT/jar/*" -d "$W/out" @"$W/srcs.txt" > "$W/build.log" 2>&1 || { echo "BUILD FAILED, see $W/build.log"; exit 2; }
"$J/javac" -nowarn -encoding UTF-8 -cp "$W/out:$ROOT/jar/*" -d "$W/testall" $(find "$REG/test" -name '*.java') > "$W/build-test.log" 2>&1 || { echo "TEST BUILD FAILED, see $W/build-test.log"; exit 2; }
CP="$W/testall:$W/out:$ROOT/jar/*"

home_for() {  # fresh home for one suite: $1 source, result in $HD
  HD=$W/home/; rm -rf "$HD"; mkdir -p "$HD"; cp -r "$1"/. "$HD"; mkdir -p "$HD/log" "$HD/cdr" "$HD/queue" "$HD/temp"
}
modules() { printf "$1" > "$HD/conf/a2p_modules.conf"; }

run() {  # name class mode
  local MOD=CoreEngine1 EXTRA=
  case "$3" in
    unit) (cd "$ROOT" && timeout 300 "$J/java" -Dknown.defects="$REG/known-defects.txt" -cp "$CP" "$2" > "$OUT/$1.txt" 2>&1) ;;
    *)
      home_for "$REG/home"
      case "$3" in
        it) ;;
        it-server) MOD=MultiSmppServer1 ;;
        it-nosr) home_for "$REG/home-nosr" ;;
        it-mod:*) MOD=${3#it-mod:}; modules '1|CoreEngine1|CoreEngine|x\n1|MultiSmppServer1|MultiSmppServer|x\n1|MultiSmppClient1|MultiSmppClient|x\n' ;;
        it-tls) MOD=SslSmppServer1; modules '1|CoreEngine1|CoreEngine|x\n1|SslSmppServer1|SslSmppServer|x\n1|MultiSmppClient1|MultiSmppClient|x\n'
          rm -f "$W/keystore"; "$J/keytool" -genkeypair -alias a2p -keyalg RSA -keysize 2048 -validity 30 -dname CN=localhost -keystore "$W/keystore" -storepass tomcata2p -keypass tomcata2p -storetype JKS 2>/dev/null
          EXTRA=-Dks=$W/keystore ;;
        it-multicore) modules '1|CoreEngine1|CoreEngine|x\n1|CoreEngine2|CoreEngine|x\n1|MultiSmppServer1|MultiSmppServer|x\n1|MultiSmppClient1|MultiSmppClient|x\n' ;;
        it-multi) modules '1|CoreEngine1|CoreEngine|x\n1|MultiSmppServer1|MultiSmppServer|x\n1|MultiSmppServer2|MultiSmppServer|x\n1|MultiSmppClient1|MultiSmppClient|x\n' ;;
      esac
      (cd "$W" && timeout 400 "$J/java" $EXTRA -Dknown.defects="$REG/known-defects.txt" -Da2p_home="$HD" -Dmodule=$MOD -DNodeID=1 -Dlog4j.configurationFile="$HD/conf/log4j2.xml" -cp "$CP" "$2" > "$OUT/$1.txt" 2>&1)
      mkdir -p "$OUT/logs"; cp "$HD/log/it.log" "$OUT/logs/$1.log" 2>/dev/null ;;
  esac
  local p f k x
  p=$(grep -c '^PASS' "$OUT/$1.txt"); f=$(grep -c '^FAIL' "$OUT/$1.txt"); k=$(grep -c '^KNOWN' "$OUT/$1.txt"); x=$(grep -c '^FIXED' "$OUT/$1.txt")
  local verdict=OK
  if [ "$f" -gt 0 ] || ! grep -q 'ALL CHECKS PASSED' "$OUT/$1.txt"; then verdict=FAILED; BAD=$((BAD+1)); fi
  printf '%-22s %-7s pass=%-4s fail=%-3s known=%-3s fixed=%s\n' "$1" "$verdict" "$p" "$f" "$k" "$x" | tee -a "$OUT/summary.txt"
  P=$((P+p)); F=$((F+f)); K=$((K+k)); X=$((X+x))
}

BAD=0; P=0; F=0; K=0; X=0
while read -r name class mode; do
  [ -z "$name" ] && continue
  if [ $# -gt 0 ]; then case " $* " in *" $name "*) ;; *) continue ;; esac; fi
  run "$name" "$class" "$mode"
done < "$REG/suites.txt"
echo "TOTAL pass=$P fail=$F known=$K fixed=$X suites_failed=$BAD" | tee -a "$OUT/summary.txt"
grep -h '^FAIL\|^FIXED' "$OUT"/*.txt 2>/dev/null | cut -c1-160
[ "$BAD" -eq 0 ]
