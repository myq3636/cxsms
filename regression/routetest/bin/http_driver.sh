#!/bin/bash
# RT_DRIVER of bin/http_case.sh: CommonHttpServer1 in Tomcat 9, rt.HttpFlow, Tomcat stopped; its logs into the result.
# RT_HTTP_SERVER=installed: the CommonHttpServer1 installed on this machine instead ($A2P/bin/CommonHttpServer1.sh,
# /usr/local/tomcat), restarted for the case's config, on RT_HTTP_PORT (default 80), and left running.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
A2P=${A2P_HOME:-/usr/local/a2p}
CASE=$1
T9=${TOMCAT9_HOME:-/usr/local/apache-tomcat-9.0.124}
BASE="$RT/.tomcat9"
PORT=$(grep '^http.port=' "$CASE/case.properties" | cut -d= -f2); PORT=${PORT:-8080}
JAVA=${JAVA_BIN:-java}
INSTALLED=false
[ "${RT_HTTP_SERVER:-}" = "installed" ] && { INSTALLED=true; PORT=${RT_HTTP_PORT:-80}; }
if $INSTALLED; then
	echo "[$(date '+%F %T')] CommonHttpServer1 installed ($(readlink -f /usr/local/tomcat)) restarted, port $PORT"
	"$A2P/bin/CommonHttpServer1.sh" stop > /dev/null 2>&1
	sleep 3
	(cd "$A2P/bin" && ./CommonHttpServer1.sh start > /dev/null 2>&1)
	LOGS=/usr/local/tomcat/logs
else
[ -x "$T9/bin/catalina.sh" ] || { echo "no Tomcat 9 in $T9 (TOMCAT9_HOME)"; exit 2; }
# a CATALINA_BASE of our own: Tomcat 9's conf with the port of the case and the Gmms context of the case
rm -rf "$BASE"; mkdir -p "$BASE"/{conf/Catalina/localhost,logs,temp,webapps,work}
cp -p "$T9"/conf/* "$BASE/conf/" 2>/dev/null
sed -i "s/port=\"8080\"/port=\"$PORT\"/; s/<Server port=\"8005\"/<Server port=\"-1\"/" "$BASE/conf/server.xml"
cp "$CASE/tomcat/web.xml" "$BASE/conf/gmms-web.xml"
sed "s#@WEBXML@#$BASE/conf/gmms-web.xml#; s#@BASE@#$BASE#" "$CASE/tomcat/Gmms.xml" > "$BASE/conf/Catalina/localhost/Gmms.xml"
export CATALINA_BASE="$BASE" CATALINA_PID="$BASE/tomcat.pid"
export CATALINA_OPTS="-Da2p_home=$A2P/ -Dmodule=CommonHttpServer1 -Dlog4j.configurationFile=$A2P/conf/log4j2.xml -Xms512m -Xmx2g -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -Djava.net.preferIPv4Stack=true"
echo "[$(date '+%F %T')] Tomcat 9 ($T9) on $PORT"
"$T9/bin/catalina.sh" start > "$BASE/logs/start.out" 2>&1
LOGS="$BASE/logs"
fi
ready=false
for i in $(seq 1 60); do
	code=$(curl -s -m 2 -o /dev/null -w "%{http_code}" -X POST -d "" "http://127.0.0.1:$PORT/Gmms/CommonJson/MTRequest")
	[ "$code" = "200" ] && { ready=true; break; }
	sleep 2
done
rc=2
if $ready; then
	sleep 5   # the servlets' Redis and customer config ready
	# the case's driver (case.properties driver=, default rt.HttpFlow); also given env.properties (the SMPP server, the customer)
	DRIVER=$(grep '^driver=' "$CASE/case.properties" | cut -d= -f2); DRIVER=${DRIVER:-rt.HttpFlow}
	"$JAVA" -cp "$RT/lib/routetest.jar" "$DRIVER" "$CASE" "$PORT" "$RT/env.properties"
	rc=$?
else
	echo "CommonHttpServer1 (Tomcat 9) did not answer on $PORT"; grep -a -E "SEVERE|Exception" "$LOGS"/* | head -20
fi
$INSTALLED || "$T9/bin/catalina.sh" stop 10 -force > /dev/null 2>&1
last=$(ls -1d "$CASE"/result/*/ 2>/dev/null | tail -1)
if [ -n "$last" ]; then
	mkdir -p "$last/tomcat"
	for f in "$LOGS"/*; do [ $(stat -c %Y "$f") -ge $(($(date +%s) - 3600)) ] && cp "$f" "$last/tomcat/"; done 2>/dev/null
	grep -a -h "" "$A2P/log/CommonHttpServer1.log" 2>/dev/null | tail -2000 > "$last/tomcat/CommonHttpServer1.tail.log"
fi
exit $rc
