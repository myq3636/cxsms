#!/bin/bash
# The HTTP servlets of CommonHttpServer through the whole flow (docs/HTTP服务器全流程测试.md):
#   http_case.sh [CASE_DIR]     (default cases/HT001_http_servers)
# bin/run_case.sh applies the case (cm, routing, conf/) and restarts the a2p modules with CommonHttpClient1, then
# bin/http_driver.sh starts CommonHttpServer1 in Tomcat 9 (TOMCAT9_HOME; Tomcat 6 of /usr/local/tomcat does not run on
# Java 17) with the case's web.xml, and rt.HttpFlow plays the customers and the vendor.
set -u
RT=$(cd "$(dirname "$0")/.." && pwd)
CASE=${1:-$RT/cases/HT001_http_servers}
export RT_MODULES=${RT_MODULES:-"CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 CommonHttpClient1 MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2"}
export RT_DRIVER="$RT/bin/http_driver.sh"
exec "$RT/bin/run_case.sh" "$CASE"
