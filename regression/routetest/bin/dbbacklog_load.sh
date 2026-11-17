#!/bin/bash
# Rows for the DB backlog test (docs/DB积压性能测试设计.md): N rows in each of the tables, copies of one existing row
# with new ids (MsgID, InMsgID, OutMsgID 'RTBL...'), waiting: ActionCode 1, NextRetryTime and ExpiryDate 7 days ahead,
# so the MQM tasks scan them but do not send them; OSsID / RSsID 9999 (no such account).
#   dbbacklog_load.sh N [TABLE ...]      default tables: wdq rdq
#   dbbacklog_load.sh --clean [TABLE ...] delete the 'RTBL%' rows
set -u
A2P=${A2P_HOME:-/usr/local/a2p}
G=$A2P/Gmms/GmmsConfig.properties
export MYSQL_PWD=$(grep -E '^DS_gmms_Password=' $G | cut -d= -f2- | tr -d '\r')
U=$(grep -E '^DS_gmms_Login=' $G | cut -d= -f2- | tr -d ' \r')
H=$(grep -E '^DS_gmms_URL=' $G | sed -E 's#.*mysql://([^:/]+).*#\1#')
M="${MYSQL:-/usr/local/mysql/bin/mysql} -h$H -u$U -N gmms"
N=$1; shift
TABLES=${*:-wdq rdq}
for t in $TABLES; do
	if [ "$N" = "--clean" ]; then
		echo "$t: deleted $($M -e "DELETE FROM $t WHERE MsgID LIKE 'RTBL%'; SELECT ROW_COUNT();")"
		continue
	fi
	cols=$($M -e "SHOW COLUMNS FROM $t" | awk '$1!="ID"{print $1}' | tr '\n' ' ')
	sel=""; list=""
	for c in $cols; do
		case $c in
			MsgID) v="CONCAT('RTBL', UUID_SHORT())" ;;
			InMsgID) v="CONCAT('RTBLI', UUID_SHORT())" ;;
			OutMsgID) v="CONCAT('RTBLO', UUID_SHORT())" ;;
			OSsID|RSsID) v="9999" ;;
			ActionCode) v="1" ;;
			DeliveryReport) v="1" ;;
			Split) v="1" ;;
			NextRetryTime|ExpiryDate) v="DATE_ADD(NOW(), INTERVAL 7 DAY)" ;;
			DateIn|TimeMark) v="DATE_SUB(NOW(), INTERVAL FLOOR(RAND()*7200) SECOND)" ;;
			Payload|UDH) v="NULL" ;;
			*) v="$c" ;;
		esac
		sel="$sel${sel:+,}$v"; list="$list${list:+,}$c"
	done
	# the seed: one existing row (or the first row of wdq for an empty table)
	src=$t; [ "$($M -e "SELECT COUNT(*) FROM $t")" = "0" ] && src=wdq
	$M -e "INSERT INTO $t ($list) SELECT $sel FROM $src WHERE MsgID NOT LIKE 'RTBL%' LIMIT 1"
	have=1
	while [ $have -lt $N ]; do
		k=$(( N - have < have ? N - have : have ))
		$M -e "INSERT INTO $t ($list) SELECT $sel FROM $t WHERE MsgID LIKE 'RTBL%' LIMIT $k"
		have=$((have + k))
	done
	echo "$t: $($M -e "SELECT COUNT(*) FROM $t WHERE MsgID LIKE 'RTBL%'") RTBL rows, $($M -e "SELECT COUNT(*) FROM $t") in all"
done
