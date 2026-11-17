# Generates the DB backlog performance test PF001 (regression/routetest/cases/, docs/DB积压性能测试设计.md). Run from
# regression/routetest; rewrites the directory (its result/ too). Run it with bin/run_dbbacklog.sh, not run_case.sh alone.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SSID.update({"rtvq1": 9975, "rtvq2": 9976})
SVC = "CustomerServiceType:RTSVCQ\n"
ROUTES = ("A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVQ1);(Priority=10,PhonePrefix=8615,Relay=RTVQ2)\n"
          "A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVQ1)\n")
def T(customer, rcpt, weight, why, **o):
    o.update({"customer": customer, "weight": str(weight)})
    return ("OTP", rcpt, "*", "", "", why, None, o)

case(id="PF001", dir="PF001_db_backlog", title="DB 积压：wdq、rdq 各 50 万行前后，同样 50 TPS 的流量，比较 MT 和 DR 的延迟，找出变慢的原因",
  refs="docs/DB积压性能测试设计.md", refdoc="`docs/DB积压性能测试设计.md`",
  cm_extra=vendor("RTVQ1", 9975) + vendor("RTVQ2", 9976) + customer("RTCQ1", 9971, "rtcq1", SVC) + customer("RTCQ2", 9972, "rtcq2", SVC),
  routing={"RTSVCQ_default": ROUTES},
  vendors="rtvq1,rtvq2", vendor_cfg={"rtvq2": {"submitStatus": "0x45"}},
  props={"customer.user": "rtcq1", "customer.rtcq2.drResp": "false",
         "soak.tps": "50", "soak.durationSec": "360", "soak.warmupSec": "60", "soak.drainSec": "60", "soak.idleSec": "0",
         "soak.memwatch": "false"},
  purpose="由 `bin/run_dbbacklog.sh` 运行两次（A：表里只有现有的数据；B：wdq、rdq 各加 50 万行），每次 50 TPS 6 分钟（前 1 分钟不计入），测试工具记下每条 MT 的三段延迟；同时采 MySQL 的表锁等待、慢查询、正在等锁的语句，B 中间对 3 个 Core 抓线程栈。最后比较 A、B，给出是否变慢和原因的证据。",
  notes="`mt.N` 是流量模板（同 MEM001）：RTCQ2（`customer.rtcq2.drResp=false`）不回 deliver_sm_resp，它的 DR 发送超时进 RDQ、由 MQM 重发，覆盖 RDQ 的写入和删除。不要单独用 run_case.sh 跑（那样只是一次压测，没有对比和灌数据）。",
  mts=[
    T("rtcq1", "86138", 70, "普通 MT + DR（每条 DR 都会 DELETE FROM WDQ）"),
    T("rtcq1", "86150", 10, "RTVQ2 回 0x45，改道到 RTVQ1"),
    T("rtcq1", "86138", 10, "3 段长短信", parts="3"),
    T("rtcq2", "86138", 10, "客户不回 deliver_sm_resp：DR 进 RDQ，MQM 重发"),
  ])

exec(_rest.split("# ================================================================================================ write", 1)[1])
