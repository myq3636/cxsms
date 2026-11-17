# Generates the memory leak test MEM001 (regression/routetest/cases/, docs/内存泄漏测试设计.md). Run from
# regression/routetest; rewrites the directory (its result/ too). The account templates and the writer are the ones
# of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SSID.update({"rtvs1": 9991, "rtvs2": 9992, "rtvs3": 9993})
SVC = "CustomerServiceType:RTSVCS\n"
ROUTES = ("A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVS1);(Priority=10,PhonePrefix=8615,Relay=RTVS2);(Priority=10,PhonePrefix=8616,Relay=RTVS3)\n"
          "A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVS1)\n")
def T(customer, rcpt, weight, why, **o):
    o.update({"customer": customer, "weight": str(weight)})
    return ("OTP", rcpt, "*", "", "", why, None, o)

case(id="MEM001", dir="MEM001_soak", title="内存泄漏：100 TPS 混合流量 55 分钟（含 5 分钟预热）+ 客户重连 + reload，之后空闲 10 分钟；看各模块 Full GC 后的堆、线程、句柄、Metaspace 和 Redis 无过期 key",
  refs="docs/内存泄漏测试设计.md", refdoc="`docs/内存泄漏测试设计.md`",
  cm_extra=vendor("RTVS1", 9991) + vendor("RTVS2", 9992) + vendor("RTVS3", 9993)
           + customer("RTCS1", 9995, "rtcs1", SVC) + customer("RTCS2", 9996, "rtcs2", SVC + "SMSOptionFakeDRTimeoutSec:5\n")
           + customer("RTCS3", 9997, "rtcs3", SVC),
  routing={"RTSVCS_default": ROUTES},
  vendors="rtvs1,rtvs2,rtvs3", vendor_cfg={"rtvs2": {"submitStatus": "0x45"}, "rtvs3": {"dr": "none"}},
  props={"customer.user": "rtcs1", "customer.rtcs3.port": "16273",
         "soak.tps": "100", "soak.durationSec": "3300", "soak.warmupSec": "300", "soak.drainSec": "60", "soak.idleSec": "600",
         "soak.sampleSec": "30", "soak.reconnectSec": "300", "soak.reloadSec": "600",
         "soak.reloadCmds": "RELOAD_ROUTING:-f,RELOAD_CUSTOMER,RELOAD_DRERROR,RELOAD_GMMS_CONFIG",
         "mem.maxSlopeMBperHour": "30", "mem.growMB": "20", "mem.growPct": "10", "mem.retainMB": "30", "mem.retainPct": "15",
         "mem.threadGrowth": "20", "mem.fdGrowth": "50", "mem.metaGrowthMB": "10", "mem.redisNoTtlKeys": "1000", "mem.redisNoTtlPctOfSent": "1"},
  purpose="按设计稿第 2 节：测试工具以 100 TPS 持续发 6 种 MT（下表的权重），每 5 分钟轮流断开重连一个客户，每 10 分钟轮流发一个 reload 命令；`bin/memwatch.sh` 每 30 秒采样一次，隔一次先 Full GC；最后 `rt.MemReport` 判断。",
  notes="`mt.N` 在这里是流量模板：号码是模板的前缀后面补随机数字到 13 位；`weight` 是比例。预期通道写 `*`（不按条检查），客户 DR 只计数。整个用例约 67 分钟（重启 1 分钟 + 流量 55 分钟 + 等 DR 1 分钟 + 空闲 10 分钟）。阈值 `mem.*` 见设计稿 2.4。跑的时候服务器上不能同时跑别的用例或回归。",
  mts=[
    T("rtcs1", "86138", 50, "普通 MT + DR（MultiSmppServer1）"),
    T("rtcs3", "86138", 15, "普通 MT + DR（MultiSmppServer2）"),
    T("rtcs1", "86150", 10, "RTVS2 回 0x45，改道到备份 RTVS1"),
    T("rtcs1", "86138", 10, "3 段长短信", parts="3"),
    T("rtcs1", "44770", 5, "没有路由：路由失败，失败 DR"),
    T("rtcs2", "86160", 10, "RTVS3 不回 DR，客户 RTCS2 的假 DR 方式 4（5 秒）回成功"),
  ])

exec(_rest.split("# ================================================================================================ write", 1)[1])
