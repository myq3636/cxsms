# Generates the server cases of the per-bind outgoing rate BT001..BT006 (regression/routetest/cases/,
# docs/SMPP外发bind级限速设计.md 3, 4, 6; case.properties version 2, docs/路由服务器测试说明.md 4.2).
# Run from regression/routetest; rewrites those directories (their result/ too). The account templates and the
# writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

def bvendor(name, ssid, extra, sessions=4):   # SessionNumber is for the channel, split over the 2 Clients: 4 = 2 binds per Client
    return vendor(name, ssid, "SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\n" + extra).replace("SessionNumber=1,", f"SessionNumber={sessions},")
def routing(vendor_name):
    return {"RTCUST": f"CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay={vendor_name})\n"}
NOTE = ("同一个发送方、号码的 MT 按 rssid + 发送方 + 号码分到同一个外发分片，所以一批 MT 由同一个 Client 发，用的是这个 Client 在这个通道上的 bind"
        "（`SessionNumber=4` 是整个通道的会话数，按 Client 平分，每个 Client 2 个；测试工具等满 4 个）。summary.txt 的 `binds:` 一行是每个 bind 收到的条数、任意 1 秒 / 100 ms 内最多几条、第一条到最后一条的时间。"
        "客户每 10 ms 发一条（比限速快），发送的快慢不会掩盖限速。")
REF = "`features/06_bind级限速/SMPP外发bind级限速设计.md` 第 6 节"

case(id="BT001", dir="BT001_per_bind_rate", title="每个 bind 10 TPS：2 个 bind 各自不超过 10（任意 1 秒 ≤ 11、任意 100 ms ≤ 2），总量约 20 TPS，不丢不重",
  refs="BR-02 BR-08 BR-10", refdoc=REF, notes=NOTE,
  cm_extra=bvendor("RTVB1", 9951, "SMSOptionOutgoingThrottlingNumPerBind:10\n"), routing=routing("RTVB1"),
  vendors="rtvb1", vendor_cfg={"rtvb1": {"minLinks": "4"}},
  purpose="通道 `SMSOptionOutgoingThrottlingNumPerBind:10`，每个 Client 2 个 bind。100 条 MT 应在约 5 秒发完，每个 bind 均匀放行、不突发；每条 MT 恰好发一次。",
  mts=[("OTP","8613800007001","rtvb1","","DELIVRD","100 条，每 10 ms 一条", None,
        {"count": "100", "after": "10", "bindMax1s": "11", "bindMax100ms": "2", "bindsUsed": "2", "spanMs": "4000,7000"})])

case(id="BT002", dir="BT002_per_bind_and_account", title="每个 bind 20 + 账号级 30：总量约 30 TPS，每个 bind 不超过 20",
  refs="BR-03", refdoc=REF, notes=NOTE,
  cm_extra=bvendor("RTVB2", 9952, "SMSOptionOutgoingThrottlingNum:30\nSMSOptionOutgoingThrottlingNumPerBind:20\n"), routing=routing("RTVB2"),
  vendors="rtvb2", vendor_cfg={"rtvb2": {"minLinks": "4"}},
  purpose="两层同时生效：账号级（每个 Client 节点）30、每个 bind 20。150 条应在约 5 秒发完（受账号级限制），每个 bind 任意 1 秒 ≤ 21。",
  mts=[("OTP","8613800007101","rtvb2","","DELIVRD","150 条，每 10 ms 一条", None,
        {"count": "150", "after": "10", "bindMax1s": "21", "bindsUsed": "2", "spanMs": "4000,7000"})])

case(id="BT003", dir="BT003_account_only", title="对照：只配账号级 100，两个 bind 都用上，约 2 秒发完",
  refs="BR-01", refdoc=REF, notes=NOTE,
  cm_extra=bvendor("RTVB3", 9953, "SMSOptionOutgoingThrottlingNum:100\n"), routing=routing("RTVB3"),
  vendors="rtvb3", vendor_cfg={"rtvb3": {"minLinks": "4"}},
  purpose="没配 bind 级时和原来一样：账号级 100，200 条（每 5 ms 一条）约 2 秒发完，2 个 bind 都用上。",
  mts=[("OTP","8613800007201","rtvb3","","DELIVRD","200 条，每 5 ms 一条", None,
        {"count": "200", "after": "5", "bindsUsed": "2", "spanMs": "1500,3500"})])

case(id="BT004", dir="BT004_slow_bind_skipped", title="配了 bind 级：一个 bind 回响应慢（窗口 5 很快满），改用同一连接里的另一个 bind，100 条约 2 秒发完",
  refs="BR-09a", refdoc=REF, notes=NOTE + "测试工具让第一个收到 submit_sm 的 bind 晚 3 秒回 submit_sm_resp（`slowFirstLink`）。",
  cm_extra=bvendor("RTVB4", 9954, "SMSOptionOutgoingThrottlingNumPerBind:50\nWindowSize:5\n"), routing=routing("RTVB4"),
  vendors="rtvb4", vendor_cfg={"rtvb4": {"minLinks": "4", "slowFirstLink": "true", "respDelayMs": "3000"}},
  purpose="每个 bind 50，窗口 5。慢的那个 bind 窗口满后，轮到它时改用另一个 bind，不整批推迟：100 条在约 2 秒内发完（客户发 1 秒、另一个 bind 50 TPS）。",
  mts=[("OTP","8613800007301","rtvb4","","DELIVRD","100 条，每 10 ms 一条", None,
        {"count": "100", "after": "10", "bindsUsed": "2", "spanMs": "0,3500"})])

case(id="BT005", dir="BT005_slow_bind_no_per_bind", title="对照：没配 bind 级，同样一个 bind 慢：轮到它时整批推迟 100 ms，发得慢很多（原有行为）",
  refs="BR-09b", refdoc=REF, notes=NOTE + "测试工具让第一个收到 submit_sm 的 bind 晚 3 秒回 submit_sm_resp（`slowFirstLink`）。",
  cm_extra=bvendor("RTVB5", 9955, "WindowSize:5\n"), routing=routing("RTVB5"),
  vendors="rtvb5", vendor_cfg={"rtvb5": {"minLinks": "4", "slowFirstLink": "true", "respDelayMs": "3000"}},
  purpose="和 BT004 一样的慢 bind、窗口 5，但不配 bind 级限速：每次轮到慢 bind 窗口满就整批推迟，100 条要 5 秒以上（BindRateIT 实测 9.3 秒）。和 BT004 对比，说明 4.3 的“找能发的 bind”只在配了 bind 级的通道上生效。",
  mts=[("OTP","8613800007401","rtvb5","","DELIVRD","100 条，每 10 ms 一条", None,
        {"count": "100", "after": "10", "spanMs": "5000,60000", "waitMs": "90000"})])

BASE = open("base/cm.cfg", encoding="utf-8").read()
V6 = bvendor("RTVB6", 9956, "SMSOptionOutgoingThrottlingNumPerBind:20\n", sessions=1)
case(id="BT006", dir="BT006_reload_rate", title="RELOAD_CUSTOMER 把每个 bind 从 20 改成 5：之后每个 bind 任意 1 秒 ≤ 6，不重启",
  refs="BR-06", refdoc=REF, notes=NOTE + "这个通道 `SessionNumber=1`（每个 Client 1 个，这批 MT 只用一个 bind）。reload 会重建这个通道的会话（原有行为），所以第 2 阶段在 reload 后等 10 秒再发。",
  cm_extra=V6, routing=routing("RTVB6"), vendors="rtvb6", vendor_cfg={"rtvb6": {"minLinks": "2"}},
  phase2={"afterMs": "5000", "conf": {"cm.cfg": BASE + V6.replace("SMSOptionOutgoingThrottlingNumPerBind:20", "SMSOptionOutgoingThrottlingNumPerBind:5")}, "reload": "RELOAD_CUSTOMER"},
  props={"phase.2.settleMs": "10000"},
  purpose="第 1 阶段每个 bind 20：20 条约 1 秒；改 cm.cfg 为 5、发 `RELOAD_CUSTOMER`，第 2 阶段 20 条约 4 秒，每个 bind 任意 1 秒 ≤ 6；Client 打 `[BIND-RATE]`。",
  mts=[("OTP","8613800007501","rtvb6","","DELIVRD","改之前：20 TPS", None, {"count": "20", "after": "10", "bindMax1s": "21", "spanMs": "700,2500"})],
  mts2=[("OTP","8613800007502","rtvb6","","DELIVRD","改之后：5 TPS", None, {"count": "20", "after": "10", "bindMax1s": "6", "spanMs": "3000,8000"})],
  checks=["check.log.1=MultiSmppClient1,MultiSmppClient2|[BIND-RATE] ssid=9956 SMSOptionOutgoingThrottlingNumPerBind now 5 TPS per bind"])

exec(_rest.split("# ================================================================================================ write", 1)[1])
