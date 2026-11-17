# Generates the server cases of the long MT LS001..LS002 (regression/routetest/cases/, docs/长短信处理流程说明.md,
# docs/长短信CDR与DR测试报告.md; case.properties version 2, docs/路由服务器测试说明.md 4.2). Run from regression/routetest;
# rewrites those directories (their result/ too). The account templates and the writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SPLIT = "MsgSplitLen:140\nSMSOptionAsciiMsgSplitLen:160\n"
SSID.update({"rtvl1": 9981, "rtvl2": 9982, "rtvl3": 9983})
VENDORS = (vendor("RTVL1", 9981)
    + vendor("RTVL2", 9982).replace(SPLIT, "MsgSplitLen:100\nSMSOptionAsciiMsgSplitLen:100\n")
    + vendor("RTVL3", 9983).replace("ChlUdhConcatenated:yes", "ChlUdhConcatenated:no"))
ROUTING = {"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVL1);(Priority=10,PhonePrefix=8615,Relay=RTVL2);(Priority=10,PhonePrefix=8616,Relay=RTVL3)\n"}
COMMON = dict(refdoc="`features/08_长短信/长短信处理流程说明.md` 第 8 节、`长短信CDR与DR测试报告.md`", cm_extra=VENDORS, routing=ROUTING, vendors="rtvl1,rtvl2,rtvl3")
NOTE = ("客户 RTCUST 把一条 368 个 ASCII 字符的消息分成 3 段（每段最多 134 字节）发：默认用 UDH（esm_class 0x40），`sar` 时用 SAR 可选参数。"
        "通道 RTVL1 按 ASCII 160 拆（带 UDH 每段 153 字符，3 段），RTVL2 按 100 拆（4 段），RTVL3 用 SAR 可选参数（`ChlUdhConcatenated:no`）。"
        "测试工具扮演通道，按 UDH 或 SAR 参数收段、拼起来和原文比较；每段外发都回一个成功的 DR。")
FULL = "IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:3,OUT_DELI_REP:3,IN_DELI_REP:3"

def M(rcpt, path, logs, dr, why, **o):
    return ("OTP", rcpt, path, logs, dr, why, None, o)

case(id="LS001", dir="LS001_long_mt_flow", title="长短信：UDH / SAR 两种发法、通道不同的拆法（3 段、4 段、SAR）、每种 CDR 的条数、客户每段一条 DR",
  refs="流程说明第 8 节；测试报告第 2 节 A、B、C；第 3 节的修复", notes=NOTE, **COMMON,
  purpose="一条长短信从 Server 到 DR 回到客户：Core 按段写 IN_SUBMIT_PARTIAL、IN_SUBMIT，拼好后按通道重新拆分，每条外发一条 OUT_SUBMIT、每个通道 DR 一条 OUT_DELI_REP；通道拆过的只有第 1 条的 DR 回客户，按客户的 3 段拆成 3 条，每段的 InMsgID 各一条（第 3 节修复前只有第 1 段）。",
  mts=[
    M("8613800009001","rtvl1","[MT-ROUTE] ossid=9801 rssid=9981","DELIVRD","3 段 UDH，通道按 160 拆回 3 段", parts="3", segments="rtvl1:3", drCount="3", drEachSeg="true", cdrCount=FULL),
    M("8613800009002","rtvl1","[MT-ROUTE] ossid=9801 rssid=9981","DELIVRD","3 段 SAR 可选参数（Server 解析出参考号、总段数、段号），结果同上", parts="3", sar="true", segments="rtvl1:3", drCount="3", drEachSeg="true", cdrCount=FULL),
    M("8615000009003","rtvl2","[MT-ROUTE] ossid=9801 rssid=9982","DELIVRD","测试报告 B：通道按 100 拆成 4 段（每段 100 字节含 6 字节 UDH：94、94、94、86）：OUT_SUBMIT、OUT_DELI_REP 各 4 条，客户仍是 3 条 DR", parts="3",
      segments="rtvl2:4", segLens="94,94,94,86", drCount="3", drEachSeg="true", cdrCount="IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:4,OUT_DELI_REP:4,IN_DELI_REP:3"),
    M("8616000009004","rtvl3","[MT-ROUTE] ossid=9801 rssid=9983","DELIVRD","通道 ChlUdhConcatenated:no：外发用 SAR 可选参数", parts="3", segments="rtvl3:3", drCount="3", drEachSeg="true", cdrCount=FULL),
    M("8613800009005","rtvl1","[MT-ROUTE] ossid=9801 rssid=9981","DELIVRD","测试报告 C（对照）：单条短信", drCount="1", cdrCount="IN_SUBMIT_PARTIAL:0,IN_SUBMIT:1,OUT_SUBMIT:1,OUT_DELI_REP:1,IN_DELI_REP:1"),
  ])

case(id="LS002", dir="LS002_long_mt_expiry", title="长短信没集齐：CsmIntegrityCacheTimeout 到期后过期扫描给已收到的每段回失败 DR，不外发，合并缓冲删掉",
  refs="流程说明第 4 节（没集齐就过期）；配置项说明 CsmIntegrityCacheTimeout、CoreCsmExpirySweep", notes=NOTE + "用例把 `CsmIntegrityCacheTimeout` 改成 30 秒、过期扫描间隔 5 秒（追加到 GmmsConfig，结束后恢复）。",
  gmms_append={"GmmsConfig.properties": "CsmIntegrityCacheTimeout=30000\nCoreCsmExpirySweep=true\nCoreCsmExpiryIntervalSec=5\n"},
  props={"mt.drWaitMs": "120000"}, **COMMON,
  purpose="3 段的长短信只发第 1、3 段：30 秒后过期扫描给这 2 段各回一条失败 DR（客户支持过期 DR 时 EXPIRED，否则 UNDELIV），通道收不到；`csm:` 合并缓冲删掉。同时发一条完整的 3 段作对照，确认 30 秒的设置不影响正常的长短信。",
  mts=[
    M("8613800009101","none","","!DELIVRD","只发第 1、3 段：过期，2 条失败 DR", parts="3", dropSeg="2", drCount="2", drEachSeg="true", waitMs="120000"),
    M("8613800009102","rtvl1","","DELIVRD","对照：完整的 3 段照常外发", parts="3", segments="rtvl1:3", drCount="3", drEachSeg="true", waitMs="120000"),
  ],
  checks=["check.cmd.1=$RTREDIS --scan --pattern 'csm:9801:OTP:8613800009101:*' | wc -l => =0",
          "check.cmd.2=$RTREDIS --scan --pattern 'csm:ids:9801:OTP:8613800009101:*' | wc -l => =0"])

exec(_rest.split("# ================================================================================================ write", 1)[1])
