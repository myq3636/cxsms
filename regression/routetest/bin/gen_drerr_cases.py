# Generates the message-level DR error code cases DE001..DE005 (regression/routetest/cases/, docs/DR错误码设计.md 8, 10;
# case.properties version 2, docs/路由服务器测试说明.md 4.2). Run from regression/routetest; rewrites those
# directories (their result/ too). The account templates and the writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

NR = "SMSOptionMTRetryPolicy:1M,1M\n"      # no SMSOptionSubmitRetryCode: a failed submit is final
SSID.update({"rtve1": 9871, "rtve2": 9872, "rtve3": 9873, "rtve4": 9874, "rtve5": 9875})
VENDORS = (vendor("RTVE1", 9871, NR) + vendor("RTVE2", 9872, NR) + vendor("RTVE3", 9873, NR + "SMSOptionDRNotPaidCode:088\n")
           + vendor("RTVE4", 9874, NR) + vendor("RTVE5", 9875, NR))
SVC = "CustomerServiceType:RTSVCE\n"
CUSTOMERS = "".join(customer(f"RTCE{i}", 9880 + i, f"rtce{i}", SVC) for i in range(1, 6)) \
    + customer("RTCE6", 9886, "rtce6", SVC + "SMSOptionSmppMapErrCod4DR:true\n")
MAPPING = """[DEFAULT]
DRErr: (Stat=UNDELIV,Err=077,Code=103)

[RTVE1]
DRNetworkErr: (Type=3,Value=001B,Code=103)
DRErr: (Stat=UNDELIV,Err=001,Code=101);(Stat=UNDELIV,Err=002~009,Code=104);(Err=088,Code=404);(Stat=UNDELIV,Code=200);(Stat=REJECTD,Code=202)

[RTVE3]
DRErr: (Err=088,Code=101)

[RTVE4]
SubmitErr: (Status=0x0B,Code=102);(Status=*,Code=601)

[RTVE5]
SubmitErr: (Status=*,Code=601)

[RTCE1]
DRErrDefault: passthrough

[RTCE2]
CustomerErr: (Code=101,Err=011);(Code=100~199,Err=033)

[RTCE3]
DRErrDefault: vendor

[RTCE4]
DRErrDefault: 000

[RTCE5]
DRErrDefault: passthrough
DRErrTLV: true
"""
ROUTES = "A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVE1);(Priority=10,PhonePrefix=8615,Relay=RTVE2);(Priority=10,PhonePrefix=8616,Relay=RTVE3);(Priority=10,PhonePrefix=8617,Relay=RTVE4);(Priority=10,PhonePrefix=8618,Relay=RTVE5)\n"
ROUTING = {"RTSVCE_default": ROUTES, "RTCUST": "CCBSSID:9801\n" + ROUTES}
COMMON = dict(refdoc="`features/02_DR错误码/DR错误码设计.md` 第 8 节", cm_extra=VENDORS + CUSTOMERS, conf={"drErrorMapping.cfg": MAPPING}, routing=ROUTING,
              vendors="rtve1,rtve2,rtve3,rtve4,rtve5", vendor_cfg={"rtve4": {"submitStatus": "0x0B"}, "rtve5": {"submitStatus": "0x45"}})
def V(stat, err, tlv=None, **more):
    o = {"vendorDr": stat, "vendorDrErr": err}
    if tlv: o["vendorTlv"] = tlv
    o.update(more)
    return o

# ------------------------------------------------------------------------------------------------ DE001
case(id="DE001", dir="DE001_vendor_err_to_code", title="通道 DR → 内部错误码：stat+err、范围、TLV 优先、只按状态、DELIVRD 不看 err、只按 err、[DEFAULT] 块、对不上、欠费码优先",
  refs="DE-01 DE-02 DE-03 DE-04 DE-11 DE-13",
  purpose="通道回不同的 stat / err / network_error_code，Core 按 `conf/drErrorMapping.cfg`（10.4 的顺序）得到内部码，客户 RTCE1 配了 `DRErrDefault: passthrough`，收到的 `err:` 就是内部码；`stat:` 按内部状态生成（和原来一样，例如 10800 是 DELETED，设计稿 5.2）。",
  props={"customer.user": "rtce1"}, **COMMON,
  mts=[
    ("OTP","8613800003001","rtve1","[DR-ERR] rssid=9871 vendor=stat=UNDELIV;err=001&&code=101 status=10402","UNDELIV","DE-01：stat+err 完全相等 → 101（10402）", None, V("UNDELIV","001", drErr="101")),
    ("OTP","8613800003002","rtve1","[DR-ERR] rssid=9871 vendor=stat=UNDELIV;err=5&&code=104 status=10405","UNDELIV","10.3、10.4 第 3 步：err 5 按数值落在 002~009 → 104（10405）", None, V("UNDELIV","5", drErr="104")),
    ("OTP","8613800003003","rtve1","nec=3:001B&&code=103 status=10402","UNDELIV","DE-02：带 network_error_code 3:001B，TLV 规则先于 stat+err（err 001 本来是 101）→ 103", None, V("UNDELIV","001","03001B", drErr="103")),
    ("OTP","8613800003004","rtve1","code=202 status=10500","REJECTD","只写 Stat 的规则：REJECTD 其他 err → 202（10500），客户 stat:REJECTD", None, V("REJECTD","050", drErr="202")),
    ("OTP","8613800003005","rtve1","code=200 status=10400","UNDELIV","只写 Stat：UNDELIV 其他 err → 200（10400）", None, V("UNDELIV","099", drErr="200")),
    ("OTP","8613800003006","rtve1","code=000 status=10000","DELIVRD","10.4：DELIVRD 不看 err（err 005 也是 000）", None, V("DELIVRD","005", drErr="000")),
    ("OTP","8613800003007","rtve1","code=404 status=10800","DELETED","DE-04、10.4 第 4 步（没写 Stat，err 相等）先于第 6 步（只写 Stat）：088 → 404 欠费（REJECTED_BYNC 10800）；客户回执的 stat 按内部状态，10800 是 DELETED（原有行为，5.2）", None, V("UNDELIV","088", drErr="404")),
    ("OTP","8615000003008","rtve2","[DR-ERR] rssid=9872 vendor=stat=UNDELIV;err=077&&code=103 status=10402","UNDELIV","RTVE2 没有自己的块，用 [DEFAULT] 块：077 → 103", None, V("UNDELIV","077", drErr="103")),
    ("OTP","8615000003009","rtve2","[DR-ERR-UNMAPPED] rssid=9872 channel=RTVE2 vendor=stat=UNDELIV;err=055&&code=200 status=10400","UNDELIV","DE-03：对不上：状态不变（10400），码取这个状态的码 200，WARN [DR-ERR-UNMAPPED]", None, V("UNDELIV","055", drErr="200")),
    ("OTP","8616000003010","rtve3","(SMSOptionDRNotPaidCode)&&code=404","DELETED","DE-13：RTVE3 的 SMSOptionDRNotPaidCode:088 先于它块里的 (Err=088,Code=101)：欠费 404，客户 stat:DELETED", None, V("UNDELIV","088", drErr="404")),
  ],
  notes="通道的 DR 由每条 MT 的 `vendorDr`、`vendorDrErr`、`vendorTlv` 指定（同一个通道不同 MT 回不同的 DR）。客户收到的 err 用 `drErr` 检查。服务器上没有 `conf/drErrorCode.cfg`，用内置码表（设计稿 10.1 加 209）。")

# ------------------------------------------------------------------------------------------------ DE002
case(id="DE002", dir="DE002_customer_err", title="内部码 → 客户：没有客户块（legacy）、passthrough、CustomerErr 精确和范围、vendor、000、DRErrTLV、旧配置 SMSOptionSmppMapErrCod4DR",
  refs="DE-05 DE-06 DE-13",
  purpose="通道 RTVE1 回 `stat:UNDELIV err:5`（内部码 104，10405；M4 是 `err:001` → 101），不同客户按自己的块（10.5）收到不同的 err。",
  **COMMON,
  mts=[
    ("OTP","8613800003101","rtve1","code=104 status=10405","UNDELIV","没有客户块：legacy 表，10405 → 016", None, V("UNDELIV","5", customer="rtcust", drErr="016")),
    ("OTP","8613800003102","rtve1","code=104","UNDELIV","passthrough：内部码 104；没配 DRErrTLV，没有 TLV", None, V("UNDELIV","5", customer="rtce1", drErr="104", drTlv="none")),
    ("OTP","8613800003103","rtve1","code=104","UNDELIV","CustomerErr 范围 100~199 → 033", None, V("UNDELIV","5", customer="rtce2", drErr="033")),
    ("OTP","8613800003104","rtve1","code=101","UNDELIV","CustomerErr 精确的 101 先于范围 → 011", None, V("UNDELIV","001", customer="rtce2", drErr="011")),
    ("OTP","8613800003105","rtve1","code=104","UNDELIV","vendor：通道原始的 err 5，补 0 到 3 位 → 005", None, V("UNDELIV","5", customer="rtce3", drErr="005")),
    ("OTP","8613800003106","rtve1","code=104","UNDELIV","000：永远 000", None, V("UNDELIV","5", customer="rtce4", drErr="000")),
    ("OTP","8613800003107","rtve1","code=104","UNDELIV","DE-06：passthrough + DRErrTLV：err:104，另带 network_error_code 类型 3、值 104（0x0068）", None, V("UNDELIV","5", customer="rtce5", drErr="104", drTlv="030068")),
    ("OTP","8613800003108","rtve1","code=104","UNDELIV","DE-13：配了旧的 SMSOptionSmppMapErrCod4DR 的客户：不再生效，按 legacy → 016", None, V("UNDELIV","5", customer="rtce6", drErr="016")),
  ],
  checks=["check.log.1=CoreEngine1|RTCE6: SMSOptionSmppMapErrCod4DR is no longer used"])

# ------------------------------------------------------------------------------------------------ DE003
case(id="DE003", dir="DE003_submit_err", title="submit 失败的码：SubmitErr 精确、Status=*；客户的失败 DR 带这个码",
  refs="DE-08",
  purpose="通道 submit_sm_resp 失败（这些通道没有 SMSOptionSubmitRetryCode，失败就回失败 DR），Core 按 `SubmitErr` 得到内部码，客户的失败 DR 带它。",
  **COMMON,
  mts=[
    ("OTP","8617000003201","rtve4","rssid=9874 vendor=submit=&&code=102 (submit)","!DELIVRD","DE-08：0x0B → 102，passthrough 客户收到 err:102", None, {"customer": "rtce1", "drErr": "102"}),
    ("OTP","8618000003202","rtve5","rssid=9875 vendor=submit=&&code=601 (submit)","!DELIVRD","0x45 → Status=* → 601", None, {"customer": "rtce1", "drErr": "601"}),
    ("OTP","8617000003203","rtve4","code=102 (submit)","!DELIVRD","没有客户块（legacy）：102 的内部状态 10402 → 012", None, {"customer": "rtcust", "drErr": "012"}),
  ])

# ------------------------------------------------------------------------------------------------ DE004
case(id="DE004", dir="DE004_reload", title="RELOAD_DRERROR：改了对照表，下一条 DR 按新表",
  refs="DE-12",
  purpose="第 1 阶段 `err:001` → 101；第 2 阶段把 RTVE1 的规则改成 001 → 103，发 `RELOAD_DRERROR`（不重启），再发一条同样的 DR → 103。",
  phase2={"afterMs": "10000", "conf": {"drErrorMapping.cfg": MAPPING.replace("(Stat=UNDELIV,Err=001,Code=101)", "(Stat=UNDELIV,Err=001,Code=103)")}, "reload": "RELOAD_DRERROR"},
  props={"customer.user": "rtce1"}, **COMMON,
  mts=[("OTP","8613800003301","rtve1","code=101 status=10402","UNDELIV","改表前：101", None, V("UNDELIV","001", drErr="101"))],
  mts2=[("OTP","8613800003302","rtve1","code=103 status=10402","UNDELIV","RELOAD_DRERROR 后：103", None, V("UNDELIV","001", drErr="103"))])

# ------------------------------------------------------------------------------------------------ DE005
case(id="DE005", dir="DE005_reload_bad_file", title="RELOAD_DRERROR：对照表写错（码不在码表里）整次拒绝，继续用原来的表",
  refs="DE-12",
  purpose="第 2 阶段把规则改成 `Code=777`（码表里没有），发 `RELOAD_DRERROR`：加载被拒绝（ERROR），DR 仍按原来的表 → 101。",
  phase2={"afterMs": "10000", "conf": {"drErrorMapping.cfg": MAPPING.replace("(Stat=UNDELIV,Err=001,Code=101)", "(Stat=UNDELIV,Err=001,Code=777)")}, "reload": "RELOAD_DRERROR"},
  props={"customer.user": "rtce1"}, **COMMON,
  mts=[("OTP","8613800003401","rtve1","code=101","UNDELIV","改表前：101", None, V("UNDELIV","001", drErr="101"))],
  mts2=[("OTP","8613800003402","rtve1","code=101 status=10402","UNDELIV","新文件被拒绝，仍是 101", None, V("UNDELIV","001", drErr="101"))],
  checks=["check.log.1=CoreEngine1|[DR-ERR] DR error code files not loaded, the tables in use are kept|777",
          "check.log.2=MultiSmppServer1|[DR-ERR] DR error code files not loaded, the tables in use are kept|777"])

exec(_rest.split("# ================================================================================================ write", 1)[1])
