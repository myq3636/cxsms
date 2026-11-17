# Generates the message-level fake DR cases FD001..FD005 (regression/routetest/cases/, docs/假DR逻辑重新设计.md 3, 4, 11;
# case.properties version 2, docs/路由服务器测试说明.md 4.2). Run from regression/routetest; rewrites those
# directories (their result/ too). The account templates and the writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

RETRY = "SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\n"
SSID.update({f"rtvf{i}": 9910 + i for i in range(1, 7)})
VENDORS = (vendor("RTVF1", 9911)
    + vendor("RTVF2", 9912, RETRY + "SMSOptionSendDRByOutSubmit:true\n")
    + vendor("RTVF3", 9913, RETRY + "SMSOptionSendDRByOutSubmit:true\nSMSOptionRecPrefixForFakeDR:86170\nSMSOptionDRDelayTimeMinMaxInSec:3,3\n")
    + vendor("RTVF4", 9914, "SMSOptionSubmitRetryCode:2300\nSMSOptionMTRetryPolicy:1M\n")
    + vendor("RTVF5", 9915, "SMSOptionMTRetryPolicy:1M\n")
    + vendor("RTVF6", 9916, RETRY + "SMSOptionSendDRBeforeOutSubmitInDC:true\n"))
SVC = "CustomerServiceType:RTSVCF\n"
CUST = {
  "RTCF1": (9891, "SMSOptionSendDRByInSubmitInCU:true\n"),
  "RTCF2": (9892, "SMSOptionSendFakeDRByInSubmitRecPrefixInCU:8613\nSMSOptionDRDelayTimeMinMaxInSec:2,4\n"),
  "RTCF3": (9893, "SMSOptionSendDRByInSubmitInCU:true\nSMSOptionFakeDRSuccessRatio:0\n"),
  "RTCF4": (9894, "SMSOptionSendDRByInSubmitInCU:true\nSMSOptionFakeDRSuccessRatio:90\nSMSOptionDRSuccessBiasValue:5\n"),
  "RTCF5": (9895, ""),
  "RTCF6": (9896, "SMSOptionFakeDRDisable:true\n"),
  "RTCF7": (9897, "SMSOptionDRSuccessRatio:100\nSMSOptionDRSuccessRatioPrefix:8613\n"),
  "RTCF8": (9898, "SMSOptionDRSuccessRatio:70\n"),
  "RTCF9": (9899, "SMSOptionFakeDRTimeoutSec:10\n"),
  "RTCF10": (9900, "SMSOptionFakeDRTimeoutSec:10\nSMSOptionFakeDRTimeoutStatus:fail\n"),
  "RTCF11": (9901, "SMSOptionSendFakeDR:true\n"),
}
CUSTOMERS = "".join(customer(n, v[0], n.lower(), SVC + v[1]) for n, v in CUST.items())
ROUTING = {"RTSVCF_default": "A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVF1);(Priority=10,PhonePrefix=8615,Relay=RTVF1)"
  ";(Priority=10,PhonePrefix=8616,Relay=RTVF2);(Priority=10,PhonePrefix=8617,Relay=RTVF3);(Priority=10,PhonePrefix=852,Relay=RTVF1)"
  ";(Priority=10,PhonePrefix=85269,Relay=RTVF4);(Priority=10,PhonePrefix=8619,Relay=RTVF5)\n"}
COMMON = dict(refdoc="`features/03_假DR/假DR逻辑重新设计.md` 第 11 节", cm_extra=VENDORS + CUSTOMERS, routing=ROUTING,
              vendors="rtvf1,rtvf2,rtvf3,rtvf4,rtvf5", vendor_cfg={"rtvf4": {"submitStatus": "0x45"}, "rtvf5": {"submitStatus": "0x0B"}})
NOTE = ("所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），"
        "路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。")

def O(cust, **more):
    o = {"customer": cust}
    o.update(more)
    return o

# ------------------------------------------------------------------------------------------------ FD001
case(id="FD001", dir="FD001_way1_in_submit", title="方式 1（收到客户消息就回）：全部 / 按前缀、延迟、之后的真实 DR 和外发失败只记 CDR、前缀外的失败照常回（DEF-C8）、比例 0 和 90±5、路由失败仍是失败 DR",
  refs="FN-01 FN-02 FN-03 FN-04", props={"mt.drWaitMs": "20000"}, notes=NOTE, **COMMON,
  purpose="方式 1 在路由成功后、发给 Client 之前回假 DR，消息照常外发；之后通道的真实 DR、外发失败都只记 CDR。",
  mts=[
    ("OTP","8613800004001","rtvf1","[FAKE-DR] mode=IN_SUBMIT ossid=9891 rssid=9911 status=10000&&[DR-SKIP] ossid=9891 status=10000","DELIVRD","FN-02：全部号码，假 DR 成功；通道真实 DR 被跳过；只有 1 条", None,
     O("rtcf1", drCount="1", cdr="OUT_SUBMIT={id};OUT_DELI_REP={id}@10000")),
    ("OTP","8613800004002","rtvf1","[FAKE-DR] mode=IN_SUBMIT&&[DR-SKIP] ossid=9891 status=10400","DELIVRD","FN-03：真实 DR 是 UNDELIV，客户仍只有那条成功的假 DR，OUT_DELI_REP 记真实的 10400", None,
     O("rtcf1", vendorDr="UNDELIV", drCount="1", cdr="OUT_DELI_REP={id}@10400")),
    ("OTP","8619000004003","rtvf5","[FAKE-DR] mode=IN_SUBMIT","DELIVRD","FN-03：外发失败（0x0B，不重试），客户只有假 DR", None, O("rtcf1", drCount="1")),
    ("OTP","8613800004004","rtvf1","[FAKE-DR] mode=IN_SUBMIT ossid=9892&&delaySec=","DELIVRD","FN-02：按前缀 8613，延迟 2～4 秒", None, O("rtcf2", drCount="1", drDelayMs="1500,7000")),
    ("OTP","85268000405","rtvf1","![FAKE-DR]","UNDELIV","FN-02：852 不在前缀里，不回假 DR，真实 DR 转发", None, O("rtcf2", vendorDr="UNDELIV", drCount="1")),
    ("OTP","85269000406","rtvf4","![FAKE-DR]","!DELIVRD","FN-03、DEF-C8：前缀外的号码，可重试的失败（0x45 → 2300）重试到最后一次（策略只有 1 个点）：客户收到失败 DR", None, O("rtcf2", drCount="1")),
    ("OTP","8613800004007","rtvf1","[FAKE-DR] mode=IN_SUBMIT ossid=9893 rssid=9911 status=10400","UNDELIV","FN-04：SMSOptionFakeDRSuccessRatio=0，假 DR 全是失败（真实 DR 是成功也不转发）", None, O("rtcf3", drCount="1")),
    ("OTP","8613800004008","rtvf1","[FAKE-DR] mode=IN_SUBMIT ossid=9894","DELIVRD","FN-04：比例 90、浮动 5，100 条里成功的约 85%～95%（容差按 10 个百分点，留出抽样误差）", None,
     O("rtcf4", count="100", drRatio="DELIVRD:90,tolerance=10")),
    ("OTP","447700004009","none","no backup route either&&![FAKE-DR]","!DELIVRD","FN-01：路由失败的消息到不了方式 1，仍是失败 DR", None, O("rtcf1", drCount="1")),
  ])

# ------------------------------------------------------------------------------------------------ FD002
case(id="FD002", dir="FD002_way2_out_submit", title="方式 2（通道 submit_sm_resp 成功就回）：通道开、客户拒绝、通道的前缀和延迟、外发失败、之后的真实 DR（含 EXPIRED，DEF-C9）只记 CDR",
  refs="FN-05 FN-06", props={"mt.drWaitMs": "20000"}, notes=NOTE, **COMMON,
  purpose="方式 2 配在通道上：RTVF2 全部号码，RTVF3 只对 86170 开头、延迟 3 秒；客户 RTCF6 配了 SMSOptionFakeDRDisable 不接受。",
  mts=[
    ("OTP","8616000004101","rtvf2","[FAKE-DR] mode=OUT_SUBMIT ossid=9895 rssid=9912&&[DR-SKIP] ossid=9895 status=10400","DELIVRD","FN-05：外发成功回假 DR；真实 UNDELIV 只记 OUT_DELI_REP", None,
     O("rtcf5", vendorDr="UNDELIV", drCount="1", cdr="OUT_DELI_REP={id}@10400")),
    ("OTP","8616000004102","rtvf2","![FAKE-DR]","UNDELIV","FN-05：客户 SMSOptionFakeDRDisable=true，不回假 DR，真实 DR 转发", None, O("rtcf6", vendorDr="UNDELIV", drCount="1")),
    ("OTP","8617000004103","rtvf3","[FAKE-DR] mode=OUT_SUBMIT ossid=9895 rssid=9913&&delaySec=3","DELIVRD","FN-05：通道前缀 86170 内，通道延迟 3 秒（`SMSOptionDRDelayTimeMinMaxInSec:3,3`；只写一个数 `3` 是 0～3 秒随机，原有的写法）", None,
     O("rtcf5", vendorDr="UNDELIV", drCount="1", drDelayMs="2500,9000")),
    ("OTP","8617100004104","rtvf3","![FAKE-DR]","UNDELIV","FN-05：86171 不在通道前缀里，真实 DR 转发", None, O("rtcf5", vendorDr="UNDELIV", drCount="1")),
    ("OTP","8616000004105","rtvf2","![FAKE-DR]","!DELIVRD","FN-05：外发失败（这条 MT 通道回 0x0B，不在重试码里）不回假 DR，回失败 DR", None, O("rtcf5", vendorSubmitStatus="0x0B", drCount="1")),
    ("OTP","8616000004106","rtvf2","[FAKE-DR] mode=OUT_SUBMIT","DELIVRD","FN-06、DEF-C9：之后通道回 EXPIRED，只记 OUT_DELI_REP，不回第二条", None,
     O("rtcf5", vendorDr="EXPIRED", drCount="1", cdr="OUT_DELI_REP={id}")),
  ])

# ------------------------------------------------------------------------------------------------ FD003
case(id="FD003", dir="FD003_way3_rewrite", title="方式 3（真实 DR 不是 DELIVERED 时按比例改成成功）：比例 100 按前缀、UNDELIV / REJECTD 改写、前缀外和 DELIVRD 不改、外发失败不改、比例 70",
  refs="FN-07", props={"mt.drWaitMs": "20000"}, notes=NOTE, **COMMON,
  purpose="客户 RTCF7：SMSOptionDRSuccessRatio=100、只对 8613；RTCF8：比例 70、全部号码。给客户的 DR 和 IN_DELI_REP 改成 DELIVERED，OUT_DELI_REP 记真实状态。",
  mts=[
    ("OTP","8613800004201","rtvf1","[DR-REWRITE] ossid=9897 from=10400 to=10000","DELIVRD","FN-07：UNDELIV 改成 DELIVRD；OUT_DELI_REP 10400、IN_DELI_REP 10000", None,
     O("rtcf7", vendorDr="UNDELIV", drCount="1", cdr="OUT_DELI_REP={id}@10400;IN_DELI_REP={id}@10000")),
    ("OTP","8613800004202","rtvf1","[DR-REWRITE] ossid=9897 from=","DELIVRD","FN-07：REJECTD 也改写", None, O("rtcf7", vendorDr="REJECTD", drCount="1")),
    ("OTP","8615000004203","rtvf1","![DR-REWRITE]","UNDELIV","FN-07：8615 不在 SMSOptionDRSuccessRatioPrefix 里，不改", None, O("rtcf7", vendorDr="UNDELIV", drCount="1")),
    ("OTP","8613800004204","rtvf1","![DR-REWRITE]","DELIVRD","真实 DR 本来就是 DELIVRD，不用改", None, O("rtcf7", drCount="1")),
    ("OTP","8619000004205","rtvf5","![DR-REWRITE]","!DELIVRD","第 2 节：外发失败的 DR 不是真实 DR，不改写", None, O("rtcf7", drCount="1")),
    ("OTP","8613800004206","rtvf1","","DELIVRD","FN-07：比例 70，100 条 UNDELIV 里约 70 条改成 DELIVRD（容差 12 个百分点）；改写的那些有 `[DR-REWRITE]`，没改的没有，所以不按条检查日志", None,
     O("rtcf8", vendorDr="UNDELIV", count="100", drRatio="DELIVRD:70,tolerance=12")),
  ])

# ------------------------------------------------------------------------------------------------ FD004
case(id="FD004", dir="FD004_way4_timeout", title="方式 4（通道 10 秒不回 DR 就自动回）：超时前真实 DR 到、超时回成功、超时后真实 DR 才到只记 CDR、状态 fail",
  refs="FN-08 FN-09", props={"mt.drWaitMs": "25000"}, notes=NOTE, **COMMON,
  purpose="客户 RTCF9：SMSOptionFakeDRTimeoutSec=10（success）；RTCF10：同样 10 秒，状态 fail。",
  mts=[
    ("OTP","8613800004301","rtvf1","![FAKE-DR]","UNDELIV","FN-08：真实 DR 0.5 秒就到，转发，计时取消，10 秒后不再回", None, O("rtcf9", vendorDr="UNDELIV", drCount="1", drDelayMs="0,5000")),
    ("OTP","8613800004302","rtvf1","[FAKE-DR] mode=TIMEOUT ossid=9899","DELIVRD","FN-09：通道不回 DR，约 10 秒后回成功", None, O("rtcf9", vendorDr="none", drCount="1", drDelayMs="9000,16000")),
    ("OTP","8613800004303","rtvf1","[FAKE-DR] mode=TIMEOUT&&[DR-SKIP] outMsgId=","DELIVRD","FN-09：真实 DR 15 秒后才到：10 秒时回了成功，真实 UNDELIV 只记 OUT_DELI_REP", None,
     O("rtcf9", vendorDr="UNDELIV", vendorDrDelayMs="15000", drCount="1", drDelayMs="9000,14000", cdr="OUT_DELI_REP={id}@10400")),
    ("OTP","8613800004304","rtvf1","[FAKE-DR] mode=TIMEOUT ossid=9900 rssid=9911 status=10400","UNDELIV","FN-09：SMSOptionFakeDRTimeoutStatus=fail", None, O("rtcf10", vendorDr="none", drCount="1", drDelayMs="9000,16000")),
  ])

# ------------------------------------------------------------------------------------------------ FD005
case(id="FD005", dir="FD005_long_sms_old_config", title="长短信 3 段 + 方式 1 / 方式 2：每段一条 DR、不重复；旧配置 SMSOptionSendFakeDR、SMSOptionSendDRBeforeOutSubmitInDC 不生效并 WARN",
  refs="FN-13 FN-14", props={"mt.drWaitMs": "20000"}, notes=NOTE, **COMMON,
  purpose="3 段的长短信（UDH），客户收到的 DR 按段算（每段一条）；配了旧配置的客户 / 通道不回假 DR，加载时打 WARN。",
  mts=[
    ("OTP","8613800004401","rtvf1","[FAKE-DR] mode=IN_SUBMIT","DELIVRD","FN-13：方式 1 + 长短信：3 条 DR，不重复", None, O("rtcf1", parts="3", segments="rtvf1:3", drCount="3")),
    ("OTP","8616000004402","rtvf2","[FAKE-DR] mode=OUT_SUBMIT","DELIVRD","FN-13：方式 2 + 长短信：3 条 DR，不重复", None, O("rtcf5", parts="3", segments="rtvf2:3", drCount="3")),
    ("OTP","8613800004403","rtvf1","![FAKE-DR]","UNDELIV","FN-14：客户配了旧的 SMSOptionSendFakeDR=true，不回假 DR，真实 DR 转发", None, O("rtcf11", vendorDr="UNDELIV", drCount="1")),
  ],
  checks=["check.log.1=CoreEngine1|RTCF11: SMSOptionSendFakeDR is no longer used",
          "check.log.2=CoreEngine1|RTVF6: SMSOptionSendDRBeforeOutSubmitInDC is no longer supported"])

exec(_rest.split("# ================================================================================================ write", 1)[1])
