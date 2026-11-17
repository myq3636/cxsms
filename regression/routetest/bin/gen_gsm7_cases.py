# Generates the server cases of the GSM 7-bit length and the 8-bit split GL001..GL002 (regression/routetest/cases/,
# docs/GSM7bit消息长度计算优化设计.md 5, 7, 8, 9; case.properties version 2, docs/路由服务器测试说明.md 4.2).
# Run from regression/routetest; rewrites those directories (their result/ too). The account templates and the
# writer are the ones of gen_routing_cases.py.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SPLIT = "MsgSplitLen:140\nSMSOptionAsciiMsgSplitLen:160\n"
def gvendor(name, ssid, extra="", split=SPLIT):
    return vendor(name, ssid, "SMSOptionMTRetryPolicy:1M,1M\n" + extra).replace(SPLIT, split)
SSID.update({f"rtvg{i}": 9970 + i for i in range(1, 7)})
VENDORS = (gvendor("RTVG1", 9971, "SMSOptionSupportOutgoingGSM7bit:1\n")
    + gvendor("RTVG2", 9972, "SMSOptionSupportOutgoingGSM7bit:2\n")
    + gvendor("RTVG3", 9973, split="")                                   # default 0, no MsgSplitLen
    + gvendor("RTVG4", 9974, "SMSOptionSupportOutgoingGSM7bit:1\n", split="MsgSplitLen:140\n")   # as VENDOR65 of Gsm7LengthIT
    + gvendor("RTVG5", 9975, "SMSOptionSupportOutgoingGSM7bit:1\n")     # MsgSplitLen 140 + SMSOptionAsciiMsgSplitLen 160, as VENDOR66
    + gvendor("RTVG6", 9976, "SMSOptionForceTrans2ASCII:true\n", split=""))
SVC = "CustomerServiceType:RTSVCG\n"
CUSTOMERS = (customer("RTCG1", 9961, "rtcg1", SVC + "SMSOptionSupportIncomingGSM7bit:1\n")
    + customer("RTCG2", 9962, "rtcg2", SVC + "SMSOptionSupportIncomingGSM7bit:2\n")
    + customer("RTCG3", 9963, "rtcg3", SVC + "SMSOptionSupportIncomingGSM7bit:1\nNeedCheckMsgSize:false\n"))
ROUTES = ("A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVG1);(Priority=10,PhonePrefix=8615,Relay=RTVG2);(Priority=10,PhonePrefix=8616,Relay=RTVG3)"
          ";(Priority=10,PhonePrefix=8617,Relay=RTVG4);(Priority=10,PhonePrefix=8618,Relay=RTVG5);(Priority=10,PhonePrefix=8619,Relay=RTVG6)\n")
COMMON = dict(refdoc="`features/07_GSM7与8bit长度/GSM7bit消息长度计算优化设计.md` 第 5、7、8、9 节", cm_extra=VENDORS + CUSTOMERS,
              routing={"RTSVCG_default": ROUTES, "RTCUST": "CCBSSID:9801\n" + ROUTES}, vendors="rtvg1,rtvg2,rtvg3,rtvg4,rtvg5,rtvg6")
NOTE = ("`segLens` 是通道收到的每段去掉 UDH 后的字节数：`MsgSplitLen:140` 是每段 short_message 的总长度（含 6 字节 UDH），所以拆出来是 134 + 26（设计稿第 9 节的 `[140, 20]` 是回归在 Core 里按拆分前的内容统计的）。"
        "客户 RTCG1 是入向模式 1（GSM 7-bit 每字符一字节），RTCG2 模式 2（压缩），RTCG3 模式 1 并且 `NeedCheckMsgSize:false`，RTCUST 默认（按 ASCII 解析，用 data_coding 3 发 ISO）。"
        "通道 RTVG1 外发 7-bit 模式 1、RTVG2 模式 2、RTVG3 默认 0 没配 MsgSplitLen、RTVG4 外发 7-bit 模式 1 只配 `MsgSplitLen:140`、RTVG5 外发 7-bit 模式 1 配 140 加 `SMSOptionAsciiMsgSplitLen:160`（和回归 Gsm7LengthIT 的 VENDOR65、VENDOR66 一样）、RTVG6 默认 0 加 `SMSOptionForceTrans2ASCII`。"
        "内容用 `a` 补到 `length` 个 GSM 字符（扩展字符 `{` 算 2 个；ISO 按字节），最后是用例标记。通道收到的每段 data_coding、字节数和按编码解出的文本在 evidence 的 `vendor got` 一行。")
CAFE, ETE, DELTA = "Café", "Café été", "Δelta"

def M(sender_rcpt, path, logs, dr, why, **o):
    return ("OTP", sender_rcpt, path, logs, dr, why, None, o)

case(id="GL001", dir="GL001_inbound_length", title="Core 入向长度检查：GSM 7-bit 按 GSM 字符数（≤160），ISO 按字节（≤140），NeedCheckMsgSize:false 不检查，模式 1 里 >0x7F 的字节打 WARN",
  refs="G7-01 G7-02 G7-03 G7-04 G7-05 G7-05b G7-05c G7-05d G7-06 G7-11 G7-12", notes=NOTE, **COMMON,
  purpose="真实客户经 Server、Core 到通道的完整链路（回归里是在 Core 这一层构造消息）。通过的 MT 都发往 RTVG1（7-bit 模式 1），通道收到 1 段、data_coding 0、GSM 编码、文本和原文一样；超长的 MT 由 Core 拒绝（OUT_SUBMIT 状态 2250），客户收到失败 DR。",
  mts=[
    M("8613800008001","rtvg1","","DELIVRD","G7-01：160 个 ASCII 字符，模式 1", customer="rtcg1", enc="gsm7", text="hello", length="160", segLens="160", vendorDc="0", vendorEnc="gsm7", vendorText="same"),
    M("8613800008002","rtvg1","","DELIVRD","G7-02、G7-12：带 é，160 个 GSM 字符，模式 1：通过（改前 2250）；IN_SUBMIT、发往 7-bit 通道的 OUT_SUBMIT 字符集都是 ASCII", customer="rtcg1", enc="gsm7", text=ETE, length="160",
      segLens="160", vendorDc="0", vendorEnc="gsm7", vendorText="same", cdr='IN_SUBMIT={id}~"ASCII";OUT_SUBMIT={id}~"ASCII"'),
    M("8613800008003","rtvg1","","DELIVRD","G7-03：带 é，160 个字符，模式 2（压缩，140 字节）", customer="rtcg2", enc="gsm7packed", text=ETE, length="160", segLens="160", vendorDc="0", vendorEnc="gsm7", vendorText="same"),
    M("8613800008004","rtvg1","","DELIVRD","G7-04：带 Δ（UCS2 字符），160 个字符，模式 1：通过（改前 2250）", customer="rtcg1", enc="gsm7", text=DELTA, length="160", segLens="160", vendorDc="0", vendorEnc="gsm7", vendorText="same"),
    M("8613800008005","none","","!DELIVRD","G7-05：带 é，161 个字符：2250", customer="rtcg1", enc="gsm7", text=CAFE, length="161", cdr="OUT_SUBMIT={id}@2250"),
    M("8613800008006","rtvg1","","DELIVRD","G7-06a：é 加 5 个 `{`（扩展字符算 2 个），一共 160 个 GSM 字符：通过", customer="rtcg1", enc="gsm7", text=CAFE + " {{{{{", length="160", vendorEnc="gsm7", vendorText="same"),
    M("8613800008007","none","","!DELIVRD","G7-06b：同上，161 个 GSM 字符：2250", customer="rtcg1", enc="gsm7", text=CAFE + " {{{{{", length="161", cdr="OUT_SUBMIT={id}@2250"),
    M("8613800008008","none","","!DELIVRD","G7-05b：data_coding 3，160 字节 ISO：2250（和改前一样）", customer="rtcust", enc="latin1", text=CAFE, length="160", cdr="OUT_SUBMIT={id}@2250"),
    M("8613800008009","rtvg1","","DELIVRD","G7-05c：data_coding 3，140 字节 ISO：通过；发往 7-bit 通道仍是 ISO（不是 7-bit），data_coding 3", customer="rtcust", enc="latin1", text=CAFE, length="140", segLens="140", vendorDc="3", vendorText="same"),
    M("8613800008010","rtvg1","","DELIVRD","G7-11：NeedCheckMsgSize:false，161 个字符也不检查", customer="rtcg3", enc="gsm7", text=CAFE, length="161", vendorMatch="dest"),
    M("8613800008011","rtvg1","{MultiSmppServer}[GSM7] ossid=9961 SMSOptionSupportIncomingGSM7bit=1: 1 byte(s) above 0x7F","DELIVRD","G7-05d：模式 1 里出现 0xE9：解析成 `?`（不改），Server 打一条 WARN", customer="rtcg1", enc="gsm7", hex="436166E9", vendorEnc="gsm7", vendorText="Caf? {text}"),
  ])

case(id="GL002", dir="GL002_outbound_coding_split", title="外发的编码和拆分：7-bit 通道模式 1 / 2、默认通道按 ISO 140 拆、ForceTrans2ASCII、只配 MsgSplitLen 140 / 加 AsciiMsgSplitLen 160、data_coding 跟内容（DEF-C6）",
  refs="G7-07 G7-08 G7-10 G7-12 G7-13 G7-16 G7-17 G7-18 PD-02a PD-02h", notes=NOTE, **COMMON,
  purpose="同一种消息（模式 1 客户带 é 的 160 个字符，或 ISO / ASCII / UCS2）发往配置不同的通道，看通道收到几段、每段多少字节、data_coding、内容。",
  mts=[
    M("8613800008101","rtvg1","","DELIVRD","G7-07、G7-12：7-bit 模式 1 的通道：1 段 160 字节，data_coding 0，GSM 编码；OUT_SUBMIT 字符集 ASCII", customer="rtcg1", enc="gsm7", text=ETE, length="160",
      segLens="160", vendorDc="0", vendorEnc="gsm7", vendorText="same", cdr='OUT_SUBMIT={id}~"ASCII"'),
    M("8615000008102","rtvg2","","DELIVRD","G7-08：7-bit 模式 2 的通道：1 段，压缩成 140 字节", customer="rtcg1", enc="gsm7", text=ETE, length="160", segLens="140", vendorDc="0", vendorEnc="gsm7packed", vendorText="same"),
    M("8616000008103","rtvg3","","DELIVRD","G7-16、PD-02h、G7-12：默认通道（没配 7-bit、没配 MsgSplitLen）：去掉 7-bit 按 ISO 发，134 + 26 字节（每段 140 含 UDH），data_coding 3（改前是 0）；OUT_SUBMIT 字符集 ISO-8859-1", customer="rtcg1", enc="gsm7", text=ETE, length="160",
      segLens="134,26", vendorDc="3", vendorText="same", cdr='OUT_SUBMIT={id}~"ISO-8859-1"'),
    M("8619000008104","rtvg6","","DELIVRD","G7-10：ForceTrans2ASCII：按 ASCII 编码，é 变成 `?`（不是 e），1 段 160", customer="rtcg1", enc="gsm7", text=ETE, length="160", segLens="160", vendorText="ascii?"),
    M("8616000008105","rtvg3","","DELIVRD","G7-13b：data_coding 3，140 字节 ISO，默认通道：1 段", customer="rtcust", enc="latin1", text=CAFE, length="140", segLens="140", vendorDc="3", vendorText="same"),
    M("8616000008106","rtvg3","","DELIVRD","G7-13c：ASCII 160，默认通道：1 段，data_coding 0", customer="rtcust", enc="ascii", text="hello", length="160", segLens="160", vendorDc="0", vendorText="same"),
    M("8616000008107","rtvg3","","DELIVRD","G7-13a：data_coding 3，160 字节 ISO（客户不检查长度），默认通道没配 MsgSplitLen：按 140 拆（改前 160 一段）", customer="rtcg3", enc="latin1", text=CAFE, length="160", segLens="134,26", vendorDc="3", vendorText="same"),
    M("8617000008108","rtvg4","","DELIVRD","G7-17：只配 MsgSplitLen 140：ASCII 160 也拆成 134 + 26（每段 140 字节含 6 字节 UDH）", customer="rtcg1", enc="gsm7", text="hello", length="160", segLens="134,26"),
    M("8617000008109","rtvg4","","DELIVRD","G7-17：只配 MsgSplitLen 140：7-bit 带 é 160 拆成 134 + 26（每段 140 字节含 6 字节 UDH）", customer="rtcg1", enc="gsm7", text=ETE, length="160", segLens="134,26", vendorEnc="gsm7", vendorText="same"),
    M("8618000008110","rtvg5","","DELIVRD","G7-18：再配 SMSOptionAsciiMsgSplitLen 160：ASCII 160 一段", customer="rtcg1", enc="gsm7", text="hello", length="160", segLens="160"),
    M("8618000008111","rtvg5","","DELIVRD","G7-18：同上，7-bit 带 é 160 一段", customer="rtcg1", enc="gsm7", text=ETE, length="160", segLens="160", vendorEnc="gsm7", vendorText="same"),
    M("8618000008112","rtvg5","","DELIVRD","G7-18：ISO 160（data_coding 3）仍按 MsgSplitLen 140 拆", customer="rtcg3", enc="latin1", text=CAFE, length="160", segLens="134,26", vendorDc="3"),
    M("8616000008113","rtvg3","","DELIVRD","PD-02a（DEF-C6）：UCS2 发往默认通道：data_coding 8（改前是 0）", customer="rtcust", enc="ucs2", text="你好", vendorDc="8", vendorText="same"),
  ])

exec(_rest.split("# ================================================================================================ write", 1)[1])
