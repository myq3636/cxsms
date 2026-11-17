# Generates the message-level routing cases RC002..RC022 (regression/routetest/cases/), case.properties version 2
# (docs/路由服务器测试说明.md 4.2). Run from regression/routetest; rewrites those directories (their result/ too).
import os, shutil
NUMS = ",".join(str(i) for i in range(1, 17))

def customer(name, ssid, user, extra=""):
    return f"""
[{name}]
A2PPlayerType:Operator
Service:A2P-SMS
CCBSSID:{ssid}
A2PChargingMethod:submit
CustomerOrOperator:Customer
ExpireTime:46
FinalExpireTime:48
InClientPull:1
MsgSplitLen:160
protocol:SMPP
ReceiverQueue:MultiSmppServer
ServerID:GMSUA2P.001
MinReceiverNumber:1
MaxReceiverNumber:4
MinSenderNumber:1
MaxSenderNumber:4
SMSOptionOriNumberLenList:{NUMS}
SMSOptionRecNumberLenList:{NUMS}
DeliveryReportMode:1
SupportMsgSplit:yes
SmsOptionIsSupportUDH:yes
ConnectionType:MC
IncomingConnection:(ConnName={name}_1,BindMode=TR,IsInit=yes,IsPersistent=yes,password=rtpw,SessionNumber=4,UserName={user},Version=V34)
SMSOptionIncomingThrottlingNum:1000
WindowSize:100
{extra}"""

def vendor(name, ssid, extra="SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\n"):
    return f"""
[{name}]
A2PPlayerType:Operator
Role:Vendor
Service:A2P-SMS
CCBSSID:{ssid}
A2PChargingMethod:submit
CustomerOrOperator:Vendor
ExpireTime:46
FinalExpireTime:48
InClientPull:1
MsgSplitLen:140
SMSOptionAsciiMsgSplitLen:160
protocol:SMPP
DrSwapAddr:false
ReceiverQueue:MultiSmppServer
ServerID:SMSCMTelcom.Tra.001
SMSOptionOriNumberLenList:{NUMS}
SMSOptionRecNumberLenList:{NUMS}
SupportMsgSplit:yes
SmsOptionIsSupportUDH:yes
ChlUdhConcatenated:yes
ChlMsgSplit:yes
ChlQueue:MultiSmppClient
ConnectionType:MC
OutgoingConnection:(ConnName={name}_{ssid},BindMode=TR,IsInit=yes,IsPersistent=yes,password=rtpw,Port=27001,SessionNumber=1,URL=172.19.4.38,UserName={name.lower()},Version=V34)
EnquireLinkTime:40
IsPersistent:yes
{extra}"""

SSID = {f"rtv{i}": 9810 + i for i in range(1, 13)}
SSID["rtvnr"] = 9830

CASES = []
def case(**k):
    CASES.append(k)

# ------------------------------------------------------------------------------------------------ RC002
case(id="RC002", dir="RC002_prefix_groups_more", title="前缀分组：三级前缀、宽泛规则优先级更小、长前缀不命中退到短前缀、多前缀规则、数字发送方完全相等",
  refs="E-05 E-06 E-07 E-09 E-10 E-11 F-01 F-03 F-06",
  purpose="补充 RC001：三级前缀由长到短、同一前缀里优先级小的先（即使它更宽泛）、长前缀组里没有规则命中时退到更短的前缀、一条规则写多个前缀、数字发送方完全相等（不是前缀）、优先级不要求连续。",
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=30,PhonePrefix=8613,Relay=RTV1);(Priority=30,PhonePrefix=861,Relay=RTV2);(Priority=30,PhonePrefix=86,Relay=RTV3)"
    ";(Priority=2,PhonePrefix=8619,Relay=RTV6);(Priority=5,PhonePrefix=8619,Sender=BANK,Relay=RTV7)"
    ";(Priority=10,PhonePrefix=8617,Sender=BANK,Relay=RTV8)"
    ";(Priority=10,PhonePrefix=64;27,Sender=PAY,Relay=RTV9)"
    ";(Priority=1,PhonePrefix=8616,Sender=10690001,Relay=RTV10);(Priority=97,PhonePrefix=8616,Relay=RTV11)"
    ";(Priority=10,PhonePrefix=8618000000212,Relay=RTV4)"
    ";(Priority=50,PhonePrefix=all,Relay=RTV12)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv6,rtv8,rtv9,rtv10,rtv11,rtv12",
  mts=[
    ("OTP","8613800000201","rtv1","by main prefix=8613 priority=30","DELIVRD","三级前缀，最长的 8613"),
    ("OTP","8612000000202","rtv2","by main prefix=861 priority=30","DELIVRD","没有 8612 组，退到 861"),
    ("OTP","8620000000203","rtv3","by main prefix=86 priority=30","DELIVRD","退到 86"),
    ("BANK","8619000000204","rtv6","by main prefix=8619 priority=2","DELIVRD","E-05：8619 组里不带条件的 Priority 2 先于 BANK 的 Priority 5"),
    ("BANK","8617000000205","rtv8","by main prefix=8617 priority=10","DELIVRD","8617 组 BANK 命中"),
    ("OTP","8617000000206","rtv2","by main prefix=861 priority=30","DELIVRD","E-06：8617 组只有 BANK 的规则，OTP 退到 861"),
    ("PAY","64210000207","rtv9","by main prefix=64 priority=10","DELIVRD","E-11：一条规则的两个前缀 64、27"),
    ("PAY","27820000208","rtv9","by main prefix=27 priority=10","DELIVRD","E-11：同一条规则的 27"),
    ("OTP","64210000209","rtv12","by main prefix=all priority=50","DELIVRD","F-06：64 只有 PAY 的规则，OTP 到 all"),
    ("10690001","8616000000210","rtv10","by main prefix=8616 priority=1","DELIVRD","F-03：数字发送方完全相等"),
    ("106900012","8616000000211","rtv11","by main prefix=8616 priority=97","DELIVRD","F-03：多一位不算命中（不是前缀匹配）；E-09 优先级 97 不连续"),
    ("OTP","8618000000212","rtv4","by main prefix=8618000000212 priority=10","DELIVRD","E-10：前缀和整个号码一样长，命中"),
  ])

# ------------------------------------------------------------------------------------------------ RC003
case(id="RC003", dir="RC003_keyword_conditions", title="内容关键字：KeywordId、ExcludeKeywordId、发送方+关键字（并且）",
  refs="F-04 F-05 F-08 G-06 G-08",
  purpose="按内容关键字（contentTemplate.cfg 的模板 id）选路：命中关键字的规则、排除关键字的规则、发送方和关键字同时满足才命中。",
  conf_append={"contentTemplate.cfg": "\n[RTK1]\nEnglish:your otp \\d+.*\n\n[RTK2]\nEnglish:promo .*\n"},
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,KeywordId=RTK1,Relay=RTV1);(Priority=20,PhonePrefix=86,ExcludeKeywordId=RTK2,Relay=RTV2);(Priority=30,PhonePrefix=86,Relay=RTV3)"
    ";(Priority=10,PhonePrefix=44,Sender=BANK,KeywordId=RTK1,Relay=RTV4);(Priority=20,PhonePrefix=44,Relay=RTV5)"
    ";(Priority=10,PhonePrefix=64,Sender=BANK,ExcludeKeywordId=RTK1,Relay=RTV6);(Priority=20,PhonePrefix=64,Relay=RTV7)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv6,rtv7",
  mts=[
    ("OTP","8613800000301","rtv1","by main prefix=86 priority=10","DELIVRD","F-04：内容命中 RTK1", "your otp 1234"),
    ("OTP","8613800000302","rtv3","by main prefix=86 priority=30","DELIVRD","G-06：内容命中 RTK2，被 Priority 20 排除，到 30", "promo big sale"),
    ("OTP","8613800000303","rtv2","by main prefix=86 priority=20","DELIVRD","F-08：普通内容，Priority 20（没有排除）", "hello"),
    ("BANK","44770000304","rtv4","by main prefix=44 priority=10","DELIVRD","F-05：BANK 并且 RTK1", "your otp 5678"),
    ("BANK","44770000305","rtv5","by main prefix=44 priority=20","DELIVRD","F-05：BANK 但不是 RTK1，不命中", "hello"),
    ("OTP","44770000306","rtv5","by main prefix=44 priority=20","DELIVRD","F-05：RTK1 但不是 BANK，不命中", "your otp 9"),
    ("BANK","64210000307","rtv6","by main prefix=64 priority=10","DELIVRD","G-08：BANK 且不是 RTK1", "hello"),
    ("BANK","64210000308","rtv7","by main prefix=64 priority=20","DELIVRD","G-08：BANK 的验证码被排除", "your otp 1"),
  ])

# ------------------------------------------------------------------------------------------------ RC004
case(id="RC004", dir="RC004_exclude_sender_prefix", title="排除条件：ExcludeSender（不区分大小写）、退到短前缀和 all、ExcludePhonePrefix、排除的 DEBUG 日志",
  refs="G-01 G-02 G-03 G-05 G-07 G-11",
  purpose="“除了某几个发送方都走某通道”，被排除的 MT 继续看同组下一条、更短的前缀、all；排除号段；排除时的 DEBUG 日志。",
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,ExcludeSender=BANK;OTP,Relay=RTV1);(Priority=20,PhonePrefix=86,Relay=RTV2)"
    ";(Priority=10,PhonePrefix=8613,ExcludeSender=PAY,Relay=RTV3)"
    ";(Priority=10,PhonePrefix=852,ExcludePhonePrefix=85269,Relay=RTV4)"
    ";(Priority=10,PhonePrefix=64,ExcludeSender=BANK,Relay=RTV5)"
    ";(Priority=50,PhonePrefix=all,Relay=RTV9)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv9",
  mts=[
    ("PAY","8620000000401","rtv1","by main prefix=86 priority=10","DELIVRD","G-01：PAY 不在排除里"),
    ("BANK","8620000000402","rtv2","not matched by ExcludeSender=bank&&by main prefix=86 priority=20","DELIVRD","G-01、G-11：BANK 被排除，同组下一条；DEBUG 日志写明排除条件"),
    ("otp","8620000000403","rtv2","by main prefix=86 priority=20","DELIVRD","G-05：排除不区分大小写"),
    ("OTP","8613800000404","rtv3","by main prefix=8613 priority=10","DELIVRD","8613 组"),
    ("PAY","8613800000405","rtv1","by main prefix=86 priority=10","DELIVRD","G-02：8613 排除了 PAY，退到 86（PAY 在 86 的 Priority 10 不被排除）"),
    ("OTP","85268000406","rtv4","by main prefix=852 priority=10","DELIVRD","852 组"),
    ("OTP","85269000407","rtv9","not matched by ExcludePhonePrefix=85269&&by main prefix=all priority=50","DELIVRD","G-07：85269 被排除，852 组没有下一条，到 all"),
    ("BANK","64210000408","rtv9","by main prefix=all priority=50","DELIVRD","G-03：64 组只有排除 BANK 的规则，BANK 到 all"),
    ("OTP","64210000409","rtv5","by main prefix=64 priority=10","DELIVRD","64 组"),
  ])

# ------------------------------------------------------------------------------------------------ RC005
case(id="RC005", dir="RC005_number_layer", title="号码层：完全相等、多个号码、同一号码按优先级、号码层先于前缀、号码带 +",
  refs="B-02 B-09 C-01 C-02 C-03 C-04",
  purpose="号码路由（A2PNumberRouting）在普通路由之前；号码完全相等才命中；同一号码多条按 Priority；号码层有规则但号码不在里面时看普通层。",
  routing={"RTCUST": "CCBSSID:9801\nA2PNumberRouting:(Priority=1,Number=8613800000501;8613800000502,Relay=RTV1);(Priority=1,Number=8613800000503,Sender=BANK,Relay=RTV2);(Priority=2,Number=8613800000503,Relay=RTV3);(Priority=1,Number=8613800000599,Relay=RTV4)\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Sender=BANK,Relay=RTV5);(Priority=50,PhonePrefix=86,Relay=RTV9)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv9",
  mts=[
    ("BANK","8613800000501","rtv1","by number number=8613800000501 priority=1","DELIVRD","B-02：号码层先于普通层（普通层 BANK 会走 RTV5）"),
    ("OTP","8613800000502","rtv1","by number number=8613800000502 priority=1","DELIVRD","C-02：同一条规则的第二个号码"),
    ("BANK","8613800000503","rtv2","by number number=8613800000503 priority=1","DELIVRD","C-03：同一号码 BANK 的 Priority 1"),
    ("OTP","8613800000503","rtv3","by number number=8613800000503 priority=2","DELIVRD","C-03：其他发送方 Priority 2"),
    ("OTP","8613800000504","rtv9","by main prefix=86 priority=50","DELIVRD","B-09：号码层有规则，但没有这个号码，看普通层"),
    ("OTP","86138000005010","rtv9","by main prefix=86 priority=50","DELIVRD","C-01：比规则里的号码多一位，不算命中"),
    ("OTP","+8613800000599","rtv4","by number number=8613800000599 priority=1","DELIVRD","C-04：号码带 +，去掉后比较"),
  ])

# ------------------------------------------------------------------------------------------------ RC006
case(id="RC006", dir="RC006_second_routing", title="二次路由：同一号码短时间内第二次发走二次层；二次层不命中往下看",
  refs="B-07 B-08 J-13",
  purpose="客户开了二次路由：号码第一次按主路由发，之后 300 秒内再发同一号码，先看二次路由层（A2PSecRouting）；二次层没有规则命中时照常看号码层、普通层。",
  cm_extra=customer("RTCUST2", 9802, "rtcust2", "smsOptionNeedSupportSecondRouting:true\nsmsOptionNeedSupportSecondRoutingExpiredTime:300\nSMSOptionSecondRoutingRecPrefix:86\n"),
  routing={"RTCUST2": "CCBSSID:9802\nA2PSecRouting:(Priority=10,PhonePrefix=8613,Relay=RTV2);(Priority=10,PhonePrefix=8615,Sender=BANK,Relay=RTV3);(Priority=10,PhonePrefix=8616,Relay=RTV4)\n"
    "A2PSecBackupRouting:(Priority=10,PhonePrefix=8616,Relay=RTV5)\nA2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV6)\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)\n"},
  props={"customer.user": "rtcust2"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5",
  vendor_cfg={"rtv4": {"submitStatus": "0x45"}},
  notes="二次路由的号码缓存在每个 Core 的内存里（smsOptionNeedSupportSecondRoutingExpiredTime 秒过期）；MT 按 ossid + 发送方 + 号码分到 Core，所以同一号码的两条用同一个发送方，一定是同一个 Core。走了二次路由的 MT 不会刷新缓存。",
  mts=[
    ("OTP","8613800000601","rtv1","by main prefix=86 priority=10","DELIVRD","第一次：号码不在二次路由缓存里，主路由；之后号码进缓存"),
    ("OTP","8613800000601","rtv2","by second prefix=8613 priority=10","DELIVRD","B-07：3 秒后同一号码，二次层", None, {"after": "3000"}),
    ("OTP","8615000000603","rtv1","by main prefix=86 priority=10","DELIVRD","第一次"),
    ("OTP","8615000000603","rtv1","by main prefix=86 priority=10","DELIVRD","B-08：二次层 8615 只有 BANK 的规则，不命中，往下看主路由", None, {"after": "3000"}),
    ("BANK","8615000000605","rtv1","by main prefix=86 priority=10","DELIVRD","第一次"),
    ("BANK","8615000000605","rtv3","by second prefix=8615 priority=10","DELIVRD","B-07：BANK 第二次，二次层 8615", None, {"after": "3000"}),
    ("OTP","8616000000607","rtv1","by main prefix=86 priority=10","DELIVRD","第一次"),
    ("OTP","8616000000607","rtv4>rtv5","by second prefix=8616 priority=10&&[MT-REROUTE] level=1 from=9814 to=9815 code=2300 attempt=1 by second backup1 prefix=8616","DELIVRD","J-13：第二次走二次层的 RTV4，RTV4 失败，用二次层的备份 1 RTV5（不是普通层备份 1 的 RTV6）", None, {"after": "3000"}),
  ])

# ------------------------------------------------------------------------------------------------ RC007
case(id="RC007", dir="RC007_default_routing", title="默认路由：没有自己路由文件的客户、客户规则都不命中时、默认路由的备份",
  refs="E-08 E-12 G-04 I-06 J-14 A-19 B-01",
  purpose="客户的四层都不命中时看 {业务类型}_default 文件（同样的规则格式）；默认路由也有备份；客户自己的规则被排除后也会落到默认路由。",
  cm_extra=customer("RTCUST3", 9803, "rtcust3", "CustomerServiceType:RTSVC\n") + customer("RTCUST4", 9804, "rtcust4", "CustomerServiceType:RTSVC\n"),
  routing={"RTSVC_default": "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV2);(Priority=10,PhonePrefix=8615,Relay=RTV6)\n"
             "A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTV7)\n",
           "RTCUST4": "CCBSSID:9804\nA2PMainRouting:(Priority=10,PhonePrefix=8613,ExcludeSender=BANK,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV5)\n"},
  vendors="rtv1,rtv2,rtv4,rtv5,rtv6,rtv7",
  vendor_cfg={"rtv6": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8613800000701","rtv1","by main prefix=86 priority=10 file=RTSVC_default","DELIVRD","RR-05：RTCUST3 没有自己的路由文件，用 RTSVC_default", None, {"as": "rtcust3"}),
    ("OTP","64210000702","rtv2","by main prefix=64 priority=10 file=RTSVC_default","DELIVRD","默认路由的 64", None, {"as": "rtcust3"}),
    ("OTP","44770000703","none","no backup route either","!DELIVRD","I-06：默认路由也没有 44，备份也没有：路由失败，失败 DR", None, {"as": "rtcust3"}),
    ("OTP","8613800000704","rtv4","by main prefix=8613 priority=10 file=RTCUST4","DELIVRD","RTCUST4 用自己的文件", None, {"as": "rtcust4"}),
    ("BANK","8613800000705","rtv1","by main prefix=86 priority=10 file=RTSVC_default","DELIVRD","G-04、E-12：自己的规则排除了 BANK、自己的四层都不命中，看默认路由", None, {"as": "rtcust4"}),
    ("OTP","8615000000707","rtv6>rtv7","[MT-REROUTE] level=1 from=9816 to=9817&&by main backup1 prefix=8615 priority=10 file=RTSVC_default","DELIVRD","J-14：默认路由的通道 RTV6 回 0x45（2300，在重试码里），用默认路由的备份 1", None, {"as": "rtcust3"}),
  ])

# ------------------------------------------------------------------------------------------------ RC008
case(id="RC008", dir="RC008_relay_weight_turn", title="Relay：权重 70/30、轮流、; 后面不生效",
  refs="H-01 H-02 H-03 H-04",
  purpose="Relay 的三种写法：权重按比例随机、逗号轮流（每条规则自己计数）、`;` 后面的部分不生效（加载时 WARN）。",
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1:70,RTV2:30);(Priority=10,PhonePrefix=64,Relay=RTV3,RTV4,RTV5);(Priority=10,PhonePrefix=27,Relay=RTV6;RTV7);(Priority=10,PhonePrefix=44,Relay=RTV8:100,RTV9:0)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv6,rtv8,rtv9",
  mts=[
    ("OTP","8613800000801","rtv1|rtv2","by main prefix=86 priority=10","DELIVRD","H-01：200 条（同一号码，一个 Core 处理），比例 70/30 左右", None, {"count": "200", "ratio": "rtv1:70,rtv2:30,tolerance=10"}),
    ("OTP","64210000802","rtv3|rtv4|rtv5","by main prefix=64 priority=10","DELIVRD","H-02：9 条（同一号码），轮流各 3 条", None, {"count": "9", "ratio": "rtv3:33.3,rtv4:33.3,rtv5:33.3,tolerance=0.1"}),
    ("OTP","27820000803","rtv6","by main prefix=27 priority=10","DELIVRD","H-03：Relay=RTV6;RTV7 只用 RTV6"),
    ("OTP","44770000804","rtv8","by main prefix=44 priority=10","DELIVRD","H-04：20 条，权重 0 的 RTV9 一条也收不到", None, {"count": "20", "ratio": "rtv8:100,rtv9:0,tolerance=0"}),
  ],
  checks=["check.log.1=CoreEngine1|Relay=RTV6;RTV7: the part after ';' is not used"])

# ------------------------------------------------------------------------------------------------ RC009
case(id="RC009", dir="RC009_main_unusable_backup", title="主路由选不出能用的通道时，选路时就从备份找：长度不允许、通道不存在、没有主路由规则；备份也没有才路由失败",
  refs="I-01 I-02 I-03 I-06 Q-04",
  purpose="主路由命中了但通道不存在 / 内容长度不允许，或者主路由一条规则都没命中：在选路时从备份 1 级找；备份也没有：路由失败，回失败 DR。",
  cm_extra=vendor("RTVLEN", 9840, "SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\nSMSSpecialSupportContentLengthByCountry:(prefix=86,value=50::deny)\n"),
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVLEN);(Priority=10,PhonePrefix=8615,Relay=RTVXX);(Priority=10,PhonePrefix=44,Relay=RTV1);(Priority=10,PhonePrefix=852,Relay=RTV4:50,RTVXX:50)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=852,Relay=RTV5)\n"},
  vendors="rtvlen,rtv1,rtv2,rtv3,rtv4,rtv5",
  mts=[
    ("OTP","8613800000901","rtvlen","by main prefix=8613 priority=10","DELIVRD","短内容，主路由的 RTVLEN 允许", "hi"),
    ("OTP","8613800000902","rtv2","content length not allowed by channel 9840&&by main backup1 prefix=86 priority=10","DELIVRD","I-02：内容超过 50 字节，RTVLEN 不允许，选路时用备份 1，routingLevel=1", "this content is longer than the fifty bytes RTVLEN allows"),
    ("OTP","8615000000903","rtv2","channel -1 not in cm.cfg&&by main backup1 prefix=86 priority=10","DELIVRD","I-01：主路由的通道 RTVXX 不在 cm.cfg，用备份 1（WARN）"),
    ("OTP","64210000904","rtv3","no main routing rule matched&&by main backup1 prefix=64 priority=10","DELIVRD","I-06：没有 64 的主路由规则，用备份 1"),
    ("OTP","27820000905","none","no backup route either","!DELIVRD","没有主路由，也没有备份：路由失败，失败 DR"),
    ("OTP","85268000906","rtv4|rtv5","","DELIVRD","I-03：40 条，Relay=RTV4:50,RTVXX:50，抽到不存在的 RTVXX 时不换 RTV4，而是用备份 RTV5：两个都收到，各一半左右", None, {"count": "40", "ratio": "rtv4:50,rtv5:50,tolerance=20"}),
  ],
  checks=["check.log.1=CoreEngine1|Relay channel RTVXX is not in cm.cfg"])

# ------------------------------------------------------------------------------------------------ RC010
case(id="RC010", dir="RC010_rop_failed_smq", title="ROP 失败模式：进 SMQ 等通道恢复，不走备份；恢复后发原来的通道",
  refs="I-04 I-05",
  purpose="主路由选出的通道在 ROP 失败模式（OperatorMode.properties 的 ROP-FailedMode）：MT 进 SMQ，不切到备份；通道恢复后（去掉 ROP-FailedMode）由 MQM 发到原来的通道。",
  conf={"OperatorMode.properties": "ROP-FailedMode=9811\n"},
  conf_append={"MQMTaskConfig.properties": "\n# RC010: SendROPFailedMessageTask only resends the SMQ messages of these rssids\nSMQ.SendROPFailedMessageTask.OPList=9811\n"},
  cm_extra=customer("RTCUSTR", 9808, "rtcustr", "SMSOptionMORetryPolicy:2M,2M\n"),
  props={"customer.user": "rtcustr"},
  notes="OperatorMode.properties 每 5 秒按修改时间重新读取，第 2 阶段只换文件、不用命令。MQM 的 SendROPFailedMessageTask 每 30 秒把 SMQ 里通道已恢复的消息重发，最多等 4 分钟。**ROP 失败模式的消息按客户的重试策略处理**：选到通道后、设置 RA2P 之前就返回了，`RetryPolicyManager` 走“RA2P=0”的分支取客户的 `SMSOptionMORetryPolicy`，客户没配时用全局 `DefaultRetryPolicy`（这台机器是 `20M*1`，只有一个点：第一次就是最后一次，不进 SMQ，直接回失败 DR）。路由重构前也是这样。所以这个用例用配了 `SMSOptionMORetryPolicy:2M,2M` 的客户 RTCUSTR；等待期间检查 `smq` 表里有这条消息。**MQM 只重发 `MQMTaskConfig.properties` 的 `SMQ.SendROPFailedMessageTask.OPList` 里列出的通道的 SMQ 消息**（不配就什么都不发；这台机器这一行是注释掉的），所以用例运行时追加 `OPList=9811`（MQM 启动时读取）。消息在 SMQ 里还要等到 `NextRetryTime`（客户策略的第一个点，2 分钟）之后才会被取出。",
  phase2={"afterMs": "20000", "conf": {"OperatorMode.properties": "ROP-FailedMode=\n"}, "reload": ""},
  routing={"RTCUSTR": "CCBSSID:9808\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)\nA2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2)\n"},
  vendors="rtv1,rtv2",
  mts=[
    ("OTP","8613800001001","rtv1","rssid=9811 by main prefix=86 priority=10 file=RTCUSTR channel in ROP failed mode: insert into SMQ","DELIVRD","I-04：先进 SMQ（这时 RTV1、RTV2 都收不到）；I-05：20 秒后去掉 ROP-FailedMode，MQM 发到 RTV1（不是备份 RTV2）", None, {"waitMs": "240000", "db": "smq:RSsID=9811"}),
  ])

# ------------------------------------------------------------------------------------------------ RC011
case(id="RC011", dir="RC011_backup_levels", title="逐级备份：每级按自己的规则选、每级只用一次、级别不连续、跳过已经发过的通道",
  refs="J-01 J-02 J-03 J-04 J-05 J-06 J-07 J-08 G-09",
  purpose="submit 失败（码在通道的 SMSOptionSubmitRetryCode 里）逐级进备份：每级的规则按前缀、优先级、发送方选；V4:50,V5:50 只选一个，失败就进下一级；某级没有命中直接看下一级；Relay 里发过的通道不再选。",
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=44,Relay=RTV7);(Priority=10,PhonePrefix=64,Relay=RTV10);(Priority=10,PhonePrefix=27,Relay=RTV1)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=27,ExcludeSender=BANK,Relay=RTV6);(Priority=20,PhonePrefix=27,Relay=RTV9);(Priority=10,PhonePrefix=86,Sender=BANK,Relay=RTV2);(Priority=20,PhonePrefix=86,Relay=RTV3);(Priority=10,PhonePrefix=44,Sender=PAY,Relay=RTV8);(Priority=10,PhonePrefix=64,Relay=RTV10)\n"
    "A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV4:50,RTV5:50);(Priority=10,PhonePrefix=64,Relay=RTV10:50,RTV11:50)\n"
    "A2PBackup3Routing:(Priority=10,PhonePrefix=all,Relay=RTV6);(Priority=10,PhonePrefix=44,Relay=RTV9)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv6,rtv7,rtv9,rtv10,rtv11",
  vendor_cfg={v: {"submitStatus": "0x45"} for v in ["rtv1","rtv2","rtv3","rtv4","rtv5","rtv7","rtv10"]},
  mts=[
    ("OTP","8615000001101","rtv1>rtv3>(rtv4|rtv5)>rtv6","[MT-REROUTE] level=1 from=9811 to=9813&&level=3&&by main backup3 prefix=all priority=10","DELIVRD","J-01、J-03、J-04：主 RTV1 失败 → 备份 1 Priority 20（OTP）RTV3 → 备份 2 RTV4 或 RTV5（只选一个）→ 备份 3 RTV6"),
    ("BANK","8615000001102","rtv1>rtv2>(rtv4|rtv5)>rtv6","[MT-REROUTE] level=1 from=9811 to=9812","DELIVRD","J-02：BANK 在备份 1 走 Priority 10 的 RTV2"),
    ("OTP","44770001103","rtv7>rtv9","[MT-REROUTE] level=3 from=9817 to=9819","DELIVRD","J-05、J-06：备份 1 只有 PAY 的规则不命中、备份 2 没有 44，直接到备份 3 的 44 组"),
    ("OTP","64210001104","rtv10>rtv11","[MT-REROUTE] level=2 from=9820 to=9821","DELIVRD","J-07、J-08：备份 1 是发过的 RTV10，跳过；备份 2 只会选没发过的 RTV11"),
    ("PAY","27820001105","rtv1>rtv6","[MT-REROUTE] level=1 from=9811 to=9816","DELIVRD","G-09：备份 1 的排除：PAY 不被排除，走 Priority 10 的 RTV6"),
    ("BANK","27820001106","rtv1>rtv9","[MT-REROUTE] level=1 from=9811 to=9819","DELIVRD","G-09：BANK 被排除，走 Priority 20 的 RTV9"),
  ])

# ------------------------------------------------------------------------------------------------ RC012
case(id="RC012", dir="RC012_backup_unusable_max_10levels", title="备份的通道不存在跳到下一级；走满 10 级；SMSOptionMaxReroute",
  refs="J-09 J-10 J-15 J-16 J-17 K-09",
  purpose="备份某一级的通道不存在，跳到下一级；10 级备份都能用到（默认 SMSOptionMaxReroute=10）；客户 SMSOptionMaxReroute=1 时改道一次后就按重试处理（重试主路由），重试次数用完回失败 DR。",
  cm_extra=customer("RTCUST5", 9805, "rtcust5", "SMSOptionMaxReroute:1\n") + vendor("RTVLEN", 9840, "SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\nSMSSpecialSupportContentLengthByCountry:(prefix=27,value=50::deny)\n"),
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=27,Relay=RTV1)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTVXX);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=27,Relay=RTVLEN)\n"
    "A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV5);(Priority=10,PhonePrefix=27,Relay=RTV2)\n"
    + "".join(f"A2PBackup{n}Routing:(Priority=10,PhonePrefix=64,Relay=RTV{n+3})\n" for n in range(3, 10))
    + "A2PBackup10Routing:(Priority=10,PhonePrefix=64,Relay=RTV2)\n",
    "RTCUST5": "CCBSSID:9805\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)\nA2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV3)\nA2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV2)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv6,rtv7,rtv8,rtv9,rtv10,rtv11,rtv12,rtvlen",
  vendor_cfg={v: {"submitStatus": "0x45"} for v in ["rtv1"] + [f"rtv{i}" for i in range(3, 13)]},
  mts=[
    ("OTP","8615000001201","rtv1>rtv2","channel -1 not in cm.cfg, next level&&[MT-REROUTE] level=2 from=9811 to=9812","DELIVRD","J-09：备份 1 的 RTVXX 不存在，跳到备份 2 的 RTV2"),
    ("OTP","64210001202","rtv3>rtv4>rtv5>rtv6>rtv7>rtv8>rtv9>rtv10>rtv11>rtv12>rtv2","[MT-REROUTE] level=10 from=9822 to=9812","DELIVRD","J-16、J-17：主路由 + 10 级备份，前 10 个都失败，第 10 级 RTV2 成功（默认 SMSOptionMaxReroute=10，第 10 次改道允许）"),
    ("OTP","8615000001203","rtv1>rtv3>rtv1","backup levels used up (level 1, rssid 9813): retried on the main route rssid 9811&&rerouted 1 times already, SMSOptionMaxReroute=1","!DELIVRD","J-15、K-09：RTCUST5 只能改道 1 次：RTV1 → RTV3；RTV3 失败后不再改道，按重试策略（1M,1M 两个点 = 一共发两轮）1 分钟后重试主路由 RTV1；RTV1 再失败，改道次数已满、重试用完，回失败 DR", None, {"as": "rtcust5", "waitMs": "240000"}),
    ("OTP","27820001204","rtv1>rtv2","rssid=9840: content length not allowed by channel 9840, next level&&[MT-REROUTE] level=2 from=9811 to=9812","DELIVRD","J-10：备份 1 的 RTVLEN 不允许这么长的内容，跳到备份 2", "this content is longer than the fifty bytes RTVLEN allows"),
  ])

# ------------------------------------------------------------------------------------------------ RC013
case(id="RC013", dir="RC013_retry_code_decides", title="改道码：失败码不在这次发送通道的 SMSOptionSubmitRetryCode 里，不改道、不重试，直接失败 DR",
  refs="K-01 K-02 K-03 K-04",
  purpose="只有失败码在这次发送的通道的 SMSOptionSubmitRetryCode 里才进下一级：主路由的码不在里面（0x0B → 2120）、通道没配重试码（RTVNR）、备份通道的码不在它自己的列表里，都直接回失败 DR，后面的级别收不到。",
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTVNR);(Priority=10,PhonePrefix=44,Relay=RTV4)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTVNR)\n"
    "A2PBackup2Routing:(Priority=10,PhonePrefix=44,Relay=RTV6)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtvnr,rtv6",
  vendor_cfg={"rtv1": {"submitStatus": "0x0B"}, "rtvnr": {"submitStatus": "0x45"}, "rtv4": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8615000001301","rtv1","not rerouted: status 2120 not in SMSOptionSubmitRetryCode of rssid 9811","!DELIVRD","K-01：RTV1 回 0x0B（2120，不在 2300,2500,2600 里），不进备份 RTV2"),
    ("OTP","64210001302","rtvnr","not rerouted: status 2300 not in SMSOptionSubmitRetryCode of rssid 9830","!DELIVRD","K-04：RTVNR 没配重试码，任何失败都不改道"),
    ("OTP","44770001303","rtv4>rtvnr","[MT-REROUTE] level=1 from=9814 to=9830&&status 2300 not in SMSOptionSubmitRetryCode of rssid 9830","!DELIVRD","K-02、K-03：RTV4 的 2300 在它的重试码里，改道到 RTVNR；RTVNR 的 2300 不在它的列表里（看这次发送的通道），不进备份 2 RTV6"),
  ])

# ------------------------------------------------------------------------------------------------ RC014
case(id="RC014", dir="RC014_levels_used_up_retry_main", title="级别用完：按重试策略重试主路由的通道；主路由再失败不再走备份；RoutingLevel 写进 DB；CDR MsgID",
  refs="K-05 K-06 K-07 K-08 K-09 N-04 N-05b P-07",
  purpose="所有级别都失败（码都在重试码里）后按重试策略（1M,1M）等 1 分钟，重试主路由的通道（不是最后失败的备份通道），级别保留；重试时主路由再失败不再走备份；DB 的 RMQ 行记着 RSsID=主路由、RoutingLevel；重试主路由的 CDR MsgID 是原来的 msgId。",
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTV5)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV6)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtv6",
  vendor_cfg={"rtv1": {"submitStatusSeq": "0x45,0"}, "rtv2": {"submitStatus": "0x45"},
              "rtv3": {"submitStatusSeq": "0x45,0x0B"}, "rtv4": {"submitStatus": "0x45"},
              "rtv5": {"submitStatus": "0x45"}, "rtv6": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8615000001401","rtv1>rtv2>rtv1","backup levels used up (level 1, rssid 9812): retried on the main route rssid 9811","DELIVRD",
     "K-05、K-08：RTV1 失败 → 备份 RTV2 失败 → 1 分钟后重试主路由 RTV1，这次成功",
     None, {"waitMs": "180000", "db": "rmq:RSsID=9811,RoutingLevel=1",
            "cdr": "OUT_SUBMIT={id};OUT_SUBMIT={id}-9812;OUT_SUBMIT={id};OUT_DELI_REP={id};IN_DELI_REP={id}"}),
    ("OTP","64210001402","rtv3>rtv4>rtv3","not rerouted: status 2120 not in SMSOptionSubmitRetryCode of rssid 9813","!DELIVRD",
     "K-07：重试主路由 RTV3 时回 0x0B（不在重试码里）：失败 DR", None, {"waitMs": "180000"}),
    ("OTP","44770001403","rtv5>rtv6>rtv5","no backup level after level 1 (rssid=9815","!DELIVRD",
     "K-06、K-09：重试主路由 RTV5 再失败，不再走备份 RTV6；重试策略 1M,1M（两个点 = 一共发两轮）用完，失败 DR，RTV6 只收到 1 次", None, {"waitMs": "240000"}),
  ])

# ------------------------------------------------------------------------------------------------ RC015
DRV = "SMSOptionSubmitRetryCode:2300,2500,2600\nSMSOptionMTRetryPolicy:1M,1M\n"
case(id="RC015", dir="RC015_dr_reroute", title="DR 失败改道：要通道开 NeedDRReroute；逐级；没开、超时、级别用完：失败 DR 回客户；submit 改道后 DR 再改道",
  refs="L-01 L-02 L-04 L-05 L-06 L-07 L-08 L-14 L-15",
  purpose="供应商回失败 DR：这次发送的通道开了 NeedDRReroute、状态在 SMSOptionDRRerouteCode 里（没配列表 = 任何失败）、在 RerouteDRDelayTimeInSec 内，进下一级，客户这时不收 DR；否则写 OUT_DELI_REP，把失败 DR 回给客户。",
  cm_extra=vendor("RTVD1", 9851, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400,10401,10402,10403,10404,10405\nRerouteDRDelayTimeInSec:300\n")
    + vendor("RTVD2", 9852, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400,10401,10402,10403,10404,10405\nRerouteDRDelayTimeInSec:300\n")
    + vendor("RTVD3", 9853, DRV + "NeedDRReroute:false\n")
    + vendor("RTVD4", 9854, DRV + "NeedDRReroute:true\nRerouteDRDelayTimeInSec:300\n")
    + vendor("RTVD5", 9855, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400,10401,10402,10403,10404,10405\nRerouteDRDelayTimeInSec:1\n"),
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD1);(Priority=10,PhonePrefix=8615,Relay=RTVD1);(Priority=10,PhonePrefix=64,Relay=RTVD3);(Priority=10,PhonePrefix=44,Relay=RTVD4);(Priority=10,PhonePrefix=27,Relay=RTVD5);(Priority=10,PhonePrefix=8616,Relay=RTV1);(Priority=10,PhonePrefix=8617,Relay=RTV2)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD2);(Priority=10,PhonePrefix=8615,Relay=RTVD4);(Priority=10,PhonePrefix=64,Relay=RTVD2);(Priority=10,PhonePrefix=44,Relay=RTVD2);(Priority=10,PhonePrefix=27,Relay=RTVD2);(Priority=10,PhonePrefix=8616,Relay=RTVD1);(Priority=10,PhonePrefix=8617,Relay=RTVD3)\n"
    "A2PBackup2Routing:(Priority=10,PhonePrefix=8616,Relay=RTVD2);(Priority=10,PhonePrefix=8617,Relay=RTVD2)\n"},
  vendors="rtvd1,rtvd2,rtvd3,rtvd4,rtvd5,rtv1,rtv2",
  vendor_cfg={"rtvd1": {"dr": "UNDELIV"}, "rtvd2": {"dr": "DELIVRD"}, "rtvd3": {"dr": "UNDELIV"}, "rtvd4": {"dr": "UNDELIV"},
              "rtvd5": {"dr": "UNDELIV", "drDelayMs": "3000"}, "rtv1": {"submitStatus": "0x45"}, "rtv2": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8613800001501","rtvd1>rtvd2","[MT-REROUTE] level=1 from=9851 to=9852","DELIVRD","L-01：RTVD1 回 UNDELIV，改道到备份 RTVD2，客户只收到 RTVD2 的 DELIVRD（一个 DR）"),
    ("OTP","8615000001502","rtvd1>rtvd4","[MT-REROUTE] level=1 from=9851 to=9854","UNDELIV","L-02、L-04：RTVD4 也 UNDELIV，没有下一级：失败 DR 回客户，不重试"),
    ("OTP","64210001503","rtvd3","","UNDELIV","L-06：RTVD3 没开 NeedDRReroute：不改道，失败 DR 回客户"),
    ("OTP","44770001504","rtvd4>rtvd2","[MT-REROUTE] level=1 from=9854 to=9852","DELIVRD","L-07：RTVD4 开了 NeedDRReroute 没配状态列表：任何失败都改道"),
    ("OTP","27820001505","rtvd5","","UNDELIV","L-05：DR 3 秒后才来，超过 RerouteDRDelayTimeInSec=1：不改道"),
    ("OTP","8616000001506","rtv1>rtvd1>rtvd2","[MT-REROUTE] level=1 from=9811 to=9851&&[MT-REROUTE] level=2 from=9851 to=9852","DELIVRD","L-14：submit 失败改道到 RTVD1（第 1 级），RTVD1 的 DR 失败从第 2 级接着找"),
    ("OTP","8617000001507","rtv2>rtvd3","[MT-REROUTE] level=1 from=9812 to=9853","UNDELIV","L-15：看这次发送的通道 RTVD3（没开 NeedDRReroute），不进第 2 级"),
  ],
  props={"mt.drWaitMs": "90000"})

# ------------------------------------------------------------------------------------------------ RC016
case(id="RC016", dir="RC016_dr_reroute_codes", title="DR 改道码：配了 SMSOptionDRRerouteCode 只有列表里的码改道；按细分后的内部状态判断；NotPaid 不改道",
  refs="L-03 L-16 L-17 L-18",
  purpose="SMSOptionDRRerouteCode 配了列表时只有状态在里面才改道；比较的是 DR 错误码对照表细分后的内部状态（drErrorMapping.cfg）；命中 SMSOptionDRNotPaidCode 的 DR 变成 REJECTED_BYNC，不改道。",
  conf={"drErrorMapping.cfg": "[RTVD7]\nDRErr: (Stat=UNDELIV,Err=005,Code=104)\n"},
  cm_extra=vendor("RTVD6", 9856, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400\nRerouteDRDelayTimeInSec:300\n")
    + vendor("RTVD7", 9857, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400\nRerouteDRDelayTimeInSec:300\n")
    + vendor("RTVD8", 9858, DRV + "NeedDRReroute:true\nSMSOptionDRRerouteCode:10400\nRerouteDRDelayTimeInSec:300\nSMSOptionDRNotPaidCode:077\n")
    + vendor("RTVD2", 9852, DRV),
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD6);(Priority=10,PhonePrefix=8615,Relay=RTVD7);(Priority=10,PhonePrefix=64,Relay=RTVD8)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTVD2);(Priority=10,PhonePrefix=64,Relay=RTVD2)\n"},
  vendors="rtvd6,rtvd7,rtvd8,rtvd2",
  vendor_cfg={"rtvd6": {"dr": "UNDELIV", "drErr": "001"}, "rtvd7": {"dr": "UNDELIV", "drErr": "005"}, "rtvd8": {"dr": "UNDELIV", "drErr": "077"}, "rtvd2": {"dr": "DELIVRD"}},
  mts=[
    ("OTP","8613800001601","rtvd6>rtvd2","[MT-REROUTE] level=1 from=9856 to=9852","DELIVRD","L-16：RTVD6 的 UNDELIV err:001 是 10400，在列表里：改道"),
    ("OTP","8615000001602","rtvd7","","UNDELIV","L-17：RTVD7 的 err:005 按对照表细分成 104（10405），不在列表 10400 里：不改道（验收时看 evidence 里 Core 的 DR 状态是 10405）"),
    ("OTP","64210001603","rtvd8","","!DELIVRD","L-18：err:077 命中 SMSOptionDRNotPaidCode，变成 REJECTED_BYNC：不改道（RTVD8 的列表是 10400，没有 10800；L-18 的前提是“列表里没有这个码”。没配列表时任何失败都改道，欠费也改道）"),
  ],
  notes="用例运行时用 `conf/drErrorMapping.cfg` 替换服务器上的对照表（只有 RTVD7 一块，没有 [DEFAULT]），结束后恢复；模块重启时加载，不用 RELOAD_DRERROR。RTVD6 的 UNDELIV 没有对照规则，状态保持 UNDELIVERABLE（10400）。验收时看 evidence 里 Core 的 `[DR-ERR] rssid=... code=... status=...` 行：M1 status=10400、M2 status=10405、M3 code=404。")

# ------------------------------------------------------------------------------------------------ RC017
case(id="RC017", dir="RC017_vendor_replacement", title="系统路由替换：选通道时替换；“发过”和改道码看替换后的通道；客户级替换优先；只换一次；替换成不存在的通道",
  refs="M-01 M-02 M-03 M-04 M-05 M-06 M-07 M-08 M-09 P-05 P-06",
  purpose="vendorRoutingReplacement.cfg（前缀:ossid:原通道:替换通道）在选通道时做：主路由和每一级备份都替换；替换后的通道发过了就算发过；客户级（ossid=9801）的替换优先于 -1；V→W 后不再查 W 的替换；替换成不存在的通道时主路由从备份找、备份跳到下一级。",
  conf={"vendorRoutingReplacement.cfg": "86:-1:9811:9812\n86:9801:9813:9814\n86:-1:9813:9816\n86:-1:9814:9815\n64:-1:9817:9999\n"},
  routing={"RTCUST": "CCBSSID:9801\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTV1);(Priority=10,PhonePrefix=8615,Relay=RTV3);(Priority=10,PhonePrefix=64,Relay=RTV7)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=8613,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV8)\n"
    "A2PBackup2Routing:(Priority=10,PhonePrefix=8613,Relay=RTV3)\n"},
  vendors="rtv2,rtv4,rtv8",
  vendor_cfg={"rtv2": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8613800001701","rtv2>rtv4","rule=9811 replaced=9812&&[MT-REROUTE] level=2 from=9812 to=9814","DELIVRD",
     "M-01、M-02、M-03：主路由 RTV1 被替换成 RTV2 发出；RTV2 失败（看 RTV2 的重试码）；备份 1 的 RTV1 又被替换成发过的 RTV2，跳过；备份 2 的 RTV3 按客户级替换成 RTV4（M-09，不是 -1 的 RTV6），而且不再按 9814:9815 换（M-08）",
     None, {"cdr": "OUT_SUBMIT={id};OUT_SUBMIT={id}-9814"}),
    ("OTP","8615000001702","rtv4","rule=9813 replaced=9814","DELIVRD","M-09：客户级替换优先"),
    ("OTP","64210001703","rtv8","channel 9999 not in cm.cfg&&by main backup1 prefix=64 priority=10","DELIVRD","M-06：主路由 RTV7 被替换成不存在的 9999：选路时从备份 1 找"),
  ],
  notes="设计稿 2.8 里“替换表出现链条时加载 WARN”当前代码没有实现，本用例只验证“只换一次”的行为。")

# ------------------------------------------------------------------------------------------------ RC018
case(id="RC018", dir="RC018_cdr_msgid", title="CDR MsgID：主路由 msgId，备份通道 msgId-{ssid}，IN_DELI_REP 总是 msgId",
  refs="P-01 P-02 P-03 P-04 P-09",
  purpose="逐级改道时每个通道自己的 OUT_SUBMIT 带自己的后缀，之后的 DR 类 CDR 带最后一个通道的后缀，IN_DELI_REP 是 msgId；没改道的 MT 全部是 msgId；失败 DR 的 CDR 带最后发送的通道。",
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV5)\n"
    "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=44,Relay=RTVNR)\nA2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV3)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4,rtv5,rtvnr",
  vendor_cfg={"rtv1": {"submitStatus": "0x45"}, "rtv2": {"submitStatus": "0x45"}, "rtv5": {"submitStatus": "0x45"}, "rtvnr": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8615000001801","rtv1>rtv2>rtv3","[MT-REROUTE] level=2 from=9812 to=9813","DELIVRD","P-02、P-03、P-04：OUT_SUBMIT 依次 msgId、msgId-9812、msgId-9813；之后的 OUT_DELI_REP msgId-9813；IN_DELI_REP msgId",
     None, {"cdr": "IN_SUBMIT={id};OUT_SUBMIT={id};OUT_SUBMIT={id}-9812;OUT_SUBMIT={id}-9813;OUT_DELI_REP={id}-9813;IN_DELI_REP={id}"}),
    ("OTP","64210001802","rtv4","by main prefix=64 priority=10","DELIVRD","P-01：没改道，全部 msgId",
     None, {"cdr": "IN_SUBMIT={id};OUT_SUBMIT={id};OUT_DELI_REP={id};IN_DELI_REP={id}"}),
    ("OTP","44770001803","rtv5>rtvnr","status 2300 not in SMSOptionSubmitRetryCode of rssid 9830","!DELIVRD","P-09：备份 RTVNR 失败、不再改道，失败 DR 的 CDR 带 -9830",
     None, {"cdr": "OUT_SUBMIT={id};OUT_SUBMIT={id}-9830"}),
  ])

# ------------------------------------------------------------------------------------------------ RC019
case(id="RC019", dir="RC019_long_sms_reroute", title="长短信：第 1 段失败改道，整条按新通道重新拆分；后面的段跟第 1 段的通道和级别",
  refs="O-01 O-03 O-05",
  purpose="3 段的长短信（UDH），主路由的通道 RTV1 回第 1 段失败：整条改道到备份 RTV2，按 RTV2 重新拆分发出，所有段都在 RTV2，客户收到 DELIVRD。",
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)\nA2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2)\n"},
  vendors="rtv1,rtv2",
  vendor_cfg={"rtv1": {"submitStatus": "0x45"}},
  mts=[
    ("OTP","8615000001901","rtv1>rtv2","[MT-REROUTE] level=1 from=9811 to=9812","DELIVRD","O-01、O-03：长短信 3 段，第 1 段在 RTV1 失败，整条到 RTV2；RTV2 收到全部的段（按内容拼回来和原文一样）",
     None, {"parts": "3", "segments": "rtv2:3"}),
  ])

# ------------------------------------------------------------------------------------------------ RC020
case(id="RC020", dir="RC020_reload_file_failure", title="RELOAD_ROUTING：一个文件写错只有它失败、保留原来的路由，其他文件照常生效；旧配置名 WARN 不生效",
  refs="A-08 A-15 A-17 A-18 A-20",
  purpose="第 1 阶段用正确的文件；第 2 阶段把 RTCUST 的文件改成同一前缀两条 Priority 10（写错），新增客户 RTCUST7 的文件（带一行旧配置名），发 RELOAD_ROUTING：RTCUST 保留原来的路由，RTCUST7 的新文件生效、旧配置名只 WARN。",
  cm_extra=customer("RTCUST7", 9807, "rtcust7"),
  routing={"RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)\n"},
  phase2={"afterMs": "0", "routing": {
            "RTCUST": "CCBSSID:9801\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV3);(Priority=10,PhonePrefix=86,Sender=BANK,Relay=RTV4)\n",
            "RTCUST7": "CCBSSID:9807\nA2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV2)\nA2PRoutingRelay:(PhonePrefix=86,Relay=RTV5)\n"},
          "reload": "RELOAD_ROUTING"},
  vendors="rtv1,rtv2",
  mts=[
    ("OTP","8613800002001","rtv1","by main prefix=86 priority=10 file=RTCUST","DELIVRD","第 1 阶段：RTCUST 的正确文件"),
  ],
  mts2=[
    ("OTP","8613800002002","rtv1","by main prefix=86 priority=10 file=RTCUST","DELIVRD","A-18：RTCUST 的新文件有错（同一前缀两条 Priority 10），这个文件失败、保留原来的路由 RTV1（不是 RTV3）"),
    ("OTP","8613800002003","rtv2","by main prefix=86 priority=10 file=RTCUST7","DELIVRD","A-17、A-15：RTCUST7 的新文件生效；旧的 A2PRoutingRelay 不生效（不走 RTV5）", None, {"as": "rtcust7"}),
  ],
  notes="第 2 阶段的 RELOAD_ROUTING 由 run_case.sh 发到 state Redis 的 `channel:gmms:control`（和 telnet 7000 的 smsui 发的是同一个命令），不重启模块。RELOAD_ROUTING 只加载有变化的文件。",
  checks=["check.log.1=CoreEngine1|routing file RTCUST not loaded, it keeps its previous routing",
          "check.log.2=CoreEngine1|have the same Priority=10 for 86",
          "check.log.3=CoreEngine1|A2PRoutingRelay: the old routing config name is not used any more",
          "check.redis.1=routingInfoCoreEngine1.Rules:RTCUST|main:0:86:10|\"Relay\":\"9811\"",
          "check.redis.2=routingInfoCoreEngine1.Rules:RTCUST7|main:0:86:10|\"Relay\":\"9812\"",
          "check.redis.3=routingInfoCoreEngine1.Rules:RTCUST7|_warnings|A2PRoutingRelay"])

# ------------------------------------------------------------------------------------------------ RC021
case(id="RC021", dir="RC021_rules_in_redis_and_logs", title="Redis 里的实际规则（Rules:{文件}）和选路 / 改道日志的格式",
  refs="Q-01 Q-02 Q-03 Q-05 C-05 A-02 A-15",
  purpose="Core 启动后 routingInfo{Core}.Rules:{文件} 是内存里实际在用的规则（Relay 为 ssid，_file、_warnings）；[MT-ROUTE] / [MT-REROUTE] / 不改道的日志格式和《路由配置说明》第 8 节一致；MT 的 routingRule 字段。",
  routing={"RTCUST": "CCBSSID:9801\nA2PNumberRouting:(Priority=1,Number=8613800002199,Relay=RTV3)\n"
    "A2PMainRouting:(Priority=10,PhonePrefix=86;852,Sender=BANK,Relay=RTV1);(Priority=50,PhonePrefix=86,Relay=RTV1:70,RTV2:30)\nA2PBackupRouting:(Priority=10,PhonePrefix=all,Relay=RTV4)\n"
    "A2PRoutingRelay:(PhonePrefix=86,Relay=RTV9)\n"},
  vendors="rtv1,rtv2,rtv3,rtv4",
  vendor_cfg={"rtv3": {"submitStatus": "0x45"}},
  mts=[
    ("BANK","8613800002101","rtv1","[MT-ROUTE] ossid=9801 rssid=9811 by main prefix=86 priority=10 file=RTCUST","DELIVRD","Q-01：选路日志"),
    ("OTP","8613800002199","rtv3>rtv4","by number number=8613800002199 priority=1&&[MT-REROUTE] level=1 from=9813 to=9814 code=2300 attempt=1 by main backup1 prefix=all priority=10 file=RTCUST","DELIVRD","Q-02：号码层的通道失败，用普通层的备份；改道日志"),
  ],
  checks=["check.redis.1=routingInfoCoreEngine1.Rules:RTCUST|_file|CCBSSID=9801",
          "check.redis.2=routingInfoCoreEngine1.Rules:RTCUST|_warnings|A2PRoutingRelay",
          "check.redis.3=routingInfoCoreEngine1.Rules:RTCUST|main:0:852:10|\"Relay\":\"9811\"",
          "check.redis.4=routingInfoCoreEngine1.Rules:RTCUST|main:0:86:50|\"Relay\":\"9811:70,9812:30\"",
          "check.redis.5=routingInfoCoreEngine1.Rules:RTCUST|main:1:all:10|\"Source\":\"RTCUST A2PBackupRouting #1\"",
          "check.redis.6=routingInfoCoreEngine1.Rules:RTCUST|number:0:8613800002199:1|\"Relay\":\"9813\""])

# ------------------------------------------------------------------------------------------------ RC022
BAD = {"RTCA05": (9862, "(PhonePrefix=86,Relay=RTV1)", "A2PMainRouting #1: no Priority (1-100)"),
       "RTCA06A": (9863, "(Priority=0,PhonePrefix=86,Relay=RTV1)", "A2PMainRouting #1: Priority=0 is not 1-100"),
       "RTCA06B": (9864, "(Priority=101,PhonePrefix=86,Relay=RTV1)", "A2PMainRouting #1: Priority=101 is not 1-100"),
       "RTCA06C": (9865, "(Priority=abc,PhonePrefix=86,Relay=RTV1)", "A2PMainRouting #1: Priority=abc is not a number (1-100)"),
       "RTCA07": (9866, "(Priority=10,PhonePrefix=86)", "A2PMainRouting #1: no Relay"),
       "RTCA11": (9867, "(Priority=10,PhonePrefix=86,Sender=BANK,ExcludeSender=bank,Relay=RTV1)", "bank is both in Sender and in ExcludeSender"),
       "RTCA12": (9868, "(Priority=10,PhonePrefix=86,ExcludePhonePrefix=44,Relay=RTV1)", "ExcludePhonePrefix=44 is not a part of PhonePrefix")}
GOOD = ("CCBSSID:9861\n"
  "A2PMainRouting:(Priority=10,PhonePrefix=86,Sender=BANK;PAY,ExcludeSender=OTP,KeywordId=RTK1,ExcludeKeywordId=RTK2,ExcludePhonePrefix=8615,Relay=RTV1)"
  ";(Priority=20,Relay=RTV2);(Priority=30,PhonePrefix=all,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTV4:100,RTV5:0)\n"
  "A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV6)\n"
  + "".join(f"A2PBackup{n}Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)\n" for n in range(2, 11))
  + "A2POPBackup3Routing:(Priority=10,Opid=4601,Relay=RTV8)\n"
  "A2PSecBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV9)\n"
  "A2PBackup11Routing:(Priority=10,PhonePrefix=86,Relay=RTV9)\n"
  "A2PServiceTypeIDRoutingRelay:(PhonePrefix=86,Relay=RTV9)\nA2PContentRoutingRelay:(PhonePrefix=86,Relay=RTV9)\n"
  "A2PSenderAddrRoutingRelay:(PhonePrefix=86,Relay=RTV9)\nA2PContentRouting:(PhonePrefix=86,Relay=RTV9)\n")
K = "routingInfoCoreEngine1.Rules:RTCA01"
case(id="RC022", dir="RC022_load_validation", title="路由文件的加载和校验：属性、备份各级配置名、不生效的配置名 WARN、每种写错的文件只有它自己加载失败",
  refs="A-01 A-03 A-04 A-05 A-06 A-07 A-09 A-10 A-11 A-12 A-13 A-14 A-16",
  purpose="模块启动时加载 8 个路由文件：RTCA01 写了所有属性、备份 1～10 级、运营商层和二次层的备份、第 11 级和旧配置名；其余 7 个各有一种错误。不发消息，只看 Core 日志和 Redis 里实际在用的规则。",
  cm_extra=customer("RTCA01", 9861, "rtca01") + "".join(customer(n, v[0], n.lower()) for n, v in BAD.items()),
  routing=dict([("RTCA01", GOOD)] + [(n, f"CCBSSID:{v[0]}\nA2PMainRouting:{v[1]}\n") for n, v in BAD.items()]),
  vendors="",
  mts=[],
  notes="每个路由文件对应 cm.extra.cfg 里同名的客户（RTCA01 9861，RTCA05～RTCA12 9862～9868）。RTCA05～RTCA12 这些文件在启动时加载失败，客户没有路由（启动时没有“原来的路由”可以保留）。错误的原文出自 `RuleSet.parse`；如果日志里的文件名写法不同（例如带 `_routingInfo.cfg`），第一次执行时按 evidence 修正检查文本。",
  checks=[f"check.log.{i}=CoreEngine1|routing file {n}|{v[2]}" for i, (n, v) in enumerate(BAD.items(), 1)]
    + [f"check.redis.{i}={K}|{f}|{t}" for i, (f, t) in enumerate([
        ("main:0:86:10", '"ExcludePhonePrefix":"8615"'), ("main:0:86:10", '"ExcludeSender":"otp"'), ("main:0:86:10", '"Sender":"bank;pay"'),
        ("main:0:86:10", '"Relay":"9811"'), ("main:0:all:20", '"Relay":"9812"'), ("main:0:all:30", '"Relay":"9813"'),
        ("main:0:44:10", '"Relay":"9814:100,9815:0"'), ("main:1:86:10", '"Relay":"9816"'), ("main:2:86:10", '"Relay":"9817"'),
        ("main:10:86:10", '"Relay":"9817"'), ("op:3:4601:10", '"Relay":"9818"'), ("second:2:86:10", '"Relay":"9819"'),
        ("_warnings", "A2PBackup11Routing: backup level 11 is not supported"),
        ("_warnings", "A2PServiceTypeIDRoutingRelay: the old routing config name is not used any more"),
        ("_warnings", "A2PContentRoutingRelay: the old routing config name is not used any more"),
        ("_warnings", "A2PSenderAddrRoutingRelay: the old routing config name is not used any more"),
        ("_warnings", "A2PContentRouting: the old routing config name is not used any more")], 1)]
    + [f"check.redisAbsent.{i}=routingInfoCoreEngine1.Rules:{n}" for i, n in enumerate(BAD, 1)])

# ================================================================================================ write
def fmt_mt(key, m):
    sender, rcpt, path, log, dr, why = m[:6]
    lines = [f"# {why}", f"{key}={sender}|{rcpt}|{path}|{log}|{dr}"]
    text = m[6] if len(m) > 6 else None
    opts = m[7] if len(m) > 7 else {}
    if text:
        lines.append(f"{key}.text={text}")
    for k, v in opts.items():
        if k == "as":
            lines.append(f"{key}.customer={v}")
        else:
            lines.append(f"{key}.{k}={v}")
    return "\n".join(lines)

def md_table(mts, label):
    rows = [f"| {label} | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |", "|---|---|---|---|---|---|"]
    for i, m in enumerate(mts, 1):
        sender, rcpt, path, log, dr, why = m[:6]
        opts = m[7] if len(m) > 7 else {}
        extra = []
        if len(m) > 6 and m[6]: extra.append(f"内容 `{m[6]}`")
        for k, v in opts.items():
            extra.append({"as": "客户", "after": "前一条之后（ms）", "count": "条数", "ratio": "比例", "waitMs": "最多等（ms）",
                          "db": "DB 行", "cdr": "CDR MsgID", "parts": "段数", "segments": "通道收到的段", "customer": "客户", "text": "内容",
                          "vendorDr": "通道 DR 的 stat", "vendorDrErr": "通道 DR 的 err", "vendorTlv": "通道 DR 的 network_error_code",
                          "drErr": "客户收到的 err", "drTlv": "客户收到的 network_error_code",
                          "drCount": "客户收到的 DR 条数", "drDelayMs": "DR 到达（ms，发出后）", "drRatio": "DR 的比例",
                          "vendorDrDelayMs": "通道多久回 DR（ms）", "vendorSubmitStatus": "通道的 submit_sm_resp"}.get(k, k) + f" `{v}`")
        logcell = "<br>".join(f"`{x}`" for x in log.split("&&")) if log else ""
        rows.append(f"| {i} | {sender} → {rcpt} | {path.replace('|', ' 或 ')} | {logcell} | {dr} | {why}" + ("；" + "；".join(extra) if extra else "") + " |")
    return "\n".join(rows)

out = "cases"
for c in CASES:
    d = os.path.join(out, c["dir"])
    if os.path.isdir(d):
        shutil.rmtree(d)
    os.makedirs(os.path.join(d, "routing"))
    for name, text in c["routing"].items():
        open(os.path.join(d, "routing", name + "_routingInfo.cfg"), "w").write(text)
    if c.get("cm_extra"):
        open(os.path.join(d, "cm.extra.cfg"), "w").write(c["cm_extra"])
    for kind in ("conf", "conf_append", "conf_sed", "gmms_append"):
        for name, text in c.get(kind, {}).items():
            os.makedirs(os.path.join(d, kind.replace("_", ".")), exist_ok=True)
            open(os.path.join(d, kind.replace("_", "."), name), "w").write(text)
    p2 = c.get("phase2")
    if p2:
        for name, text in p2.get("routing", {}).items():
            os.makedirs(os.path.join(d, "phase2", "routing"), exist_ok=True)
            open(os.path.join(d, "phase2", "routing", name + "_routingInfo.cfg"), "w").write(text)
        for name, text in p2.get("conf", {}).items():
            os.makedirs(os.path.join(d, "phase2", "conf"), exist_ok=True)
            open(os.path.join(d, "phase2", "conf", name), "w").write(text)
        if p2.get("gmms"):
            os.makedirs(os.path.join(d, "phase2", "gmms.append"), exist_ok=True)
            open(os.path.join(d, "phase2", "gmms.append", "GmmsConfig.properties"), "w").write(p2["gmms"])
    props = [f"# {c['id']} {c['title']}", f"# 用例清单：{c['refs']}", f"case.id={c['id']}", f"case.title={c['title']}", f"case.refs={c['refs']}",
             f"vendors={c['vendors']}"]
    for k, v in c.get("props", {}).items():
        props.append(f"{k}={v}")
    for v, cfg in c.get("vendor_cfg", {}).items():
        for k, x in cfg.items():
            props.append(f"vendor.{v}.{k}={x}")
    props.append("# mt.N = 发送方 | 号码 | 预期通道（a>b 依次收到；(a|b) 其中一个；none 不发出） | Core 日志里要有的文本（&& 分开，都要有） | 客户收到的 DR（!DELIVRD = 任何失败）")
    for i, m in enumerate(c["mts"], 1):
        props.append(fmt_mt(f"mt.{i}", m))
    if p2:
        props.append(f"phase.2.afterMs={p2.get('afterMs', '0')}")
        if p2.get("reload"):
            props.append(f"phase.2.reload={p2['reload']}")
        for i, m in enumerate(c.get("mts2", []), 1):
            props.append(fmt_mt(f"mt2.{i}", m))
    for ch in c.get("checks", []):
        props.append(ch)
    open(os.path.join(d, "case.properties"), "w").write("\n".join(props) + "\n")
    # README
    r = [f"# {c['id']} {c['title']}", "", f"**对应用例清单**：{c['refs']}（{c.get('refdoc', '`features/01_路由重构/路由重构测试用例清单.md`')}）", "", f"**目的**：{c['purpose']}", "",
         "## 配置", ""]
    for name, text in c["routing"].items():
        r += [f"`routing/{name}_routingInfo.cfg`：", "", "```text", text.strip(), "```", ""]
    if c.get("cm_extra"):
        blocks = [l[1:-1] for l in c["cm_extra"].splitlines() if l.startswith("[")]
        r += [f"`cm.extra.cfg`（追加到 base/cm.cfg）：{', '.join(blocks)}。", ""]
    for kind, label in (("conf", "运行时替换 conf 下的文件，结束后恢复"), ("conf_append", "运行时追加到 conf 下的文件，结束后恢复"), ("conf_sed", "运行时用这个 sed 脚本改 conf 下的同名文件，结束后恢复"),
                        ("gmms_append", "运行时追加到 Gmms/GmmsConfig.properties（后写的生效），结束后恢复")):
        for name, text in c.get(kind, {}).items():
            r += [f"`{kind.replace('_', '.')}/{name}`（{label}）：", "", "```text", text.strip(), "```", ""]
    if c.get("vendor_cfg"):
        r += ["通道（测试工具）的行为：", "", "| 通道 | 行为 |", "|---|---|"]
        for v, cfg in c["vendor_cfg"].items():
            r.append(f"| {v}（ssid {SSID.get(v, '见 cm.extra.cfg')}） | " + "，".join(f"{k}={x}" for k, x in cfg.items()) + " |")
        r += ["", "（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）", ""]
    r += ["## 步骤和预期", "", md_table(c["mts"], "MT"), ""]
    if p2:
        what = []
        if p2.get("routing"): what.append("换成 `phase2/routing/` 的路由文件")
        if p2.get("conf"): what.append("换成 `phase2/conf/` 的文件")
        if p2.get("gmms"): what.append("在 GmmsConfig 后面追加 `phase2/gmms.append/GmmsConfig.properties`（" + "、".join("`" + l + "`" for l in p2["gmms"].split("\n") if l.strip()) + "）")
        if p2.get("reload"): what.append(f"发 `{p2['reload']}`")
        r += [f"**第 2 阶段**：第 1 阶段的 MT 发完 {int(p2.get('afterMs', '0'))//1000} 秒后，" + "、".join(what) + "，再发：", ""]
        if c.get("mts2"):
            r += [md_table(c["mts2"], "MT"), ""]
    if c.get("checks"):
        r += ["**用例级检查**（这次运行期间）：", ""]
        for ch in c["checks"]:
            k, v = ch.split("=", 1)
            parts = v.split("|")
            if k.startswith("check.cmd") or k.startswith("check.redisCmd"):
                cmd, _, want = v.partition(" => ")
                r.append(f"- {'命令' if k.startswith('check.cmd') else 'Redis 命令'} `{cmd}` 的输出" + (f"等于 `{want[1:]}`" if want.startswith("=") else f"包含 `{want}`"))
                continue
            if k.startswith("check.metric"):
                r.append(f"- 指标日志（`{parts[0]}`）里本次运行期间有指标 `…{parts[1]}` 的 `{parts[2]}` 在 {parts[3]}～{parts[4]} 之间")
                continue
            if k.startswith("check.log"):
                r.append(f"- `{parts[0]}` 日志里有一行同时包含 " + "、".join(f"`{x}`" for x in parts[1:]))
            elif k.startswith("check.redisAbsent"):
                r.append(f"- Redis 里没有 `{parts[0]}`（这个文件没有加载成功）")
            elif k.startswith("check.redis"):
                r.append(f"- Redis `HGET {parts[0]} {parts[1]}` 包含 `{parts[2]}`")
        r.append("")
    r += ["## 验收标准", "",
          "- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR" + ("、CDR、DB" if any(len(m) > 7 and ("cdr" in m[7] or "db" in m[7]) for m in c["mts"]) else "") + "）" + ("，用例级检查全部通过" if c.get("checks") else "") + "；",
          "- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致" + ("；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。" if c.get("checks") else "。"), ""]
    if c.get("notes"):
        r += ["## 注意", "", c["notes"], ""]
    r += ["## 运行", "", f"`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/{c['dir']}`（换配置并重启模块）。", ""]
    open(os.path.join(d, "README.md"), "w").write("\n".join(r))
print(len(CASES), "cases written")
