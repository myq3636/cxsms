# Generates the server case of the module path PP001 (regression/routetest/cases/, docs/消息经过模块路径设计.md 3, 4;
# case.properties version 2, docs/路由服务器测试说明.md 4.2). Run from regression/routetest; rewrites the directory
# (its result/ too). The account templates and the writer are the ones of gen_routing_cases.py.
# The regexes have no backslash (case.properties would eat it): "MT-OUT]" (a lone "]" is literal; Java reads "[[]" as a nested class), [0-9] for a digit, (?![^ ]) for the end.
import os
_gen = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen_routing_cases.py"), encoding="utf-8").read()
_head, _rest = _gen.split("CASES = []", 1)
exec(_head)
CASES = []
def case(**k):
    CASES.append(k)

SSID.update({"rtvp1": 9941, "rtvp2": 9942})
ROUTES = ("A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVP2);(Priority=10,PhonePrefix=8615,Relay=RTVP1)\n"
          "A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVP2)\n")
C, CL, S = "CoreEngine[1-3]", "MultiSmppClient[12]", "MultiSmppServer"
END = "(?![^ ])"   # a space or the end, without "|" (it separates the fields of mt.N)
def mt_logs(server):
    return "&&".join([
        f"{{CoreEngine}}~MT-TO-CLIENT].* path={server}>{C}{END}",
        f"{{MultiSmppClient}}~MT-OUT].* path={server}>{C}>{CL}{END}",
        f"{{CoreEngine}}~DR-TO-SERVER].* path={CL}>{C}{END}",
        f"{{MultiSmppServer}}~DR-OUT].* path={CL}>{C}>{server}{END}"])
def mt_cdr(server):
    return ";".join([f'IN_SUBMIT={{id}}~"{server}:{C}"', f'OUT_SUBMIT={{id}}~"{server}:{C}:{CL}:{C}"',
                     f'OUT_DELI_REP={{id}}~"{CL}:{C}"', f'IN_DELI_REP={{id}}~"{CL}:{C}:{server}:{C}"'])

case(id="PP001", dir="PP001_module_path", title="消息经过的模块：Server / Core / Client 日志的 path、CDR 的 DeliveryChannel；两个 Server、改道、Core 生成的失败 DR、长短信",
  refs="设计稿第 4 节；PathChainIT PC-05～PC-12 在服务器上的对应", refdoc="`features/05_消息经过模块路径/消息经过模块路径设计.md`",
  cm_extra=vendor("RTVP1", 9941) + vendor("RTVP2", 9942) + customer("RTCPP", 9931, "rtcpp"),
  routing={"RTCUST": "CCBSSID:9801\n" + ROUTES, "RTCPP": "CCBSSID:9931\n" + ROUTES},
  vendors="rtvp1,rtvp2", vendor_cfg={"rtvp1": {"submitStatus": "0x45"}},
  props={"customer.rtcpp.port": "16273"},
  purpose="这台机器有 2 个 Server（RTCUST bind 到 MultiSmppServer1 16272，RTCPP bind 到 MultiSmppServer2 16273）、3 个 Core、2 个 Client。每条 MT 走哪个 Core、哪个 Client 不固定，用正则检查每一段：Core `[MT-TO-CLIENT]`、Client `[MT-OUT]`、Core `[DR-TO-SERVER]`、Server `[DR-OUT]` 的 `path=`，以及 4 种 CDR 的 DeliveryChannel 列（`:` 分隔，最后一段是写 CDR 的 Core）。",
  notes="日志检查的 `{CoreEngine}`、`{MultiSmppClient}`、`{MultiSmppServer}` 指定在哪类模块的日志里找，`~` 后面是正则（不能有反斜杠）。路径和 CDR 列的具体值（经过了哪个 Core、哪个 Client）在每条 MT 的 evidence 里。",
  mts=[
    ("OTP","8613800006001","rtvp2",mt_logs(S + "1"),"DELIVRD","PC-05～PC-12：经过 MultiSmppServer1 的 MT 和它的 DR，日志 4 处、CDR 4 种", None, {"cdr": mt_cdr(S + "1")}),
    ("OTP","8613800006002","rtvp2",mt_logs(S + "2"),"DELIVRD","同上，经过 MultiSmppServer2；DR 回到 MultiSmppServer2", None, {"customer": "rtcpp", "cdr": mt_cdr(S + "2")}),
    ("OTP","8615000006003","rtvp1>rtvp2",
     f"{{MultiSmppClient}}~MT-OUT].* path={S}1>{C}>{CL}{END}&&{{MultiSmppClient}}~MT-OUT].* path={S}1>{C}>{CL}>{C}>{CL}{END}","DELIVRD",
     "改道：第二次外发的路径接着第一次（Client → Core 处理结果 → 另一次 Client）", None, {"cdr": f'OUT_SUBMIT={{id}}-9942~"{S}1:{C}:{CL}:{C}:{CL}:{C}"'}),
    ("OTP","447700006004","none",
     f"no backup route either&&{{CoreEngine}}~DR-TO-SERVER].* path={S}1>{C}{END}&&{{MultiSmppServer}}~DR-OUT].* path={S}1>{C}>{S}1{END}","!DELIVRD",
     "路由失败，Core 生成的失败 DR 带着 MT 的路径（不是从 Client 开始）"),
    ("OTP","8613800006005","rtvp2",f"{{MultiSmppClient}}~MT-OUT].* path={S}1>{C}>{CL}{END}","DELIVRD","长短信 3 段：Core 拼好再拆分发出，每段的 [MT-OUT] 都有路径", None,
     {"parts": "3", "segments": "rtvp2:3"}),
  ])

exec(_rest.split("# ================================================================================================ write", 1)[1])
