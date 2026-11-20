# Generates the many-connection performance case PF008_multiconn (regression/routetest/cases/): N customers with B binds
# each, every other one on MultiSmppServer2 (16273), the others on MultiSmppServer1 (16272); N channels with B binds
# each (SessionNumber is for the channel, split over the 2 Clients); customer i sends to channel i only (prefix 87 + i).
#   python3 bin/gen_multiconn_case.py [N] [B] [TPS] [SHARDS]   (run from regression/routetest; default 2000 8 1000 8)
# SHARDS: StreamShardsDefault of the case (gmms.append), the shards of each ssid's streams; 0 = TotalShards (64).
# Rewrites cases/PF008_multiconn (its result/ too). Run it with bin/perf_step.sh cases/PF008_multiconn "1000 ..." --no-replicas.
import os, shutil, sys

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
B = int(sys.argv[2]) if len(sys.argv) > 2 else 8
TPS = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
SHARDS = int(sys.argv[4]) if len(sys.argv) > 4 else 8
CUST_SSID, VEND_SSID = 20000, 30000
DIR = os.path.join("cases", "PF008_multiconn")

def cust(i):
    return f"rtc{i:04d}"

def vend(i):
    return f"rtv{i:04d}"

def prefix(i):
    return f"87{i:04d}"

def customer_block(i):
    u = cust(i)
    return f"""[{u.upper()}]
A2PPlayerType:Operator
Service:A2P-SMS
CCBSSID:{CUST_SSID + i}
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
SMSOptionOriNumberLenList:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
SMSOptionRecNumberLenList:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
DeliveryReportMode:1
SupportMsgSplit:yes
SmsOptionIsSupportUDH:yes
ConnectionType:MC
IncomingConnection:(ConnName={u.upper()}_1,BindMode=TR,IsInit=yes,IsPersistent=yes,password=rtpw,SessionNumber={B},UserName={u},Version=V34)
SMSOptionIncomingThrottlingNum:10000
WindowSize:5000
CustomerServiceType:RTSVCS

"""

def vendor_block(i):
    v = vend(i)
    return f"""[{v.upper()}]
A2PPlayerType:Operator
Role:Vendor
Service:A2P-SMS
CCBSSID:{VEND_SSID + i}
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
SMSOptionOriNumberLenList:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
SMSOptionRecNumberLenList:1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
SupportMsgSplit:yes
SmsOptionIsSupportUDH:yes
ChlUdhConcatenated:yes
ChlMsgSplit:yes
ChlQueue:MultiSmppClient
ConnectionType:MC
OutgoingConnection:(ConnName={v.upper()}_{VEND_SSID + i},BindMode=TR,IsInit=yes,IsPersistent=yes,password=rtpw,Port=27001,SessionNumber={B},URL=172.19.4.38,UserName={v},Version=V34)
EnquireLinkTime:40
IsPersistent:yes
SMSOptionSubmitRetryCode:2300,2500,2600
SMSOptionMTRetryPolicy:1M,1M
SMSOptionOutgoingThrottlingNum:10000
WindowSize:1000

"""

shutil.rmtree(DIR, ignore_errors=True)
os.makedirs(os.path.join(DIR, "routing"))
os.makedirs(os.path.join(DIR, "gmms.append"))
with open(os.path.join(DIR, "gmms.append", "GmmsConfig.properties"), "w", encoding="utf-8") as f:
    f.write(f"# PF008: each ssid's streams on {SHARDS or 'all the'} shards (docs/多连接性能分析与优化设计.md 3.1)\nStreamShardsDefault={SHARDS}\n")
with open(os.path.join(DIR, "cm.extra.cfg"), "w", encoding="utf-8") as f:
    f.write("\n\n")
    for i in range(1, N + 1):
        f.write(vendor_block(i))
    for i in range(1, N + 1):
        f.write(customer_block(i))
with open(os.path.join(DIR, "routing", "RTSVCS_default_routingInfo.cfg"), "w", encoding="utf-8") as f:
    f.write("A2PMainRouting:" + ";".join(f"(Priority=10,PhonePrefix={prefix(i)},Relay={vend(i).upper()})" for i in range(1, N + 1)) + "\n")
lines = [
    f"# PF008 多连接：{N} 个客户、{N} 个通道，各 {B} 个 bind；客户 i 只发通道 i（号码前缀 87 + 4 位序号）。",
    f"# 生成：python3 bin/gen_multiconn_case.py {N} {B} {TPS} {SHARDS}；运行：bin/perf_step.sh cases/PF008_multiconn \"1000 2000 ...\" --no-replicas",
    "case.id=PF008",
    f"case.title=多连接：{N} 个客户、{N} 个通道各 {B} 个 bind，启动和每档 10 分钟的积压",
    "case.refs=docs/性能测试部署与执行手册.md",
    "vendors=" + ",".join(vend(i) for i in range(1, N + 1)),
    f"vendor.minLinks={B}",
    "vendor.bindWaitMs=1800000",
    f"customer.user={cust(1)}",
    f"soak.tps={TPS}",
    "soak.durationSec=600",
    "soak.warmupSec=60",
    "soak.drainSec=300",
    "soak.idleSec=5",
    "soak.sampleSec=30",
    f"soak.binds={B}",
    "soak.reconnectSec=0",
    "soak.reloadSec=0",
    "soak.memwatch=false",
]
for i in range(1, N + 1):
    if i % 2 == 0:
        lines.append(f"customer.{cust(i)}.port=16273")
for i in range(1, N + 1):
    lines.append(f"mt.{i}=MC{i:04d}|{prefix(i)}|*||")
    lines.append(f"mt.{i}.customer={cust(i)}")
with open(os.path.join(DIR, "case.properties"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
with open(os.path.join(DIR, "README.md"), "w", encoding="utf-8") as f:
    f.write(f"""# PF008 多连接：{N} 个客户、{N} 个通道，各 {B} 个 bind

由 `bin/gen_multiconn_case.py {N} {B} {TPS} {SHARDS}` 生成，不要手改。

- `gmms.append`：`StreamShardsDefault={SHARDS}`，每个客户、通道的 stream 只用 {SHARDS or 'TotalShards'} 个分片（0 = 全部 64 个，即改进前的样子；《多连接性能分析与优化设计》3.1）。

- 客户 `{cust(1)}`～`{cust(N)}`（CCBSSID {CUST_SSID + 1}～{CUST_SSID + N}），每个 {B} 个 bind（`IncomingConnection` 的 `SessionNumber={B}`，测试工具 `soak.binds={B}`）；序号为奇数的连 MultiSmppServer1（16272），偶数的连 MultiSmppServer2（16273），每个 Server {N // 2} 个客户、{N // 2 * B} 个 bind。
- 通道 `{vend(1)}`～`{vend(N)}`（CCBSSID {VEND_SSID + 1}～{VEND_SSID + N}），每个 {B} 个 bind（`SessionNumber={B}` 是整个通道的会话数，两个 Client 各 {B // 2} 个），每个 Client {N * B // 2} 个 bind，都连测试工具的 27001。测试工具等每个通道 {B} 个 bind 都到齐（`vendor.minLinks`，最多等 30 分钟）才开始发。
- 路由：号码前缀 `87` + 4 位序号 → 同序号的通道；客户 i 只发 `87{{i:04d}}` 开头的号码，所以每个通道的流量来自一个客户。
- 流量平均分到 {N} 个客户：{TPS} TPS 时每个客户约 {TPS / N:g} TPS。每档 10 分钟（预热 60 秒，排空 300 秒）。

运行：`bin/perf_step.sh cases/PF008_multiconn "1000 2000 ..." --no-replicas`（《性能测试部署与执行手册》4.2）。测试工具每个连接一个线程（2 × {N * B} 个连接），运行前 `vm.max_map_count` 要调大（默认 65530 不够）。
""")
print(f"{DIR}: {N} customers, {N} channels, {B} binds each, {TPS} TPS")
