# PP001 消息经过的模块：Server / Core / Client 日志的 path、CDR 的 DeliveryChannel；两个 Server、改道、Core 生成的失败 DR、长短信

**对应用例清单**：设计稿第 4 节；PathChainIT PC-05～PC-12 在服务器上的对应（`features/05_消息经过模块路径/消息经过模块路径设计.md`）

**目的**：这台机器有 2 个 Server（RTCUST bind 到 MultiSmppServer1 16272，RTCPP bind 到 MultiSmppServer2 16273）、3 个 Core、2 个 Client。每条 MT 走哪个 Core、哪个 Client 不固定，用正则检查每一段：Core `[MT-TO-CLIENT]`、Client `[MT-OUT]`、Core `[DR-TO-SERVER]`、Server `[DR-OUT]` 的 `path=`，以及 4 种 CDR 的 DeliveryChannel 列（`:` 分隔，最后一段是写 CDR 的 Core）。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVP2);(Priority=10,PhonePrefix=8615,Relay=RTVP1)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVP2)
```

`routing/RTCPP_routingInfo.cfg`：

```text
CCBSSID:9931
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVP2);(Priority=10,PhonePrefix=8615,Relay=RTVP1)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVP2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVP1, RTVP2, RTCPP。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvp1（ssid 9941） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800006001 | rtvp2 | `{CoreEngine}~MT-TO-CLIENT].* path=MultiSmppServer1>CoreEngine[1-3](?![^ ])`<br>`{MultiSmppClient}~MT-OUT].* path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppClient[12](?![^ ])`<br>`{CoreEngine}~DR-TO-SERVER].* path=MultiSmppClient[12]>CoreEngine[1-3](?![^ ])`<br>`{MultiSmppServer}~DR-OUT].* path=MultiSmppClient[12]>CoreEngine[1-3]>MultiSmppServer1(?![^ ])` | DELIVRD | PC-05～PC-12：经过 MultiSmppServer1 的 MT 和它的 DR，日志 4 处、CDR 4 种；CDR MsgID `IN_SUBMIT={id}~"MultiSmppServer1:CoreEngine[1-3]";OUT_SUBMIT={id}~"MultiSmppServer1:CoreEngine[1-3]:MultiSmppClient[12]:CoreEngine[1-3]";OUT_DELI_REP={id}~"MultiSmppClient[12]:CoreEngine[1-3]";IN_DELI_REP={id}~"MultiSmppClient[12]:CoreEngine[1-3]:MultiSmppServer1:CoreEngine[1-3]"` |
| 2 | OTP → 8613800006002 | rtvp2 | `{CoreEngine}~MT-TO-CLIENT].* path=MultiSmppServer2>CoreEngine[1-3](?![^ ])`<br>`{MultiSmppClient}~MT-OUT].* path=MultiSmppServer2>CoreEngine[1-3]>MultiSmppClient[12](?![^ ])`<br>`{CoreEngine}~DR-TO-SERVER].* path=MultiSmppClient[12]>CoreEngine[1-3](?![^ ])`<br>`{MultiSmppServer}~DR-OUT].* path=MultiSmppClient[12]>CoreEngine[1-3]>MultiSmppServer2(?![^ ])` | DELIVRD | 同上，经过 MultiSmppServer2；DR 回到 MultiSmppServer2；客户 `rtcpp`；CDR MsgID `IN_SUBMIT={id}~"MultiSmppServer2:CoreEngine[1-3]";OUT_SUBMIT={id}~"MultiSmppServer2:CoreEngine[1-3]:MultiSmppClient[12]:CoreEngine[1-3]";OUT_DELI_REP={id}~"MultiSmppClient[12]:CoreEngine[1-3]";IN_DELI_REP={id}~"MultiSmppClient[12]:CoreEngine[1-3]:MultiSmppServer2:CoreEngine[1-3]"` |
| 3 | OTP → 8615000006003 | rtvp1>rtvp2 | `{MultiSmppClient}~MT-OUT].* path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppClient[12](?![^ ])`<br>`{MultiSmppClient}~MT-OUT].* path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppClient[12]>CoreEngine[1-3]>MultiSmppClient[12](?![^ ])` | DELIVRD | 改道：第二次外发的路径接着第一次（Client → Core 处理结果 → 另一次 Client）；CDR MsgID `OUT_SUBMIT={id}-9942~"MultiSmppServer1:CoreEngine[1-3]:MultiSmppClient[12]:CoreEngine[1-3]:MultiSmppClient[12]:CoreEngine[1-3]"` |
| 4 | OTP → 447700006004 | none | `no backup route either`<br>`{CoreEngine}~DR-TO-SERVER].* path=MultiSmppServer1>CoreEngine[1-3](?![^ ])`<br>`{MultiSmppServer}~DR-OUT].* path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppServer1(?![^ ])` | !DELIVRD | 路由失败，Core 生成的失败 DR 带着 MT 的路径（不是从 Client 开始） |
| 5 | OTP → 8613800006005 | rtvp2 | `{MultiSmppClient}~MT-OUT].* path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppClient[12](?![^ ])` | DELIVRD | 长短信 3 段：Core 拼好再拆分发出，每段的 [MT-OUT] 都有路径；段数 `3`；通道收到的段 `rtvp2:3` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

日志检查的 `{CoreEngine}`、`{MultiSmppClient}`、`{MultiSmppServer}` 指定在哪类模块的日志里找，`~` 后面是正则（不能有反斜杠）。路径和 CDR 列的具体值（经过了哪个 Core、哪个 Client）在每条 MT 的 evidence 里。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/PP001_module_path`（换配置并重启模块）。
