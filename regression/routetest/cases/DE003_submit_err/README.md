# DE003 submit 失败的码：SubmitErr 精确、Status=*；客户的失败 DR 带这个码

**对应用例清单**：DE-08（`features/02_DR错误码/DR错误码设计.md` 第 8 节）

**目的**：通道 submit_sm_resp 失败（这些通道没有 SMSOptionSubmitRetryCode，失败就回失败 DR），Core 按 `SubmitErr` 得到内部码，客户的失败 DR 带它。

## 配置

`routing/RTSVCE_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVE1);(Priority=10,PhonePrefix=8615,Relay=RTVE2);(Priority=10,PhonePrefix=8616,Relay=RTVE3);(Priority=10,PhonePrefix=8617,Relay=RTVE4);(Priority=10,PhonePrefix=8618,Relay=RTVE5)
```

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVE1);(Priority=10,PhonePrefix=8615,Relay=RTVE2);(Priority=10,PhonePrefix=8616,Relay=RTVE3);(Priority=10,PhonePrefix=8617,Relay=RTVE4);(Priority=10,PhonePrefix=8618,Relay=RTVE5)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVE1, RTVE2, RTVE3, RTVE4, RTVE5, RTCE1, RTCE2, RTCE3, RTCE4, RTCE5, RTCE6。

`conf/drErrorMapping.cfg`（运行时替换 conf 下的文件，结束后恢复）：

```text
[DEFAULT]
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
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtve4（ssid 9874） | submitStatus=0x0B |
| rtve5（ssid 9875） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8617000003201 | rtve4 | `rssid=9874 vendor=submit=`<br>`code=102 (submit)` | !DELIVRD | DE-08：0x0B → 102，passthrough 客户收到 err:102；客户 `rtce1`；客户收到的 err `102` |
| 2 | OTP → 8618000003202 | rtve5 | `rssid=9875 vendor=submit=`<br>`code=601 (submit)` | !DELIVRD | 0x45 → Status=* → 601；客户 `rtce1`；客户收到的 err `601` |
| 3 | OTP → 8617000003203 | rtve4 | `code=102 (submit)` | !DELIVRD | 没有客户块（legacy）：102 的内部状态 10402 → 012；客户 `rtcust`；客户收到的 err `012` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/DE003_submit_err`（换配置并重启模块）。
