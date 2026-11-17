# DE001 通道 DR → 内部错误码：stat+err、范围、TLV 优先、只按状态、DELIVRD 不看 err、只按 err、[DEFAULT] 块、对不上、欠费码优先

**对应用例清单**：DE-01 DE-02 DE-03 DE-04 DE-11 DE-13（`features/02_DR错误码/DR错误码设计.md` 第 8 节）

**目的**：通道回不同的 stat / err / network_error_code，Core 按 `conf/drErrorMapping.cfg`（10.4 的顺序）得到内部码，客户 RTCE1 配了 `DRErrDefault: passthrough`，收到的 `err:` 就是内部码；`stat:` 按内部状态生成（和原来一样，例如 10800 是 DELETED，设计稿 5.2）。

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
| 1 | OTP → 8613800003001 | rtve1 | `[DR-ERR] rssid=9871 vendor=stat=UNDELIV;err=001`<br>`code=101 status=10402` | UNDELIV | DE-01：stat+err 完全相等 → 101（10402）；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `001`；客户收到的 err `101` |
| 2 | OTP → 8613800003002 | rtve1 | `[DR-ERR] rssid=9871 vendor=stat=UNDELIV;err=5`<br>`code=104 status=10405` | UNDELIV | 10.3、10.4 第 3 步：err 5 按数值落在 002~009 → 104（10405）；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户收到的 err `104` |
| 3 | OTP → 8613800003003 | rtve1 | `nec=3:001B`<br>`code=103 status=10402` | UNDELIV | DE-02：带 network_error_code 3:001B，TLV 规则先于 stat+err（err 001 本来是 101）→ 103；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `001`；通道 DR 的 network_error_code `03001B`；客户收到的 err `103` |
| 4 | OTP → 8613800003004 | rtve1 | `code=202 status=10500` | REJECTD | 只写 Stat 的规则：REJECTD 其他 err → 202（10500），客户 stat:REJECTD；通道 DR 的 stat `REJECTD`；通道 DR 的 err `050`；客户收到的 err `202` |
| 5 | OTP → 8613800003005 | rtve1 | `code=200 status=10400` | UNDELIV | 只写 Stat：UNDELIV 其他 err → 200（10400）；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `099`；客户收到的 err `200` |
| 6 | OTP → 8613800003006 | rtve1 | `code=000 status=10000` | DELIVRD | 10.4：DELIVRD 不看 err（err 005 也是 000）；通道 DR 的 stat `DELIVRD`；通道 DR 的 err `005`；客户收到的 err `000` |
| 7 | OTP → 8613800003007 | rtve1 | `code=404 status=10800` | DELETED | DE-04、10.4 第 4 步（没写 Stat，err 相等）先于第 6 步（只写 Stat）：088 → 404 欠费（REJECTED_BYNC 10800）；客户回执的 stat 按内部状态，10800 是 DELETED（原有行为，5.2）；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `088`；客户收到的 err `404` |
| 8 | OTP → 8615000003008 | rtve2 | `[DR-ERR] rssid=9872 vendor=stat=UNDELIV;err=077`<br>`code=103 status=10402` | UNDELIV | RTVE2 没有自己的块，用 [DEFAULT] 块：077 → 103；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `077`；客户收到的 err `103` |
| 9 | OTP → 8615000003009 | rtve2 | `[DR-ERR-UNMAPPED] rssid=9872 channel=RTVE2 vendor=stat=UNDELIV;err=055`<br>`code=200 status=10400` | UNDELIV | DE-03：对不上：状态不变（10400），码取这个状态的码 200，WARN [DR-ERR-UNMAPPED]；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `055`；客户收到的 err `200` |
| 10 | OTP → 8616000003010 | rtve3 | `(SMSOptionDRNotPaidCode)`<br>`code=404` | DELETED | DE-13：RTVE3 的 SMSOptionDRNotPaidCode:088 先于它块里的 (Err=088,Code=101)：欠费 404，客户 stat:DELETED；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `088`；客户收到的 err `404` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

通道的 DR 由每条 MT 的 `vendorDr`、`vendorDrErr`、`vendorTlv` 指定（同一个通道不同 MT 回不同的 DR）。客户收到的 err 用 `drErr` 检查。服务器上没有 `conf/drErrorCode.cfg`，用内置码表（设计稿 10.1 加 209）。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/DE001_vendor_err_to_code`（换配置并重启模块）。
