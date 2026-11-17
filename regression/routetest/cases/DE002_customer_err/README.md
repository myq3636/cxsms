# DE002 内部码 → 客户：没有客户块（legacy）、passthrough、CustomerErr 精确和范围、vendor、000、DRErrTLV、旧配置 SMSOptionSmppMapErrCod4DR

**对应用例清单**：DE-05 DE-06 DE-13（`features/02_DR错误码/DR错误码设计.md` 第 8 节）

**目的**：通道 RTVE1 回 `stat:UNDELIV err:5`（内部码 104，10405；M4 是 `err:001` → 101），不同客户按自己的块（10.5）收到不同的 err。

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
| 1 | OTP → 8613800003101 | rtve1 | `code=104 status=10405` | UNDELIV | 没有客户块：legacy 表，10405 → 016；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtcust`；客户收到的 err `016` |
| 2 | OTP → 8613800003102 | rtve1 | `code=104` | UNDELIV | passthrough：内部码 104；没配 DRErrTLV，没有 TLV；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce1`；客户收到的 err `104`；客户收到的 network_error_code `none` |
| 3 | OTP → 8613800003103 | rtve1 | `code=104` | UNDELIV | CustomerErr 范围 100~199 → 033；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce2`；客户收到的 err `033` |
| 4 | OTP → 8613800003104 | rtve1 | `code=101` | UNDELIV | CustomerErr 精确的 101 先于范围 → 011；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `001`；客户 `rtce2`；客户收到的 err `011` |
| 5 | OTP → 8613800003105 | rtve1 | `code=104` | UNDELIV | vendor：通道原始的 err 5，补 0 到 3 位 → 005；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce3`；客户收到的 err `005` |
| 6 | OTP → 8613800003106 | rtve1 | `code=104` | UNDELIV | 000：永远 000；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce4`；客户收到的 err `000` |
| 7 | OTP → 8613800003107 | rtve1 | `code=104` | UNDELIV | DE-06：passthrough + DRErrTLV：err:104，另带 network_error_code 类型 3、值 104（0x0068）；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce5`；客户收到的 err `104`；客户收到的 network_error_code `030068` |
| 8 | OTP → 8613800003108 | rtve1 | `code=104` | UNDELIV | DE-13：配了旧的 SMSOptionSmppMapErrCod4DR 的客户：不再生效，按 legacy → 016；通道 DR 的 stat `UNDELIV`；通道 DR 的 err `5`；客户 `rtce6`；客户收到的 err `016` |

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `RTCE6: SMSOptionSmppMapErrCod4DR is no longer used`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/DE002_customer_err`（换配置并重启模块）。
