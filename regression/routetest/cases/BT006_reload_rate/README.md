# BT006 RELOAD_CUSTOMER 把每个 bind 从 20 改成 5：之后每个 bind 任意 1 秒 ≤ 6，不重启

**对应用例清单**：BR-06（`features/06_bind级限速/SMPP外发bind级限速设计.md` 第 6 节）

**目的**：第 1 阶段每个 bind 20：20 条约 1 秒；改 cm.cfg 为 5、发 `RELOAD_CUSTOMER`，第 2 阶段 20 条约 4 秒，每个 bind 任意 1 秒 ≤ 6；Client 打 `[BIND-RATE]`。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTVB6)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVB6。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvb6（ssid 见 cm.extra.cfg） | minLinks=2 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800007501 | rtvb6 |  | DELIVRD | 改之前：20 TPS；条数 `20`；前一条之后（ms） `10`；bindMax1s `21`；spanMs `700,2500` |

**第 2 阶段**：第 1 阶段的 MT 发完 5 秒后，换成 `phase2/conf/` 的文件、发 `RELOAD_CUSTOMER`，再发：

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800007502 | rtvb6 |  | DELIVRD | 改之后：5 TPS；条数 `20`；前一条之后（ms） `10`；bindMax1s `6`；spanMs `3000,8000` |

**用例级检查**（这次运行期间）：

- `MultiSmppClient1,MultiSmppClient2` 日志里有一行同时包含 `[BIND-RATE] ssid=9956 SMSOptionOutgoingThrottlingNumPerBind now 5 TPS per bind`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

同一个发送方、号码的 MT 按 rssid + 发送方 + 号码分到同一个外发分片，所以一批 MT 由同一个 Client 发，用的是这个 Client 在这个通道上的 bind（`SessionNumber=4` 是整个通道的会话数，按 Client 平分，每个 Client 2 个；测试工具等满 4 个）。summary.txt 的 `binds:` 一行是每个 bind 收到的条数、任意 1 秒 / 100 ms 内最多几条、第一条到最后一条的时间。客户每 10 ms 发一条（比限速快），发送的快慢不会掩盖限速。这个通道 `SessionNumber=1`（每个 Client 1 个，这批 MT 只用一个 bind）。reload 会重建这个通道的会话（原有行为），所以第 2 阶段在 reload 后等 10 秒再发。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/BT006_reload_rate`（换配置并重启模块）。
