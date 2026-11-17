# BT005 对照：没配 bind 级，同样一个 bind 慢：轮到它时整批推迟 100 ms，发得慢很多（原有行为）

**对应用例清单**：BR-09b（`features/06_bind级限速/SMPP外发bind级限速设计.md` 第 6 节）

**目的**：和 BT004 一样的慢 bind、窗口 5，但不配 bind 级限速：每次轮到慢 bind 窗口满就整批推迟，100 条要 5 秒以上（BindRateIT 实测 9.3 秒）。和 BT004 对比，说明 4.3 的“找能发的 bind”只在配了 bind 级的通道上生效。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTVB5)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVB5。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvb5（ssid 见 cm.extra.cfg） | minLinks=4，slowFirstLink=true，respDelayMs=3000 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800007401 | rtvb5 |  | DELIVRD | 100 条，每 10 ms 一条；条数 `100`；前一条之后（ms） `10`；spanMs `5000,60000`；最多等（ms） `90000` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

同一个发送方、号码的 MT 按 rssid + 发送方 + 号码分到同一个外发分片，所以一批 MT 由同一个 Client 发，用的是这个 Client 在这个通道上的 bind（`SessionNumber=4` 是整个通道的会话数，按 Client 平分，每个 Client 2 个；测试工具等满 4 个）。summary.txt 的 `binds:` 一行是每个 bind 收到的条数、任意 1 秒 / 100 ms 内最多几条、第一条到最后一条的时间。客户每 10 ms 发一条（比限速快），发送的快慢不会掩盖限速。测试工具让第一个收到 submit_sm 的 bind 晚 3 秒回 submit_sm_resp（`slowFirstLink`）。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/BT005_slow_bind_no_per_bind`（换配置并重启模块）。
