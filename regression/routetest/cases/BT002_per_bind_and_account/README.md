# BT002 每个 bind 20 + 账号级 30：总量约 30 TPS，每个 bind 不超过 20

**对应用例清单**：BR-03（`features/06_bind级限速/SMPP外发bind级限速设计.md` 第 6 节）

**目的**：两层同时生效：账号级（每个 Client 节点）30、每个 bind 20。150 条应在约 5 秒发完（受账号级限制），每个 bind 任意 1 秒 ≤ 21。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTVB2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVB2。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvb2（ssid 见 cm.extra.cfg） | minLinks=4 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800007101 | rtvb2 |  | DELIVRD | 150 条，每 10 ms 一条；条数 `150`；前一条之后（ms） `10`；bindMax1s `21`；bindsUsed `2`；spanMs `4000,7000` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

同一个发送方、号码的 MT 按 rssid + 发送方 + 号码分到同一个外发分片，所以一批 MT 由同一个 Client 发，用的是这个 Client 在这个通道上的 bind（`SessionNumber=4` 是整个通道的会话数，按 Client 平分，每个 Client 2 个；测试工具等满 4 个）。summary.txt 的 `binds:` 一行是每个 bind 收到的条数、任意 1 秒 / 100 ms 内最多几条、第一条到最后一条的时间。客户每 10 ms 发一条（比限速快），发送的快慢不会掩盖限速。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/BT002_per_bind_and_account`（换配置并重启模块）。
