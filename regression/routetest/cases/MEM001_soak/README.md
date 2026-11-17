# MEM001 内存泄漏：100 TPS 混合流量 55 分钟（含 5 分钟预热）+ 客户重连 + reload，之后空闲 10 分钟；看各模块 Full GC 后的堆、线程、句柄、Metaspace 和 Redis 无过期 key

**对应用例清单**：docs/内存泄漏测试设计.md（`docs/内存泄漏测试设计.md`）

**目的**：按设计稿第 2 节：测试工具以 100 TPS 持续发 6 种 MT（下表的权重），每 5 分钟轮流断开重连一个客户，每 10 分钟轮流发一个 reload 命令；`bin/memwatch.sh` 每 30 秒采样一次，隔一次先 Full GC；最后 `rt.MemReport` 判断。

## 配置

`routing/RTSVCS_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVS1);(Priority=10,PhonePrefix=8615,Relay=RTVS2);(Priority=10,PhonePrefix=8616,Relay=RTVS3)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVS1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVS1, RTVS2, RTVS3, RTCS1, RTCS2, RTCS3。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvs2（ssid 9992） | submitStatus=0x45 |
| rtvs3（ssid 9993） | dr=none |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 86138 | * |  |  | 普通 MT + DR（MultiSmppServer1）；客户 `rtcs1`；weight `50` |
| 2 | OTP → 86138 | * |  |  | 普通 MT + DR（MultiSmppServer2）；客户 `rtcs3`；weight `15` |
| 3 | OTP → 86150 | * |  |  | RTVS2 回 0x45，改道到备份 RTVS1；客户 `rtcs1`；weight `10` |
| 4 | OTP → 86138 | * |  |  | 3 段长短信；段数 `3`；客户 `rtcs1`；weight `10` |
| 5 | OTP → 44770 | * |  |  | 没有路由：路由失败，失败 DR；客户 `rtcs1`；weight `5` |
| 6 | OTP → 86160 | * |  |  | RTVS3 不回 DR，客户 RTCS2 的假 DR 方式 4（5 秒）回成功；客户 `rtcs2`；weight `10` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

`mt.N` 在这里是流量模板：号码是模板的前缀后面补随机数字到 13 位；`weight` 是比例。预期通道写 `*`（不按条检查），客户 DR 只计数。整个用例约 67 分钟（重启 1 分钟 + 流量 55 分钟 + 等 DR 1 分钟 + 空闲 10 分钟）。阈值 `mem.*` 见设计稿 2.4。跑的时候服务器上不能同时跑别的用例或回归。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/MEM001_soak`（换配置并重启模块）。
