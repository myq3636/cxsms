# MO 测试（CoreMoIT、ServerMoIT、ClientResultIT 的 MO 用例）

> 日期：2026-11-27。MO 经过 Client、Core、Server 三个模块，用例分在三个套件里：
>
> | 模块 | 套件 | 模式 | MO 的用例 |
> |---|---|---|---|
> | Client | ClientResultIT | it-mod:MultiSmppClient1 | VD-10、VD-11（详见《Client SMPP连接与PDU测试》4.6） |
> | Core | CoreMoIT | it | MO-01～MO-09，9 项 |
> | Server | ServerMoIT | it-server | SM-00～SM-08，9 项（详见《Server功能测试》4.9） |
>
> 2026-11-26 的修复（提交 `8996900`）：在这之前 Core 把 MO 写进了 `stream:mt:routed`（Client 发往供应商的流），MO 到不了客户。

## 1. 什么是 MO，怎么走

MO 是手机发给客户的消息：供应商（cm.cfg 里 `Role: Vendor`）用一条不是回执的 deliver_sm 送进网关，网关转给客户。CDR 里用 `Role: Vendor` 和 ServiceTypeID 3 标记它。

| 步骤 | 模块 | 做法 |
|---|---|---|
| 1 | Client | 收到供应商的 deliver_sm（esm_class 不是回执），回 deliver_sm_resp，把它当作一条 Submit 写进 `stream:mt:pending`，ossid 是供应商；写不进 Redis 时回 `ESME_RX_T_APPN` 让供应商重发；`SMSOptionRemoveUdh` 去掉 UDH |
| 2 | Core | 按**供应商的路由**选出接收方。接收方是客户：作为 Delivery 写进客户的 Server 流 `stream:dr:pending:{客户}:*`，日志 `[MO-TO-SERVER]`，写 IN_SUBMIT CDR。接收方是另一个供应商：仍是普通 MT，写 `stream:mt:routed`。客户发给客户的不是 MO |
| 3 | Server | 从客户的流读出，发 deliver_sm 给客户（esm_class 0，源地址是手机号，目的地址是客户的号码）；客户的 deliver_sm_resp 当作 submit_resp 报给 Core（Core 再写 OUT_SUBMIT CDR，和 MT 一样） |

## 2. 怎么跑、怎么判定

```
regression/run.sh CoreMoIT ServerMoIT ClientResultIT
cat regression/.work/results/CoreMoIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- CoreMoIT：进程内启动真实的 Core；本程序扮演 Client，往 `stream:mt:pending:{供应商}:*` 写 MO，然后看它被确认了没有、写到了哪个流、CDR 和日志。
- ServerMoIT、ClientResultIT 的做法见各自的文档。

## 3. 配置（CoreMoIT）

cm.cfg 追加（公共部分 `CCBSSID`、`A2PPlayerType: operator`、`Protocol: SMPP`）：

| 块 | CCBSSID | 额外配置 | 角色 |
|---|---|---|---|
| V701 | 701 | `Role: Vendor` | 发 MO 的供应商 |
| C702 | 702 | — | 收 MO 的客户 |
| V703 | 703 | `Role: Vendor` | 另一个供应商 |
| C704 | 704 | — | 客户（对照） |

路由：

```
CCBSSID: 701
A2PMainRouting: (Priority=30,PhonePrefix=106601,Relay=C702);(Priority=31,PhonePrefix=86,Relay=V703)

CCBSSID: 704
A2PMainRouting: (Priority=30,PhonePrefix=106601,Relay=C702)
```

MO：发送方（手机）`8613800000001`，接收号码 `106601`（客户的号码），内容 `mo text`。

## 4. 用例

### 4.1 Core（CoreMoIT）

| ID | 情况 | 预期 |
|---|---|---|
| MO-01 | V701 的 MO，接收号码 106601 → C702 | Core 读出并确认 |
| MO-02 | — | 不写 `stream:mt:routed`（那是 Client 的） |
| MO-03 | — | 写进客户的 Server 流 `stream:dr:pending:702`，1 条 |
| MO-04 | — | 消息类型是 Delivery |
| MO-05 | — | 内容 `mo text`、发送方 `8613800000001` 保留 |
| MO-06 | — | 日志 `[MO-TO-SERVER]` |
| MO-07 | — | 写了 IN_SUBMIT CDR |
| MO-08 | 同一个供应商发往 86 号段（路由到供应商 V703） | 仍是 MT，写 `stream:mt:routed:703` |
| MO-09 | 客户 C704 发给客户 C702 | 不是 MO：照以前写 `stream:mt:routed:702`，不进 C702 的 Server 流 |

### 4.2 Server（ServerMoIT，客户 31）

| ID | 情况 | 预期 |
|---|---|---|
| SM-01～03 | Core 写来的 MO | 写在客户自己的流里（`stream:dr:pending:31:MultiSmppServer1:1`），客户收到 deliver_sm，内容是 MO 的内容 |
| SM-04 | — | esm_class 0（不是回执） |
| SM-05 | — | source 是手机号，destination 是客户的号码 |
| SM-06 | 客户回 deliver_sm_resp | 报给 Core 成功的 submit_resp |
| SM-07 | 客户不回（`BufferTimeout` 3 秒） | 报给 Core 失败的 submit_resp 2700 |
| SM-08 | 客户没绑定 | 失败的 submit_resp 2500 |

### 4.3 Client（ClientResultIT）

| ID | 情况 | 预期 |
|---|---|---|
| VD-10a/b | 供应商发来 MO | 回 ROK；写进 `stream:mt:pending`，ossid 是供应商 |
| VD-10c | Redis 写不进去 | 回 0x64（`ESME_RX_T_APPN`），供应商会重发 |
| VD-11a～c | 带 UDH 的 MO | 接受；`SMSOptionRemoveUdh` 的通道去掉 UDH，没有这个选项的保留 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | CoreMoIT 9、ServerMoIT 9、ClientResultIT 87 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 一条 MO 经过真实的 Client、Core、Server 三个进程 | 三段分开测，没有串起来（PathChainIT 只走 MT 和 DR） |
| 长 MO（多段） | 没有用例 |
| HTTP 客户收 MO | 没有用例 |
| MO 的重试（`SMSOptionMORetryPolicy`） | 只在 CoreRetryIT 里测了策略的优先级（RE-05b），没有 MO 本身的重试 |
