# 假 DR 可疑点测试报告

> **2026-11-10 更新：两个缺陷已随假 DR 重新设计修复（`docs/假DR逻辑重新设计.md`），`FakeDrSuspectIT` 去掉，场景并入 `FakeDrIT`（FN-03、FN-06）。下面是修复前的记录。**
>
> 日期：2026-11-10。用例 `regression/test/it/FakeDrSuspectIT.java`（`suites.txt`）：真实的 Core（MT 消费、结果消费、入向 DR 消费、延迟 DR 分发），测试进程扮演 Client 写外发结果和供应商 DR。两个缺陷登记在 `known-defects.txt`（DEF-C8、DEF-C9），检查项写的是配置要求的行为，现在打 KNOWN，修好后会显示 FIXED。**代码没有改。**

## 1. 结论

| 编号 | 可疑点 | 结论 |
|---|---|---|
| DEF-C8 | 配了入向按前缀回假 DR（`SMSOptionSendFakeDRByInSubmitRecPrefixInCU`）后，前缀以外的号码外发失败收不到失败 DR | **确认，但只在“可重试错误码、重试到最后一次”时出现**。不可重试的失败走另一个分支，判断正确 |
| DEF-C9 | 供应商回 EXPIRED、我们自己还没过期时，判断“回没回过假 DR”取错了客户 | **确认，两个方向都错**：没回过假 DR 的消息不保留，客户收不到 DR；回过假 DR 的消息反而保留 |

## 2. DEF-C8：前缀以外的号码，最后一次重试失败后收不到 DR

**配置：** 客户 `SMSOptionSendFakeDRByInSubmitRecPrefixInCU: 8613`；通道 `SMSOptionSubmitRetryCode: 2500`、`SMSOptionMTRetryPolicy: 1S`（一个重试点，第一次失败就是最后一次重试）。

| 编号 | 场景 | 结果 |
|---|---|---|
| FD-01a | 接收方 852，不在前缀里 | 入向不回 DR（正确） |
| FD-01b | 同上，供应商回 UNDELIVERABLE（不可重试） | 客户收到失败 DR 10400（正确） |
| FD-01c | 对照：客户不配假 DR，同上 | 失败 DR 10400 |
| FD-01d/e | 接收方 8613（在前缀里） | 入向回 DR；外发失败后不再回第二条（设计如此） |
| FD-01f | 接收方 852，供应商接收后回 DR UNDELIV | 转发给客户 10400（正确） |
| **FD-01h** | 接收方 852，供应商回 2500（可重试），最后一次重试失败 | **客户 0 条 DR**（KNOWN） |
| FD-01h 对照 | 客户不配假 DR，同上 | 失败 DR 10405 |

**原因：** `MessageStoreManager.handleOutSubmitRes` 里，可重试错误到最后一次重试的分支（`MessageStoreManager.java:637`、`646`）用的是：

```java
if (!oinfo.isNullFakeDRByInSubmitRecPrefixInCU() || oinfo.isSmsOptionSendDRByInSubmitInCU()) {
    // not need to send dr
} else {
    sendDRMessage(message);
}
```

`isNullFakeDRByInSubmitRecPrefixInCU()` 只看“前缀列表是否为空”，配了列表就对**所有号码**都不发 DR。不可重试的分支（`MessageStoreManager.java:880`）用的是 `isNeedFakeDRByInSubmitRecPrefixInCU(接收号码)`，按号码判断，所以 FD-01b 正常。

**影响：** 配了这个前缀列表的客户，前缀以外的号码，只要外发失败的错误码在通道的重试列表里，重试完仍然失败，客户就一条 DR 都收不到。

## 3. DEF-C9：EXPIRED 分支取错了客户

**背景：** 供应商回 EXPIRED，但我们这边这条消息还没到过期时间，`handleOutDeliveryReportRes` 的设计是：没给客户回过假 DR 的消息先放进 WDQ 等我们自己过期再回 DR；回过的只记 CDR。判断“回没回过”要看**通道**的 `SMSOptionRecPrefixForFakeDR`（外发成功后回假 DR 时就是按通道的这个列表判断的）。

**代码：** `MessageStoreManager.java:1437`、`1561`：

```java
A2PCustomerInfo rCustomerInfo = ctm.getCustomerBySSID(message.getOSsID());   // 取到的是客户，不是通道
```

同一个方法的其他分支（如 `1606` 行）用的是 `getRSsID()`。

| 编号 | 配置 | 结果 |
|---|---|---|
| FD-02a | 客户 `SMSOptionSendFakeDR`；通道 `SMSOptionRecPrefixForFakeDR: 8615`；接收方 8613 | 外发成功后不回假 DR（正确，前缀不匹配） |
| **FD-02b** | 同上，供应商回 EXPIRED | **没进 WDQ，客户 0 条 DR**（KNOWN）。客户的前缀列表为空，被当成“全部匹配、已回过假 DR” |
| FD-02c/d | 对照：两边都不配前缀 | 回了假 DR；EXPIRED 后不进 WDQ，客户只有 1 条 DR（正确） |
| FD-02e | 前缀只配在客户上（8615），通道不配 | 回了假 DR（按通道判断，全部匹配） |
| **FD-02f** | 同上，供应商回 EXPIRED | **进了 WDQ**（KNOWN）。按客户的 8615 判断成“没回过假 DR” |

**影响：**

- FD-02b 的情况：消息不进 WDQ，DR 查找记录在这次处理后删除，**客户永远收不到这条消息的 DR**。
- FD-02f 的情况：多了一条 WDQ 记录。消息过期时 MQM 处理这条记录；它的假 DR 标志在回假 DR 时已清掉，`sendDRMessage` 对配了 `SMSOptionSendFakeDR` 的客户只在标志还在时才发，所以**大概率不会回第二条 DR**，但这一步（MQM 过期处理）没有测。
- 只有供应商回 EXPIRED 且我们这边还没过期时才走这个分支；其他状态的 DR 不受影响。

## 4. 修改建议（未改）

- DEF-C8：`MessageStoreManager.java:637`、`646` 改为按号码判断：`oinfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()) || oinfo.isSmsOptionSendDRByInSubmitInCU()`，和不可重试分支一致。
- DEF-C9：`MessageStoreManager.java:1437`、`1561` 改为 `ctm.getCustomerBySSID(message.getRSsID())`。

两处都只改判断条件，修好后 `FakeDrSuspectIT` 的 3 个 KNOWN 会变成 FIXED，再从 `known-defects.txt` 删掉。
