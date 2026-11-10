# GSM 7-bit 消息长度计算优化设计

> 日期：2026-11-09。状态：**3.1 已实现**（第 7 节）；**3.2 的 DEF-C6 已按做法 B 修复**（第 8 节）。第 2 节来自读当前 `main`（`2934318`）的代码，没有实测；第 5 节的用例实现时补上。

## 1. 问题和结论

客户用 SMPP 发 `data_coding=0` 的 GSM 7-bit 消息，内容里有 é、ü、£、¥ 这类 GSM 字符时，系统把它的 `contentType` 定为 `ISO-8859-1`（有 €、希腊字母时定为 `UCS2`）。一条 GSM 7-bit 单条能放 160 个字符，但系统在两个地方会按“ISO 8 位编码只能放 140 字节”来处理：

1. **Core 入向长度检查直接拒绝**（2.2）：`NeedCheckMsgSize` 默认开启，非 ASCII 且超过 140 就回 2250。这是代码问题，可以修，改动小。
2. **发给不支持 7-bit 的通道时按 ISO 字节数拆分**（2.3）：通道没配 `SMSOptionSupportOutgoingGSM7bit` 时，消息不再按 7-bit 处理，按 ISO 字节数和通道的 `MsgSplitLen` 拆。这一处要分两种情况：
   - 通道其实支持 GSM 7-bit（多数运营商 SMSC 的 `data_coding=0` 就是 GSM 7-bit），只是没配：**改配置即可**，保持 160。
   - 通道确实只收 8 位 Latin-1：单条 140 字节是协议上限，不是 bug；可选的只有拆条、转 UCS2，或用 `SMSOptionForceTrans2ASCII` 把 é 变成 e 保持 160。

另外，第 2 种情况下发出去的 `data_coding` 是 0，但字节是 Latin-1，这是已登记的已知缺陷 **DEF-C6**（2.4）。它和本问题同源，要不要一起修需要确认（第 6 节）。

## 2. 现状

### 2.1 入向：7-bit 内容怎么变成 ISO

`Smpp.java:2744`：`data_coding` 为 0（或 `1111 x0xx`），并且来源客户配了 `SMSOptionSupportIncomingGSM7bit` 为 1 或 2 时，走 `handleGSM7bitMessageContent`（`Smpp.java:2823`）；没配（默认 0）时按 ASCII 字节直接解析，é 这类字符会变成乱码，不会变成 ISO。

`handleGSM7bitMessageContent` 逐个 GSM 字符查表转成 Java 字符：

| 字符 | 例子 | 查的表 | `contentType` |
|---|---|---|---|
| GSM 里与 ASCII 相同的 | a-z、0-9、@ | `gsm2ASCII` | `ASCII` |
| GSM 里在 Latin-1 有的 | é è ü Ä ñ £ ¥ § | `gsm2ISO` | `ISO-8859-1` |
| GSM 里只有 UCS2 有的 | Δ Φ Γ Λ Ω Π Ψ Σ Θ Ξ、扩展字符 € | `gsm2UCS2`、转义 27+101 | `UCS2` |
| 扩展字符 | `{ } [ ] ~ \ ^ \| ` | 转义 27 后查 | 不改变 |

同时 `gsm7bit=true`，`textContent` 是正确的 Java 字符串，`messageSize` 是**转换前的字节数**：

| `SMSOptionSupportIncomingGSM7bit` | 160 个字符的消息 | `messageSize` |
|---|---|---|
| 1（每字符一字节，不压缩） | 160 字节 | 160 |
| 2（压缩） | 140 字节 | 140 |

`gsm7bit` 存在 `milterActionCode` 的第 8 位（`MessageBase.setGsm7bit`），`SerializableHandler` 会把 `milterActionCode` 写进 stream 并在 Core 恢复，所以 Core 能看到这个标记。

**`contentType=ISO-8859-1` 在这里的意思是“这些字符需要 Latin-1 才能表示”，不是“它是 8 位编码”。** 真正说明“它是 7-bit”的是 `gsm7bit` 标记。问题出在下面这些只看 `contentType`、不看 `gsm7bit` 的地方。

### 2.2 Core 入向长度检查（会拒绝）

`InternalCoreEngineSession.java:1983`（`processInnerAck`，单条短信路径；长短信分段不走这里）：

```java
if (oCustomer.isNeedCheckMsgSize()) {                       // NeedCheckMsgSize，默认 true
    if (!"ASCII".equalsIgnoreCase(msg.getContentType()) && msg.getMessageSize() > 140) {
        // 2250 MSG_SIZE_OVERLENGTH，进黑洞
```

| 客户发来的 160 字符 7-bit 消息 | 现在 |
|---|---|
| 全是 ASCII 范围的字符 | 通过（`contentType=ASCII`） |
| 带 é，入向模式 1 | **被拒 2250**（ISO，`messageSize=160`） |
| 带 é，入向模式 2 | 通过（`messageSize` 是压缩后的 140） |
| 带 €，入向模式 1 | **被拒 2250**（UCS2，`messageSize=160`） |

同样内容的消息，只因入向模式不同，一个被拒一个通过。

### 2.3 外发：拆分和编码

`MessageProcessor.java:308`（拆分、转码之前）：

```java
if (message.isGsm7bit() && cm != null) {
    if (cm.getSmsOptionOutgoingGSM7bit() == 0) {          // 通道不支持 7-bit（默认）
        if (cm.getSmsOptionForceTrans2ASCII()) {           // 强转 ASCII：é -> e
            ... contentType=ASCII, gsm7bit=false, messageSize=ASCII 字节数; return;
        } else {
            message.setGsm7bit(false);                     // 不再是 7-bit
            message.setMessageSize(text.getBytes(contentType).length);   // ISO 字节数
        }
    } else {
        return;                                            // 通道支持 7-bit：保持原样
    }
}
```

拆分上限 `A2PCustomerManager.getRopSupportLength`（`A2PCustomerManager.java:3678`）：`gsm7bit` 的消息按 ASCII 取（通道的 `SMSOptionAsciiMsgSplitLen`，没配时用 `MsgSplitLen`，再没配用模块的 `ContentLength`，默认 160）；否则按 `contentType` 取 `MsgSplitLen`。`MessageProcessor.java:72-127` 再按通道的外发模式把上限和 `messageSize` 换算成同一单位（模式 2 时 160 字符 = 140 字节）。

| 通道 | 带 é 的 160 字符消息 |
|---|---|
| `SMSOptionSupportOutgoingGSM7bit: 1` 或 `2` | 保持 7-bit，按 160 字符判断，**不拆**，发 `data_coding=0` 的 GSM 编码。正确 |
| 默认 0，`SMSOptionForceTrans2ASCII: true` | é 变成 e，按 160 判断，不拆 |
| 默认 0，其他 | 变成 ISO、160 字节，按 `MsgSplitLen` 拆：**配了 140 就拆成 2 条**；没配按 160 不拆 |
| 默认 0，带 € 的（UCS2） | 按 UCS2 字节数（320）拆，**变成 3 条** |

### 2.4 外发的 `data_coding`（DEF-C6）

`Smpp.createSmppSubmitSM`（`Smpp.java:589`）：

```java
if ((cst.getSmsOptionOutgoingGSM7bit() == 0) || gmmsMsg.isGsm7bit()) {
    contentType = "SMSCdefaultCharset";                    // data_coding = 0
}
```

通道是默认 0 时，**不管内容是什么**都标 `data_coding=0`：ISO 的消息发出去是“Latin-1 字节 + `data_coding=0`”，UCS2 的是“UTF-16 字节 + `data_coding=0`”。`ClientPduIT` PD-02a 已登记为已知缺陷 DEF-C6。Server 给客户下发的同类代码（`Smpp.java:960`）多了 `&& ASCII`，是对的。

所以 2.3 最后两行的消息，供应商 SMSC 收到的是被标成默认字母表的 Latin-1 或 UTF-16 字节。能不能正确显示取决于对方的“默认字母表”到底是什么。

这段代码从仓库第一个提交（`f49bd6d`）就是这样。

**实测（2026-11-10，消息直接写进 Client 的外发 stream，假供应商抓 submit_sm）：**

| Core 交给 Client 的消息 | 通道 0（默认） | 通道 1 | 通道 2 |
|---|---|---|---|
| ASCII `hello` | dcs 0，`68656C6C6F` | dcs 1，同字节 | dcs 1，同字节 |
| ISO `Café`，非 7-bit | **dcs 0**，`436166E9`（Latin-1） | dcs 3，`436166E9` | dcs 3，`436166E9` |
| ISO `Café`，7-bit | **dcs 0**，`436166E9` | dcs 0，`43616605`（GSM） | dcs 0，`C3B0B900`（压缩） |
| UCS2 `你好`，非 7-bit | **dcs 0**，`4F60597D`（UTF-16） | dcs 8，`4F60597D` | dcs 8，`4F60597D` |
| UCS2 `aΔ`，7-bit | **dcs 0**，`00610394` | dcs 0，`6110`（GSM） | dcs 0，`6108`（压缩） |

加粗的是 DEF-C6。“ISO、7-bit”发往通道 0 实际不会从 Core 过来：Core 外发前就去掉了 `gsm7bit`（2.3），到 Client 时是“ISO、非 7-bit”，结果相同。

**线上：** `Client模块功能与测试用例清单.md` 12.1 节已确认线上通道都配了 1，DEF-C6 不触发。所以 3.1 改完后，以前被误拒的 7-bit 带 é 的消息在线上走通道 1 那一列：GSM 编码、`data_coding=0`、不拆，正确。**按线上现在的配置，2.3 的拆分问题和 DEF-C6 都不会出现。**

上表是直接写进 Client 外发 stream 的结果，没有经过 Core。线上通道一般是 `SMSOptionSupportOutgoingGSM7bit: 1` 加 `SMSOptionConvert2GSM7bit: yes`，后者在 Core 外发处理时把 ASCII 消息也转成 7-bit（`MessageProcessor.java:322`）。

**按线上配置走完整链路的实测（Core `processSubmitFromStream` → Client → 假供应商）：**

| 消息 | `1` + `Convert2GSM7bit: yes`（线上） | 只配 `1` |
|---|---|---|
| ASCII，无 `gsm7bit`（如 HTTP 客户） | dcs 0，GSM | dcs 1 |
| ASCII，模式 1 客户的 7-bit | dcs 0 | dcs 0 |
| ISO，客户 `data_coding=3` | dcs 3，Latin-1 | dcs 3 |
| ISO，模式 1 客户的 7-bit（带 é） | dcs 0，GSM（é=`05`） | dcs 0 |
| UCS2，客户 `data_coding=8` | dcs 8，UTF-16 | dcs 8 |
| UCS2，模式 1 客户的 7-bit（带 Δ） | dcs 0，GSM（Δ=`10`） | dcs 0 |

按线上配置：GSM 7-bit 和 ASCII 都是 `data_coding=0`，客户明确用 ISO、UCS2 发的是 3、8。只有没配 `Convert2GSM7bit` 的通道，无 `gsm7bit` 的 ASCII 才是 `data_coding=1`。

### 2.5 不受影响的

- 长短信分段：入向不走 2.2 的检查；Redis 组装时 `CsmUtility.assembleCsm` 保留 `gsm7bit`，拼好后按 2.3 处理。
- CDR：字符集列写的是 `gsm7bit ? ASCII : contentType`（`CDRManager.java:159`、`386`），看写 CDR 时消息上还有没有 `gsm7bit` 标记：

  | CDR | 那时 `gsm7bit` | 字符集列 |
  |---|---|---|
  | `IN_SUBMIT`、`IN_SUBMIT_PARTIAL`、2250 拒绝时写的那条 | 有 | `ASCII` |
  | `OUT_SUBMIT`、`OUT_DELI_REP`，通道 `SMSOptionSupportOutgoingGSM7bit: 1/2` | 有 | `ASCII` |
  | 同上，通道默认 0 | 外发处理时清掉（`MessageProcessor.java:316`） | `ISO-8859-1` |
  | 同上，通道 `SMSOptionForceTrans2ASCII` | 清掉，内容已转 ASCII | `ASCII` |

  本设计不改 `contentType`、不改 `gsm7bit`，CDR 不变。

## 3. 方案

### 3.1 Core 入向长度检查：真正的 7-bit 消息按 GSM 字符数判断（推荐，必做）

“入向长度检查”就是 2.2 那一处：Core 收到客户的消息后，按 `NeedCheckMsgSize` 判断是否超长、回 2250。

**已确认的前提：**

- 被误拒的客户配的是模式 1（`SMSOptionSupportIncomingGSM7bit: 1`），发的是真正的 GSM 7-bit。
- 用 ISO 发 160 字符的客户用的是 `data_coding=3`，这一路解析成 ISO、**不标 `gsm7bit`**，超过 140 字节回 2250，**现在就对，改后不变**。
- 模式 1 的客户不会发 ISO 字节。

所以系统不需要逐条猜编码：`data_coding` 加客户配置已经说明了它是哪种，而 `gsm7bit` 标记只会出现在 `data_coding=0` 且客户配了模式 1 或 2 的消息上。

**改法：** 带 `gsm7bit` 标记的消息，不再看 `contentType`，按 GSM 7-bit 字符数判断：

```text
contentType 是 ASCII  -> 不检查（和现在一样；包括 message_payload 里的长 ASCII 消息）
gsm7bit               -> septets = 每个字符 1 个，扩展字符（{ } [ ] ~ \ ^ | € 换页）2 个，> 160 -> 2250
其他（data_coding=3）  -> messageSize > 140 -> 2250（和现在一样）
```

| 客户发来的 | 客户配置 | 现在 | 改后 |
|---|---|---|---|
| `data_coding=3`，160 字节 ISO | 任意 | 2250 | **不变**，2250 |
| `data_coding=3`，140 字节 ISO | 任意 | 通过 | 不变 |
| `data_coding=0`，160 字符带 é | 模式 1 | **2250（误拒）** | 通过 |
| `data_coding=0`，161 字符带 é | 模式 1 | 2250 | 2250 |
| `data_coding=0`，160 字符带 €（UCS2） | 模式 1 | **2250（误拒）** | 通过 |
| `data_coding=0`，155 字符 + 5 个 `{` | 模式 1 | 看字符集 | 160 个 GSM 字符，通过；再多 1 个 2250 |
| `data_coding=0`，160 字符带 é | 模式 2 | 通过（压缩后 140 字节） | 通过（结果不变，两种模式从此一致） |
| `data_coding=0` | 默认 0 | 按 ASCII 解析，不检查 | 不变 |

- 新增 `gsm7Septets(String)`：把 `MessageProcessor.java:87` 已有的双字节字符表抽成一个公共方法，两处共用。
- 不改 `messageSize` 的含义（2.1 那张表），它还用在拆分、CDR 里。
- 只改 Core 这一处判断，Server 入向解析不改。

**模式 1 下出现大于 0x7F 的字节（防配错，只记日志）：** GSM 7-bit 非压缩的每个字节都在 0x00～0x7F。现在遇到更大的字节会换成 `?`（63），其余字符照常转换，`contentType` 按其余字符定，照样标 `gsm7bit`，不报错（`Smpp.java:2849`，字节有符号，0xE9 读成负数落进 `else`）。

| 例：模式 1、160 字节、含一个 0xE9 | 现在 | 改后（默认） |
|---|---|---|
| 解析 | 0xE9 变 `?` | 不变 |
| `contentType` | 其余都是 ASCII 时 `ASCII` | 不变 |
| Core 长度检查 | `ASCII` 不检查，通过 | 160 个 GSM 字符，通过 |
| 外发、CDR | 内容里是 `?` | 不变 |
| 日志 | 无 | 限频 WARN（每个客户 10 分钟最多一条，带次数） |

**实测（2026-11-10，直接调用 `handleGSM7bitMessageContent`，模式 1）：**

| 输入字节 | 解析结果 | `contentType` | `gsm7bit` |
|---|---|---|---|
| `43 61 66 05`（GSM 的 Café） | `Café` | `ISO-8859-1` | 是 |
| `43 61 66 E9`（ISO 的 Café） | `Caf?` | `ASCII` | 是 |
| `43 61 66 E9 A3`（ISO 的 é、£） | `Caf??` | `ASCII` | 是 |
| `61 7F`（0x7F 本身） | `aà` | `ISO-8859-1` | 是 |
| `61 1B E9 62`（转义后跟 0xE9） | `a??b` | `ASCII` | 是 |
| `05 A3`（GSM 的 é + ISO 的 £） | `é?` | `ISO-8859-1` | 是 |

0x7F 是合法的 GSM 字符（à）；只有 0x80～0xFF 会被换成 `?`。被换掉的字节不参与决定 `contentType`，其余都是 ASCII 时整条标成 `ASCII`，Core 长度检查直接跳过。转义符后跟这种字节时变成两个 `?`。

按已确认的前提这不应该发生，所以**不改解析行为**，只让它能被看见。（“拒绝”“按 ISO 重新解析”两个备选已确认不做。）

### 3.2 外发：先用配置解决，代码只修 DEF-C6（需确认）

**通道支持 GSM 7-bit：** 配 `SMSOptionSupportOutgoingGSM7bit: 1`（每字符一字节）或 `2`（压缩），按供应商对接文档选。消息保持 7-bit、按 160 判断。**不需要改代码**。实施前先按第 6 节核对线上通道。

**通道确实只收 8 位：** 140 字节是协议上限。可以选的：

| 做法 | 结果 |
|---|---|
| 现状（按 `MsgSplitLen` 拆） | 2 条，内容不变 |
| `SMSOptionForceTrans2ASCII: true` | 1 条，é 变成 e |
| 转 UCS2 | 3 条，内容不变，最贵 |

**DEF-C6 的修法：** `Smpp.java:589` 改成和 Server 侧一样：只有 `ASCII` 或 `gsm7bit` 才标 0，ISO 标 3、UCS2 标 8（`getDataCoding` 已有映射）。（原来还写了“非 7-bit 的 ISO 拆分上限默认 140”，实现时没有做，见第 8 节。）

**风险：** 改 `data_coding` 只改变配了 0 的通道收到的 PDU。线上通道都配了 1（2.4），修完对线上没有变化。唯一的风险是线上其实有配 0 的 SMPP 通道、对方默认字母表正好是 Latin-1、一直“凑巧能用”。

**两种做法：**

- **A 不修：** 维持登记（PD-02a），守住“新通道必须配 `SMSOptionSupportOutgoingGSM7bit: 1`”（Client 清单 12.1 已写）。
- **B 修（倾向）：** 先确认线上 cm.cfg 里没有 `SMSOptionSupportOutgoingGSM7bit` 为 0 或没写的 SMPP 供应商；确认没有就直接修，不加开关。修完 PD-02a 从已知缺陷转为通过。

### 3.3 不做的

- **不新增 `GSM7` 这种 `contentType`：** 代码里按 `contentType` 判断的地方很多（转码、拆分、CDR、HTTP 推送），影响面太大。继续用 `gsm7bit` 标记表达“它是 7-bit”，把只看 `contentType` 的地方补上判断即可。
- **不自动猜通道支持不支持 7-bit。**

## 4. 改动范围

| 方案 | 文件 | 改动 |
|---|---|---|
| 3.1 | `InternalCoreEngineSession.java:1983` | `gsm7bit` 时按 GSM 字符数（≤ 160）判断，否则不变 |
| 3.1 | `Smpp.handleGSM7bitMessageContent` | 模式 1 下出现 > 0x7F 的字节：限频 WARN，解析不变 |
| 3.1 | `CsmUtility` 或 `Convert` | `gsm7Septets(String)`，`MessageProcessor` 复用 |
| 3.2（确认后） | `Smpp.java:589` | `data_coding` 跟内容走，可加通道开关 |
| 3.2（确认后） | `A2PCustomerManager.getRopSupportLength` | 8 位 ISO 默认上限 140 |
| 3.2（确认后） | `A2PCustomerInfo` | `SMSOptionDataCodingByContent`（如加开关），写进 `配置项说明.md` |

3.1 没有新配置项。

## 5. 回归用例（实现时补）

真实 SMPP 客户连 Server，Core 处理，Client 发给假供应商，抓供应商收到的 PDU：

| 编号 | 场景 | 预期 |
|---|---|---|
| G7-01 | 160 个 ASCII 字符，入向模式 1 | 通过，1 条（对照，现在就对） |
| G7-02 | 160 字符带 é，入向模式 1 | 通过（现在 2250） |
| G7-03 | 160 字符带 é，入向模式 2 | 通过（现在就通过，修后不变） |
| G7-04 | 160 字符带 €（UCS2），入向模式 1 | 通过（现在 2250） |
| G7-05 | 161 字符带 é | 2250；长短信由客户分段，不受影响 |
| G7-05b | `data_coding=3`，160 字节 ISO | 2250（对照，现在就对，改后不变） |
| G7-05c | `data_coding=3`，140 字节 ISO | 通过（对照） |
| G7-05d | 模式 1 客户，`data_coding=0`，含 0xE9 字节 | 解析和现在一样（`?`），日志有一条 WARN |
| G7-12 | G7-02 的各类 CDR | `IN_SUBMIT`、`IN_SUBMIT_PARTIAL` 字符集 `ASCII`；发往 7-bit 通道的 `OUT_SUBMIT` `ASCII`，发往默认通道的 `ISO-8859-1`（和现在一样） |
| G7-06 | 155 字符带 é 和 5 个扩展字符 `{` | 160 个 GSM 字符，通过；再多 1 个就 2250 |
| G7-07 | G7-02 发往 `SupportOutgoingGSM7bit: 1` 的通道 | 1 条，`data_coding=0`，GSM 编码 |
| G7-08 | G7-02 发往 `SupportOutgoingGSM7bit: 2` 的通道 | 1 条，压缩，140 字节 |
| G7-09 | G7-02 发往默认通道，`MsgSplitLen: 140` | 现状：2 条，`data_coding=0`（记录现状；3.2 确认后改断言） |
| G7-10 | G7-02 发往默认通道，`ForceTrans2ASCII` | 1 条，é 变成 e |
| G7-11 | `NeedCheckMsgSize: false` | 不检查（对照） |

## 6. 需要确认

**已确认：** 现象是 2250；被误拒的客户是模式 1；用 ISO 发的客户用 `data_coding=3`；模式 1 的客户不发 ISO 字节。3.1 可以直接实现。

**还要确认（只影响 3.2，不影响 3.1）：**

1. **线上通道的配置：** 发这类消息的通道有没有配 `SMSOptionSupportOutgoingGSM7bit`、`MsgSplitLen` 配的多少。3.1 改完后，这类消息能进系统了，发到没配 7-bit 的通道时就会遇到 2.3 的拆分。没配 7-bit、但供应商其实支持的，改配置即可。
2. **DEF-C6：** 已选 B 并修复（第 8 节）。
3. **€、希腊字母（UCS2）是否一起按 3.1 处理**：建议一起，规则相同，已写进 3.1 的表。

## 7. 实现和测试结果（3.1）

- `Convert.gsm7Septets(String)`：GSM 字符数，扩展字符算 2 个。`MessageProcessor` 里原有的双字节统计没有改。
- `InternalCoreEngineSession.overOneSms(msg)`：按 3.1 的三条规则；原来的检查改为调用它。
- `Smpp.handleGSM7bitMessageContent`：统计大于 0x7F 的字节，有则打 WARN `[GSM7] ossid=... byte(s) above 0x7F, no GSM 7-bit character, replaced with '?'`（每个客户 10 分钟最多一条，带次数）。解析结果不变。
- 没有新配置项；`配置项说明.md` 里 `NeedCheckMsgSize` 的说明已改。

**`Gsm7LengthIT`（在 `suites.txt`）：** 改后 15 项全部通过。同一个用例放在改前的代码上，失败的正好是 G7-02、G7-04、G7-06a（160 个 GSM 字符被 2250）和 G7-05f、G7-05g（没有告警），其余结果相同。

| 编号 | 场景 | 结果 |
|---|---|---|
| G7-01 / 01b | 7-bit，160 / 200 个 ASCII 字符 | 通过，不检查（和改前一样） |
| G7-02 | 7-bit，带 é，160 字符 | 通过（改前 2250） |
| G7-04 | 7-bit，带 Δ（UCS2），160 字符 | 通过（改前 2250） |
| G7-05 | 7-bit，带 é，161 字符 | 2250 |
| G7-06a / 06b | é + 5 个 `{`：160 / 161 个 GSM 字符 | 通过 / 2250 |
| G7-05b / 05c | `data_coding=3`，160 / 140 字节 | 2250 / 通过（和改前一样） |
| G7-11 | `NeedCheckMsgSize: false` | 不检查 |
| G7-12 | 7-bit 消息的 `IN_SUBMIT` | 字符集 `ASCII`（和改前一样） |
| G7-05d～05g | 模式 1，`43 61 66 E9` | 解析和改前一样（`Caf?`、`ASCII`）；一条 WARN，10 分钟内第二次不再打 |

用例在 Core 这一层构造“Server 解析后”的消息，直接走 `processSubmitFromStream`；Server 的解析器用反射单独调用。没有用真实 SMPP 客户端走一遍 Server → Core 的完整链路。

## 8. DEF-C6 的修复（3.2 做法 B）

`Smpp.createSmppSubmitSM` 的 `data_coding`：

```java
boolean gsmBytes = gmmsMsg.isGsm7bit() && cst.getSmsOptionOutgoingGSM7bit() != 0;   // 上面编成了 GSM 7-bit
if (gsmBytes || (cst.getSmsOptionOutgoingGSM7bit() == 0 && ASCII.equalsIgnoreCase(contentType))) {
    contentType = "SMSCdefaultCharset";                                            // data_coding 0
}
// 否则 getDataCoding(contentType)：ASCII 1、ISO-8859-1 3、UCS2 8、二进制 4 ……
```

- 按“字节实际是什么”决定：通道是 0 时，即使带 `gsm7bit`，字节也是按 `contentType` 编的，所以不标 0。
- 通道配了 1、2 的结果和原来完全一样（PD-02b～e 不变），线上不受影响。
- 顺带修了二进制：通道是 0 时原来也被标成 0，现在是 4。

**用例（`ClientPduIT`，40 项全部通过）：** PD-02a（UCS2，默认通道）从已知缺陷变为通过，已从 `known-defects.txt` 删除；新增 PD-02f（ISO→3）、PD-02g（二进制→4）、PD-02h（带 `gsm7bit` 的 ISO 到默认通道→3）。

**没做：** 3.2 原来写的“非 7-bit 的 ISO 拆分上限默认 140”。它和通道是不是 0 无关（客户 `data_coding=3` 的 ISO 长短信发往配了 1 的通道也会用到），线上通道一般配了 `MsgSplitLen`，改默认值会改变没配的通道的拆分，需要时单独确认。
