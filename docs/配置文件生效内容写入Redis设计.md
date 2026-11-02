# 配置文件生效内容写入 Redis 设计

## 1. 目标

GmmsConfig 已经在启动和热加载后写到 `config:effective:{模块}:{NodeID}`。这次把其他配置文件也写进 state Redis，供 portal 查询"各模块现在用的是什么配置"：

- 写的是模块**实际在用**的内容。例如 cm.cfg reload 时某个客户块写错了，这个客户继续用旧配置，Redis 里写的也是旧配置。
- 启动加载完成后写一次，每次 reload 后再写一次；reload 失败时记录失败，内容保持不变。
- 按用户决定：**黑白名单不写**（全局黑白名单，以及按客户的发送方、接收方、内容黑白名单）；写到 **state Redis**。

## 2. 收录的配置

| 名称 | 文件 | 内存里的数据 | 写入的内容 | 写入时机 |
|---|---|---|---|---|
| `cm` | `conf/cm.cfg` | 客户快照的块（`CustomerSnapshot.confBlockMap`，失败的客户保留旧块） | 每个客户一项 | 启动、`RELOAD_CUSTOMER` |
| `routing` | `conf/routing/` | `routingInfoMap` | 每个块一项 | 启动、`RELOAD_ROUTING` |
| `phonePrefix` | `conf/senderReplacement.cfg` | `phonePrefixMap` | 每个块一项 | 启动、`RELOAD_PHONE_PREFIX` |
| `vendorTemplate` | `conf/vendorTemplateReplacement.cfg` | `vendorTemplatePrefixMap` | 每个块一项 | 启动、`RELOAD_VENDOR_REPLACE` |
| `systemVendorReplace` | `conf/vendorRoutingReplacement.cfg` | `systemRoutingReplacementMap`（国家 → 供应商） | 每个国家一项 | 启动、`RELOAD_SYSTEM_REPLACE` |
| `recipientRule` | `conf/recipientRuleFile.cfg` | 加载成功后的文件内容 | 每行一项 | 启动、`RELOAD_RECIPIENT_RULE` |
| `antiSpam` | `conf/antiSpam.cfg` | `AntiSpamFilter` 的块 | 每个块一项 | 启动、`RELOAD_ANTISPAM` |
| `contentTemplate` / `portalContentTemplate` / `templateId` | `conf/` 下同名 `.cfg` | 加载成功后的文件内容 | 每行一项 | 启动、`RELOAD_CONTENT_TEMPLATE` |
| `operatorMode` | `conf/OperatorMode.properties` | 读到的 Properties | 每个 key 一项 | 启动、文件修改后（每 5 秒检查） |
| `mqmTask` | `conf/MQMTaskConfig.properties` | MQM 读到的 Properties | 每个 key 一项 | MQM 启动 |

"加载成功后的文件内容"：这几个文件在内存里只有加工后的结构（正则等），所以写的是加载成功后立刻读取的文件文本。如果恰好在加载和读取之间（毫秒级）文件又被修改，写入的是新内容，下一次 reload 时纠正。

## 3. Redis 结构

内容**按版本存一份，所有模块共用**；版本是内容的 md5（排序后计算），内容相同的模块得到同一个版本，只写一次。

| key | 类型 | 内容 |
|---|---|---|
| `config:file:{名称}:{版本}` | Hash | 每一项：块的名字 → 这个块的 JSON（`{"key":"value",...}`，key 保持文件里的写法，如 `CCBSSID`、`SMSOptionOutgoingThrottlingNum`）；Properties 的 key → value；文本文件的行号（6 位，从 000001 开始）→ 这一行 |
| `config:file:{名称}:{版本}:meta` | Hash | `name`、`version`、`file`、`format`（blocks/properties/lines）、`entries`（项数）、`writtenAt`、`writer`（第一个写入的模块） |
| `config:file:cm:{版本}:ssid` | Hash | ssid → shortName，按 ssid 查客户 |
| `config:file:routing:{版本}:channel`、`:channelRoutes` | Hash | 按通道查客户及路由明细，见《路由增量reload与通道路由索引设计》 |
| `config:files:{模块名}:{NodeID}` | Hash | 这个模块在用的每个文件：`{名称}.version`、`{名称}.loadedAt`、`{名称}.result`（`ok`；`partial` 表示部分客户加载失败；`failed` 表示 reload 失败、仍用上一个版本）、`{名称}.file`、`{名称}.entries`；cm 另有 `cm.failed`（加载失败、仍用旧配置的客户） |
| `config:effective:modules` | Set | 模块列表（与 GmmsConfig 共用） |

portal 查询方式：

```
SMEMBERS config:effective:modules                          # 有哪些模块
HGET config:files:CoreEngine1:1 cm.version                 # CoreEngine1 用的 cm.cfg 版本
HGET config:file:cm:{版本} VENDOR9                         # 某个客户的配置（JSON）
HGET config:file:cm:{版本}:ssid 9                          # ssid 9 是哪个客户
HKEYS config:file:cm:{版本}                                # 所有客户
HGETALL config:file:routing:{版本}                         # 路由
```

### 3.1 清理与过期

- **模块换用新版本后，如果旧版本已经没有任何模块在用，立即删除旧版本**（检查 `config:effective:modules` 里每个模块的 `config:files:*` 记录）。所以 Redis 里通常每个文件只有一个版本，各模块版本不一致时才有多个。
- 兜底：`config:file:*` 设 7 天过期，每个模块每小时为自己在用的版本续期。只要有模块在用就不会过期；漏删的旧版本 7 天后自动清掉。
- `config:files:{模块}:{NodeID}` 没有过期时间（同 `config:effective:*`），模块下线后手工删除。

### 3.2 写入方式

- 版本已经存在（有 `:meta`）时只续期，不重写。
- 不存在时，按每批 500 项写 Hash，最后写 `:meta`。`:meta` 存在才表示内容完整，portal 应先确认 `:meta` 存在。
- 几个模块同时写同一个版本时，写的内容相同，不会冲突。
- 写入在后台单线程执行，不占用 reload 线程；同一个文件有多次待写入时只保留最新一次。Redis 还没连上（启动早期）或出错时，每 5 秒重试，直到写入成功或被更新的版本替代。
- 密码类的值打码：key 含 `password`、`passwd`、`pwd`、`authkey`、`secret` 时值写成 `******`；值里形如 `password=xxx`（连接定义）的部分也替换为 `password=******`。

## 4. 新增配置

| 配置项 | 文件 | 默认 | 生效 | 作用 |
|---|---|---|---|---|
| `ConfigFilePublishEnable` | GmmsConfig | `true` | 热加载 | 是否把配置文件的生效内容写到 Redis。关闭后不再写入、不再续期，已写入的 7 天后过期 |

## 5. 容量

按每个客户块约 2 到 4KB 估算，5000 个客户的 cm.cfg 一个版本约 10 到 20MB。旧版本在没有模块使用后立即删除，所以通常只占一个版本的空间；滚动重启或各模块 reload 不同步的短时间内会同时存在两个版本。路由等其他文件通常远小于 cm.cfg。
