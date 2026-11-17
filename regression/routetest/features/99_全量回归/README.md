# 99_全量回归

`newsms/regression/suites.txt` 里的全部回归套件（72 个），在服务器上用 `newsms/regression/run.sh` 跑：自己的 Redis（127.0.0.1:16379）、测试端口 17775～18099，不影响正在跑的 a2p 和消息级用例。没有消息级用例。

| 文件 | 内容 |
|---|---|
| `regression.txt` | 全部套件，和仓库 `regression/suites.txt` 一致；套件有增减时重新生成：`grep -v '^\s*#' newsms/regression/suites.txt \| awk 'NF{print $1}' > features/99_全量回归/regression.txt` |
| `baseline.txt` | 基线：每个套件 `套件 PASS 数 KNOWN 数`。第一次全部 OK 的运行自动写出 |
| `cases.txt` | 空 |

手工套件（`manual-suites.txt` 的 BindSpreadIT、MemSoakIT）不在里面，要按名字单独跑：`cd newsms && regression/run.sh BindSpreadIT`。

## 运行

```text
/usr/local/a2p/routetest/bin/run_feature.sh 99_全量回归
```

结果在 `result/{时间}/`：`summary.txt`（每个套件一行，`CHECK` 行，最后 `RESULT: PASS/FAIL`）、`regression/{套件}.txt`（每个检查的 PASS/FAIL/KNOWN/FIXED）、`regression/{套件}.log`（套件的模块日志）、`regression/run.out`（run.sh 的全部输出，含编译）。

## 验收标准

`summary.txt` 最后一行 `RESULT: PASS`，它要求：

1. **每个套件都跑了**：`regression.txt` 里的每个套件在结果里都有一行（没跑，例如编译失败、被跳过：`CHECK FAIL ... did not run`）。
2. **每个套件 OK**：没有 FAIL，并且打出了 `ALL CHECKS PASSED`（超时被杀的套件没有这一行，算 FAILED）。`known-defects.txt` 里登记的已知缺陷打 KNOWN，不算失败。
3. **和基线比**（有 `baseline.txt` 时）：PASS 数不少于基线（少了说明有检查没执行到）、KNOWN 数不多于基线（多了说明新出现了已登记的缺陷）。PASS 变多、KNOWN 变少（FIXED）都可以；要接受新的数字，删掉 `baseline.txt` 再跑一次全部 OK 的，或手工改。
4. `TOTAL ... suites_failed=0`。

失败时看 `summary.txt` 的 `CHECK FAIL` 行和 `FAIL` 行，再看对应套件的 `regression/{套件}.txt`、`.log`。

这套检查（`baseline.txt`）对 01～08 的功能目录同样生效：功能目录里放了 `baseline.txt` 就按它比。
