# 本地并发任务执行引擎（go-concurrency-engine）

单进程、仅依赖 Go 标准库的本地并发任务执行引擎。面向“任务持续涌入”的负载，
解决五件事：**受控并发与有界背压**、**按来源分组的隔离与公平调度**、
**失败分类与有限重试**、**取消/关闭的确定性**、**结果汇总的一致性与投递幂等**，
并提供可观测指标与可复现的本地基准。

> 无外部服务、无真实凭据、无付费资源；全部结论可用常规 `go` 命令在本地复现。

## 一分钟上手

```go
e := engine.New(engine.Config{
    Workers:     4,                       // 全局受控并发度
    QueueSize:   8,                       // 有界等待队列（满则背压）
    Retry:       engine.DefaultRetryPolicy(),
    Deduplicate: true,                    // 相同 Task.ID 幂等
    GroupLimits: map[string]int{"ingest": 2}, // 可选：按来源限制同时在跑数
})
e.Start()

res, err := e.Submit(ctx, engine.Task{
    ID:    "job-1",
    Group: "ingest",                      // 可选：标记来源分组；不填进默认分组
    Fn: func(ctx context.Context) error {
        // ctx 在引擎 Cancel 或本组 CancelGroup 时取消
        return engine.TempError(errors.New("稍后重试"))
    },
})

_ = e.Shutdown(ctx)        // 优雅排空；或 e.Cancel() 触发取消式关闭
m := e.Snapshot()          // 全局指标快照
gs := e.GroupSnapshots()   // 按分组的受理/成功/失败/取消计数
```

可直接运行演示：`go run ./cmd/demo`。

## 1. 并发与背压模型

- **固定工作协程池**：`Workers` 个 goroutine 从各分组的 FIFO 队列中取任务；
  全部分组的排队总量严格有界（≤ `QueueSize`，`QueueSize=0` 时仅与空闲
  工作协程直接交接），系统内“在执行 + 已缓冲”的任务数 ≤ `Workers + QueueSize`
  （外加提交瞬间的少量在途对象），**不会无界缓冲**。
- **背压是“阻塞提交方”，不是丢弃**：`Submit` 在全局排队总量占满时阻塞，
  直到出现空位；提交方因此被天然降速到处理能力附近。阻塞中的提交方按
  FIFO 等待者队列排队，每个释放的空位只点名唤醒一人（避免惊群）。
  提交也可以带 `ctx`，`ctx` 结束即放弃本次等待——此时任务**尚未受理**，
  不会执行、不会计入结果。
- **显式降载入口**：`TrySubmit` 在队列满时立即返回 `accepted=false`（而非静默丢弃），
  由调用方决定重试/降级/上报。
- **在途对象的接收/撤销是确定的**：提交在“入队成功”这一点才算受理；若在背压
  阻塞期间被 `ctx` 或引擎关闭唤醒而未入队，则回滚受理计数并把该条目标记为
  “受理撤销”，唤醒任何已共享该条目的并发投递，绝不出现半完成结果或永久挂起。
- `outstanding` WaitGroup 在**入队之前**计数，保证关闭排空不会与晚入队发生竞态。

## 1.5 分组隔离与公平调度

任务可通过 `Task.Group` 标记来源分组（空串为默认分组，**不指定分组的旧用法
行为完全不变**）。分组解决两个问题：某一路猛灌时其它路被压在后面、以及
无法单独停掉/排空某一路。

- **分组额度与全局并发叠加**：`Config.GroupLimits[name]` 限制该组“同时在跑”
  的上限（未配置或 ≤0 表示不单独限流）。一个任务被调度执行需同时满足
  组内额度与全局 `Workers` 上限。组额度占满时，该组的任务留在自己的 FIFO
  队列里等，**不占用执行位置**，其它组照常调度；不会因此卡住、丢任务或
  产生半截结果（排队任务要么等到执行，要么在取消时拿到确定的取消裁决）。
- **公平性**：调度器维护“有任务且额度未满”的就绪分组轮转列表，工作协程
  每取一个任务就把该组排到队尾。持续过载时，猛灌的一路与其它路交替获得
  执行机会，任何一路都不会被无限期饿死（测试：单工作协程下受害方在全局
  执行序列中的位置从纯 FIFO 的 ≥20 提前到 ≤3）。公平不靠无界堆积或丢活
  换取：全局排队总量始终有界，压力始终传回提交方。
- **单独取消一路**：`CancelGroup(name)` 使该组正在执行的任务其 ctx 立即
  取消（退避同样立即结束），已排队任务当场拿到确定的 `ClassCanceled` 裁决
  （不执行、不重试、只统计一次），之后向该组的提交返回 `context.Canceled`；
  对不存在的组调用会留下取消墓碑。其它分组的受理、排队、执行完全不受影响，
  该组释放的队列空位立即可被其它组使用。
- **单独排空一路**：`DrainGroup(ctx, name)` 立即停止受理该组新提交
  （返回 `ErrGroupClosed`），并等待该组所有已受理任务到达终态；`ctx` 到期
  仍未排空时返回 `ctx.Err()`（组保持关闭受理状态）。其它分组不受影响。
- **分组指标**：`GroupSnapshots()`（或 `GroupSnapshot(name)`）给出每一路的
  受理、成功、永久失败、重试耗尽、取消计数，口径与全局 `Metrics` 一致——
  **所有分组各项之和恒等于全局汇总**，各组内部“终态四类之和 == 受理数”，
  多次读取结果一致。全局 `Submitted` 等汇总语义不变。
- 取消/排空某一组时，全局指标只反映该组任务的终态变化，其它组计数不受
  任何影响；引擎级 `Cancel`/`Shutdown` 对所有分组级联生效。
- 幂等去重仍按 `Task.ID` 全局生效：同一 ID 的重复投递共享首次投递确定的
  分组与同一个结果。

## 2. 失败分类与有限重试

错误通过 `Classify` 区分为四类（`FailureClass`）：

| 类别 | 来源 | 是否重试 | 指标列 |
| --- | --- | --- | --- |
| `ClassSuccess` | `nil` | — | `Succeeded` |
| `ClassTemporary` | `TempError(err)`，或未分类普通 error（保守默认可重试） | 是，至上限 | 耗尽时计入 `RetryExhausted` |
| `ClassPermanent` | `PermError(err)` | 否，立即终态 | `PermanentFailures` |
| `ClassCanceled` | `context.Canceled` / `context.DeadlineExceeded`（含被 `%w` 包装） | 否，立即终态 | `Canceled` |

- 重试策略 `RetryPolicy`：`MaxAttempts`（含首次）+ 确定性指数退避
  `BaseDelay * Multiplier^(attempt-1)`，以 `MaxDelay` 封顶。刻意**不加随机抖动**，
  以保证测试与基准结果可复现。
- **重试只影响执行尝试，不影响结果计数**：一个任务无论执行多少次，终态只裁决一次、
  只累计一次；`Attempts` 记录实际尝试数，`Retries = Attempts-1` 仅用于观测。
- 重试耗尽（仍是临时失败）与永久失败是**两个可区分的终态**；它们都不会阻塞其它任务
  （固定池 + 每个任务独立终态）。
- 退避等待可被取消立即打断。

## 3. 取消与关闭规则

- `Shutdown(ctx)`：停止受理新任务 → **确定排空所有已受理任务**（每个都拿到终态结果）
  → 回收工作协程。`ctx` 到期仍未排空时取消在途任务并返回 `ctx.Err()`；
  尊重传入 ctx 的任务会很快结束。
- `Cancel()`：等价于“取消 + 关闭”：
  - 正在执行的任务其 ctx 立即取消，正在进行的退避立即结束；
  - 已入队、尚未开始的任务得到确定的 `ClassCanceled` 裁决（**不丢弃、不执行**）；
  - 仍在背压阻塞、尚未入队的提交被唤醒并返回 `context.Canceled`（未受理、不计结果）；
  - 此后提交返回 `context.Canceled`。
- 任务的执行生命周期绑定的是**引擎的 ctx**，与某个提交方的 `ctx` 解耦：提交方提前
  放弃等待，不影响已受理任务的执行与结果（配合幂等，其它投递方仍能拿到结果）。
- 每个任务的终态只写入一次（`finalize` 幂等保护），不存在重复提交导致的重复结果。

## 4. 结果一致性与投递幂等

- 开启 `Deduplicate` 后，按 `Task.ID` 去重：
  - **在途重复投递**：并发或先后提交同一 ID，共享同一次执行与同一个 `Result`，
    只统计一次，额外投递计入 `DuplicateDeliveries`；
  - **晚到重复投递**：任务终态条目会被保留，之后相同 ID 的投递直接复用该终态结果，
    不会重新执行、不会重复累计（全局幂等，而非仅“在途窗口”幂等）。
- 终态结果由内部聚合器串行累加（互斥保护），与任务完成顺序无关；
  `成功 + 永久失败 + 重试耗尽 + 取消 == Submitted` 恒成立。
- `Results()` 返回每个去重任务恰好一条的结果副本；多次汇总得到相同计数。

## 5. 可观测指标（`Metrics`）

计数器（单调累计）：

- `Submitted`：去重后实际受理的唯一任务数；
- `DuplicateDeliveries`：被幂等拦截的重复投递次数；
- `Succeeded` / `PermanentFailures` / `RetryExhausted` / `Canceled`：四类终态数；
- `Retries` / `Attempts`：重试次数 / 总尝试次数；
- `TaskTimeTotal`：所有任务从开始执行至终态裁决的耗时之和（含退避）；
- `BackpressureWaits` / `BackpressureTime`：提交方因队列满等待的次数与累计时长。

瞬时量表（仅快照）：`QueueDepth`（全部分组排队总量）、`ActiveWorkers`（执行/退避中的任务数）。

分组计数（`GroupSnapshots()` / `GroupSnapshot(name)`）：每组 `Submitted` /
`Succeeded` / `PermanentFailures` / `RetryExhausted` / `Canceled`，口径与全局一致，
各组之和恒等于全局汇总。

## 6. 本地验证

```bash
# 常规构建与静态检查
go build ./...
go vet ./...

# 全量测试（含持续过载背压、中途取消、重试耗尽、重复投递、一致性，
# 以及分组公平性/分组额度/单组取消与排空/分组指标一致性），打印输入与判定依据
go test ./... -v

# 竞态检测，建议多跑几轮
go test ./... -race -count=5

# 标准基准（受控并发 vs 串行）
go test -bench . -run '^$'

# 对照实验（受控引擎 vs 串行 vs 无界缓冲：吞吐/延迟/goroutine 峰值）
go test ./... -run TestBenchmarkComparison -v
```

### 一次本机实测（2000 个 20µs I/O 型任务，W=8, Q=64）

```
串行实现      : 吞吐 ~8.3e2   tasks/s
受控引擎      : 吞吐 ~9.9e4   tasks/s（相对串行约 100x；标准 bench 口径约 7x）
无界缓冲实现  : 吞吐 ~3.0e5   tasks/s，但 goroutine 峰值≈1667
受控引擎      : goroutine 严格有界（≤ W+Q = 72）
```

结论：受控引擎把 I/O 等待在有界并发内重叠，吞吐远高于串行、接近无界实现，
同时把 goroutine/内存占用严格限制在 `W+Q`；无界缓冲实现用不可控的资源占用
换取吞吐，在持续过载下会放大内存与调度压力，且不具备取消与背压语义。
具体数值随机器波动，可用上面的命令在本机复现趋势。

## 7. 包结构

- `task.go`：`Task`（含 `Group` 分组标记）/ `Result` / `FailureClass` 与错误分类（`TempError`/`PermError`/`Classify`）。
- `retry.go`：`RetryPolicy` 与确定性指数退避。
- `metrics.go`：`Metrics`、分组计数与顺序无关的聚合器。
- `engine.go`：分组 FIFO 队列、就绪轮转调度、工作协程池、背压、幂等、取消/关闭、终态裁决。
- `group.go`：分组状态与 `CancelGroup` / `DrainGroup` / `GroupSnapshots`。
- `cmd/demo`：可运行的行为演示（含分组隔离与单组取消）。
- `*_test.go`：场景测试与基准/对照实验；`group_test.go` 覆盖分组能力。

## 8. 兼容范围

- 不设置 `Task.Group`（零值 `""`）的任务进入默认分组，不单独限流，
  调度行为与引入分组前一致；既有代码无需修改。
- `Config.GroupLimits` 为 nil 时所有分组仅受全局并发约束，与旧行为等价。
- `Submit` / `TrySubmit` / `Cancel` / `Shutdown` / `Snapshot` / `Results`
  的签名与语义不变；`Result` 新增 `Group` 字段（零值兼容）。
- 新增 API：`Task.Group`、`Config.GroupLimits`、`CancelGroup`、`DrainGroup`、
  `GroupSnapshot` / `GroupSnapshots`、`ErrGroupClosed`，均为纯增量。
