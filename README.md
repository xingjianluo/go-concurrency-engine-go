# 本地并发任务执行引擎（go-concurrency-engine）

单进程、仅依赖 Go 标准库的本地并发任务执行引擎。面向“任务持续涌入”的负载，
解决四件事：**受控并发与有界背压**、**失败分类与有限重试**、**取消/关闭的确定性**、
**结果汇总的一致性与投递幂等**，并提供可观测指标与可复现的本地基准。

> 无外部服务、无真实凭据、无付费资源；全部结论可用常规 `go` 命令在本地复现。

## 一分钟上手

```go
e := engine.New(engine.Config{
    Workers:     4,                       // 受控并发度
    QueueSize:   8,                       // 有界等待队列（满则背压）
    Retry:       engine.DefaultRetryPolicy(),
    Deduplicate: true,                    // 相同 Task.ID 幂等
})
e.Start()

res, err := e.Submit(ctx, engine.Task{
    ID: "job-1",
    Fn: func(ctx context.Context) error {
        // ctx 在引擎 Cancel 时取消；返回 nil / TempError / PermError / ctx.Err()
        return engine.TempError(errors.New("稍后重试"))
    },
})

_ = e.Shutdown(ctx) // 优雅排空；或 e.Cancel() 触发取消式关闭
m := e.Snapshot()   // 一致的指标快照
```

可直接运行演示：`go run ./cmd/demo`。

## 1. 并发与背压模型

- **固定工作协程池**：`Workers` 个 goroutine 从单一有界通道 `queue`（容量 `QueueSize`）
  取任务，因此系统内“在执行 + 已缓冲”的任务数严格有界（≤ `Workers + QueueSize`，
  外加提交瞬间的少量在途对象），**不会无界缓冲**。
- **背压是“阻塞提交方”，不是丢弃**：`Submit` 在队列满时阻塞，直到出现空位；
  提交方因此被天然降速到处理能力附近。提交也可以带 `ctx`，`ctx` 结束即放弃
  本次等待——此时任务**尚未受理**，不会执行、不会计入结果。
- **显式降载入口**：`TrySubmit` 在队列满时立即返回 `accepted=false`（而非静默丢弃），
  由调用方决定重试/降级/上报。
- **在途对象的接收/撤销是确定的**：提交在“入队成功”这一点才算受理；若在背压
  阻塞期间被 `ctx` 或引擎关闭唤醒而未入队，则回滚受理计数并把该条目标记为
  “受理撤销”，唤醒任何已共享该条目的并发投递，绝不出现半完成结果或永久挂起。
- `outstanding` WaitGroup 在**入队之前**计数，保证关闭排空不会与晚入队发生竞态。

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

瞬时量表（仅快照）：`QueueDepth`（队列深度）、`ActiveWorkers`（执行/退避中的任务数）。

## 6. 本地验证

```bash
# 常规构建与静态检查
go build ./...
go vet ./...

# 全量测试（含持续过载背压、中途取消、重试耗尽、重复投递、一致性），打印输入与判定依据
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

- `task.go`：`Task` / `Result` / `FailureClass` 与错误分类（`TempError`/`PermError`/`Classify`）。
- `retry.go`：`RetryPolicy` 与确定性指数退避。
- `metrics.go`：`Metrics` 与顺序无关的聚合器。
- `engine.go`：有界队列、工作协程池、背压、幂等、取消/关闭、终态裁决。
- `cmd/demo`：可运行的行为演示。
- `*_test.go`：场景测试与基准/对照实验。
