# 本地并发任务执行引擎（go-concurrency-engine）

单进程、仅依赖 Go 标准库的本地并发任务执行引擎。面向“任务持续涌入”的负载，
解决五件事：**受控并发与有界背压**、**失败分类与有限重试**、**取消/关闭的确定性**、
**结果汇总的一致性与投递幂等**、**按来源分组隔离的公平调度与单路取消/排空**，
并提供可观测指标与可复现的本地基准。

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

按来源分组（多路隔离）只需在 `Config.GroupLimits` 配置各路额度、在任务上标注 `Source`，
需要时只取消/排空某一路：

```go
e := engine.New(engine.Config{
    Workers: 4, QueueSize: 16, Deduplicate: true,
    GroupLimits: map[string]int{"orders": 3, "reports": 1}, // 各路在跑上限；全局 4 仍生效
})
e.Start()

_, _ = e.Submit(ctx, engine.Task{ID: "o1", Source: "orders",  Fn: doWork})
_, _ = e.Submit(ctx, engine.Task{ID: "r1", Source: "reports", Fn: doWork})

_ = e.CancelGroup("reports")                       // 只取消这一路；orders 照常
g, _ := e.GroupSnapshot("orders")                  // 按路看受理/成功/失败/取消
// 所有 GroupSnapshot 同名计数之和 == Snapshot 全局计数
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

## 5. 按来源分组隔离的调度

引擎在“全局统一调度”之上增加了**来源分组（`Task.Source`）**维度，让本地执行器
能同时接多路来源的活，而不会被其中某一路猛灌拖垮，也支持只对某一路做取消或排空。

### 5.1 分组额度与全局并发怎么叠加

- `Config.GroupLimits` 为每个来源单独配置“同时在跑（含退避等待）”的任务数上限；
  未配置的分组只受全局上限约束。空字符串 `""` 是**默认分组**，也是不指定 `Source`
  的旧用法所在的分组。
- 两层上限**同时生效、取更紧者**：一个任务要开始执行，必须同时满足
  “本路在跑数 < 本路 `limit`”且“全局在跑数 < `Workers`”。
  例如两组各配 `limit=3`、`Workers=4`，稳态绝不会出现 `3+3=6`：两组之和最多 4
  （典型 `3+1` / `2+2`），而任一组也不会超过 3。
- 某一路把自己的额度占满时，它后续的活只在**本路排队**等待；执行位会让给其它路，
  本路不会占着执行位置导致别路干不了活，也不会因此卡死、丢任务。
- 受理（进入系统）仍受**全局有界缓冲** `QueueSize` 约束：满了就在提交方处阻塞背压，
  绝不靠无界堆积换取公平；`TrySubmit` 在没有空位时立即返回 `accepted=false`。

### 5.2 分组之间怎么保证公平

调度在单个调度 goroutine 内完成，派发执行位时对分组做**轮转（round-robin）**：

- 从轮转游标起逐组考察：本路触顶就跳过；本路缓冲有任务就派缓冲队首；缓冲为空但
  本路有正在等待受理的提交方，就直接会合派发它的一个任务；本路没活则继续下一组。
- 每真正派发一次，游标推进到该组之后。因此在持续过载下，即使某一路猛灌把有界缓冲
  占满，轮到其它路时只要它还有等待者，就一定能拿到执行位——**其它路不会被无限期
  饿死**（本机测试中，A 路 64 协程猛灌时，B 路任务“提交→开始执行”的最大等待约
  2~3ms，与一个轮转周期同量级）。
- 等待受理的提交方按 FIFO 进入有界等待队列；公平由派发阶段的分组轮转保证，
  FIFO 只保证提交方本身不会被无限期搁置。缓冲只用于提升吞吐，不影响分组公平。

### 5.3 只取消 / 只排空某一路

- `CancelGroup(source)`：**只**影响该路——
  - 本路在途任务的 ctx 立即取消、正在进行的退避立即结束；
  - 本路已受理、排队中的任务得到确定的 `ClassCanceled` 裁决（未执行、`Attempts=0`，
    **不再重试、不重复计数、不丢弃**）；
  - 本路停止受理，之后向该路提交返回 `ErrGroupCanceled`；
  - **其它路完全不受影响**，继续正常提交与执行。调用阻塞至本路已受理任务全部终态。
- `ShutdownGroup(ctx, source)`：**只排空关闭**该路——停止受理（之后提交返回
  `ErrGroupClosed`），本路已受理任务**照常执行到终态**（不取消、重试语义不变），
  `ctx` 用于限定排空等待时间；其它路继续正常工作。
- 对从未出现且未在 `GroupLimits` 中配置的分组调用查询返回 `ErrUnknownGroup`；
  重复调用取消/排空是幂等的。

### 5.4 分组指标与全局一致

- `GroupSnapshot(source)` 返回某一路的计数（受理/成功/永久失败/重试耗尽/取消/尝试/
  重试/背压）与本路瞬时量表（`QueueDepth`、`ActiveWorkers`）；`Groups()` 列出全部分组。
- 每个任务的终态用**同一段累加代码**同时计入全局与其所属分组，因此对任意时刻恒有：
  **所有分组同名计数之和 == 全局同名计数**；且各组
  `成功 + 永久失败 + 重试耗尽 + 取消 == 本路受理`。终态先落库再唤醒提交方，
  因而提交方拿到结果后立刻 `Snapshot` 也一定能看到对应计数，重复多次统计结果一致。
- 幂等重复投递事件（`DuplicateDeliveries`）计入发起提交的那一路；任务本身的执行与
  终态仍归属它首次被受理的那一路，因此该任务只执行一次、只在一路计一次。

## 6. 兼容范围

- **不指定 `Task.Source`（空字符串）的旧用法照常工作**：任务进入默认分组，
  不配 `GroupLimits` 时该组不单独限流，行为与引入分组前一致（仍受全局 `Workers` 与
  `QueueSize` 约束）。`Task` 仅新增了一个可选字段 `Source`、`Result` 新增 `Source`，
  其余字段与全部既有方法（`Submit`/`TrySubmit`/`Cancel`/`Shutdown`/`Snapshot`/
  `Results`）签名与语义保持不变。

## 7. 可观测指标（`Metrics`）

计数器（单调累计）：

- `Submitted`：去重后实际受理的唯一任务数；
- `DuplicateDeliveries`：被幂等拦截的重复投递次数；
- `Succeeded` / `PermanentFailures` / `RetryExhausted` / `Canceled`：四类终态数；
- `Retries` / `Attempts`：重试次数 / 总尝试次数；
- `TaskTimeTotal`：所有任务从开始执行至终态裁决的耗时之和（含退避）；
- `BackpressureWaits` / `BackpressureTime`：提交方因队列满等待的次数与累计时长。

瞬时量表（仅快照）：`QueueDepth`（队列深度）、`ActiveWorkers`（执行/退避中的任务数）。
`GroupSnapshot(source)` 返回的同名指标只统计该来源分组，全局快照与各分组合计口径一致。

## 8. 本地验证

```bash
# 常规构建与静态检查
go build ./...
go vet ./...

# 全量测试（含持续过载背压、中途取消、重试耗尽、重复投递、一致性，
# 以及分组公平、单路取消/排空、分组额度×全局并发触顶），打印输入与判定依据
go test ./... -v

# 竞态检测，建议多跑几轮（验收默认就按这套带竞态、多轮重复的方式）
go test ./... -race -count=10

# 只跑分组相关新能力
go test ./... -race -v -run 'TestGroup|TestDefaultGroup'

# 行为演示：基础能力 / 分组隔离（分组额度叠加全局、公平、单路取消、分路计数）
go run ./cmd/demo
go run ./cmd/groupdemo

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

## 9. 包结构

- `task.go`：`Task`（含新增的 `Source` 分组键）/ `Result`（含 `Source`）/ `FailureClass`
  与错误分类（`TempError`/`PermError`/`Classify`）。
- `retry.go`：`RetryPolicy` 与确定性指数退避。
- `metrics.go`：`Metrics` 与顺序无关、全局/分组同口径的聚合器。
- `group.go`：分组调度器——各路有界队列、分组轮转公平派发、分组额度×全局并发叠加、
  单路取消/排空、会合式有界受理（背压）。
- `engine.go`：工作协程池、背压入口、幂等、引擎整体取消/关闭、终态裁决与生命周期。
- `cmd/demo`：基础能力演示；`cmd/groupdemo`：分组隔离（额度叠加/公平/单路取消/计数）演示。
- `*_test.go`：场景测试与基准/对照实验；`group_test.go` 覆盖分组公平、单路取消/排空、
  额度触顶、分组背压有界、分组指标一致与默认分组兼容。
