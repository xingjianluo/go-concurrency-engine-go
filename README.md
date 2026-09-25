# go-concurrency-engine-go

单进程本地并发任务执行引擎：在持续涌入的任务负载下保持**受控并发、有界缓冲、结果可预期、性能可观测**。仅依赖 Go 标准库。

## 快速开始

```go
e := engine.New(engine.Config{
    Workers:   8,          // 并发 worker 数
    QueueSize: 64,         // 有界缓冲容量（不含在途任务）
    Retry: engine.RetryPolicy{
        MaxAttempts: 3,               // 总尝试上限（含首次）
        BaseBackoff: time.Millisecond,
        MaxBackoff:  50 * time.Millisecond,
    },
})

// 提交：队列满时阻塞（背压）；相同 ID 重复投递被幂等忽略
err := e.Submit(ctx, engine.Task{ID: "job-1", Run: func(ctx context.Context) error {
    return engine.Temporary(errors.New("try again later")) // 或 engine.Permanent(...)
}})

// 消费结果：每个被接受任务恰好产生一个 Result
for r := range e.Results() { /* r.Kind / r.Attempts / r.Duration */ }

// 关闭：在途任务（含重试退避中）执行到终态后，结果通道关闭
e.Shutdown(context.Background())
```

## 并发与背压模型

- **受控并发**：固定数量 worker（`Workers`）从有界队列（`QueueSize`）取任务，在途任务数恒 ≤ `Workers`。
- **背压**：队列满时 `Submit` 阻塞，直到出现容量、调用方 `ctx` 取消或引擎关闭。引擎**不无界缓冲、不静默丢弃**任何已接受任务。
- **关闭语义**：`Shutdown` 后新提交返回 `ErrClosed`，阻塞中的提交被唤醒并返回 `ErrClosed`；已在途任务（含重试与退避中的任务）继续执行到达终态，每个被接受任务**恰好产生一个 `Result`**，随后结果通道关闭。`Shutdown` 的 `ctx` 超时则强制取消在途任务，它们以 `KindCancelled` 收尾——取消过程中不存在半完成或丢失结果。
- **结果通道不阻塞 worker**：内部转发器将结果无界转发到对外通道，消费方慢不会反噬执行侧（背压只作用于提交侧）。

## 失败分类、重试与取消规则

| 分类 | 触发 | 行为 |
|---|---|---|
| `KindSuccess` | `Run` 返回 nil | 计成功 |
| `KindTemporary` | `engine.Temporary(err)` 或未分类错误，重试至 `MaxAttempts` 耗尽 | 计临时失败，只计一次结果 |
| `KindPermanent` | `engine.Permanent(err)` | 立即终态，**不重试**，不阻塞其余任务 |
| `KindCancelled` | 强制关闭时任务未开始/退避中/`Run` 返回 `context.Canceled` | 计取消 |

- 重试采用指数退避：`BaseBackoff * 2^(attempt-1)`，封顶 `MaxBackoff`；退避可被强制取消打断。
- 重试在**同一任务上下文**内进行，无论重试多少次，该任务只产生一个终态 `Result`，不会重复累计。
- 未分类错误按临时失败处理（可重试）。

## 幂等与结果一致性

- `Task.ID` 是幂等键：相同 ID 的重复投递只执行一次、只计一次结果，其余计入 `Deduped`。
- 汇总一致性：`Succeeded + TempFailed + PermFailed + Cancelled == Completed == Submitted`（关闭后排空结果通道时），与任务完成顺序无关；`TotalTime` 为各任务耗时之和，可手工复算核对。

## 可观测指标

`e.Metrics().Snapshot()` 返回一致性快照：

| 字段 | 含义 |
|---|---|
| `Submitted` / `Deduped` | 接受的提交数 / 幂等忽略的重复投递数 |
| `Succeeded` / `TempFailed` / `PermFailed` / `Cancelled` | 四类终态计数 |
| `Retries` | 实际发生的重试次数 |
| `Completed` / `TotalTime` / `MeanLatency` | 完成总数 / 总耗时 / 平均单任务耗时 |

## 本地验证

```bash
go build ./...                 # 构建
go test ./engine/ -v           # 单元测试（日志打印输入与判定依据）
go test ./engine/ -race        # 竞态检测
go test ./engine/ -run XXX -bench .   # 基准与对照实验
```

测试覆盖：持续过载触发背压、阻塞中取消、重试耗尽、永久失败不重试、重复投递幂等、混合结局汇总一致性、优雅关闭完成在途任务、强制关闭取消在途任务、关闭后拒绝提交。

## 基准与对照实验（本机 Xeon 6982P-C, GOMAXPROCS=4）

```
BenchmarkEngine-4        682641     1532 ns/op      # 受控并发引擎
BenchmarkSerial-4       3040406      343.5 ns/op    # 串行对照（单任务成本）
BenchmarkOverloadComparison/engine-4      254   18 avg-µs/task    572761 tasks/s
BenchmarkOverloadComparison/unbounded-4   597  323 avg-µs/task   1915488 tasks/s
```

- 引擎相对串行：8 worker 下吞吐约 4.5 倍于串行（受 GOMAXPROCS=4 限制）。
- 持续过载对照：无界实现提交不阻塞、瞬时吞吐数字更高，但任务堆积内存队列，**平均端到端延迟约为有界引擎的 18 倍**（323µs vs 18µs），且内存随提交速率无界增长；有界引擎以提交侧背压换取有界内存与可预期的低延迟。
