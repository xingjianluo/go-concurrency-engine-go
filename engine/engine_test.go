package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func shutdownAndDrain(t *testing.T, e *Engine) []Result {
	t.Helper()
	var out []Result
	done := make(chan struct{})
	go func() {
		for r := range e.Results() {
			out = append(out, r)
		}
		close(done)
	}()
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("结果通道未在 Shutdown 后关闭")
	}
	return out
}

// TestBackpressureUnderOverload 持续过载：提交速率远高于处理能力，
// 验证有界队列对提交方施加背压，且所有任务被确定处理、无丢失无重复。
func TestBackpressureUnderOverload(t *testing.T) {
	const total = 200
	const workers = 4
	const queueSize = 8
	t.Logf("输入: total=%d workers=%d queueSize=%d 每任务耗时=5ms", total, workers, queueSize)

	e := New(Config{Workers: workers, QueueSize: queueSize})
	var inFlight atomic.Int64
	var maxInFlight atomic.Int64

	start := time.Now()
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("bp-%d", i)
		err := e.Submit(context.Background(), Task{ID: id, Run: func(ctx context.Context) error {
			n := inFlight.Add(1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return nil
		}})
		if err != nil {
			t.Fatalf("Submit(%s): %v", id, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("提交 %d 个任务耗时 %v（背压生效：受 %d workers 限流）", total, elapsed, workers)

	results := shutdownAndDrain(t, e)
	// 判定依据：在途并发数不得超过 workers + queueSize + 少量调度余量
	if m := maxInFlight.Load(); m > workers {
		t.Fatalf("在途任务数 %d 超过 worker 数 %d", m, workers)
	}
	if len(results) != total {
		t.Fatalf("结果数 %d != 提交数 %d（丢失或重复）", len(results), total)
	}
	seen := map[string]int{}
	for _, r := range results {
		seen[r.ID]++
		if r.Kind != KindSuccess {
			t.Fatalf("任务 %s 非成功: %v", r.ID, r.Kind)
		}
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("任务 %s 出现 %d 次结果", id, c)
		}
	}
	snap := e.Metrics().Snapshot()
	t.Logf("判定依据: results=%d submitted=%d completed=%d maxInFlight=%d",
		len(results), snap.Submitted, snap.Completed, maxInFlight.Load())
	if snap.Submitted != total || snap.Succeeded != total || snap.Completed != total {
		t.Fatalf("指标不一致: %+v", snap)
	}
}

// TestSubmitCancelWhileBlocked 队列满时 Submit 阻塞，调用方取消后返回 ctx 错误，
// 且该任务不产生结果、不占用幂等键。
func TestSubmitCancelWhileBlocked(t *testing.T) {
	e := New(Config{Workers: 1, QueueSize: 1})
	release := make(chan struct{})
	// 占满 worker 与队列
	if err := e.Submit(context.Background(), Task{ID: "blocker", Run: func(ctx context.Context) error {
		<-release
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Submit(context.Background(), Task{ID: "queued", Run: func(ctx context.Context) error { return nil }}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := e.Submit(ctx, Task{ID: "will-cancel", Run: func(ctx context.Context) error { return nil }})
	t.Logf("输入: 队列满时提交 will-cancel，ctx 50ms 超时；判定依据: 返回 ctx.DeadlineExceeded，实际=%v", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 DeadlineExceeded，实际 %v", err)
	}

	close(release)
	results := shutdownAndDrain(t, e)
	snap := e.Metrics().Snapshot()
	t.Logf("判定依据: submitted=%d results=%d（will-cancel 不得计入）", snap.Submitted, len(results))
	if snap.Submitted != 2 || len(results) != 2 {
		t.Fatalf("取消的提交被计入: submitted=%d results=%d", snap.Submitted, len(results))
	}
}

// TestRetryExhausted 临时失败重试至上限后计为 KindTemporary，只计一次结果。
func TestRetryExhausted(t *testing.T) {
	const maxAttempts = 3
	e := New(Config{Workers: 2, QueueSize: 4,
		Retry: RetryPolicy{MaxAttempts: maxAttempts, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}})
	var calls atomic.Int64
	err := e.Submit(context.Background(), Task{ID: "flaky", Run: func(ctx context.Context) error {
		calls.Add(1)
		return Temporary(errors.New("boom"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	results := shutdownAndDrain(t, e)
	if len(results) != 1 {
		t.Fatalf("结果数 %d != 1（重试不得重复累计）", len(results))
	}
	r := results[0]
	t.Logf("输入: 任务始终返回临时失败, MaxAttempts=%d；判定依据: Kind=%s Attempts=%d calls=%d",
		maxAttempts, r.Kind, r.Attempts, calls.Load())
	if r.Kind != KindTemporary || r.Attempts != maxAttempts || calls.Load() != maxAttempts {
		t.Fatalf("重试耗尽判定失败: kind=%s attempts=%d calls=%d", r.Kind, r.Attempts, calls.Load())
	}
	snap := e.Metrics().Snapshot()
	if snap.TempFailed != 1 || snap.Retries != maxAttempts-1 || snap.Completed != 1 {
		t.Fatalf("指标不一致: %+v", snap)
	}
}

// TestPermanentFailureNoRetry 永久失败不重试，且不阻塞其余任务完成。
func TestPermanentFailureNoRetry(t *testing.T) {
	e := New(Config{Workers: 2, QueueSize: 4,
		Retry: RetryPolicy{MaxAttempts: 5, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}})
	var permCalls atomic.Int64
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("ok-%d", i)
		if err := e.Submit(context.Background(), Task{ID: id, Run: func(ctx context.Context) error { return nil }}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Submit(context.Background(), Task{ID: "bad", Run: func(ctx context.Context) error {
		permCalls.Add(1)
		return Permanent(errors.New("fatal"))
	}}); err != nil {
		t.Fatal(err)
	}
	results := shutdownAndDrain(t, e)
	var perm, succ int
	for _, r := range results {
		switch r.Kind {
		case KindPermanent:
			perm++
			if r.Attempts != 1 {
				t.Fatalf("永久失败被重试: attempts=%d", r.Attempts)
			}
		case KindSuccess:
			succ++
		}
	}
	t.Logf("输入: 5 个成功任务 + 1 个永久失败(MaxAttempts=5)；判定依据: perm=%d succ=%d permCalls=%d",
		perm, succ, permCalls.Load())
	if perm != 1 || succ != 5 || permCalls.Load() != 1 {
		t.Fatalf("永久失败判定失败: perm=%d succ=%d calls=%d", perm, succ, permCalls.Load())
	}
}

// TestDuplicateSubmissionIdempotent 相同 ID 重复投递只执行一次、只计一次结果。
func TestDuplicateSubmissionIdempotent(t *testing.T) {
	e := New(Config{Workers: 2, QueueSize: 4})
	var calls atomic.Int64
	mk := func() Task {
		return Task{ID: "dup", Run: func(ctx context.Context) error {
			calls.Add(1)
			return nil
		}}
	}
	for i := 0; i < 5; i++ {
		if err := e.Submit(context.Background(), mk()); err != nil {
			t.Fatal(err)
		}
	}
	results := shutdownAndDrain(t, e)
	snap := e.Metrics().Snapshot()
	t.Logf("输入: 同一 ID 投递 5 次；判定依据: results=%d calls=%d deduped=%d",
		len(results), calls.Load(), snap.Deduped)
	if len(results) != 1 || calls.Load() != 1 || snap.Deduped != 4 {
		t.Fatalf("幂等判定失败: results=%d calls=%d deduped=%d", len(results), calls.Load(), snap.Deduped)
	}
}

// TestSummaryConsistency 混合结局（成功/临时耗尽/永久失败）下，
// 无论完成顺序如何，分类计数与总耗时汇总一致：分项之和=完成数=提交数。
func TestSummaryConsistency(t *testing.T) {
	const perKind = 20
	e := New(Config{Workers: 8, QueueSize: 16,
		Retry: RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}})
	for i := 0; i < perKind; i++ {
		mustSubmit(t, e, fmt.Sprintf("succ-%d", i), func(ctx context.Context) error {
			time.Sleep(time.Duration(i%3) * time.Millisecond)
			return nil
		})
		mustSubmit(t, e, fmt.Sprintf("temp-%d", i), func(ctx context.Context) error {
			return Temporary(errors.New("retry me"))
		})
		mustSubmit(t, e, fmt.Sprintf("perm-%d", i), func(ctx context.Context) error {
			return Permanent(errors.New("no retry"))
		})
	}
	results := shutdownAndDrain(t, e)
	snap := e.Metrics().Snapshot()
	t.Logf("输入: 成功/临时耗尽/永久失败 各 %d 个；判定依据: %+v", perKind, snap)

	var succ, temp, perm, cancel int
	var total time.Duration
	ids := map[string]bool{}
	for _, r := range results {
		if ids[r.ID] {
			t.Fatalf("任务 %s 结果重复", r.ID)
		}
		ids[r.ID] = true
		total += r.Duration
		switch r.Kind {
		case KindSuccess:
			succ++
		case KindTemporary:
			temp++
		case KindPermanent:
			perm++
		case KindCancelled:
			cancel++
		}
	}
	if succ != perKind || temp != perKind || perm != perKind || cancel != 0 {
		t.Fatalf("分类计数不一致: succ=%d temp=%d perm=%d cancel=%d", succ, temp, perm, cancel)
	}
	if snap.Completed != 3*perKind || snap.Submitted != 3*perKind {
		t.Fatalf("汇总不一致: %+v", snap)
	}
	if snap.TotalTime != total {
		t.Fatalf("总耗时不一致: metrics=%v 手工汇总=%v", snap.TotalTime, total)
	}
}

// TestGracefulShutdownCompletesInFlight Shutdown 时在途任务（含重试退避中）必须完成。
func TestGracefulShutdownCompletesInFlight(t *testing.T) {
	e := New(Config{Workers: 2, QueueSize: 2,
		Retry: RetryPolicy{MaxAttempts: 3, BaseBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}})
	var attempts atomic.Int64
	mustSubmit(t, e, "slow-ok", func(ctx context.Context) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	})
	mustSubmit(t, e, "flaky-ok", func(ctx context.Context) error {
		if attempts.Add(1) < 3 {
			return Temporary(errors.New("again"))
		}
		return nil
	})
	results := shutdownAndDrain(t, e)
	t.Logf("输入: 1 个慢任务 + 1 个第 3 次才成功的任务，Shutdown 等待；判定依据: 全部成功")
	for _, r := range results {
		if r.Kind != KindSuccess {
			t.Fatalf("任务 %s 未优雅完成: %s", r.ID, r.Kind)
		}
	}
	if len(results) != 2 {
		t.Fatalf("结果数 %d != 2", len(results))
	}
}

// TestForcedShutdownCancelsInFlight Shutdown ctx 超时强制取消在途任务，
// 任务以 KindCancelled 获得确定结果，结果通道随后关闭。
func TestForcedShutdownCancelsInFlight(t *testing.T) {
	e := New(Config{Workers: 1, QueueSize: 1})
	mustSubmit(t, e, "stuck", func(ctx context.Context) error {
		<-ctx.Done()     // 响应取消
		return ctx.Err() // 以取消错误收尾
	})
	// 等任务进入执行
	time.Sleep(20 * time.Millisecond)

	var out []Result
	done := make(chan struct{})
	go func() {
		for r := range e.Results() {
			out = append(out, r)
		}
		close(done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := e.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 DeadlineExceeded，实际 %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("结果通道未关闭")
	}
	t.Logf("输入: 在途任务阻塞至 ctx 取消，Shutdown 超时 50ms；判定依据: err=%v kind=%s",
		err, kindOf(out, "stuck"))
	if len(out) != 1 || out[0].Kind != KindCancelled {
		t.Fatalf("在途任务未以取消收尾: %+v", out)
	}
	snap := e.Metrics().Snapshot()
	if snap.Cancelled != 1 || snap.Completed != 1 {
		t.Fatalf("指标不一致: %+v", snap)
	}
}

// TestSubmitAfterShutdown 关闭后提交返回 ErrClosed。
func TestSubmitAfterShutdown(t *testing.T) {
	e := New(Config{Workers: 1})
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := e.Submit(context.Background(), Task{ID: "late", Run: func(ctx context.Context) error { return nil }})
	t.Logf("输入: Shutdown 后提交；判定依据: 返回 ErrClosed，实际=%v", err)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("期望 ErrClosed，实际 %v", err)
	}
}

func mustSubmit(t *testing.T, e *Engine, id string, run func(ctx context.Context) error) {
	t.Helper()
	if err := e.Submit(context.Background(), Task{ID: id, Run: run}); err != nil {
		t.Fatalf("Submit(%s): %v", id, err)
	}
}

func kindOf(results []Result, id string) Kind {
	for _, r := range results {
		if r.ID == id {
			return r.Kind
		}
	}
	return -1
}
