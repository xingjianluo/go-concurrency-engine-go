package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubmitGuardErrors(t *testing.T) {
	// Start 之前
	e := New(Config{Workers: 1, QueueSize: 1, Retry: NoRetry(), Deduplicate: true})
	if _, err := e.Submit(context.Background(), quickTask("x", 0, nil)); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Start 前提交期望 ErrNotStarted, got %v", err)
	}
	e.Start()

	// nil Fn
	if _, err := e.Submit(context.Background(), Task{ID: "nofn"}); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("nil Fn 期望 ErrInvalidTask, got %v", err)
	}
	// 幂等开启时空 ID
	if _, err := e.Submit(context.Background(), Task{Fn: func(context.Context) error { return nil }}); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("空 ID 期望 ErrInvalidTask, got %v", err)
	}

	// Start 幂等：重复调用不应改变工作协程数量（任务仍正常执行）。
	e.Start()
	r, err := e.Submit(context.Background(), quickTask("ok", time.Millisecond, nil))
	if err != nil || r.Class != ClassSuccess {
		t.Fatalf("重复 Start 后任务异常: %+v %v", r, err)
	}

	// 关闭后提交
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), quickTask("late", 0, nil)); !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("Shutdown 后提交期望 ErrEngineClosed, got %v", err)
	}
	if _, accepted, err := e.TrySubmit(quickTask("late2", 0, nil)); err == nil || accepted {
		t.Fatalf("Shutdown 后 TrySubmit 期望错误且不受理, got accepted=%v err=%v", accepted, err)
	}
	t.Log("判定依据: Start 前/nil Fn/空 ID/关闭后提交 均返回可判定错误，未发生 panic 或半受理")
}

func TestShutdownTimeoutReturnsContextError(t *testing.T) {
	e := New(Config{Workers: 1, QueueSize: 0, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	release := make(chan struct{})
	go func() {
		_, _ = e.Submit(context.Background(), Task{ID: "stuck", Fn: func(ctx context.Context) error {
			select {
			case <-release: // 故意不尊重 ctx，模拟无法立即取消的任务
				return nil
			}
		}})
	}()
	time.Sleep(10 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := e.Shutdown(ctx)
	t.Logf("输入: 任务无视取消信号 => Shutdown(30ms ctx) 返回 err=%v（期望 deadline exceeded）", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 DeadlineExceeded, got %v", err)
	}
	close(release) // 释放后引擎最终排空
	select {
	case <-e.stoppedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("任务释放后引擎未停止")
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	// 首次临时失败后进入长退避；取消必须立即打断退避，裁决取消，不再重试。
	e := New(Config{Workers: 1, QueueSize: 1,
		Retry:       RetryPolicy{MaxAttempts: 5, BaseDelay: 10 * time.Second, Multiplier: 2},
		Deduplicate: true})
	e.Start()
	var calls int64
	done := make(chan Result, 1)
	go func() {
		r, _ := e.Submit(context.Background(), Task{ID: "backoff", Fn: func(ctx context.Context) error {
			calls++
			return TempError(errors.New("t"))
		}})
		done <- r
	}()
	time.Sleep(20 * time.Millisecond) // 确保已执行首次尝试并进入退避
	e.Cancel()
	select {
	case r := <-done:
		t.Logf("输入: 10s 退避中的临时失败任务，中途 Cancel => class=%s attempts=%d（期望 canceled/1）调用=%d",
			r.Class, r.Attempts, calls)
		if r.Class != ClassCanceled || r.Attempts != 1 || calls != 1 {
			t.Fatalf("got %+v calls=%d", r, calls)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("取消未能打断退避等待")
	}
}

func TestCallerAbandonAfterEnqueued(t *testing.T) {
	// 任务已受理并执行时，提交方放弃等待不影响任务执行与结果统计。
	e := New(Config{Workers: 2, QueueSize: 4, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	finished := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		defer cancel()
		_, err := e.Submit(ctx, Task{ID: "slow-accepted", Fn: func(ctx context.Context) error {
			time.Sleep(60 * time.Millisecond)
			close(finished)
			return nil
		}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("提交方期望 DeadlineExceeded, got %v", err)
		}
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("任务未在提交方放弃后继续执行完成")
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := e.Snapshot()
	t.Logf("提交方中途放弃等待后，任务仍执行完成 => Submitted=%d Succeeded=%d（期望 1/1）", m.Submitted, m.Succeeded)
	if m.Submitted != 1 || m.Succeeded != 1 {
		t.Fatalf("metrics=%+v", m)
	}
}

func TestErrorHelpersAndBackoffEdges(t *testing.T) {
	if TempError(nil) != nil || PermError(nil) != nil {
		t.Fatal("TempError/PermError(nil) 必须返回 nil")
	}
	if ClassUnknown.String() != "unknown" {
		t.Fatalf("ClassUnknown.String=%s", ClassUnknown.String())
	}
	if got := (&classifiedError{cause: nil, class: ClassPermanent}).Error(); got != "permanent" {
		t.Fatalf("nil cause Error()=%s", got)
	}
	// BaseDelay<=0 / attempt 非法 => 不退避；Multiplier<=1 恒定。
	if (RetryPolicy{BaseDelay: 0}).nextBackoff(1) != 0 {
		t.Fatal("BaseDelay=0 应返回 0")
	}
	p := RetryPolicy{BaseDelay: 10 * time.Millisecond, MaxDelay: 25 * time.Millisecond, Multiplier: 2}
	if d := p.nextBackoff(3); d != 25*time.Millisecond {
		t.Fatalf("退避封顶错误: %v", d)
	}
	p2 := RetryPolicy{BaseDelay: 5 * time.Millisecond, Multiplier: 1}
	if d := p2.nextBackoff(4); d != 5*time.Millisecond {
		t.Fatalf("恒定退避错误: %v", d)
	}
	// DefaultRetryPolicy 可用：首次临时失败、第二次成功。
	e := New(Config{Workers: 1, QueueSize: 1, Retry: DefaultRetryPolicy(), Deduplicate: true})
	e.Start()
	var n int
	r, err := e.Submit(context.Background(), Task{ID: "d", Fn: func(context.Context) error {
		n++
		if n < 2 {
			return TempError(errors.New("t"))
		}
		return nil
	}})
	if err != nil || r.Class != ClassSuccess || r.Attempts != 2 {
		t.Fatalf("DefaultRetryPolicy 下结果异常: %+v %v", r, err)
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("判定依据: 错误助手 nil 语义、未知类别、退避零值/封顶/恒定与默认策略均符合预期")
}
