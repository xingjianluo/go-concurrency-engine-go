package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// runEngine 构造并启动一个引擎，测试结束自动优雅关闭。
func runEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e := New(cfg)
	e.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	})
	return e
}

// quickTask 构造一个睡眠 d 后返回 err 的任务。
func quickTask(id string, d time.Duration, err error) Task {
	return Task{ID: id, Fn: func(ctx context.Context) error {
		select {
		case <-time.After(d):
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
}

func TestClassify(t *testing.T) {
	base := errors.New("boom")
	cases := []struct {
		name string
		err  error
		want FailureClass
	}{
		{"nil", nil, ClassSuccess},
		{"plain error defaults temporary", base, ClassTemporary},
		{"temp wrapped", TempError(base), ClassTemporary},
		{"perm wrapped", PermError(base), ClassPermanent},
		{"canceled", context.Canceled, ClassCanceled},
		{"deadline", context.DeadlineExceeded, ClassCanceled},
		{"wrapped canceled", fmt.Errorf("x: %w", context.Canceled), ClassCanceled},
		{"wrapped temp", fmt.Errorf("y: %w", TempError(base)), ClassTemporary},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Classify(c.err)
			t.Logf("输入 err=%v => 判定 %s（期望 %s）", c.err, got, c.want)
			if got != c.want {
				t.Fatalf("Classify(%v)=%s, want %s", c.err, got, c.want)
			}
		})
	}
}

func TestSuccessAndCounts(t *testing.T) {
	e := runEngine(t, Config{Workers: 2, QueueSize: 4, Retry: NoRetry(), Deduplicate: true})
	n := 10
	for i := 0; i < n; i++ {
		res, err := e.Submit(context.Background(), quickTask(fmt.Sprintf("t%d", i), time.Millisecond, nil))
		if err != nil {
			t.Fatalf("task %d submit error: %v", i, err)
		}
		if res.Class != ClassSuccess || res.Attempts != 1 {
			t.Fatalf("task %d result=%+v", i, res)
		}
	}
	m := e.Snapshot()
	t.Logf("输入 %d 个成功任务 => Succeeded=%d Submitted=%d Attempts=%d", n, m.Succeeded, m.Submitted, m.Attempts)
	if m.Submitted != int64(n) || m.Succeeded != int64(n) || m.Attempts != int64(n) {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestPermanentFailureNoRetry(t *testing.T) {
	e := runEngine(t, Config{Workers: 1, QueueSize: 1,
		Retry: RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond}, Deduplicate: true})
	var calls int64
	task := Task{ID: "perm", Fn: func(ctx context.Context) error {
		atomic.AddInt64(&calls, 1)
		return PermError(errors.New("bad config"))
	}}
	res, err := e.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入永久失败任务 => class=%s attempts=%d（期望 permanent/1）, err=%v",
		res.Class, res.Attempts, res.Err)
	if res.Class != ClassPermanent || res.Attempts != 1 {
		t.Fatalf("got %+v", res)
	}
	if calls != 1 {
		t.Fatalf("永久失败不应重试，实际调用 %d 次", calls)
	}
	m := e.Snapshot()
	if m.PermanentFailures != 1 || m.Retries != 0 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestRetryExhausted(t *testing.T) {
	var calls int64
	e := runEngine(t, Config{Workers: 1, QueueSize: 1,
		Retry:       RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2},
		Deduplicate: true})
	task := Task{ID: "flaky", Fn: func(ctx context.Context) error {
		atomic.AddInt64(&calls, 1)
		return TempError(errors.New("transient")) // 永远临时失败
	}}
	res, err := e.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入持续临时失败任务，MaxAttempts=3 => class=%s attempts=%d（期望 temporary/3）",
		res.Class, res.Attempts)
	if res.Class != ClassTemporary || res.Attempts != 3 {
		t.Fatalf("got class=%s attempts=%d", res.Class, res.Attempts)
	}
	if calls != 3 {
		t.Fatalf("期望恰好执行 3 次，实际 %d 次", calls)
	}
	m := e.Snapshot()
	if m.RetryExhausted != 1 || m.Retries != 2 || m.Attempts != 3 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	var calls int64
	e := runEngine(t, Config{Workers: 1, QueueSize: 1,
		Retry: RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, Multiplier: 2}, Deduplicate: true})
	task := Task{ID: "recover", Fn: func(ctx context.Context) error {
		if atomic.AddInt64(&calls, 1) < 3 {
			return TempError(errors.New("not ready"))
		}
		return nil
	}}
	res, err := e.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入前两次临时失败第三次成功的任务 => class=%s attempts=%d（期望 success/3）",
		res.Class, res.Attempts)
	if res.Class != ClassSuccess || res.Attempts != 3 {
		t.Fatalf("got %+v", res)
	}
	m := e.Snapshot()
	if m.Succeeded != 1 || m.RetryExhausted != 0 || m.Retries != 2 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestRetryExhaustedDoesNotBlockOthers(t *testing.T) {
	// 耗尽重试的慢任务不得阻塞其余任务：工作协程数 >1，后续任务仍能完成。
	e := runEngine(t, Config{Workers: 4, QueueSize: 8,
		Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: 2 * time.Millisecond}, Deduplicate: true})
	var slowCalls int64
	if _, err := e.Submit(context.Background(), Task{ID: "slow", Fn: func(ctx context.Context) error {
		atomic.AddInt64(&slowCalls, 1)
		time.Sleep(5 * time.Millisecond)
		return TempError(errors.New("t"))
	}}); err != nil {
		t.Fatal(err)
	}
	ok := 0
	for i := 0; i < 12; i++ {
		res, err := e.Submit(context.Background(), quickTask(fmt.Sprintf("ok%d", i), time.Millisecond, nil))
		if err != nil {
			t.Fatal(err)
		}
		if res.Class == ClassSuccess {
			ok++
		}
	}
	m := e.Snapshot()
	t.Logf("输入 1 个重试耗尽慢任务 + 12 个正常任务 => 成功=%d 重试耗尽=%d（期望 12/1）",
		m.Succeeded, m.RetryExhausted)
	if ok != 12 || m.RetryExhausted != 1 {
		t.Fatalf("metrics=%+v ok=%d", m, ok)
	}
}
