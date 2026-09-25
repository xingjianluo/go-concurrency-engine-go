package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestAggregatorOrderIndependent：同一批终态结果以任意顺序/并发聚合，
// 最终计数与总耗时必须完全一致且可复现。
func TestAggregatorOrderIndependent(t *testing.T) {
	mk := func() []Result {
		rs := make([]Result, 0, 40)
		for i := 0; i < 20; i++ {
			rs = append(rs, Result{TaskID: fmt.Sprintf("s%d", i), Class: ClassSuccess, Attempts: 1, Duration: time.Millisecond})
		}
		for i := 0; i < 8; i++ {
			rs = append(rs, Result{TaskID: fmt.Sprintf("p%d", i), Class: ClassPermanent, Attempts: 1, Duration: 2 * time.Millisecond})
		}
		for i := 0; i < 7; i++ {
			rs = append(rs, Result{TaskID: fmt.Sprintf("r%d", i), Class: ClassTemporary, Attempts: 3, Duration: 3 * time.Millisecond})
		}
		for i := 0; i < 5; i++ {
			rs = append(rs, Result{TaskID: fmt.Sprintf("c%d", i), Class: ClassCanceled, Attempts: 2, Duration: time.Millisecond})
		}
		return rs
	}
	want := Metrics{Succeeded: 20, PermanentFailures: 8, RetryExhausted: 7, Canceled: 5,
		Retries: 19, Attempts: 59, TaskTimeTotal: 20*time.Millisecond + 16*time.Millisecond + 21*time.Millisecond + 5*time.Millisecond}

	// 正序
	a1 := newAggregator()
	for _, r := range mk() {
		a1.recordResult(r)
	}
	// 逆序
	rs := mk()
	a2 := newAggregator()
	for i := len(rs) - 1; i >= 0; i-- {
		a2.recordResult(rs[i])
	}
	// 并发乱序
	a3 := newAggregator()
	var wg sync.WaitGroup
	for _, r := range mk() {
		wg.Add(1)
		go func(r Result) { defer wg.Done(); a3.recordResult(r) }(r)
	}
	wg.Wait()

	for name, a := range map[string]*aggregator{"正序": a1, "逆序": a2, "并发乱序": a3} {
		got := a.snapshot()
		t.Logf("输入: 40 个结果(成功20/永久8/耗尽7/取消5) 以%s聚合 => 成功=%d 永久=%d 耗尽=%d 取消=%d 尝试=%d 重试=%d 总耗时=%v",
			name, got.Succeeded, got.PermanentFailures, got.RetryExhausted, got.Canceled,
			got.Attempts, got.Retries, got.TaskTimeTotal)
		if got != want {
			t.Fatalf("%s 聚合不一致:\n got=%+v\nwant=%+v", name, got, want)
		}
	}
}

// TestMixedOutcomesConsistency：端到端混合负载，重复跑两次引擎，
// 成功/永久/耗尽/取消计数必须完全一致、可复现，且四类之和恒等于受理数。
func TestMixedOutcomesConsistency(t *testing.T) {
	runOnce := func() Metrics {
		e := New(Config{Workers: 4, QueueSize: 8,
			Retry:       RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, Multiplier: 2},
			Deduplicate: true})
		e.Start()
		var wg sync.WaitGroup
		for i := 0; i < 40; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				switch {
				case i%4 == 0:
					err = TempError(errors.New("t")) // 重试 1 次后耗尽
				case i%4 == 1:
					err = PermError(errors.New("p"))
				}
				_, _ = e.Submit(context.Background(), quickTask(fmt.Sprintf("mix%d", i), time.Millisecond, err))
			}()
		}
		wg.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		return e.Snapshot()
	}
	m1 := runOnce()
	m2 := runOnce()
	t.Logf("两次独立运行混合负载（成功/永久失败/重试耗尽）:")
	t.Logf("  run1: 受理=%d 成功=%d 永久=%d 耗尽=%d 尝试=%d 重试=%d", m1.Submitted, m1.Succeeded, m1.PermanentFailures, m1.RetryExhausted, m1.Attempts, m1.Retries)
	t.Logf("  run2: 受理=%d 成功=%d 永久=%d 耗尽=%d 尝试=%d 重试=%d", m2.Submitted, m2.Succeeded, m2.PermanentFailures, m2.RetryExhausted, m2.Attempts, m2.Retries)
	// 计数可复现（耗时类字段不比较）
	for _, f := range []struct {
		name string
		a, b int64
	}{
		{"Submitted", m1.Submitted, m2.Submitted},
		{"Succeeded", m1.Succeeded, m2.Succeeded},
		{"Permanent", m1.PermanentFailures, m2.PermanentFailures},
		{"Exhausted", m1.RetryExhausted, m2.RetryExhausted},
		{"Attempts", m1.Attempts, m2.Attempts},
		{"Retries", m1.Retries, m2.Retries},
	} {
		if f.a != f.b {
			t.Fatalf("%s 两次运行不一致: %d vs %d", f.name, f.a, f.b)
		}
	}
	if m1.Succeeded+m1.PermanentFailures+m1.RetryExhausted+m1.Canceled != m1.Submitted {
		t.Fatal("终态四类之和不等于受理数")
	}
}
