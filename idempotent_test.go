package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDuplicateConcurrentIdempotent：同一任务 ID 被大量并发重复投递，
// 实际执行必须恰好一次，所有提交方拿到同一结果，汇总只计一次。
func TestDuplicateConcurrentIdempotent(t *testing.T) {
	e := runEngine(t, Config{Workers: 4, QueueSize: 8,
		Retry:       RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Multiplier: 2},
		Deduplicate: true})

	const dup = 50
	var executions int64
	task := Task{ID: "same-job", Fn: func(ctx context.Context) error {
		n := atomic.AddInt64(&executions, 1)
		time.Sleep(5 * time.Millisecond)
		if n == 1 {
			return TempError(errors.New("first transient")) // 首次临时失败，重试一次成功
		}
		return nil
	}}

	var wg sync.WaitGroup
	results := make([]Result, dup)
	errs := make([]error, dup)
	start := make(chan struct{})
	wg.Add(dup)
	for i := 0; i < dup; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = e.Submit(context.Background(), task)
		}()
	}
	close(start)
	wg.Wait()

	for i := 0; i < dup; i++ {
		if errs[i] != nil {
			t.Fatalf("提交 %d 返回错误: %v", i, errs[i])
		}
		// 唯一一次执行链：首次临时失败、重试成功 => class=success attempts=2
		if results[i].TaskID != "same-job" || results[i].Class != ClassSuccess || results[i].Attempts != 2 {
			t.Fatalf("提交 %d 结果异常: %+v", i, results[i])
		}
	}
	ref := results[0]
	m := e.Snapshot()
	t.Logf("输入: 同一 ID 并发重复投递 %d 次（首次临时失败、第二次成功）", dup)
	t.Logf("判定依据: Fn 实际调用次数=%d(期望2:1 次执行+1 次重试) 所有提交方结果一致(class=%s,attempts=%d) Submitted=%d(期望1) Duplicate=%d(期望%d) Succeeded=%d Retries=%d",
		executions, ref.Class, ref.Attempts,
		m.Submitted, m.DuplicateDeliveries, dup-1, m.Succeeded, m.Retries)

	if executions != 2 {
		t.Fatalf("幂等失效：Fn 被调用 %d 次（期望 2：仅 1 次执行 + 其 1 次重试）", executions)
	}
	if m.Submitted != 1 || m.DuplicateDeliveries != dup-1 {
		t.Fatalf("去重计数异常: %+v", m)
	}
	if m.Succeeded != 1 || m.Retries != 1 || m.PermanentFailures != 0 || m.RetryExhausted != 0 {
		t.Fatalf("结果被重复累计或分类错误: %+v", m)
	}
}

// TestIdempotentAcrossFailureClass：重复投递一个永久失败任务，
// 只产生一次永久失败计数，所有副本得到同一失败结果与错误原因。
func TestIdempotentAcrossFailureClass(t *testing.T) {
	e := runEngine(t, Config{Workers: 2, QueueSize: 4, Retry: NoRetry(), Deduplicate: true})
	var executions int64
	task := Task{ID: "perm-dup", Fn: func(ctx context.Context) error {
		atomic.AddInt64(&executions, 1)
		return PermError(errors.New("schema invalid"))
	}}
	const dup = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	classes := make([]FailureClass, dup)
	wg.Add(dup)
	for i := 0; i < dup; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			r, err := e.Submit(context.Background(), task)
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			classes[i] = r.Class
		}()
	}
	close(start)
	wg.Wait()
	m := e.Snapshot()
	t.Logf("输入: 同一永久失败 ID 并发重复投递 %d 次 => 执行=%d(期望1) Submitted=%d(期望1) Duplicate=%d PermanentFailures=%d(期望1)",
		dup, executions, m.Submitted, m.DuplicateDeliveries, m.PermanentFailures)
	for i, c := range classes {
		if c != ClassPermanent {
			t.Fatalf("副本 %d class=%s", i, c)
		}
	}
	if executions != 1 || m.PermanentFailures != 1 || m.Submitted != 1 {
		t.Fatalf("metrics=%+v executions=%d", m, executions)
	}
}

// TestUniqueAndDuplicateMix：混合投递唯一任务与重复 ID，
// 终态四类计数之和恒等于去重后受理数，且与完成顺序无关。
func TestUniqueAndDuplicateMix(t *testing.T) {
	e := runEngine(t, Config{Workers: 3, QueueSize: 16,
		Retry: RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond}, Deduplicate: true})

	plan := []struct {
		id   string
		dup  int // 含首次的投递次数
		errF func(call int64) error
	}{
		{"a", 3, func(int64) error { return nil }},
		{"b", 2, func(int64) error { return PermError(errors.New("perm")) }},
		{"c", 4, func(int64) error { return TempError(errors.New("always")) }},
		{"d", 1, func(int64) error { return nil }},
		{"e", 2, func(int64) error { return nil }},
	}
	var wg sync.WaitGroup
	for _, p := range plan {
		p := p
		var calls int64
		for k := 0; k < p.dup; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = e.Submit(context.Background(), Task{ID: p.id, Fn: func(ctx context.Context) error {
					return p.errF(atomic.AddInt64(&calls, 1))
				}})
			}()
		}
	}
	wg.Wait()
	m := e.Snapshot()
	terminal := m.Succeeded + m.PermanentFailures + m.RetryExhausted + m.Canceled
	t.Logf("输入: 5 个唯一 ID，共 12 次投递（含 7 次重复），混合成功/永久/重试耗尽")
	t.Logf("判定依据: Submitted=%d(期望5) Duplicate=%d(期望7) 成功=%d(期望3) 永久=%d(期望1) 重试耗尽=%d(期望1) 四类之和=%d",
		m.Submitted, m.DuplicateDeliveries, m.Succeeded, m.PermanentFailures, m.RetryExhausted, terminal)
	if m.Submitted != 5 || m.DuplicateDeliveries != 7 ||
		m.Succeeded != 3 || m.PermanentFailures != 1 || m.RetryExhausted != 1 {
		t.Fatalf("计数与预期不符: %+v", m)
	}
	if terminal != m.Submitted {
		t.Fatalf("终态四类之和 %d != 受理数 %d", terminal, m.Submitted)
	}
	// Results 中每个 ID 恰好一条。
	rs := e.Results()
	seen := map[string]int{}
	for _, r := range rs {
		seen[r.TaskID]++
	}
	if len(rs) != 5 {
		t.Fatalf("结果条数=%d，期望 5", len(rs))
	}
	for id, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("任务 %s 出现 %d 次结果", id, cnt)
		}
	}
}
