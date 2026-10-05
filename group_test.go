package engine

import (
	"errors"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startLog 记录任务开始执行的先后顺序（按分组名），用于判定调度公平性。
type startLog struct {
	mu   sync.Mutex
	seq  []string
}

func (l *startLog) add(group string) {
	l.mu.Lock()
	l.seq = append(l.seq, group)
	l.mu.Unlock()
}

func (l *startLog) firstIndex(group string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, g := range l.seq {
		if g == group {
			return i
		}
	}
	return -1
}

func (l *startLog) count(group string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, g := range l.seq {
		if g == group {
			n++
		}
	}
	return n
}

// TestGroupFairnessUnderFlood：单工作协程下，一路持续猛灌占满队列时，
// 另一路的任务必须在有界步数内拿到执行机会（轮转公平），而不是排在
// 猛灌方全部积压之后（纯 FIFO 下第一路受害者要等到第 21 个之后）。
func TestGroupFairnessUnderFlood(t *testing.T) {
	const (
		nFlood  = 20
		nVictim = 3
	)
	e := New(Config{Workers: 1, QueueSize: 64, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	var log startLog
	var wg sync.WaitGroup
	// 猛灌方：20 个 10ms 任务，迅速占满唯一的执行位并形成积压。
	for i := 0; i < nFlood; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("flood-%d", i),
				Group: "flood",
				Fn: func(ctx context.Context) error {
					log.add("flood")
					time.Sleep(10 * time.Millisecond)
					return nil
				},
			})
		}()
	}
	// 等积压形成（队列里已有足够多 flood 任务）再提交受害方。
	deadline := time.Now().Add(2 * time.Second)
	for e.Snapshot().QueueDepth < int64(nFlood/2) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("输入: 猛灌方 %d 个任务已积压（队列深度=%d），再提交 %d 个受害方任务",
		nFlood, e.Snapshot().QueueDepth, nVictim)

	for i := 0; i < nVictim; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("victim-%d", i),
				Group: "victim",
				Fn: func(ctx context.Context) error {
					log.add("victim")
					return nil
				},
			})
		}()
	}
	wg.Wait()

	firstVictim := log.firstIndex("victim")
	t.Logf("判定依据: 首个受害方任务在全局执行序列中的位置=%d（共 %d 个任务；"+
		"纯 FIFO 下应 >= %d，轮转公平下应远小于它）", firstVictim, nFlood+nVictim, nFlood)
	if firstVictim < 0 {
		t.Fatal("受害方任务未被执行")
	}
	if firstVictim >= nFlood/2 {
		t.Fatalf("受害方被猛灌方积压饿死：首个执行位置 %d，期望远小于 %d", firstVictim, nFlood)
	}
	m := e.Snapshot()
	if m.Submitted != int64(nFlood+nVictim) || m.Succeeded != int64(nFlood+nVictim) {
		t.Fatalf("受理/成功计数不符: %+v", m)
	}
	t.Logf("指标: Submitted=%d Succeeded=%d（两路全部完成，无丢失）", m.Submitted, m.Succeeded)
}

// TestGroupLimitAndGlobalConcurrency：分组额度与全局并发同时触顶时，
// 两组上限各自严格生效：受限组同时在跑不超过其额度，全局同时在跑
// 不超过 Workers；受限组排队的任务不得占着执行位置，自由组照常推进。
func TestGroupLimitAndGlobalConcurrency(t *testing.T) {
	const (
		workers  = 3
		capLimit = 1
		nCap     = 4
		nFree    = 6
	)
	e := New(Config{
		Workers:     workers,
		QueueSize:   16,
		Retry:       NoRetry(),
		Deduplicate: true,
		GroupLimits: map[string]int{"cap": capLimit},
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	var capRunning, capMax int64
	var freeDone int64

	var wg sync.WaitGroup
	// 受限组：任务较慢，记录该组同时在跑的最大值。
	for i := 0; i < nCap; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("cap-%d", i),
				Group: "cap",
				Fn: func(ctx context.Context) error {
					cur := atomic.AddInt64(&capRunning, 1)
					for {
						old := atomic.LoadInt64(&capMax)
						if cur <= old || atomic.CompareAndSwapInt64(&capMax, old, cur) {
							break
						}
					}
					time.Sleep(30 * time.Millisecond)
					atomic.AddInt64(&capRunning, -1)
					return nil
				},
			})
			if err != nil {
				t.Errorf("cap-%d: %v", i, err)
			} else if r.Class != ClassSuccess {
				t.Errorf("cap-%d class=%s", i, r.Class)
			}
		}()
	}
	// 等受限组额度被占住，再提交自由组。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&capRunning) < int64(capLimit) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	freeStart := time.Now()
	for i := 0; i < nFree; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("free-%d", i),
				Group: "free",
				Fn: func(ctx context.Context) error {
					atomic.AddInt64(&freeDone, 1)
					return nil
				},
			})
			if err != nil {
				t.Errorf("free-%d: %v", i, err)
			} else if r.Class != ClassSuccess {
				t.Errorf("free-%d class=%s", i, r.Class)
			}
		}()
	}
	wg.Wait()
	freeElapsed := time.Since(freeStart)

	t.Logf("判定依据: 受限组额度=%d 全局并发=%d => 受限组同时在跑峰值=%d（应<=%d），"+
		"自由组 %d 个任务在受限组积压期间全部完成（耗时=%v）",
		capLimit, workers, atomic.LoadInt64(&capMax), capLimit, nFree, freeElapsed)
	if got := atomic.LoadInt64(&capMax); got > int64(capLimit) {
		t.Fatalf("受限组并发峰值 %d 超过额度 %d", got, capLimit)
	}
	if got := atomic.LoadInt64(&freeDone); got != nFree {
		t.Fatalf("自由组完成数 %d，期望 %d（受限组排队不得挡住其它组）", got, nFree)
	}
	m := e.Snapshot()
	gs := e.GroupSnapshots()
	t.Logf("指标: 全局 Submitted=%d Succeeded=%d；分组=%v", m.Submitted, m.Succeeded, gs)
	if m.Submitted != int64(nCap+nFree) || m.Succeeded != int64(nCap+nFree) {
		t.Fatalf("全局计数不符: %+v", m)
	}
	if gs["cap"].Succeeded != nCap || gs["free"].Succeeded != nFree {
		t.Fatalf("分组计数不符: %v", gs)
	}
}

// TestCancelGroupIsolation：单独取消一路：该组在执行任务收到取消、排队
// 任务得到确定的取消裁决（不执行、不重复计数），之后该组提交被拒绝；
// 另一路的受理与执行完全不受影响，分组计数与全局汇总对得上。
func TestCancelGroupIsolation(t *testing.T) {
	const (
		workers = 2
		nLong   = 2 // 占满执行位的长任务
		nQueued = 8 // 积压在该组队列里的任务
		nOther  = 5 // 另一路任务
	)
	e := New(Config{Workers: workers, QueueSize: 16, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	type outcome struct {
		r   Result
		err error
	}
	outcomes := make(chan outcome, nLong+nQueued+nOther)
	var longStarted, queuedFnCalls int64

	// 目标组：2 个长任务占住执行位 + 8 个排队任务。
	for i := 0; i < nLong; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("a-long-%d", i),
				Group: "a",
				Fn: func(ctx context.Context) error {
					atomic.AddInt64(&longStarted, 1)
					<-ctx.Done()
					return ctx.Err()
				},
			})
			outcomes <- outcome{r, err}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&longStarted) < nLong && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < nQueued; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("a-queued-%d", i),
				Group: "a",
				Fn: func(ctx context.Context) error {
					atomic.AddInt64(&queuedFnCalls, 1)
					return nil
				},
			})
			outcomes <- outcome{r, err}
		}()
	}
	// 另一路：在目标组积压期间提交，先被背压挡住，目标组取消后应正常完成。
	otherSubmitted := make(chan struct{})
	go func() {
		defer close(otherSubmitted)
		for i := 0; i < nOther; i++ {
			i := i
			go func() {
				r, err := e.Submit(context.Background(), Task{
					ID:    fmt.Sprintf("b-%d", i),
					Group: "b",
					Fn:    func(ctx context.Context) error { return nil },
				})
				outcomes <- outcome{r, err}
			}()
		}
	}()
	// 等目标组队列积压形成。
	deadline = time.Now().Add(2 * time.Second)
	for e.Snapshot().QueueDepth < int64(nQueued) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("输入: 组 a 在执行=%d 排队=%d，组 b %d 个任务在途 => 单独取消组 a",
		e.Snapshot().ActiveWorkers, e.Snapshot().QueueDepth, nOther)

	e.CancelGroup("a")

	gotCanceled, gotRejected, gotSuccess := 0, 0, 0
	for i := 0; i < nLong+nQueued+nOther; i++ {
		o := <-outcomes
		switch {
		case o.err == nil && o.r.Class == ClassCanceled:
			gotCanceled++
		case o.err != nil:
			gotRejected++
		case o.err == nil && o.r.Class == ClassSuccess:
			gotSuccess++
		default:
			t.Errorf("结论不确定: %+v", o)
		}
	}
	<-otherSubmitted

	m := e.Snapshot()
	ga, okA := e.GroupSnapshot("a")
	gb, okB := e.GroupSnapshot("b")
	t.Logf("判定依据: 组 a 取消裁决=%d 未受理拒绝=%d（合计应=%d），组 b 成功=%d（应=%d）；"+
		"组 a 排队任务执行次数=%d（应为 0）",
		gotCanceled, gotRejected, nLong+nQueued, gotSuccess, nOther, atomic.LoadInt64(&queuedFnCalls))
	t.Logf("指标: 全局=%+v 组a=%+v 组b=%+v", m, ga, gb)

	if gotCanceled+gotRejected != nLong+nQueued {
		t.Fatalf("组 a 结论不确定: canceled=%d rejected=%d", gotCanceled, gotRejected)
	}
	if got := atomic.LoadInt64(&queuedFnCalls); got != 0 {
		t.Fatalf("组 a 排队任务被执行了 %d 次，应为 0", got)
	}
	if gotSuccess != nOther {
		t.Fatalf("组 b 成功数=%d，期望 %d（其它组不受影响）", gotSuccess, nOther)
	}
	if !okA || !okB {
		t.Fatalf("分组快照缺失: okA=%v okB=%v", okA, okB)
	}
	if ga.Submitted != ga.Canceled || ga.Submitted != int64(gotCanceled) {
		t.Fatalf("组 a 计数不符: %+v（取消裁决数=%d）", ga, gotCanceled)
	}
	if gb.Succeeded != int64(nOther) || gb.Submitted != int64(nOther) {
		t.Fatalf("组 b 计数不符: %+v", gb)
	}
	if m.Submitted != ga.Submitted+gb.Submitted || m.Canceled != ga.Canceled || m.Succeeded != gb.Succeeded {
		t.Fatalf("分组与全局汇总对不上: 全局=%+v 组a=%+v 组b=%+v", m, ga, gb)
	}

	// 取消后该组提交被拒绝（context.Canceled），其它组照常受理。
	if _, err := e.Submit(context.Background(), Task{ID: "a-after", Group: "a", Fn: func(ctx context.Context) error { return nil }}); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后向组 a 提交期望 context.Canceled，got %v", err)
	}
	r, err := e.Submit(context.Background(), Task{ID: "b-after", Group: "b", Fn: func(ctx context.Context) error { return nil }})
	if err != nil || r.Class != ClassSuccess {
		t.Fatalf("取消组 a 后组 b 提交应正常: r=%+v err=%v", r, err)
	}
	t.Logf("取消后: 组 a 新提交=context.Canceled，组 b 新提交照常成功")
}

// TestDrainGroupIsolation：单独排空一路：停止受理该组新提交，已受理任务
// 全部确定完成后 DrainGroup 返回；其它组照常提交与执行。
func TestDrainGroupIsolation(t *testing.T) {
	const (
		nDrain = 4
		nOther = 4
	)
	e := New(Config{Workers: 2, QueueSize: 8, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	var drainStarted int64
	drained := make(chan Result, nDrain)
	for i := 0; i < nDrain; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("d-%d", i),
				Group: "d",
				Fn: func(ctx context.Context) error {
					atomic.AddInt64(&drainStarted, 1)
					time.Sleep(30 * time.Millisecond)
					return nil
				},
			})
			if err != nil {
				t.Errorf("d-%d: %v", i, err)
				return
			}
			drained <- r
		}()
	}
	// 等该组任务全部受理开跑，再发起排空。
	deadline := time.Now().Add(2 * time.Second)
	for e.Snapshot().ActiveWorkers < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("输入: 组 d %d 个 30ms 任务在途 => 调用 DrainGroup(d)，期间组 o 照常提交", nDrain)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- e.DrainGroup(context.Background(), "d")
	}()

	// 排空期间：该组新提交被拒绝，其它组照常完成。
	time.Sleep(5 * time.Millisecond)
	_, err := e.Submit(context.Background(), Task{ID: "d-late", Group: "d", Fn: func(ctx context.Context) error { return nil }})
	t.Logf("排空中向组 d 提交 => err=%v（期望 ErrGroupClosed）", err)
	if !errors.Is(err, ErrGroupClosed) {
		t.Fatalf("排空中向组 d 提交期望 ErrGroupClosed，got %v", err)
	}
	for i := 0; i < nOther; i++ {
		r, err := e.Submit(context.Background(), Task{
			ID:    fmt.Sprintf("o-%d", i),
			Group: "o",
			Fn:    func(ctx context.Context) error { return nil },
		})
		if err != nil || r.Class != ClassSuccess {
			t.Fatalf("排空中其它组任务应正常完成: r=%+v err=%v", r, err)
		}
	}

	if err := <-drainDone; err != nil {
		t.Fatalf("DrainGroup: %v", err)
	}
	// DrainGroup 返回时，该组所有已受理任务必须已有终态。
	for i := 0; i < nDrain; i++ {
		r := <-drained
		if r.Class != ClassSuccess {
			t.Fatalf("组 d 任务应成功排空: %+v", r)
		}
	}
	gd, _ := e.GroupSnapshot("d")
	go_, _ := e.GroupSnapshot("o")
	m := e.Snapshot()
	t.Logf("判定依据: DrainGroup 返回后 组d=%+v（4 受理 4 成功）组o=%+v 全局 Submitted=%d",
		gd, go_, m.Submitted)
	if gd.Submitted != nDrain || gd.Succeeded != nDrain {
		t.Fatalf("组 d 计数不符: %+v", gd)
	}
	if go_.Succeeded != nOther {
		t.Fatalf("组 o 计数不符: %+v", go_)
	}
	if m.Submitted != gd.Submitted+go_.Submitted {
		t.Fatalf("分组与全局汇总对不上: 全局=%+v 组d=%+v 组o=%+v", m, gd, go_)
	}
}

// TestGroupMetricsConsistency：多分组混合终态下，各路计数之和恒等于全局
// 汇总，且多次统计结果一致；不指定分组的旧用法归入默认分组。
func TestGroupMetricsConsistency(t *testing.T) {
	e := New(Config{Workers: 3, QueueSize: 16,
		Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond}, Deduplicate: true})
	e.Start()

	submit := func(id, group string, fn func(ctx context.Context) error) {
		t.Helper()
		if _, err := e.Submit(context.Background(), Task{ID: id, Group: group, Fn: fn}); err != nil {
			t.Fatalf("submit %s/%s: %v", group, id, err)
		}
	}
	ok := func(ctx context.Context) error { return nil }
	// 组 x：3 成功 + 2 永久失败 + 1 重试耗尽；组 y：2 成功；默认组：1 成功。
	for i := 0; i < 3; i++ {
		submit(fmt.Sprintf("x-ok-%d", i), "x", ok)
	}
	for i := 0; i < 2; i++ {
		submit(fmt.Sprintf("x-perm-%d", i), "x", func(ctx context.Context) error { return PermError(errors.New("bad")) })
	}
	submit("x-temp", "x", func(ctx context.Context) error { return TempError(errors.New("flaky")) })
	for i := 0; i < 2; i++ {
		submit(fmt.Sprintf("y-ok-%d", i), "y", ok)
	}
	submit("legacy", "", ok) // 不指定分组的旧用法
	// 组 z：提交后整组取消，计入取消。
	var zWG sync.WaitGroup
	for i := 0; i < 2; i++ {
		i := i
		zWG.Add(1)
		go func() {
			defer zWG.Done()
			_, _ = e.Submit(context.Background(), Task{
				ID:    fmt.Sprintf("z-%d", i),
				Group: "z",
				Fn:    func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			})
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for e.Snapshot().ActiveWorkers < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	e.CancelGroup("z")
	zWG.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	m := e.Snapshot()
	gs := e.GroupSnapshots()
	t.Logf("输入: 组x=3成功+2永久+1耗尽 组y=2成功 默认组=1成功 组z=2取消")
	t.Logf("全局: Submitted=%d Succeeded=%d Permanent=%d RetryExhausted=%d Canceled=%d",
		m.Submitted, m.Succeeded, m.PermanentFailures, m.RetryExhausted, m.Canceled)
	for name, g := range gs {
		t.Logf("分组 %q: %+v", name, g)
	}
	var sum GroupMetrics
	for _, g := range gs {
		sum.Submitted += g.Submitted
		sum.Succeeded += g.Succeeded
		sum.PermanentFailures += g.PermanentFailures
		sum.RetryExhausted += g.RetryExhausted
		sum.Canceled += g.Canceled
	}
	if sum.Submitted != m.Submitted || sum.Succeeded != m.Succeeded ||
		sum.PermanentFailures != m.PermanentFailures || sum.RetryExhausted != m.RetryExhausted ||
		sum.Canceled != m.Canceled {
		t.Fatalf("分组之和与全局汇总对不上: sum=%+v 全局=%+v", sum, m)
	}
	// 各组内部：终态四类之和 == 受理数。
	for name, g := range gs {
		if g.Succeeded+g.PermanentFailures+g.RetryExhausted+g.Canceled != g.Submitted {
			t.Fatalf("组 %q 终态之和不等于受理数: %+v", name, g)
		}
	}
	if gx := gs["x"]; gx.Succeeded != 3 || gx.PermanentFailures != 2 || gx.RetryExhausted != 1 {
		t.Fatalf("组 x 计数不符: %+v", gx)
	}
	if gz := gs["z"]; gz.Canceled != 2 || gz.Submitted != 2 {
		t.Fatalf("组 z 计数不符: %+v", gz)
	}
	if gd := gs[""]; gd.Submitted != 1 || gd.Succeeded != 1 {
		t.Fatalf("默认分组（旧用法）计数不符: %+v", gd)
	}
	// 多次统计结果一致。
	again := e.GroupSnapshots()
	for name, g := range gs {
		if again[name] != g {
			t.Fatalf("多次统计不一致: 组 %q 第一次=%+v 第二次=%+v", name, g, again[name])
		}
	}
	if len(e.Results()) != int(m.Submitted) {
		t.Fatalf("Results 数=%d 与 Submitted=%d 不符", len(e.Results()), m.Submitted)
	}
	t.Logf("校验通过: 分组之和==全局汇总，各组终态四类之和==受理数，多次统计一致")
}
