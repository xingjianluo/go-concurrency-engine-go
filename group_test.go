package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// groupQuickTask 构造一个带来源分组、睡眠 d 后返回 err 的任务。
func groupQuickTask(id, source string, d time.Duration, err error) Task {
	return Task{ID: id, Source: source, Fn: func(ctx context.Context) error {
		select {
		case <-time.After(d):
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
}

// blockTask 构造一个开始时登记 started、阻塞到 release 关闭（或 ctx 取消）的任务。
func blockTask(id, source string, started *int64, release <-chan struct{}) Task {
	return Task{ID: id, Source: source, Fn: func(ctx context.Context) error {
		if started != nil {
			atomic.AddInt64(started, 1)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
}

// TestGroupFairnessUnderFlood：A 路持续猛灌，B 路零散提交。
// 即使 A 把缓冲与提交窗口占满，B 也必须持续拿到执行机会，不被无限期饿死：
// 每个 B 任务“提交 -> 开始执行”的等待都有一个很小的上界，且全部 B 在 A 仍在
// 猛灌期间就能完成，而不是排到 A 全部清空后才动。
func TestGroupFairnessUnderFlood(t *testing.T) {
	const (
		workers   = 4
		queueSize = 16
		nA        = 400
		nB        = 24
		taskWork  = 2 * time.Millisecond
		flooders  = 64
	)
	e := New(Config{
		Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"A": workers, "B": 1}, // B 只要 1 个执行位即可证明不饿死
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	var aSeq, aDone int64
	var maxBWaitNs int64
	type bRes struct {
		wait time.Duration
		err  error
	}
	bResCh := make(chan bRes, nB)

	// A 路：大量提交方持续猛灌，制造持续过载。
	start := make(chan struct{})
	var awg sync.WaitGroup
	for i := 0; i < flooders; i++ {
		awg.Add(1)
		go func() {
			defer awg.Done()
			<-start
			for {
				idx := atomic.AddInt64(&aSeq, 1)
				if idx > nA {
					return
				}
				_, err := e.Submit(context.Background(), Task{
					ID: fmt.Sprintf("a%d", idx), Source: "A",
					Fn: func(context.Context) error {
						time.Sleep(taskWork)
						atomic.AddInt64(&aDone, 1)
						return nil
					},
				})
				if err != nil {
					return
				}
			}
		}()
	}
	close(start)

	// B 路：在 A 猛灌期间逐个提交，测量提交到开始执行的延迟。
	go func() {
		for i := 0; i < nB; i++ {
			enq := time.Now()
			_, err := e.Submit(context.Background(), Task{
				ID: fmt.Sprintf("b%d", i), Source: "B",
				Fn: func(context.Context) error {
					w := time.Since(enq)
					for {
						old := atomic.LoadInt64(&maxBWaitNs)
						if int64(w) <= old ||
							atomic.CompareAndSwapInt64(&maxBWaitNs, old, int64(w)) {
							break
						}
					}
					bResCh <- bRes{wait: w}
					time.Sleep(taskWork)
					return nil
				},
			})
			if err != nil {
				bResCh <- bRes{err: err}
			}
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	bOK := 0
	bMax := time.Duration(0)
	for bOK < nB {
		if time.Now().After(deadline) {
			t.Fatalf("B 路被饿死：仅完成 %d/%d（同期 A 已完成 %d）", bOK, nB, atomic.LoadInt64(&aDone))
		}
		br := <-bResCh
		if br.err != nil {
			t.Fatalf("B 提交错误: %v", br.err)
		}
		if br.wait > bMax {
			bMax = br.wait
		}
		bOK++
	}

	gmA, _ := e.GroupSnapshot("A")
	gmB, _ := e.GroupSnapshot("B")
	t.Logf("输入: A 路 %d 个 %v 任务由 %d 协程持续猛灌，同期 B 路逐个提交 %d 个（workers=%d queue=%d A上限=%d B上限=1）",
		nA, taskWork, flooders, nB, workers, queueSize, workers)
	t.Logf("判定依据: B 在 A 猛灌期间完成=%d/%d，B 提交->开始执行最大等待=%v（收集完时 A 已完成=%d，证明 B 没等 A 清空）",
		bOK, nB, bMax, atomic.LoadInt64(&aDone))
	t.Logf("分组指标: A[受理=%d 成功=%d] B[受理=%d 成功=%d]（关闭排空后复核）",
		gmA.Submitted, gmA.Succeeded, gmB.Submitted, gmB.Succeeded)

	// 公平上界：轮转节奏约 workers*taskWork≈8ms；放宽到 500ms 以容纳慢 CI。
	fairBound := 500 * time.Millisecond
	if bMax > fairBound {
		t.Fatalf("B 路等待过久（疑似被 A 饿死）: max=%v 上界=%v", bMax, fairBound)
	}
	if bOK != nB {
		t.Fatalf("B 完成数=%d 期望 %d", bOK, nB)
	}
}

// TestCancelGroupIsolatesOthers：只取消 B 路：
//   - B 在途任务收到 ctx 取消、B 排队任务得到确定的取消裁决（不重试、不丢失）；
//   - 之后向 B 提交返回 ErrGroupCanceled；
//   - A 路完全不受影响，继续提交并成功执行；
//   - 各分组计数与全局汇总对得上，取消的不重复计数。
func TestCancelGroupIsolatesOthers(t *testing.T) {
	const (
		workers   = 4
		queueSize = 32
		nBQueued  = 20
	)
	e := New(Config{
		Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"A": 0, "B": 1}, // B 上限 1：1 个在途 + 其余排队
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	releaseB := make(chan struct{})
	var bRunning int64
	type oc struct {
		r   Result
		err error
	}
	out := make(chan oc, nBQueued+1)

	// B 在途长任务：占住 B 的唯一执行位，尊重 ctx。
	go func() {
		r, err := e.Submit(context.Background(), blockTask("b-run", "B", &bRunning, releaseB))
		out <- oc{r, err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&bRunning) < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// B 排队任务：B 上限=1，全部在缓冲中等待（缓冲容量 32 足够）。
	for i := 0; i < nBQueued; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(),
				groupQuickTask(fmt.Sprintf("bq%d", i), "B", 100*time.Millisecond, nil))
			out <- oc{r, err}
		}()
	}
	// 等待 B 排队任务被受理进缓冲。
	waitUntil := time.Now().Add(2 * time.Second)
	for {
		gm, _ := e.GroupSnapshot("B")
		if gm.QueueDepth >= int64(nBQueued) || time.Now().After(waitUntil) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	gmB0, _ := e.GroupSnapshot("B")
	t.Logf("取消前 B: 在执行=%d 队列深度=%d 受理=%d（期望 1/%d/%d）",
		gmB0.ActiveWorkers, gmB0.QueueDepth, gmB0.Submitted, nBQueued, nBQueued+1)

	// 只取消 B 路。
	if err := e.CancelGroup("B"); err != nil {
		t.Fatalf("CancelGroup: %v", err)
	}
	close(releaseB)

	// 收集 B 的 nBQueued+1 个结论：在途 1 个 + 排队 nBQueued 个，全部应是确定取消。
	bCanceled := 0
	for i := 0; i < nBQueued+1; i++ {
		got := <-out
		switch {
		case got.err == nil && got.r.Class == ClassCanceled:
			bCanceled++
		case got.err != nil:
			t.Fatalf("B 已受理任务不应返回提交错误（应拿取消裁决）: %v", got.err)
		default:
			t.Fatalf("B 任务结论不确定: %+v", got.r)
		}
	}

	// 取消后向 B 提交：ErrGroupCanceled；任务不执行。
	var bAfterRan int64
	if _, err := e.Submit(context.Background(), Task{
		ID: "b-after", Source: "B", Fn: func(context.Context) error {
			atomic.AddInt64(&bAfterRan, 1)
			return nil
		},
	}); err != ErrGroupCanceled {
		t.Fatalf("取消后向 B 提交期望 ErrGroupCanceled, got %v", err)
	}

	// A 路完全不受影响：继续提交并成功。
	const nA = 12
	aOK := 0
	for i := 0; i < nA; i++ {
		r, err := e.Submit(context.Background(),
			groupQuickTask(fmt.Sprintf("a%d", i), "A", time.Millisecond, nil))
		if err != nil {
			t.Fatalf("A 路在 B 取消后提交失败: %v", err)
		}
		if r.Class == ClassSuccess {
			aOK++
		}
	}

	m := e.Snapshot()
	gmA, _ := e.GroupSnapshot("A")
	gmB, _ := e.GroupSnapshot("B")
	t.Logf("输入: B 1 在途 + %d 排队后只取消 B；随后 B 再提 1 个、A 提交 %d 个", nBQueued, nA)
	t.Logf("判定依据: B 确定取消裁决=%d(期望%d) 取消后提交=ErrGroupCanceled(未执行=%d) A 成功=%d(期望%d)",
		bCanceled, nBQueued+1, atomic.LoadInt64(&bAfterRan), aOK, nA)
	t.Logf("指标: 全局[受理=%d 成功=%d 取消=%d] A[受理=%d 成功=%d 取消=%d] B[受理=%d 成功=%d 取消=%d]",
		m.Submitted, m.Succeeded, m.Canceled,
		gmA.Submitted, gmA.Succeeded, gmA.Canceled,
		gmB.Submitted, gmB.Succeeded, gmB.Canceled)

	if bCanceled != nBQueued+1 {
		t.Fatalf("B 取消裁决数=%d 期望 %d", bCanceled, nBQueued+1)
	}
	if atomic.LoadInt64(&bAfterRan) != 0 {
		t.Fatal("取消后提交给 B 的任务不应被执行")
	}
	if aOK != nA {
		t.Fatalf("A 路受 B 取消影响: 成功 %d/%d", aOK, nA)
	}
	// 分路计数之和必须等于全局。
	if gmA.Submitted+gmB.Submitted != m.Submitted ||
		gmA.Succeeded+gmB.Succeeded != m.Succeeded ||
		gmA.Canceled+gmB.Canceled != m.Canceled {
		t.Fatalf("分组计数与全局对不上: 全局=%+v A=%+v B=%+v", m, gmA, gmB)
	}
	// B: 受理=排队+在途，全部取消；A: 全部成功。
	if gmB.Submitted != int64(nBQueued+1) || gmB.Canceled != int64(nBQueued+1) || gmB.Succeeded != 0 {
		t.Fatalf("B 计数异常: %+v", gmB)
	}
	if gmA.Submitted != nA || gmA.Succeeded != nA || gmA.Canceled != 0 {
		t.Fatalf("A 计数异常: %+v", gmA)
	}
	// 四类终态之和恒等于受理。
	if m.Succeeded+m.PermanentFailures+m.RetryExhausted+m.Canceled != m.Submitted {
		t.Fatalf("终态之和不等于受理: %+v", m)
	}
}

// TestShutdownGroupDrainsOnlyItsGroup：只排空关闭 B 路：
//   - B 已受理（在途+排队）任务照常执行到终态，不取消、不丢弃；
//   - 排空后向 B 提交返回 ErrGroupClosed；
//   - A 路继续正常提交与执行。
func TestShutdownGroupDrainsOnlyItsGroup(t *testing.T) {
	const (
		workers   = 4
		queueSize = 32
		nBQueued  = 15
	)
	e := New(Config{
		Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"B": 1},
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	releaseB := make(chan struct{})
	var bRunning int64
	type oc struct{ r Result }
	out := make(chan oc, nBQueued+1)

	go func() {
		r, _ := e.Submit(context.Background(), blockTask("b-run", "B", &bRunning, releaseB))
		out <- oc{r}
	}()
	for atomic.LoadInt64(&bRunning) < 1 {
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < nBQueued; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(),
				groupQuickTask(fmt.Sprintf("bd%d", i), "B", time.Millisecond, nil))
			if err != nil {
				t.Errorf("B 排队任务提交不应失败: %v", err)
				return
			}
			out <- oc{r}
		}()
	}
	waitUntil := time.Now().Add(2 * time.Second)
	for {
		gm, _ := e.GroupSnapshot("B")
		if gm.QueueDepth >= int64(nBQueued) || time.Now().After(waitUntil) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// 请求排空 B 但先不释放在途任务：ShutdownGroup 应在其排空期间等待。
	doneDrain := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		doneDrain <- e.ShutdownGroup(ctx, "B")
	}()
	// 给 ShutdownGroup 一点时间进入等待，确认它不会提前返回或取消在途。
	select {
	case err := <-doneDrain:
		t.Fatalf("在途任务未释放前不应排空完成: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	// 排空期间 B 已停止受理。
	if _, err := e.Submit(context.Background(),
		groupQuickTask("b-late", "B", time.Millisecond, nil)); err != ErrGroupClosed {
		t.Fatalf("排空 B 后提交期望 ErrGroupClosed, got %v", err)
	}
	// A 不受影响。
	if r, err := e.Submit(context.Background(),
		groupQuickTask("a-ok", "A", time.Millisecond, nil)); err != nil || r.Class != ClassSuccess {
		t.Fatalf("排空 B 期间 A 应正常执行: r=%+v err=%v", r, err)
	}

	close(releaseB) // 放行在途 B，排空随即完成
	if err := <-doneDrain; err != nil {
		t.Fatalf("ShutdownGroup: %v", err)
	}

	bSuccess := 0
	for i := 0; i < nBQueued+1; i++ {
		got := <-out
		if got.r.Class != ClassSuccess {
			t.Fatalf("排空模式下 B 任务应正常成功，实际 %+v", got.r)
		}
		bSuccess++
	}

	m := e.Snapshot()
	gmA, _ := e.GroupSnapshot("A")
	gmB, _ := e.GroupSnapshot("B")
	t.Logf("输入: B 1 在途 + %d 排队后只排空 B（在途阻塞 30ms 期间验证未提前返回/未取消）", nBQueued)
	t.Logf("判定依据: B 全部正常成功=%d(期望%d，无一取消) 排空中提交=ErrGroupClosed A 正常成功",
		bSuccess, nBQueued+1)
	t.Logf("指标: 全局[受理=%d 成功=%d 取消=%d] A[受理=%d 成功=%d] B[受理=%d 成功=%d 取消=%d]",
		m.Submitted, m.Succeeded, m.Canceled,
		gmA.Submitted, gmA.Succeeded, gmB.Submitted, gmB.Succeeded, gmB.Canceled)

	if bSuccess != nBQueued+1 {
		t.Fatalf("B 排空成功数=%d 期望 %d", bSuccess, nBQueued+1)
	}
	if gmB.Submitted != int64(nBQueued+1) || gmB.Succeeded != int64(nBQueued+1) || gmB.Canceled != 0 {
		t.Fatalf("B 排空计数异常: %+v", gmB)
	}
	if gmA.Succeeded != 1 || gmA.Canceled != 0 {
		t.Fatalf("A 不应受 B 排空影响: %+v", gmA)
	}
	if gmA.Submitted+gmB.Submitted != m.Submitted || gmA.Succeeded+gmB.Succeeded != m.Succeeded {
		t.Fatalf("分组与全局计数对不上: 全局=%+v A=%+v B=%+v", m, gmA, gmB)
	}
}

// TestGroupAndGlobalLimitsBothSaturate：分组额度与全局并发同时触顶。
//
// 设两组各自 limit=3、全局 Workers=4。稳态下：
//   - 任一组最多 3 个在跑（分组上限生效）；
//   - 两组之和最多 4（全局上限生效）——即典型稳态为 3+1 或 2+2，绝无 3+3；
//   - 每组都持续有活在跑（公平），多余任务在有界缓冲中等待，不丢、不卡死。
func TestGroupAndGlobalLimitsBothSaturate(t *testing.T) {
	const (
		workers   = 4
		queueSize = 16
		limit     = 3
		nEach     = 60
		taskWork  = 2 * time.Millisecond
	)
	e := New(Config{
		Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"A": limit, "B": limit},
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	// 短时任务 + 任务体内登记并发峰值：在稳态过载下采样各组在跑数与两者之和。
	var aRun, bRun int32
	var aMax, bMax, sumMax int32

	mk := func(id, src string, run, mx *int32) Task {
		return Task{ID: id, Source: src, Fn: func(ctx context.Context) error {
			cur := atomic.AddInt32(run, 1)
			updateMax(mx, cur)
			// 本地计数尚未减，此刻求和才能反映真实的两组并发重叠。
			updateMax(&sumMax, atomic.LoadInt32(&aRun)+atomic.LoadInt32(&bRun))
			time.Sleep(taskWork)
			atomic.AddInt32(run, -1)
			return nil
		}}
	}

	var wg sync.WaitGroup
	for i := 0; i < nEach; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), mk(fmt.Sprintf("la%d", i), "A", &aRun, &aMax))
		}()
		go func() {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), mk(fmt.Sprintf("lb%d", i), "B", &bRun, &bMax))
		}()
	}
	wg.Wait()

	gmA, _ := e.GroupSnapshot("A")
	gmB, _ := e.GroupSnapshot("B")
	t.Logf("输入: A/B 各 %d 个 %v 任务，A上限=B上限=%d 全局workers=%d queue=%d",
		nEach, taskWork, limit, workers, queueSize)
	t.Logf("判定依据: 采样峰值 A在跑=%d B在跑=%d（各组≤%d） 两组之和峰值=%d（全局≤%d，故不会出现 %d+%d=%d）",
		aMax, bMax, limit, sumMax, workers, limit, limit, 2*limit)
	t.Logf("指标: A[受理=%d 成功=%d] B[受理=%d 成功=%d]", gmA.Submitted, gmA.Succeeded, gmB.Submitted, gmB.Succeeded)

	if aMax > limit || bMax > limit {
		t.Fatalf("分组并发上限被突破: A峰值=%d B峰值=%d 上限=%d", aMax, bMax, limit)
	}
	if sumMax > workers {
		t.Fatalf("全局并发上限被突破: 两组之和峰值=%d 上限=%d", sumMax, workers)
	}
	// 两组都必须真正跑到过各自多并发（证明额度与全局叠加后两路都在跑，而非一路独占）。
	if aMax < 2 || bMax < 2 {
		t.Fatalf("某一路未获得足够执行机会: A峰值=%d B峰值=%d", aMax, bMax)
	}
	if gmA.Succeeded != nEach || gmB.Succeeded != nEach {
		t.Fatalf("成功数异常: A=%d B=%d（期望各 %d，不丢任务）", gmA.Succeeded, gmB.Succeeded, nEach)
	}
}

// updateMax 用 CAS 把 *mx 更新为 max(*mx, v)。
func updateMax(mx *int32, v int32) {
	for {
		old := atomic.LoadInt32(mx)
		if v <= old || atomic.CompareAndSwapInt32(mx, old, v) {
			return
		}
	}
}

// TestGroupBackpressureBoundedAndConsistent：某一路猛灌时缓冲仍全局有界，
// 背压传回提交方；各分组计数之和恒等于全局，且重复多次 Snapshot 结果一致。
func TestGroupBackpressureBoundedAndConsistent(t *testing.T) {
	const (
		workers   = 2
		queueSize = 6
		nA        = 120
		nB        = 40
		taskWork  = 2 * time.Millisecond
	)
	e := New(Config{
		Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"A": 1, "B": 1},
	})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	}()

	var maxDepth int64
	var blockedA, blockedB int64
	stopObs := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopObs:
				return
			default:
				d := e.Snapshot().QueueDepth
				for {
					old := atomic.LoadInt64(&maxDepth)
					if d <= old || atomic.CompareAndSwapInt64(&maxDepth, old, d) {
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	flood := func(src string, n int, blocked *int64, wg *sync.WaitGroup) {
		for i := 0; i < n; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := time.Now()
				r, err := e.Submit(context.Background(),
					groupQuickTask(fmt.Sprintf("%s%d", src, i), src, taskWork, nil))
				if err != nil {
					t.Errorf("submit %s%d: %v", src, i, err)
					return
				}
				if r.Class != ClassSuccess {
					t.Errorf("%s%d class=%s", src, i, r.Class)
				}
				if time.Since(s) > taskWork/2 {
					atomic.AddInt64(blocked, 1)
				}
			}()
		}
	}
	var wg sync.WaitGroup
	flood("A", nA, &blockedA, &wg)
	flood("B", nB, &blockedB, &wg)
	wg.Wait()
	close(stopObs)

	m := e.Snapshot()
	gmA, _ := e.GroupSnapshot("A")
	gmB, _ := e.GroupSnapshot("B")
	t.Logf("输入: A %d 个 + B %d 个 %v 任务并发涌入（workers=%d queue=%d 各路上限=1）",
		nA, nB, taskWork, workers, queueSize)
	t.Logf("判定依据: 队列深度峰值=%d(应≤%d) A 观察到背压=%d B 观察到背压=%d（压力传回提交方、不无界堆积）",
		atomic.LoadInt64(&maxDepth), queueSize, atomic.LoadInt64(&blockedA), atomic.LoadInt64(&blockedB))
	t.Logf("指标: 全局[受理=%d 成功=%d 背压事件=%d] A[受理=%d 成功=%d] B[受理=%d 成功=%d]",
		m.Submitted, m.Succeeded, m.BackpressureWaits, gmA.Submitted, gmA.Succeeded, gmB.Submitted, gmB.Succeeded)

	if atomic.LoadInt64(&maxDepth) > queueSize {
		t.Fatalf("分组调度下缓冲无界: 峰值=%d 容量=%d", maxDepth, queueSize)
	}
	if m.Succeeded != int64(nA+nB) {
		t.Fatalf("有任务丢失: 成功=%d 期望 %d", m.Succeeded, nA+nB)
	}
	if atomic.LoadInt64(&blockedA) == 0 && atomic.LoadInt64(&blockedB) == 0 {
		t.Fatal("持续过载却未观察到背压")
	}
	// 分组计数之和 == 全局。
	if gmA.Submitted+gmB.Submitted != m.Submitted ||
		gmA.Succeeded+gmB.Succeeded != m.Succeeded {
		t.Fatalf("分组计数与全局不一致: 全局=%+v A=%+v B=%+v", m, gmA, gmB)
	}
	// 重复多次快照结果一致（计数已稳定：全部任务已终态）。
	diff := func(name string, a, b int64) {
		if a != b {
			t.Errorf("第 %s 个快照字段不一致: %d vs %d", name, a, b)
		}
	}
	diffD := func(name string, a, b time.Duration) {
		if a != b {
			t.Errorf("快照时长字段 %s 不一致: %v vs %v", name, a, b)
		}
	}
	for i := 0; i < 5; i++ {
		mm := e.Snapshot()
		a, _ := e.GroupSnapshot("A")
		b, _ := e.GroupSnapshot("B")
		diff(fmt.Sprintf("g%d.Submitted", i), mm.Submitted, m.Submitted)
		diff(fmt.Sprintf("g%d.Succeeded", i), mm.Succeeded, m.Succeeded)
		diff(fmt.Sprintf("g%d.Perm", i), mm.PermanentFailures, m.PermanentFailures)
		diff(fmt.Sprintf("g%d.Exh", i), mm.RetryExhausted, m.RetryExhausted)
		diff(fmt.Sprintf("g%d.Canc", i), mm.Canceled, m.Canceled)
		diff(fmt.Sprintf("g%d.Attempts", i), mm.Attempts, m.Attempts)
		diff(fmt.Sprintf("g%d.Retries", i), mm.Retries, m.Retries)
		diff(fmt.Sprintf("g%d.BpWaits", i), mm.BackpressureWaits, m.BackpressureWaits)
		diffD(fmt.Sprintf("g%d.TaskTime", i), mm.TaskTimeTotal, m.TaskTimeTotal)
		diffD(fmt.Sprintf("g%d.BpTime", i), mm.BackpressureTime, m.BackpressureTime)
		if a.Submitted != gmA.Submitted || a.Succeeded != gmA.Succeeded {
			t.Errorf("第 %d 次 A 快照不一致", i)
		}
		if b.Submitted != gmB.Submitted || b.Succeeded != gmB.Succeeded {
			t.Errorf("第 %d 次 B 快照不一致", i)
		}
	}
	// 各组四类终态之和等于各自受理。
	for name, gm := range map[string]Metrics{"A": gmA, "B": gmB} {
		if gm.Succeeded+gm.PermanentFailures+gm.RetryExhausted+gm.Canceled != gm.Submitted {
			t.Fatalf("分组 %s 终态之和 != 受理: %+v", name, gm)
		}
	}
}

// TestDefaultGroupBackwardCompatible：不指定 Source（空串）的旧用法照常工作：
// 任务进入默认分组，行为与引入分组前一致。
func TestDefaultGroupBackwardCompatible(t *testing.T) {
	e := New(Config{Workers: 2, QueueSize: 4, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() { _ = e.Shutdown(ctx) }()

	// 旧风格 Task（无 Source 字段）。
	for i := 0; i < 10; i++ {
		r, err := e.Submit(context.Background(), quickTask(fmt.Sprintf("old%d", i), time.Millisecond, nil))
		if err != nil || r.Class != ClassSuccess {
			t.Fatalf("旧用法任务异常: %+v %v", r, err)
		}
	}
	gm, err := e.GroupSnapshot("")
	if err != nil {
		t.Fatalf("默认分组应可查询: %v", err)
	}
	if gm.Submitted != 10 || gm.Succeeded != 10 {
		t.Fatalf("默认分组计数异常: %+v", gm)
	}
	if err := e.ShutdownGroup(ctx, ""); err != nil {
		t.Fatalf("排空默认分组: %v", err)
	}
	// 默认分组排空后旧用法提交应得到 ErrGroupClosed。
	if _, err := e.Submit(context.Background(), quickTask("late", 0, nil)); err != ErrGroupClosed {
		t.Fatalf("默认分组排空后提交期望 ErrGroupClosed, got %v", err)
	}
}

// TestUnknownGroupSnapshot：查询从未出现且未配置的分组返回 ErrUnknownGroup。
func TestUnknownGroupSnapshot(t *testing.T) {
	e := New(Config{Workers: 1, QueueSize: 1, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	defer func() { _ = e.Shutdown(context.Background()) }()
	if _, err := e.GroupSnapshot("nope"); err != ErrUnknownGroup {
		t.Fatalf("期望 ErrUnknownGroup, got %v", err)
	}
	// 配置了上限但还没来活的分组应可观测（预建）。
	e2 := New(Config{Workers: 1, QueueSize: 1, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"pre": 2}})
	e2.Start()
	defer func() { _ = e2.Shutdown(context.Background()) }()
	if _, err := e2.GroupSnapshot("pre"); err != nil {
		t.Fatalf("预配置分组应可观测: %v", err)
	}
}

// TestShutdownThenCancelUpgradesToCancel：先发起 Shutdown（在途任务阻塞排空），
// 在其排空等待期间调用 Cancel：关闭必须升级为取消——尊重 ctx 的在途任务收到取消、
// 排队任务得到取消裁决，Shutdown 随之确定返回（引擎停止），不丢任务。
func TestShutdownThenCancelUpgradesToCancel(t *testing.T) {
	e := New(Config{Workers: 1, QueueSize: 4, Retry: NoRetry(), Deduplicate: true,
		GroupLimits: map[string]int{"X": 1}})
	e.Start()

	var started int64
	type oc struct {
		r   Result
		err error
	}
	resCh := make(chan oc, 3)
	// 1 个尊重 ctx 的在途长任务（占住 X 的执行位）。
	go func() {
		r, err := e.Submit(context.Background(), blockTask("x-long", "X", &started, make(chan struct{})))
		resCh <- oc{r, err}
	}()
	for atomic.LoadInt64(&started) < 1 {
		time.Sleep(time.Millisecond)
	}
	// 2 个排队任务（X 上限=1，缓冲等待）。
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(),
				groupQuickTask(fmt.Sprintf("xq%d", i), "X", 100*time.Millisecond, nil))
			resCh <- oc{r, err}
		}()
	}
	waitUntil := time.Now().Add(2 * time.Second)
	for {
		gm, _ := e.GroupSnapshot("X")
		if gm.QueueDepth >= 2 || time.Now().After(waitUntil) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- e.Shutdown(context.Background()) }()
	time.Sleep(30 * time.Millisecond) // 让 Shutdown 进入排空等待
	e.Cancel()                        // 排空等待中升级为取消

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("取消升级后 Shutdown 应确定返回 nil, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown 未在取消后停止")
	}

	canceled := 0
	for i := 0; i < 3; i++ {
		got := <-resCh
		if got.err != nil || got.r.Class != ClassCanceled {
			t.Fatalf("期望全部得到取消裁决, got %+v err=%v", got.r, got.err)
		}
		canceled++
	}
	gm, _ := e.GroupSnapshot("X")
	t.Logf("输入: X 1 在途+2 排队，先 Shutdown 排空等待再 Cancel => 取消裁决=%d(期望3) 分组取消计数=%d Shutdown 确定停止",
		canceled, gm.Canceled)
	if gm.Canceled != 3 {
		t.Fatalf("X 取消计数=%d 期望 3", gm.Canceled)
	}
}
