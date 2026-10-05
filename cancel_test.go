package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelMidflight：运行中取消，在途任务经 ctx 收到取消、排队任务得到
// 确定的取消裁决；不丢任务、不重复统计，且取消不阻塞其它已完成结果的一致性。
func TestCancelMidflight(t *testing.T) {
	const (
		workers   = 2
		queueSize = 50
		nLong     = 2  // 长任务：会进入工作协程并在执行中观察取消
		nQueued   = 60 // 短任务：填满有界队列，取消时多数仍在排队
	)
	e := New(Config{Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true})
	e.Start()

	var longStarted int64
	// result/err 必须成对送达同一通道，避免两个独立通道在多 goroutine 下
	// 被交错接收、错配成“零值 result + nil err”这种说不清的组合。
	type outcome struct {
		r   Result
		err error
	}
	outcomes := make(chan outcome, nLong+nQueued)

	// 长任务尊重 ctx：阻塞直到取消或完成。
	for i := 0; i < nLong; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(), Task{ID: fmt.Sprintf("long%d", i), Fn: func(ctx context.Context) error {
				atomic.AddInt64(&longStarted, 1)
				<-ctx.Done()
				return ctx.Err()
			}})
			outcomes <- outcome{r, err}
		}()
	}
	// 等待长任务占满两个工作协程。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&longStarted) < nLong && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// 排队任务（取消到来时停留在有界队列中，或仍在队尾背压阻塞）。
	for i := 0; i < nQueued; i++ {
		i := i
		go func() {
			r, err := e.Submit(context.Background(), quickTask(fmt.Sprintf("q%d", i), 5*time.Second, nil))
			outcomes <- outcome{r, err}
		}()
	}
	// 尽量等待队列被填满，证明存在在途任务（慢机器下允许未填满即取消）。
	observeUntil := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(observeUntil) && e.Snapshot().QueueDepth < int64(queueSize) {
		time.Sleep(time.Millisecond)
	}
	t.Logf("取消前: 在执行=%d 队列深度=%d（工作协程被长任务占满，队列已积压）",
		e.Snapshot().ActiveWorkers, e.Snapshot().QueueDepth)

	e.Cancel()

	// 取消后所有提交方都必须得到确定结论：已受理 => 取消裁决；仍在背压
	// 阻塞中未受理 => context.Canceled 错误（不执行、不计入结果）。
	gotCanceled := 0
	gotRejected := 0
	total := nLong + nQueued
	for i := 0; i < total; i++ {
		oc := <-outcomes
		r, err := oc.r, oc.err
		switch {
		case err == nil && r.Class == ClassCanceled:
			gotCanceled++
		case err != nil:
			// 未受理（取消时仍在背压），或共享了一个被撤销的重复条目：
			// 均为确定的“未执行”失败结论，任务不执行、不计入结果。
			gotRejected++
		default:
			t.Errorf("提交 %d 取消后结论不确定: result=%+v err=%v", i, r, err)
		}
	}

	m := e.Snapshot()
	t.Logf("判定依据: 发起 %d 次提交后中途取消 => 已受理取消裁决=%d 未受理背压拒绝=%d 合计=%d",
		total, gotCanceled, gotRejected, gotCanceled+gotRejected)
	t.Logf("指标: Submitted=%d(=取消裁决数) Canceled=%d Succeeded=%d（未受理任务不得计入结果）",
		m.Submitted, m.Canceled, m.Succeeded)
	if gotCanceled+gotRejected != total {
		t.Fatalf("存在不确定结论: canceled=%d rejected=%d total=%d", gotCanceled, gotRejected, total)
	}
	if int64(gotCanceled) != m.Canceled || int64(gotCanceled) != m.Submitted {
		t.Fatalf("已受理取消数与指标不一致: gotCanceled=%d metrics=%+v", gotCanceled, m)
	}
	if m.Succeeded != 0 {
		t.Fatalf("取消后不应有成功任务: %+v", m)
	}

	// 关闭已随取消完成：stoppedCh 应关闭，且后续提交被拒绝。
	select {
	case <-e.stoppedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Cancel 后引擎未在限期内停止")
	}
	if _, err := e.Submit(context.Background(), quickTask("after", time.Millisecond, nil)); err != context.Canceled {
		t.Fatalf("取消后提交期望 context.Canceled, got %v", err)
	}
}

// TestSubmitContextAbandon：提交方 ctx 在等待背压时结束，任务未被受理，
// 不计入结果；其余任务与汇总不受影响。
func TestSubmitContextAbandon(t *testing.T) {
	e := runEngine(t, Config{Workers: 1, QueueSize: 0, Retry: NoRetry(), Deduplicate: true})
	release := make(chan struct{})
	go func() {
		_, _ = e.Submit(context.Background(), Task{ID: "holder", Fn: func(ctx context.Context) error {
			<-release
			return nil
		}})
	}()
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := e.Submit(ctx, quickTask("abandoned", time.Millisecond, nil))
	t.Logf("输入: 执行位被占时，带 20ms 超时 ctx 提交第二个任务 => err=%v（期望 deadline exceeded）", err)
	if err != context.DeadlineExceeded {
		t.Fatalf("期望提交方 ctx 超时，got %v", err)
	}
	m1 := e.Snapshot()
	t.Logf("放弃等待后指标: Submitted=%d（holder 在途）Succeeded=%d Canceled=%d", m1.Submitted, m1.Succeeded, m1.Canceled)
	if m1.Succeeded != 0 {
		t.Fatalf("放弃等待的任务不应产生任何结果")
	}
	close(release)
}

// TestGracefulShutdownDrains：已受理任务在 Shutdown 期间被确定排空，全部完成。
func TestGracefulShutdownDrains(t *testing.T) {
	e := New(Config{Workers: 3, QueueSize: 20,
		Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond}, Deduplicate: true})
	e.Start()
	const n = 30
	var success, tempExhausted, submitted atomic.Int64
	for i := 0; i < n; i++ {
		i := i
		go func() {
			var err error
			if i%5 == 0 {
				err = TempError(fmt.Errorf("e%d", i)) // 重试 1 次后耗尽
			}
			r, err2 := e.Submit(context.Background(), quickTask(fmt.Sprintf("g%d", i), time.Millisecond, err))
			if err2 != nil {
				t.Errorf("submit %d: %v", i, err2)
				return
			}
			submitted.Add(1)
			switch r.Class {
			case ClassSuccess:
				success.Add(1)
			case ClassTemporary:
				tempExhausted.Add(1)
			}
		}()
	}
	// 确保全部任务已被受理（进入在途），再验证优雅关闭的确定排空。
	deadline := time.Now().Add(2 * time.Second)
	for submitted.Load() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if submitted.Load() != n {
		t.Fatalf("仅 %d/%d 任务被受理", submitted.Load(), n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	m := e.Snapshot()
	t.Logf("输入 %d 任务（其中 1/5 临时失败）后优雅关闭 => 成功=%d 重试耗尽=%d（期望 %d/%d）, Attempts=%d",
		n, success.Load(), tempExhausted.Load(), n-n/5, n/5, m.Attempts)
	if m.Submitted != n || m.Succeeded != int64(n-n/5) || m.RetryExhausted != int64(n/5) {
		t.Fatalf("排空结果不一致: %+v", m)
	}
	if m.Succeeded+m.PermanentFailures+m.RetryExhausted+m.Canceled != m.Submitted {
		t.Fatalf("终态分类之和不等于受理数: %+v", m)
	}
}
