package engine

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestSustainedOverloadBackpressure：提交速率持续高于处理能力时，
// 提交方必须被阻塞背压，且不丢任务、不无限缓冲（内存占用有界）。
func TestSustainedOverloadBackpressure(t *testing.T) {
	const (
		workers   = 2
		queueSize = 4
		taskN     = 200
		taskWork  = 2 * time.Millisecond
	)
	e := New(Config{Workers: workers, QueueSize: queueSize, Retry: NoRetry(), Deduplicate: true})
	e.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	var blocked, done int64
	var maxQueueDepth int64
	start := time.Now()

	// 提交方：高速涌入，单个任务处理能力远低于提交速度。
	for i := 0; i < taskN; i++ {
		go func(i int) {
			s := time.Now()
			res, err := e.Submit(context.Background(), quickTask(fmt.Sprintf("w%d", i), taskWork, nil))
			waited := time.Since(s)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			if waited > taskWork/2 { // 明显等待才算观察到背压
				atomic.AddInt64(&blocked, 1)
			}
			if res.Class == ClassSuccess {
				atomic.AddInt64(&done, 1)
			}
		}(i)
	}

	// 观测期：周期性采样队列深度，验证缓冲始终有界。
	stopObs := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopObs:
				return
			default:
				if d := e.Snapshot().QueueDepth; d > atomic.LoadInt64(&maxQueueDepth) {
					atomic.StoreInt64(&maxQueueDepth, d)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := e.Submit(ctx, quickTask("sentinel", taskWork, nil)) // 等所有在它之前受理的任务推进
	if err != nil {
		t.Fatalf("sentinel: %v", err)
	}
	if res.Class != ClassSuccess {
		t.Fatalf("sentinel class=%s", res.Class)
	}
	// sentinel 之后再等待后台全部完成
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&done) < taskN && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	close(stopObs)

	m := e.Snapshot()
	elapsed := time.Since(start)
	t.Logf("输入: %d 个 %v 任务持续涌入, workers=%d queue=%d", taskN, taskWork, workers, queueSize)
	t.Logf("判定依据: 完成=%d(期望%d) 观察到背压等待的提交数=%d 队列深度峰值=%d(应≤%d) 总耗时=%v 吞吐=%.0f tasks/s",
		atomic.LoadInt64(&done), taskN, atomic.LoadInt64(&blocked),
		atomic.LoadInt64(&maxQueueDepth), queueSize, elapsed,
		float64(taskN)/elapsed.Seconds())
	t.Logf("指标: Submitted=%d Succeeded=%d 背压等待事件=%d 背压累计等待=%v",
		m.Submitted, m.Succeeded, m.BackpressureWaits, m.BackpressureTime)

	if atomic.LoadInt64(&done) != taskN {
		t.Fatalf("有任务丢失：完成 %d/%d", done, taskN)
	}
	if atomic.LoadInt64(&maxQueueDepth) > queueSize {
		t.Fatalf("缓冲无界：队列峰值 %d > 容量 %d", maxQueueDepth, queueSize)
	}
	if atomic.LoadInt64(&blocked) == 0 || m.BackpressureWaits == 0 {
		t.Fatalf("持续过载却未观察到任何背压，模型未生效")
	}
	if m.Succeeded != int64(taskN+1) { // +1 sentinel
		t.Fatalf("成功数不一致: %+v", m)
	}
}

// TestTrySubmitNonBlocking：满队列时 TrySubmit 立即被拒，由调用方决定降载。
func TestTrySubmitNonBlocking(t *testing.T) {
	e := runEngine(t, Config{Workers: 1, QueueSize: 1, Retry: NoRetry(), Deduplicate: true})
	release := make(chan struct{})
	blocking := Task{ID: "blocker", Fn: func(ctx context.Context) error {
		<-release
		return nil
	}}
	go func() {
		_, _ = e.Submit(context.Background(), blocking)
	}()
	time.Sleep(10 * time.Millisecond) // 让 blocker 进入执行位
	// 填满队列（受理后会阻塞至执行完成，故放后台；受理动作本身非阻塞）
	queuedDone := make(chan struct{})
	go func() {
		_, accepted, err := e.TrySubmit(quickTask("queued", time.Millisecond, nil))
		if err != nil || !accepted {
			t.Errorf("期望 queued 被受理: accepted=%v err=%v", accepted, err)
		}
		close(queuedDone)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for e.Snapshot().QueueDepth < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.Snapshot().QueueDepth < 1 {
		t.Fatal("queued 任务未能进入有界队列")
	}
	acceptedRejected := 0
	// 队列已满 + 工作协程被占：后续 TrySubmit 必须立即拒绝
	for i := 0; i < 20; i++ {
		_, accepted, err := e.TrySubmit(quickTask(fmt.Sprintf("drop%d", i), time.Millisecond, nil))
		if err != nil {
			t.Fatal(err)
		}
		if !accepted {
			acceptedRejected++
		}
	}
	close(release)
	<-queuedDone
	t.Logf("输入: 1 执行位被占 + 1 容量队列被填满后连续 TrySubmit 20 次 => 立即被拒 %d 次（期望全部被拒），无静默丢弃",
		acceptedRejected)
	if acceptedRejected != 20 {
		t.Fatalf("期望 20 次非阻塞拒绝，实际 %d", acceptedRejected)
	}
}
