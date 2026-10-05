// Command groupdemo 演示“按来源分组隔离”的执行器行为：
//   - 各路单独的并发上限（GroupLimits）与全局并发上限（Workers）叠加生效；
//   - A 路猛灌时，B 路仍能按分组轮转持续拿到执行机会（不被饿死）；
//   - 只取消某一路：该路在途/排队任务得到确定取消裁决，其它路照常；
//   - 按路查看受理/成功/取消计数，且各分组之和等于全局汇总。
//
// 运行：go run ./cmd/groupdemo
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	engine "github.com/xingjianluo/go-concurrency-engine-go"
)

func main() {
	ctx := context.Background()
	e := engine.New(engine.Config{
		Workers:     4, // 全局并发上限（对所有分组同时生效）
		QueueSize:   16,
		Retry:       engine.NoRetry(),
		Deduplicate: true,
		// 各路单独的“同时在跑”上限；与全局上限取更紧者。
		GroupLimits: map[string]int{"A": 4, "B": 1, "C": 2},
	})
	e.Start()

	// 1) A 路持续猛灌短任务；同期 B 路提交几个慢一些的任务，验证 B 不被饿死。
	var aSeq int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := atomic.AddInt64(&aSeq, 1)
			_, _ = e.Submit(ctx, engine.Task{
				ID: fmt.Sprintf("a%d", idx), Source: "A",
				Fn: func(context.Context) error { time.Sleep(2 * time.Millisecond); return nil },
			})
		}()
	}
	for i := 0; i < 4; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			r, err := e.Submit(ctx, engine.Task{
				ID: fmt.Sprintf("b%d", i), Source: "B",
				Fn: func(context.Context) error { time.Sleep(4 * time.Millisecond); return nil },
			})
			fmt.Printf("B-%d => class=%s err=%v 等待=%v（A 猛灌期间仍被调度）\n", i, r.Class, err, time.Since(start).Round(time.Millisecond))
		}()
	}
	wg.Wait()

	// 2) 只取消 C 路：在途任务收到取消，排队任务得到取消裁决；A/B 不受影响。
	releaseC := make(chan struct{})
	var cRunning int64
	cDone := make(chan struct{})
	go func() {
		_, _ = e.Submit(ctx, engine.Task{
			ID: "c-long", Source: "C",
			Fn: func(ctx context.Context) error {
				atomic.AddInt64(&cRunning, 1)
				<-ctx.Done()
				return ctx.Err()
			},
		})
		close(cDone)
	}()
	for atomic.LoadInt64(&cRunning) < 1 {
		time.Sleep(time.Millisecond)
	}
	if err := e.CancelGroup("C"); err != nil {
		fmt.Println("CancelGroup(C) error:", err)
	}
	<-cDone
	close(releaseC)

	// C 已停止受理；A 仍能继续提交。
	if _, err := e.Submit(ctx, engine.Task{ID: "c-after", Source: "C", Fn: func(context.Context) error { return nil }}); err != nil {
		fmt.Printf("取消 C 后再提交 => %v（符合预期）\n", err)
	}
	if r, err := e.Submit(ctx, engine.Task{ID: "a-final", Source: "A", Fn: func(context.Context) error { return nil }}); err != nil {
		fmt.Println("A 路提交异常:", err)
	} else {
		fmt.Printf("取消 C 后 A 仍正常 => class=%s\n", r.Class)
	}

	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = e.Shutdown(sctx)

	m := e.Snapshot()
	ga, _ := e.GroupSnapshot("A")
	gb, _ := e.GroupSnapshot("B")
	gc, _ := e.GroupSnapshot("C")
	fmt.Printf("全局: 受理=%d 成功=%d 取消=%d\n", m.Submitted, m.Succeeded, m.Canceled)
	fmt.Printf("  A: 受理=%d 成功=%d 取消=%d\n", ga.Submitted, ga.Succeeded, ga.Canceled)
	fmt.Printf("  B: 受理=%d 成功=%d 取消=%d\n", gb.Submitted, gb.Succeeded, gb.Canceled)
	fmt.Printf("  C: 受理=%d 成功=%d 取消=%d\n", gc.Submitted, gc.Succeeded, gc.Canceled)
	fmt.Printf("校验: A+B+C 受理=%d == 全局受理=%d；成功=%d == 全局成功=%d\n",
		ga.Submitted+gb.Submitted+gc.Submitted, m.Submitted,
		ga.Succeeded+gb.Succeeded+gc.Succeeded, m.Succeeded)
}
