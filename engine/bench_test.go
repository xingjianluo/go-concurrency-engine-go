package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// work 模拟一个固定成本的 CPU 任务。
func work() {
	var x uint64
	for i := uint64(0); i < 2000; i++ {
		x += i * 2654435761
	}
	_ = x
}

// BenchmarkEngine 受控并发 + 有界缓冲：吞吐基准。
func BenchmarkEngine(b *testing.B) {
	e := New(Config{Workers: 8, QueueSize: 64})
	done := make(chan struct{})
	go func() {
		for range e.Results() {
		}
		close(done)
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Submit(context.Background(), Task{ID: fmt.Sprintf("b-%d", i), Run: func(ctx context.Context) error {
			work()
			return nil
		}}); err != nil {
			b.Fatal(err)
		}
	}
	if err := e.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
	<-done
}

// BenchmarkSerial 串行执行对照：同一任务成本，无任何并发。
func BenchmarkSerial(b *testing.B) {
	for i := 0; i < b.N; i++ {
		work()
	}
}

// runOverload 以固定任务成本压测一种实现，返回吞吐（tasks/s）与平均端到端延迟。
func runOverload(b *testing.B, mode string, workers int) (throughput float64, meanLatency time.Duration) {
	const total = 4000
	var latSum atomic.Int64
	start := time.Now()

	switch mode {
	case "engine": // 有界队列 + 背压
		e := New(Config{Workers: workers, QueueSize: workers * 2})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range e.Results() {
			}
		}()
		for i := 0; i < total; i++ {
			submitAt := time.Now()
			err := e.Submit(context.Background(), Task{ID: fmt.Sprintf("o-%d", i), Run: func(ctx context.Context) error {
				latSum.Add(int64(time.Since(submitAt)))
				work()
				return nil
			}})
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := e.Shutdown(context.Background()); err != nil {
			b.Fatal(err)
		}
		wg.Wait()

	case "unbounded": // 无界缓冲：提交永不阻塞，任务全部堆积在内存队列
		type item struct {
			at time.Time
		}
		q := make(chan item, total) // 等价于无界：容量足以吞下全部任务
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for it := range q {
					latSum.Add(int64(time.Since(it.at)))
					work()
				}
			}()
		}
		for i := 0; i < total; i++ {
			q <- item{at: time.Now()} // 无背压：瞬间全部入队
		}
		close(q)
		wg.Wait()
	}

	elapsed := time.Since(start)
	return float64(total) / elapsed.Seconds(), time.Duration(latSum.Load() / total)
}

// BenchmarkOverloadComparison 对照实验：持续过载（提交速率 >> 处理能力）下，
// 比较有界引擎与无界缓冲实现的吞吐与端到端延迟。
// 无界实现提交不阻塞，但任务在内存中排队更久，端到端延迟显著更高。
func BenchmarkOverloadComparison(b *testing.B) {
	const workers = 8
	for _, mode := range []string{"engine", "unbounded"} {
		b.Run(mode, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tp, lat := runOverload(b, mode, workers)
				b.ReportMetric(tp, "tasks/s")
				b.ReportMetric(float64(lat.Microseconds()), "avg-µs/task")
			}
		})
	}
}
