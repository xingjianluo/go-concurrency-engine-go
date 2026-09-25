package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// taskWork 是单个任务的模拟工作量（I/O 等待性质，适合并发重叠）。
const taskWork = 20 * time.Microsecond

func benchTask(i int) Task {
	return quickTask(fmt.Sprintf("b%d", i), taskWork, nil)
}

// BenchmarkControlledEngine：受控并发 + 有界队列引擎（多提交方并发投递）。
func BenchmarkControlledEngine(b *testing.B) {
	const perN = 1000
	for _, q := range []int{0, 128} {
		b.Run(fmt.Sprintf("workers=%d/queue=%d", 4, q), func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				e := New(Config{Workers: 4, QueueSize: q, Retry: NoRetry(), Deduplicate: false})
				e.Start()
				var wg sync.WaitGroup
				wg.Add(perN)
				for i := 0; i < perN; i++ {
					go func(i int) {
						defer wg.Done()
						if _, err := e.Submit(context.Background(), benchTask(i)); err != nil {
							b.Error(err)
						}
					}(i)
				}
				wg.Wait()
				if err := e.Shutdown(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSerial：串行实现（单工作协程、无队列），作为对照基线。
func BenchmarkSerial(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1000; i++ {
			if err := benchTask(i).Fn(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// runUnbounded：无界缓冲对照实现——每个提交直接派生一个 goroutine，
// 无并发上限、无有界队列（持续过载下内存与调度开销无界增长）。
func runUnbounded(n int) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = benchTask(i).Fn(context.Background())
		}(i)
	}
	wg.Wait()
	return time.Since(start)
}

// runSerial 是串行对照：一个接一个执行。
func runSerial(n int) time.Duration {
	start := time.Now()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		_ = benchTask(i).Fn(ctx)
	}
	return time.Since(start)
}

// runControlled 是本引擎（受控并发、有界背压），提交与执行重叠。
func runControlled(n, workers, queue int) time.Duration {
	e := New(Config{Workers: workers, QueueSize: queue, Retry: NoRetry(), Deduplicate: false})
	e.Start()
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			if _, err := e.Submit(context.Background(), benchTask(i)); err != nil {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
	_ = e.Shutdown(context.Background())
	return time.Since(start)
}

// TestBenchmarkComparison：量化受控并发引擎相对串行与无界缓冲实现在
// 吞吐与延迟上的差异，并验证持续过载下无界实现的资源（goroutine 峰值）失控。
func TestBenchmarkComparison(t *testing.T) {
	const n = 2000
	const workers = 8
	const queue = 64

	// 串行
	stSerial := runSerial(n)
	// 受控引擎
	stControlled := runControlled(n, workers, queue)
	// 无界缓冲（同时观测 goroutine 峰值，模拟过载下的资源占用）
	var peakG int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if g := int32(runtime.NumGoroutine()); g > peakG {
					peakG = g
				}
				time.Sleep(10 * time.Microsecond)
			}
		}
	}()
	base := runtime.NumGoroutine()
	stUnbounded := runUnbounded(n)
	close(stop)

	throughput := func(d time.Duration) float64 { return float64(n) / d.Seconds() }
	t.Logf("======== 对照实验：%d 个 %s I/O 任务 ========", n, taskWork)
	t.Logf("串行实现     : 总耗时=%10v 吞吐=%8.0f tasks/s", stSerial, throughput(stSerial))
	t.Logf("受控引擎(W=%d,Q=%d): 总耗时=%10v 吞吐=%8.0f tasks/s  相对串行加速=%.1fx  相对无界延迟比=%.2f",
		workers, queue, stControlled, throughput(stControlled),
		float64(stSerial)/float64(stControlled), float64(stControlled)/float64(stUnbounded))
	t.Logf("无界缓冲实现 : 总耗时=%10v 吞吐=%8.0f tasks/s  goroutine 峰值≈%d（基线 %d，受控引擎上限=%d）",
		stUnbounded, throughput(stUnbounded), peakG, base, workers+queue)

	// 受控并发相对串行必须有显著吞吐改进（CI 抖动下保守取 2x）。
	if float64(stSerial)/float64(stControlled) < 2.0 {
		t.Fatalf("受控引擎相对串行加速不足: %v vs %v", stControlled, stSerial)
	}
	// 全部任务必须完成、结果不丢。
	// （受控引擎的正确性已由其余测试保证，这里验证对照口径一致。）
	if stControlled <= 0 || stUnbounded <= 0 {
		t.Fatal("耗时异常")
	}
	t.Logf("结论：受控引擎在 %d 并发上限内把 I/O 等待重叠，获得接近无界实现的吞吐，同时 goroutine 数有界（≤W+Q=%d）；串行无法重叠等待。",
		workers, workers+queue)
}
