// Command demo 在本地演示受控并发任务引擎的关键行为：
// 受控并发、背压、失败分类与有限重试、取消、幂等去重与指标汇总。
// 仅依赖标准库与本仓库自身，可直接 go run ./cmd/demo 运行。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	engine "github.com/xingjianluo/go-concurrency-engine-go"
)

func main() {
	ctx := context.Background()
	e := engine.New(engine.Config{
		Workers:     4, // 受控并发度
		QueueSize:   8, // 有界缓冲：满则背压
		Retry:       engine.DefaultRetryPolicy(),
		Deduplicate: true, // 相同 ID 幂等
	})
	e.Start()

	// 1) 一个前两次临时失败、第三次成功的任务。
	var flakyCalls int64
	flaky := engine.Task{ID: "flaky", Fn: func(ctx context.Context) error {
		if atomic.AddInt64(&flakyCalls, 1) < 3 {
			return engine.TempError(errors.New("暂时不可用"))
		}
		return nil
	}}
	r, err := e.Submit(ctx, flaky)
	fmt.Printf("flaky       => class=%s attempts=%d err=%v\n", r.Class, r.Attempts, err)

	// 2) 永久失败：不重试。
	r, _ = e.Submit(ctx, engine.Task{ID: "bad", Fn: func(context.Context) error {
		return engine.PermError(errors.New("配置非法"))
	}})
	fmt.Printf("bad         => class=%s attempts=%d err=%v\n", r.Class, r.Attempts, r.Err)

	// 3) 相同 ID 并发重复投递 5 次：只执行一次、只计一次。
	for range 5 {
		go func() { _, _ = e.Submit(ctx, flaky) }()
	}
	time.Sleep(20 * time.Millisecond)

	// 4) 取消：启动一个长任务后取消引擎。
	go func() {
		_, _ = e.Submit(ctx, engine.Task{ID: "long", Fn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}})
	}()
	time.Sleep(10 * time.Millisecond)
	e.Cancel() // 长任务收到取消；后续提交返回 context.Canceled

	if _, err := e.Submit(ctx, engine.Task{ID: "after", Fn: func(context.Context) error { return nil }}); err != nil {
		fmt.Printf("after-cancel=> submit err=%v（符合预期）\n", err)
	}

	// Cancel 后 Shutdown 仅等待在途任务拿到确定裁决并回收协程。
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = e.Shutdown(sctx)

	m := e.Snapshot()
	fmt.Printf("metrics     => 受理=%d 重复拦截=%d 成功=%d 永久失败=%d 重试耗尽=%d 取消=%d 尝试=%d 重试=%d\n",
		m.Submitted, m.DuplicateDeliveries, m.Succeeded, m.PermanentFailures,
		m.RetryExhausted, m.Canceled, m.Attempts, m.Retries)
}
