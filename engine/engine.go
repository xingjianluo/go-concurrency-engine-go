package engine

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrClosed 表示引擎已关闭，拒绝新提交。
var ErrClosed = errors.New("engine: closed")

// Config 引擎配置。
type Config struct {
	Workers   int // 并发 worker 数，<=0 时默认 4
	QueueSize int // 有界缓冲容量（不含在途任务），<0 按 0 处理
	Retry     RetryPolicy
}

// Engine 是受控并发、有界缓冲的任务执行引擎。
//
// 并发与背压模型：队列容量有界，队列满时 Submit 阻塞（背压），
// 直到有容量、调用方 ctx 取消或引擎关闭；引擎不静默丢弃任何已接受任务。
//
// 关闭语义：Shutdown 后新提交返回 ErrClosed，阻塞中的 Submit 被唤醒；
// 已在途任务（含重试与退避）继续执行到终态，每个被接受任务恰好产生一个 Result。
// 若 Shutdown 的 ctx 超时，则强制取消在途任务，它们以 KindCancelled 收尾。
type Engine struct {
	cfg     Config
	queue   chan Task
	inbox   chan Result   // worker -> 转发器（无界转发，worker 永不阻塞）
	results chan Result   // 对外结果通道
	stopCh  chan struct{} // Shutdown 开始时关闭，唤醒阻塞的 Submit
	fwdDone chan struct{} // 转发器退出信号

	ctx    context.Context // 传给 Task.Run；仅在强制取消时 cancel
	cancel context.CancelFunc

	mu      sync.Mutex
	seen    map[string]struct{} // 幂等去重
	closed  bool
	metrics *Metrics

	wg       sync.WaitGroup // 追踪被接受但未达终态的任务
	workerWg sync.WaitGroup // worker goroutine
}

// New 创建并启动引擎。
func New(cfg Config) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:     cfg,
		queue:   make(chan Task, cfg.QueueSize),
		inbox:   make(chan Result),
		results: make(chan Result),
		stopCh:  make(chan struct{}),
		fwdDone: make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
		seen:    make(map[string]struct{}),
		metrics: &Metrics{},
	}
	go e.forward()
	for i := 0; i < cfg.Workers; i++ {
		e.workerWg.Add(1)
		go e.worker()
	}
	return e
}

// Submit 提交任务；队列满时阻塞（背压），直到有容量、引擎关闭或 ctx 取消。
// 相同 ID 的重复投递被幂等忽略，返回 nil 且不重复计入结果。
func (e *Engine) Submit(ctx context.Context, t Task) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if _, dup := e.seen[t.ID]; dup {
		e.mu.Unlock()
		e.metrics.deduped.Add(1)
		return nil
	}
	e.seen[t.ID] = struct{}{}
	// 在持锁状态下 Add，保证 Shutdown 置 closed 后不会再有新的 Add。
	e.wg.Add(1)
	e.mu.Unlock()

	select {
	case e.queue <- t:
		e.metrics.submitted.Add(1)
		return nil
	case <-ctx.Done():
		e.untrack(t.ID)
		return ctx.Err()
	case <-e.stopCh:
		e.untrack(t.ID)
		return ErrClosed
	}
}

// untrack 撤销一次未成功入队的提交。
func (e *Engine) untrack(id string) {
	e.mu.Lock()
	delete(e.seen, id)
	e.mu.Unlock()
	e.wg.Done()
}

// Results 返回结果通道；每个被接受的任务恰好产生一个 Result。
// 通道在 Shutdown 完成且所有结果排空后关闭。
func (e *Engine) Results() <-chan Result {
	return e.results
}

// Shutdown 停止接收新任务，等待在途任务完成（含重试），随后关闭结果通道。
// 若 ctx 超时，强制取消在途任务（以 KindCancelled 收尾）并返回 ctx.Err()。
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.mu.Unlock()
	close(e.stopCh) // 唤醒阻塞中的 Submit

	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		e.cancel() // 强制取消在途任务，它们以 KindCancelled 产生确定结果
		<-done
		err = ctx.Err()
	}

	// 所有任务已达终态，不会再有人向 queue 发送，安全关闭。
	close(e.queue)
	e.workerWg.Wait() // 等待 worker 退出
	close(e.inbox)    // 通知转发器收尾：排空残留结果后关闭 results
	<-e.fwdDone
	e.cancel()
	return err
}

// worker 从队列取任务并执行到达终态。
func (e *Engine) worker() {
	defer e.workerWg.Done()
	for t := range e.queue {
		e.execute(t)
	}
}

// execute 执行任务直至终态：成功、永久失败、重试耗尽或被取消。
// 未分类的错误按临时失败处理（可重试）。
func (e *Engine) execute(t Task) {
	defer e.wg.Done()
	start := time.Now()
	res := Result{ID: t.ID}
	maxAttempts := e.cfg.Retry.MaxAttempts

	attempt := 0
loop:
	for {
		if err := e.ctx.Err(); err != nil {
			res.Kind, res.Err = KindCancelled, err
			break
		}
		attempt++
		err := t.Run(e.ctx)
		res.Attempts = attempt
		switch {
		case err == nil:
			res.Kind = KindSuccess
			break loop
		case e.ctx.Err() != nil && errors.Is(err, context.Canceled):
			res.Kind, res.Err = KindCancelled, err
			break loop
		case IsPermanent(err):
			res.Kind, res.Err = KindPermanent, err
			break loop
		case attempt >= maxAttempts:
			res.Kind, res.Err = KindTemporary, err
			break loop
		}
		// 临时失败：退避后重试，退避可被强制取消打断。
		select {
		case <-time.After(e.cfg.Retry.backoff(attempt)):
		case <-e.ctx.Done():
			res.Kind, res.Err = KindCancelled, e.ctx.Err()
			break loop
		}
	}

	res.Duration = time.Since(start)
	e.metrics.record(res)
	e.inbox <- res // 转发器保证不阻塞
}

// forward 将 worker 的结果无界转发到对外通道，保证 worker 永不因结果消费慢而阻塞。
func (e *Engine) forward() {
	defer close(e.fwdDone)
	var pending []Result
	inbox := e.inbox
	for {
		var outCh chan<- Result
		var head Result
		if len(pending) > 0 {
			outCh = e.results
			head = pending[0]
		}
		select {
		case r, ok := <-inbox:
			if !ok {
				inbox = nil
				if len(pending) == 0 {
					close(e.results)
					return
				}
				continue
			}
			pending = append(pending, r)
		case outCh <- head:
			pending = pending[1:]
			if inbox == nil && len(pending) == 0 {
				close(e.results)
				return
			}
		}
	}
}
