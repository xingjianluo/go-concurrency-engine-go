package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// 引擎生命周期相关的可判定错误。
var (
	// ErrEngineClosed 在关闭/取消已开始后调用 Submit/TrySubmit 时返回。
	ErrEngineClosed = errors.New("engine: closed")
	// ErrNotStarted 在 Start 之前提交任务时返回。
	ErrNotStarted = errors.New("engine: not started")
	// ErrQueueFull 表示共享了一个因队列满而非阻塞拒绝的任务条目。
	ErrQueueFull = errors.New("engine: queue full")
	// ErrInvalidTask 表示任务缺少 ID（开启幂等时）或 Fn。
	ErrInvalidTask = errors.New("engine: invalid task")
)

// Config 描述引擎参数：受控并发 + 有界缓冲 + 有限重试 + 幂等去重。
type Config struct {
	// Workers 为工作协程数（受控并发度）；<=0 时取 runtime.NumCPU()。
	Workers int
	// QueueSize 为有界等待队列容量；0 表示无缓冲（满一个工作协程才不阻塞）。
	// 队列占满时 Submit 对调用方施加背压，绝不无界缓冲或静默丢弃。
	QueueSize int
	// Retry 为重试策略；零值等价于 NoRetry。
	Retry RetryPolicy
	// Deduplicate 为 true 时，相同 ID 的在途重复投递共享一次执行与一个结果。
	Deduplicate bool
}

// inflight 表示一个已受理且至多执行一次的任务。终态结果（含因提交方
// 放弃受理而产生的受理撤销）写入后关闭 done，所有投递方共享同一裁决。
type inflight struct {
	t Task

	mu       sync.Mutex
	result   Result
	finished bool
	abortErr error // 非 nil 表示受理被撤销（任务从未执行）
	aborted  bool

	// done 在终态结果写入或受理撤销后关闭，所有重复投递的提交方等待同一个结果。
	done chan struct{}
}

// await 等待终态后返回结果；若该任务的受理被撤销则返回撤销原因。
func (it *inflight) await() (Result, error) {
	<-it.done
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.aborted {
		return Result{}, it.abortErr
	}
	return it.result, nil
}

// abort 标记该条目的受理已撤销（任务从未执行），并唤醒所有等待者。
func (it *inflight) abort(err error) {
	it.mu.Lock()
	if it.aborted || it.finished {
		it.mu.Unlock()
		return
	}
	it.aborted = true
	it.abortErr = err
	it.mu.Unlock()
	close(it.done)
}

// awaitCtx 等待终态或提交方 ctx 结束。ctx 结束仅表示提交方放弃等待结果，
// 已受理任务的执行与统计不受影响；若任务受理被撤销则返回撤销原因。
func (it *inflight) awaitCtx(ctx context.Context) (Result, error) {
	select {
	case <-it.done:
		return it.await()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// Engine 是单进程本地并发任务执行引擎。零值不可用，须经 New + Start。
type Engine struct {
	cfg Config
	agg *aggregator

	queue chan *inflight // 有界缓冲：满则阻塞提交方（背压）

	mu       sync.Mutex
	pending  map[string]*inflight
	closed   bool
	canceled bool // Cancel 已触发（影响提交方拿到的错误语义）

	started atomic.Bool

	runCtx   context.Context // 派生给每个任务；Cancel 时取消
	cancelFn context.CancelFunc

	outstanding sync.WaitGroup // 已入队但未终态的任务数（排空依据）

	workerWg sync.WaitGroup
	stopCh   chan struct{} // 队列排空后关闭，工作协程据此退出

	active atomic.Int64 // 正在执行（含退避）的任务数

	shutdownOnce sync.Once
	stoppedCh    chan struct{} // 所有工作协程退出后关闭
}

// New 创建引擎（尚未启动）。
func New(cfg Config) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry = NoRetry()
	}
	runCtx, cancel := context.WithCancel(context.Background())
	return &Engine{
		cfg:       cfg,
		agg:       newAggregator(),
		queue:     make(chan *inflight, cfg.QueueSize),
		pending:   make(map[string]*inflight),
		runCtx:    runCtx,
		cancelFn:  cancel,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// Start 启动固定数量的工作协程。可重复调用，仅首次生效。
func (e *Engine) Start() {
	if !e.started.CompareAndSwap(false, true) {
		return
	}
	e.workerWg.Add(e.cfg.Workers)
	for i := 0; i < e.cfg.Workers; i++ {
		go e.worker()
	}
}

// Submit 提交任务并阻塞至该任务终态，返回其幂等结果。
//
// 背压：内部有界队列占满时阻塞，直到有空位；提交方可通过 ctx 放弃等待，
// 此时任务尚未受理（不会执行、不计入结果）。
// 幂等：开启 Deduplicate 时，相同 ID 且仍在途的重复投递共享同一次执行，
// 等待并拿到同一个 Result，该 ID 只统计一次。
// 取消/关闭：引擎进入关闭后提交返回 ErrEngineClosed；Cancel 后返回
// context.Canceled。已在途任务不受提交方 ctx 取消的影响（仅放弃等待结果）。
func (e *Engine) Submit(ctx context.Context, t Task) (Result, error) {
	item, err := e.admit(ctx, t, false)
	if err != nil {
		return Result{}, err
	}
	return item.awaitCtx(ctx)
}

// TrySubmit 非阻塞尝试提交：队列无空位时 accepted=false（err 为 nil），
// 调用方可据此自行降载，而不会被静默丢弃。
func (e *Engine) TrySubmit(t Task) (result Result, accepted bool, err error) {
	item, err := e.admit(context.Background(), t, true)
	if err != nil {
		return Result{}, false, err
	}
	if item == nil {
		return Result{}, false, nil // 队列无空位：非阻塞拒绝，由调用方降载
	}
	res, err := item.await()
	return res, true, err
}

// admit 完成校验、在途幂等去重与有界入队（含背压）。
func (e *Engine) admit(ctx context.Context, t Task, nonblocking bool) (*inflight, error) {
	if !e.started.Load() {
		return nil, ErrNotStarted
	}
	if t.Fn == nil {
		return nil, ErrInvalidTask
	}
	if e.cfg.Deduplicate && t.ID == "" {
		return nil, ErrInvalidTask
	}

	e.mu.Lock()
	if e.closed {
		err := error(ErrEngineClosed)
		if e.canceled {
			err = context.Canceled
		}
		e.mu.Unlock()
		return nil, err
	}
	if e.cfg.Deduplicate {
		if existing, ok := e.pending[t.ID]; ok {
			e.mu.Unlock()
			// 在途重复投递：不重复入队、不重复统计，仅等待共享结果。
			e.agg.addDuplicate()
			return existing, nil
		}
	}
	item := &inflight{t: t, done: make(chan struct{})}
	if e.cfg.Deduplicate {
		e.pending[t.ID] = item
	}
	e.agg.addSubmitted(1)
	e.mu.Unlock()

	// 先于入队计数：只要任务可能进入队列，outstanding 就必须 > 0，
	// 避免“排空完成 -> 计数器归零 -> 任务才入队”的竞态。
	e.outstanding.Add(1)

	// 有界入队：队列满即对提交方形成背压。
	waitStart := time.Now()
	enqueued := false
	if nonblocking {
		select {
		case e.queue <- item:
			enqueued = true
		default:
		}
	} else {
		// runCtx.Done 在 Cancel 时关闭，确保关闭/取消能唤醒因队列满而
		// 阻塞的提交方，而不是让它们一直卡在背压上。
		select {
		case e.queue <- item:
			enqueued = true
		case <-ctx.Done():
		case <-e.runCtx.Done():
		}
	}
	if waited := time.Since(waitStart); waited > 0 {
		e.agg.addBackpressure(waited)
	}

	closedNow := false
	canceledNow := false
	e.mu.Lock()
	closedNow = e.closed
	canceledNow = e.canceled
	e.mu.Unlock()

	if !enqueued {
		// 未受理：撤销计数，并把该条目标记为“受理撤销”，唤醒可能已共享
		// 此条目的重复投递方（它们拿到同样的失败原因，绝不永久挂起）。
		// 条目保留在 pending 中：之后相同 ID 的投递也拿到同一撤销结果，
		// 不会再次执行、不会重复计数。
		e.outstanding.Add(-1)
		var reason error
		switch {
		case nonblocking:
			reason = ErrQueueFull
		case canceledNow:
			reason = context.Canceled
		case closedNow:
			reason = ErrEngineClosed
		default:
			reason = ctx.Err()
		}
		item.abort(reason)
		e.agg.addSubmitted(-1)
		if nonblocking {
			// TrySubmit 的非阻塞拒绝：撤销条目不长期占用 pending，交给 GC；
			// 同一批次中抢到该条目的并发副本已由 abort 唤醒。
			e.mu.Lock()
			if e.pending[t.ID] == item {
				delete(e.pending, t.ID)
			}
			e.mu.Unlock()
			return nil, nil
		}
		return nil, reason
	}

	return item, nil
}

// worker 是固定大小工作协程池中的一员。
func (e *Engine) worker() {
	defer e.workerWg.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case item := <-e.queue:
			e.active.Add(1)
			e.process(item)
			e.active.Add(-1)
			e.outstanding.Done()
		}
	}
}

// process 执行单个任务：有限重试 + 退避 + 分类终态，且只终态化一次。
func (e *Engine) process(item *inflight) {
	start := time.Now()
	maxAttempts := e.cfg.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	// 取消发生在任务开始之前：给出确定的取消裁决（未执行，Attempts=0）。
	if err := e.runCtx.Err(); err != nil {
		e.finalize(item, Result{
			TaskID: item.t.ID, Class: ClassCanceled, Attempts: 0,
			Duration: time.Since(start), Err: err,
		})
		return
	}

	var lastErr error
	class := ClassUnknown
	attempts := 0
	for attempts < maxAttempts {
		attempts++
		err := item.t.Fn(e.runCtx)
		lastErr = err
		class = Classify(err)

		// 成功、永久失败、取消：立即终态，不再重试。
		if class != ClassTemporary {
			break
		}
		// 引擎已取消：不再退避/重试，统一裁决为取消。
		if e.runCtx.Err() != nil {
			class = ClassCanceled
			lastErr = e.runCtx.Err()
			break
		}
		// 临时失败但重试次数耗尽：保持 ClassTemporary 退出循环，
		// 终态结果由聚合器计入 RetryExhausted。
		if attempts >= maxAttempts {
			break
		}

		// 有限退避等待；取消立即打断，裁决为取消（不消耗剩余重试次数）。
		wait := e.cfg.Retry.nextBackoff(attempts)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-e.runCtx.Done():
				timer.Stop()
				e.finalize(item, Result{
					TaskID: item.t.ID, Class: ClassCanceled, Attempts: attempts,
					Duration: time.Since(start), Err: e.runCtx.Err(),
				})
				return
			}
		}
	}
	e.finalize(item, Result{
		TaskID: item.t.ID, Class: class, Attempts: attempts,
		Duration: time.Since(start), Err: lastErr,
	})
}

// finalize 写入唯一终态结果、唤醒所有等待者、计入聚合并释放幂等占位。
func (e *Engine) finalize(item *inflight, res Result) {
	item.mu.Lock()
	if item.finished {
		// 理论不可达：每个 inflight 只被一个工作协程处理。
		item.mu.Unlock()
		return
	}
	item.result = res
	item.finished = true
	item.mu.Unlock()
	close(item.done)

	e.agg.recordResult(res)
	// 刻意不从 pending 删除：终态条目保留下来，使晚到的相同 ID 投递
	// 复用同一结果，保证全局幂等、不重复执行、不重复累计。
}

// Cancel 触发取消并开始关闭：
//   - 在途任务的 ctx 立即取消，正在进行的退避立即结束；
//   - 已入队未开始的任务获得确定的取消裁决（不丢弃、不执行）；
//   - 之后的提交返回 context.Canceled。
func (e *Engine) Cancel() {
	e.mu.Lock()
	e.canceled = true
	e.closed = true
	e.mu.Unlock()
	e.cancelFn()
	e.beginShutdown()
}

// Shutdown 优雅关闭：停止受理新任务，确定地排空所有已受理任务
// （每个任务都有终态结果），然后回收工作协程。
// ctx 到期仍未排空时，取消在途任务并返回 ctx.Err()；尊重 ctx 的任务会很快排空。
func (e *Engine) Shutdown(ctx context.Context) error {
	e.beginShutdown()
	select {
	case <-e.stoppedCh:
		return nil
	case <-ctx.Done():
		e.cancelFn() // 催促无视取消信号的任务尽快结束
		return ctx.Err()
	}
}

// beginShutdown 幂等地启动“拒绝提交 -> 排空 -> 停止工作协程”的流程。
func (e *Engine) beginShutdown() {
	e.shutdownOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.mu.Unlock()

		go func() {
			// 等待所有已受理任务拿到终态（取消时也会快速得到取消裁决）。
			e.outstanding.Wait()
			// 排空后再取消，确保退避中的任务也被纳入排空等待。
			e.cancelFn()
			close(e.stopCh)
			e.workerWg.Wait()
			close(e.stoppedCh)
		}()
	})
}

// Snapshot 返回一致的指标快照（含队列深度与在执行数等瞬时量表）。
func (e *Engine) Snapshot() Metrics {
	m := e.agg.snapshot()
	m.QueueDepth = int64(len(e.queue))
	m.ActiveWorkers = e.active.Load()
	return m
}

// Results 返回全部终态结果的副本（通常在 Shutdown 后调用）。
// 每个去重后的任务恰好出现一次，且与其完成顺序无关地可复现。
func (e *Engine) Results() []Result { return e.agg.results() }
