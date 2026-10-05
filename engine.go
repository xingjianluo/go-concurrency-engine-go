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

// Config 描述引擎参数：受控并发 + 有界缓冲 + 有限重试 + 幂等去重 + 分组隔离。
type Config struct {
	// Workers 为工作协程数（全局受控并发度）；<=0 时取 runtime.NumCPU()。
	Workers int
	// QueueSize 为全部分组共享的有界等待队列总容量；0 表示无缓冲
	// （仅当有工作协程空闲等待时才直接交接）。队列占满时 Submit 对调用方
	// 施加背压，绝不无界缓冲或静默丢弃。
	QueueSize int
	// Retry 为重试策略；零值等价于 NoRetry。
	Retry RetryPolicy
	// Deduplicate 为 true 时，相同 ID 的在途重复投递共享一次执行与一个结果。
	Deduplicate bool
	// GroupLimits 按分组名配置“同时在跑”的上限；未配置或 <=0 的分组
	// 不单独限流（仍受 Workers 的全局并发上限约束）。分组额度与全局并发
	// 叠加生效：任务被调度执行需同时满足两者。
	GroupLimits map[string]int
}

// slotWaiter 是一个因队列满而阻塞的提交方。空位释放时被“点名”唤醒
// （ch 关闭、woken 置位）；ctx 放弃的等待者标记 cancelled 后惰性剔除。
// 字段由 Engine.mu 保护。
type slotWaiter struct {
	ch        chan struct{}
	woken     bool
	cancelled bool
}

// inflight 表示一个已受理且至多执行一次的任务。终态结果（含因提交方
// 放弃受理而产生的受理撤销）写入后关闭 done，所有投递方共享同一裁决。
type inflight struct {
	t Task

	grp *group // 受理时确定的分组（去重共享时与首次投递一致）

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

	mu       sync.Mutex
	pending  map[string]*inflight
	closed   bool
	canceled bool // Cancel 已触发（影响提交方拿到的错误语义）

	// 分组调度状态（均由 mu 保护）：
	// 每组一个 FIFO 队列，queuedTotal 为各组排队数之和（全局有界缓冲）；
	// ready 为“有可调度任务”的分组轮转列表，工作协程按轮转顺序取任务，
	// 使持续猛灌的分组无法挤占其它分组的执行机会。
	groups         map[string]*group
	ready          []*group
	queuedTotal    int
	waitingWorkers int          // 正在空转等待任务的工作协程数（无缓冲交接依据）
	workCond       *sync.Cond   // 有新可调度任务或停止时 Signal/Broadcast
	waiters        []*slotWaiter // 因队列满而阻塞的提交方 FIFO（每个空位点名唤醒一个）

	// changeCh 为代际广播信道：仅在有人调用 DrainGroup 等待时维护，
	// 任务终态/组取消等状态变化时关闭并重建，唤醒排空等待方。
	changeCh      chan struct{}
	drainWaiters  int

	started atomic.Bool

	runCtx   context.Context // 派生给每个分组（再派生给每个任务）；Cancel 时取消
	cancelFn context.CancelFunc

	outstanding sync.WaitGroup // 已受理但未终态的任务数（排空依据）

	workerWg sync.WaitGroup
	stopped  bool // 队列排空后置位，工作协程据此退出（配合 workCond）

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
	e := &Engine{
		cfg:       cfg,
		agg:       newAggregator(),
		pending:   make(map[string]*inflight),
		groups:    make(map[string]*group),
		changeCh:  make(chan struct{}),
		runCtx:    runCtx,
		cancelFn:  cancel,
		stoppedCh: make(chan struct{}),
	}
	e.workCond = sync.NewCond(&e.mu)
	return e
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

// admit 完成校验、在途幂等去重、分组路由与有界入队（含背压）。
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
	g := e.getOrCreateLocked(t.Group)
	if g.closed {
		// 该分组已被取消或关闭受理：确定地拒绝，不影响其它分组。
		err := error(ErrGroupClosed)
		if g.canceled {
			err = context.Canceled
		}
		e.mu.Unlock()
		return nil, err
	}
	item := &inflight{t: t, grp: g, done: make(chan struct{})}
	if e.cfg.Deduplicate {
		e.pending[t.ID] = item
	}
	e.agg.addSubmitted(t.Group, 1)
	g.outstanding++
	e.mu.Unlock()

	// 先于入队计数：只要任务可能进入队列，outstanding 就必须 > 0，
	// 避免“排空完成 -> 计数器归零 -> 任务才入队”的竞态。
	e.outstanding.Add(1)

	// 有界入队：全局排队总量占满即对提交方形成背压。
	waitStart := time.Now()
	enqueued := false
	var waitErr error
	if nonblocking {
		e.mu.Lock()
		if len(e.waiters) == 0 && e.canEnqueueLocked() {
			e.enqueueLocked(item)
			enqueued = true
		}
		e.mu.Unlock()
	} else {
		// 等待者 FIFO：每个释放的空位只点名唤醒一个提交方，避免惊群；
		// runCtx.Done 在 Cancel 时关闭，确保关闭/取消能唤醒因队列满而
		// 阻塞的提交方，而不是让它们一直卡在背压上。
		e.mu.Lock()
		var w *slotWaiter
		for {
			if e.closed || g.closed {
				break
			}
			// 无等待者时可直接入队；被点名唤醒的提交方持有一个空位授予，
			// 不受此限（见下方 w.woken 分支）。
			if len(e.waiters) == 0 && e.canEnqueueLocked() {
				e.enqueueLocked(item)
				enqueued = true
				break
			}
			if w == nil {
				w = &slotWaiter{ch: make(chan struct{})}
				e.waiters = append(e.waiters, w)
			}
			e.mu.Unlock()
			select {
			case <-w.ch:
			case <-ctx.Done():
				waitErr = ctx.Err()
			case <-e.runCtx.Done():
			}
			e.mu.Lock()
			if waitErr != nil {
				break
			}
			if w.woken {
				// 被点名：本次空位授予属于我们，直接尝试入队；
				// 用不上（关闭/被抢）时把授予让给下一个等待者，绝不弄丢。
				w = nil
				if e.closed || g.closed {
					e.wakeNextLocked()
					break
				}
				if e.canEnqueueLocked() {
					e.enqueueLocked(item)
					enqueued = true
					break
				}
				e.wakeNextLocked()
			}
			// 否则是 runCtx 唤醒：回到循环重新判定 closed。
		}
		if w != nil {
			// 退出等待：未被点名的标记放弃（惰性剔除）；已被点名的
			// 把空位授予让给下一个等待者，绝不弄丢空位。
			if w.woken {
				e.wakeNextLocked()
			} else {
				w.cancelled = true
			}
		}
		e.mu.Unlock()
	}
	if waited := time.Since(waitStart); waited > 0 {
		e.agg.addBackpressure(waited)
	}

	closedNow := false
	canceledNow := false
	groupClosed := false
	groupCanceled := false
	e.mu.Lock()
	closedNow = e.closed
	canceledNow = e.canceled
	groupClosed = g.closed
	groupCanceled = g.canceled
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
		case groupCanceled:
			reason = context.Canceled
		case groupClosed:
			reason = ErrGroupClosed
		case waitErr != nil:
			reason = waitErr
		default:
			reason = ctx.Err()
		}
		item.abort(reason)
		e.agg.addSubmitted(t.Group, -1)
		e.mu.Lock()
		g.outstanding--
		e.broadcastChangeLocked()
		e.mu.Unlock()
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

// canEnqueueLocked 报告当前是否有全局队列空位。QueueSize=0 时等待中的
// 工作协程视为空位（无缓冲交接：提交方与空闲工作协程直接 rendezvous）；
// QueueSize>0 时排队总量严格不超过 QueueSize。调用方必须持有 e.mu。
func (e *Engine) canEnqueueLocked() bool {
	limit := e.cfg.QueueSize
	if limit == 0 {
		limit = e.waitingWorkers
	}
	return e.queuedTotal < limit
}

// enqueueLocked 把任务挂入其分组的 FIFO 队尾，并在该组有可运行额度时
// 将其加入就绪轮转列表、唤醒工作协程。调用方必须持有 e.mu。
func (e *Engine) enqueueLocked(item *inflight) {
	g := item.grp
	g.queue = append(g.queue, item)
	e.queuedTotal++
	e.makeReadyLocked(g)
}

// makeReadyLocked 在分组既有排队任务又有可运行额度时，把它加入就绪轮转
// 列表（若尚未在列）并唤醒一个工作协程。调用方必须持有 e.mu。
func (e *Engine) makeReadyLocked(g *group) {
	if g.canceled || len(g.queue) == 0 || g.inReady {
		return
	}
	if g.limit > 0 && g.running >= g.limit {
		return
	}
	g.inReady = true
	e.ready = append(e.ready, g)
	e.workCond.Signal()
}

// wakeNextLocked 把下一个未放弃的等待提交方点名唤醒（FIFO）。
// 调用方必须持有 e.mu。
func (e *Engine) wakeNextLocked() {
	for len(e.waiters) > 0 {
		w := e.waiters[0]
		e.waiters = e.waiters[1:]
		if w.cancelled {
			continue
		}
		w.woken = true
		close(w.ch)
		return
	}
}

// wakeAllWaitersLocked 唤醒全部等待提交方（关闭/取消/组状态变化时使用）。
// 调用方必须持有 e.mu。
func (e *Engine) wakeAllWaitersLocked() {
	for len(e.waiters) > 0 {
		e.wakeNextLocked()
	}
}

// broadcastChangeLocked 唤醒所有 DrainGroup 等待方；没有等待者时不做事
// （避免热路径上的信道分配）。调用方必须持有 e.mu。
func (e *Engine) broadcastChangeLocked() {
	if e.drainWaiters == 0 {
		return
	}
	close(e.changeCh)
	e.changeCh = make(chan struct{})
}

// worker 是固定大小工作协程池中的一员。
func (e *Engine) worker() {
	defer e.workerWg.Done()
	for {
		e.mu.Lock()
		var item *inflight
		for {
			if item = e.pickLocked(); item != nil {
				break
			}
			if e.stopped {
				e.mu.Unlock()
				return
			}
			// 空闲等待：登记为可交接空位并点名唤醒一个等待提交方，
			// 随后挂起直到有新任务。
			e.waitingWorkers++
			e.wakeNextLocked()
			e.workCond.Wait()
			e.waitingWorkers--
		}
		e.mu.Unlock()

		e.active.Add(1)
		e.process(item)
		e.active.Add(-1)

		e.mu.Lock()
		g := item.grp
		g.running--
		g.outstanding--
		e.makeReadyLocked(g)      // 组额度释放：若仍有排队任务则重新参与轮转
		e.broadcastChangeLocked() // 可能有 DrainGroup 在等待该组排空
		e.mu.Unlock()
		e.outstanding.Done()
	}
}

// pickLocked 按轮转顺序从就绪分组中取出一个可调度任务：每取一个就把该组
// 排到就绪列表队尾，使持续猛灌的分组与其它分组交替获得执行机会。
// 取走任务会释放一个全局队列空位并广播给等待空位的提交方。
// 没有可调度任务时返回 nil。调用方必须持有 e.mu。
func (e *Engine) pickLocked() *inflight {
	for len(e.ready) > 0 {
		g := e.ready[0]
		e.ready = e.ready[1:]
		g.inReady = false
		if g.canceled || len(g.queue) == 0 {
			continue // 已取消/已排空的分组：惰性剔除
		}
		if g.limit > 0 && g.running >= g.limit {
			continue // 组额度占满：本轮不参与调度，待 running 下降后重新就绪
		}
		item := g.queue[0]
		g.queue = g.queue[1:]
		g.running++
		e.queuedTotal--
		e.wakeNextLocked() // 释放一个全局队列空位：点名唤醒一个等待提交方
		e.makeReadyLocked(g)
		return item
	}
	return nil
}

// process 执行单个任务：有限重试 + 退避 + 分类终态，且只终态化一次。
// 任务的生命周期绑定其分组 ctx：引擎 Cancel 会级联取消所有分组，
// CancelGroup 只取消该组，二者都使在途任务收到取消、退避立即结束。
func (e *Engine) process(item *inflight) {
	start := time.Now()
	maxAttempts := e.cfg.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	gctx := item.grp.ctx

	// 取消发生在任务开始之前：给出确定的取消裁决（未执行，Attempts=0）。
	if err := gctx.Err(); err != nil {
		e.finalize(item, Result{
			TaskID: item.t.ID, Group: item.grp.name, Class: ClassCanceled, Attempts: 0,
			Duration: time.Since(start), Err: err,
		})
		return
	}

	var lastErr error
	class := ClassUnknown
	attempts := 0
	for attempts < maxAttempts {
		attempts++
		err := item.t.Fn(gctx)
		lastErr = err
		class = Classify(err)

		// 成功、永久失败、取消：立即终态，不再重试。
		if class != ClassTemporary {
			break
		}
		// 引擎/分组已取消：不再退避/重试，统一裁决为取消。
		if gctx.Err() != nil {
			class = ClassCanceled
			lastErr = gctx.Err()
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
			case <-gctx.Done():
				timer.Stop()
				e.finalize(item, Result{
					TaskID: item.t.ID, Group: item.grp.name, Class: ClassCanceled, Attempts: attempts,
					Duration: time.Since(start), Err: gctx.Err(),
				})
				return
			}
		}
	}
	e.finalize(item, Result{
		TaskID: item.t.ID, Group: item.grp.name, Class: class, Attempts: attempts,
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
		e.wakeAllWaitersLocked() // 唤醒因队列满阻塞的提交方，让它们得到确定拒绝
		e.mu.Unlock()

		go func() {
			// 等待所有已受理任务拿到终态（取消时也会快速得到取消裁决）。
			e.outstanding.Wait()
			// 排空后再取消，确保退避中的任务也被纳入排空等待。
			e.cancelFn()
			e.mu.Lock()
			e.stopped = true
			e.workCond.Broadcast()
			e.mu.Unlock()
			e.workerWg.Wait()
			close(e.stoppedCh)
		}()
	})
}

// Snapshot 返回一致的指标快照（含队列深度与在执行数等瞬时量表）。
func (e *Engine) Snapshot() Metrics {
	m := e.agg.snapshot()
	e.mu.Lock()
	m.QueueDepth = int64(e.queuedTotal)
	e.mu.Unlock()
	m.ActiveWorkers = e.active.Load()
	return m
}

// Results 返回全部终态结果的副本（通常在 Shutdown 后调用）。
// 每个去重后的任务恰好出现一次，且与其完成顺序无关地可复现。
func (e *Engine) Results() []Result { return e.agg.results() }
