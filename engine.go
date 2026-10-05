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
	// ErrUnknownGroup 表示查询/操作了一个从未出现过且未配置的分组。
	ErrUnknownGroup = errors.New("engine: unknown group")
	// ErrGroupClosed 表示该来源分组已排空关闭，停止受理新任务。
	ErrGroupClosed = errors.New("engine: group closed")
	// ErrGroupCanceled 表示该来源分组已被单独取消，新提交被拒绝。
	ErrGroupCanceled = errors.New("engine: group canceled")
)

// Config 描述引擎参数：受控并发 + 有界缓冲 + 有限重试 + 幂等去重 + 分组隔离。
type Config struct {
	// Workers 为工作协程数（全局受控并发度）；<=0 时取 runtime.NumCPU()。
	Workers int
	// QueueSize 为全局有界等待队列容量；0 表示无缓冲（满一个工作协程才不阻塞）。
	// 队列占满时 Submit 对调用方施加背压，绝不无界缓冲或静默丢弃。
	QueueSize int
	// Retry 为重试策略；零值等价于 NoRetry。
	Retry RetryPolicy
	// Deduplicate 为 true 时，相同 ID 的在途重复投递共享一次执行与一个结果。
	Deduplicate bool
	// GroupLimits 为各来源分组单独配置“同时在跑（含退避等待）”的任务数上限。
	// 键为 Task.Source；空字符串键配置默认分组。未配置的分组仅受 Workers
	// 这一全局并发上限约束。全局上限对所有分组同时生效（取两者更紧者）。
	GroupLimits map[string]int
}

// inflight 表示一个已受理且至多执行一次的任务。终态结果（含因提交方
// 放弃受理而产生的受理撤销）写入后关闭 done，所有投递方共享同一裁决。
type inflight struct {
	t Task

	// group 由调度器在受理时设置；任务执行使用该分组的 ctx，
	// 分组取消只影响本分组，引擎整体取消则影响全部分组。
	group *groupState

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

// drainGate 是 sync.WaitGroup 的替代：允许 Add/Done 与“等待归零”由不同
// goroutine 并发调用而不触发 data race（sync.WaitGroup 文档不保证这种并发）。
// 内部用互斥保护计数与归零信号通道；归零恰好广播一次。
type drainGate struct {
	mu   sync.Mutex
	n    int64
	zero chan struct{} // 非 nil 表示已有等待者登记
}

var alreadyZero = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// add 调整计数；归零且有等待者登记时，关闭其信号通道。
func (d *drainGate) add(delta int64) {
	d.mu.Lock()
	d.n += delta
	if d.n == 0 && d.zero != nil {
		close(d.zero)
		d.zero = nil
	}
	d.mu.Unlock()
}

// waitCh 返回一个在计数归零时关闭的通道（已归零时立即关闭）。
func (d *drainGate) waitCh() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.n == 0 {
		return alreadyZero
	}
	if d.zero == nil {
		d.zero = make(chan struct{})
	}
	return d.zero
}

// Engine 是单进程本地并发任务执行引擎。零值不可用，须经 New + Start。
type Engine struct {
	cfg Config
	agg *aggregator

	// sched 为分组调度器：集中持有各路有界队列、受理等待队列与轮转派发逻辑。
	sched *scheduler

	mu       sync.Mutex
	pending  map[string]*inflight
	closed   bool
	canceled bool // Cancel 已触发（影响提交方拿到的错误语义）

	started atomic.Bool

	runCtx   context.Context // 全部分组 ctx 的父上下文；引擎 Cancel 时取消
	cancelFn context.CancelFunc

	outstanding drainGate // 已受理但未终态的任务数（整体排空依据；并发安全）

	workerWg   sync.WaitGroup
	dispatchCh chan *inflight // 调度器 -> 工作协程（容量 Workers，派发永不阻塞）
	stopCh     chan struct{}  // 队列排空后关闭，工作协程与调度器据此退出

	active     atomic.Int64 // 正在执行（含退避）的任务数（工作协程视角）
	queueDepth atomic.Int64 // 已受理、等待派发的任务总数（调度器在稳态点发布）

	shutdownOnce sync.Once
	stoppedCh    chan struct{} // 所有工作协程退出后关闭

	// abortWaitCh 为合并信号：任一提交方在背压等待中放弃（其 ctx 结束）时
	// 往这里非阻塞发一个信号，调度器醒来后统一摘除已放弃的等待者。
	abortWaitCh chan struct{}
}

// canceledByCaller 报告引擎是否处于取消式关闭（决定提交方拿到的错误语义）。
func (e *Engine) canceledByCaller() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.canceled
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
		cfg:         cfg,
		agg:         newAggregator(),
		pending:     make(map[string]*inflight),
		runCtx:      runCtx,
		cancelFn:    cancel,
		dispatchCh:  make(chan *inflight, cfg.Workers),
		stopCh:      make(chan struct{}),
		stoppedCh:   make(chan struct{}),
		abortWaitCh: make(chan struct{}, 1),
	}
	e.sched = newScheduler(e)
	return e
}

// Start 启动调度器与固定数量的工作协程。可重复调用，仅首次生效。
func (e *Engine) Start() {
	if !e.started.CompareAndSwap(false, true) {
		return
	}
	e.sched.ensureDefaultGroups()
	go e.sched.run()
	e.workerWg.Add(e.cfg.Workers)
	for i := 0; i < e.cfg.Workers; i++ {
		go e.worker()
	}
}

// Submit 提交任务并阻塞至该任务终态，返回其幂等结果。
//
// 分组：Task.Source 标出任务来自哪一路来源；空字符串为默认分组。
// 各路单独的并发上限（Config.GroupLimits）与全局上限 Workers 同时生效。
//
// 背压：全局有界缓冲占满且没有空闲执行位时阻塞，直到有空位；提交方可通过
// ctx 放弃等待，此时任务尚未受理（不会执行、不计入结果）。
// 幂等：开启 Deduplicate 时，相同 ID 且仍在途的重复投递共享同一次执行，
// 等待并拿到同一个 Result，该 ID 只统计一次。
// 取消/关闭：引擎进入关闭后提交返回 ErrEngineClosed；Cancel 后返回
// context.Canceled；某一分组单独取消/排空后，向该路提交分别返回
// ErrGroupCanceled / ErrGroupClosed，其它路不受影响。
func (e *Engine) Submit(ctx context.Context, t Task) (Result, error) {
	item, err := e.admit(ctx, t, false)
	if err != nil {
		return Result{}, err
	}
	return item.awaitCtx(ctx)
}

// TrySubmit 非阻塞尝试提交：无缓冲/执行空位时 accepted=false（err 为 nil），
// 调用方可据此自行降载，而不会被静默丢弃。
func (e *Engine) TrySubmit(t Task) (result Result, accepted bool, err error) {
	item, err := e.admit(context.Background(), t, true)
	if err != nil {
		return Result{}, false, err
	}
	if item == nil {
		return Result{}, false, nil // 无空位：非阻塞拒绝，由调用方降载
	}
	res, err := item.await()
	return res, true, err
}

// admit 完成校验、在途幂等去重与有界受理（含背压）。
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
			e.agg.addDuplicateFor(t.Source)
			return existing, nil
		}
	}
	item := &inflight{t: t, done: make(chan struct{})}
	if e.cfg.Deduplicate {
		e.pending[t.ID] = item
	}
	e.agg.addSubmitted(t.Source, 1)
	e.mu.Unlock()

	// outstanding 只在调度器“确认受理”的那一刻 Add（调度器串行且先于派发），
	// 未受理任务从不计数，因此不存在回滚 Add(-1) 与整体排空 Wait() 并发的竞态。

	// 有界受理：缓冲满且无执行空位即对提交方形成背压（由调度器统一裁决）。
	waitStart := time.Now()
	ok, reason := e.sched.requestAdmit(ctx, item, nonblocking)
	if waited := time.Since(waitStart); waited > 0 {
		e.agg.addBackpressure(t.Source, waited)
	}

	if !ok {
		// 未受理（任务从未被调度器收下）：回滚受理计数，并把该条目标记为
		// “受理撤销”，唤醒可能已共享此条目的重复投递方（它们拿到同样的失败
		// 原因，绝不永久挂起）。
		item.abort(reason)
		e.agg.addSubmitted(t.Source, -1)
		e.mu.Lock()
		if nonblocking {
			// TrySubmit 的非阻塞拒绝：撤销条目不长期占用 pending，交给 GC；
			// 同一批次中抢到该条目的并发副本已由 abort 唤醒。
			if e.pending[t.ID] == item {
				delete(e.pending, t.ID)
			}
		}
		e.mu.Unlock()
		if nonblocking && reason == ErrQueueFull {
			return nil, nil
		}
		return nil, reason
	}

	return item, nil
}

// worker 是固定大小工作协程池中的一员。
//
// 退出规则：只在循环顶部确认 stopCh 已关闭且没有已派发任务待领取时退出；
// 一旦领取了任务，就必须执行到终态并把 finish 信号无条件送达调度器——
// 绝不能因 stopCh 关闭而丢弃一个已受理任务（否则它永远拿不到终态、
// 计数对不上，整体排空也会卡住）。
func (e *Engine) worker() {
	defer e.workerWg.Done()
	for {
		select {
		case <-e.stopCh:
			// 排空完成后 stopCh 才关闭，此刻不会再有新派发；排空通道里可能
			// 残留的极少数已派发项也要先消费干净。
			select {
			case item := <-e.dispatchCh:
				e.runOne(item)
				continue
			default:
				return
			}
		case item := <-e.dispatchCh:
			e.runOne(item)
		}
	}
}

// runOne 执行一个已派发任务并归还全部计数、送达 finish 信号（不可中断）。
func (e *Engine) runOne(item *inflight) {
	e.active.Add(1)
	e.process(item)
	e.active.Add(-1)
	// 终态后归还分组/整体在途计数。
	item.group.wg.Done()
	e.outstanding.add(-1)
	// finish 必须无条件送达：finishCh 容量为 Workers，且每个完成都对应先前
	// 一次派发占用的执行位，调度器必然持续取走，故此发送不会阻塞。
	e.sched.finishCh <- item
}

// process 执行单个任务：有限重试 + 退避 + 分类终态，且只终态化一次。
// 执行 ctx 取自任务所属分组：分组单独取消或引擎整体取消都会令其结束。
func (e *Engine) process(item *inflight) {
	ctx := item.group.ctx
	start := time.Now()
	maxAttempts := e.cfg.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	// 取消发生在任务开始之前：给出确定的取消裁决（未执行，Attempts=0）。
	if err := ctx.Err(); err != nil {
		e.finalize(item, Result{
			TaskID: item.t.ID, Source: item.t.Source, Class: ClassCanceled,
			Attempts: 0, Duration: time.Since(start), Err: err,
		})
		return
	}

	var lastErr error
	class := ClassUnknown
	attempts := 0
	for attempts < maxAttempts {
		attempts++
		err := item.t.Fn(ctx)
		lastErr = err
		class = Classify(err)

		// 成功、永久失败、取消：立即终态，不再重试。
		if class != ClassTemporary {
			break
		}
		// 分组/引擎已取消：不再退避/重试，统一裁决为取消。
		if ctx.Err() != nil {
			class = ClassCanceled
			lastErr = ctx.Err()
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
			case <-ctx.Done():
				timer.Stop()
				e.finalize(item, Result{
					TaskID: item.t.ID, Source: item.t.Source, Class: ClassCanceled,
					Attempts: attempts, Duration: time.Since(start), Err: ctx.Err(),
				})
				return
			}
		}
	}
	e.finalize(item, Result{
		TaskID: item.t.ID, Source: item.t.Source, Class: class, Attempts: attempts,
		Duration: time.Since(start), Err: lastErr,
	})
}

// finalize 写入唯一终态结果、计入聚合，再唤醒所有等待者。
//
// 顺序刻意为“先 recordResult，后 close(done)”：任何从 await/awaitCtx 醒来的
// 提交方随后 Snapshot，都必然能观察到本任务的终态计数，杜绝“拿到了结果、
// 汇总里却还没有它”的不一致；整体排空（outstanding 归零）发生在 finalize 之后，
// 因而 Shutdown 返回时计数同样必定落库。
func (e *Engine) finalize(item *inflight, res Result) {
	item.mu.Lock()
	if item.finished {
		// 理论不可达：每个 inflight 只在一个地方被终态化（工作协程或调度器取消队列）。
		item.mu.Unlock()
		return
	}
	item.result = res
	item.finished = true
	item.mu.Unlock()

	e.agg.recordResult(res)
	close(item.done)
	// 刻意不从 pending 删除：终态条目保留下来，使晚到的相同 ID 投递
	// 复用同一结果，保证全局幂等、不重复执行、不重复累计。
}

// Cancel 触发取消并开始关闭：
//   - 全部分组在途任务的 ctx 立即取消，正在进行的退避立即结束；
//   - 已入队未开始的任务获得确定的取消裁决（不丢弃、不执行）；
//   - 之后的提交返回 context.Canceled。
func (e *Engine) Cancel() {
	e.mu.Lock()
	e.canceled = true
	e.closed = true
	e.mu.Unlock()
	e.cancelFn()
	e.sched.closeAll(true)
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

// beginShutdown 幂等地启动“拒绝提交 -> 通知调度器 -> 排空 -> 停止工作协程”的流程。
func (e *Engine) beginShutdown() {
	e.shutdownOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.mu.Unlock()
		e.sched.closeAll(false) // 排空而非取消；等待中的提交方收到 ErrEngineClosed

		go func() {
			// 等待所有已受理任务拿到终态（取消时也会快速得到取消裁决）。
			<-e.outstanding.waitCh()
			// 排空后再关闭执行通道，确保退避中的任务也被纳入排空等待。
			close(e.stopCh)
			e.workerWg.Wait()
			close(e.stoppedCh)
		}()
	})
}

// CancelGroup 只取消指定来源分组：
//   - 本路在途任务的 ctx 立即取消，正在进行的退避立即结束；
//   - 本路已受理、排队中的任务得到确定的 ClassCanceled 裁决（不再重试、
//     不重复计数、不丢弃）；
//   - 本路停止受理新任务（后续提交返回 ErrGroupCanceled）；
//   - 其它路不受影响，可继续正常提交与执行。
//
// 调用会阻塞直到本路所有已受理任务拿到确定终态。
func (e *Engine) CancelGroup(source string) error {
	if !e.started.Load() {
		return ErrNotStarted
	}
	if err := e.sched.requestCloseGroup(source, true); err != nil {
		return err
	}
	return e.sched.waitGroupDrained(context.Background(), source)
}

// ShutdownGroup 只排空关闭指定来源分组：停止受理本路新任务（后续提交返回
// ErrGroupClosed），本路已受理任务照常执行到终态（不取消、不重试语义不变），
// 其它路继续正常工作。ctx 到期仍未排空时返回 ctx.Err()，但不强制取消本路。
func (e *Engine) ShutdownGroup(ctx context.Context, source string) error {
	if !e.started.Load() {
		return ErrNotStarted
	}
	if err := e.sched.requestCloseGroup(source, false); err != nil {
		return err
	}
	return e.sched.waitGroupDrained(ctx, source)
}

// GroupSnapshot 返回指定来源分组的一致指标快照（累计计数 + 本路瞬时量表）。
// 分组从未出现且未在 Config.GroupLimits 中配置时返回 ErrUnknownGroup。
func (e *Engine) GroupSnapshot(source string) (Metrics, error) {
	m, aggOK := e.agg.groupSnapshot(source)
	queued, active, gaugeOK := e.sched.groupSnapshotGauges(source)
	if !aggOK && !gaugeOK {
		return Metrics{}, ErrUnknownGroup
	}
	m.QueueDepth = queued
	m.ActiveWorkers = active
	return m, nil
}

// Groups 返回当前已知的全部分组名（含已关闭分组；副本，便于遍历核查）。
func (e *Engine) Groups() []string {
	names := e.sched.knownGroups()
	have := make(map[string]struct{}, len(names))
	for _, n := range names {
		have[n] = struct{}{}
	}
	for _, n := range e.agg.groupNames() {
		if _, ok := have[n]; !ok {
			names = append(names, n)
		}
	}
	return names
}

// Snapshot 返回一致的全局指标快照（含队列深度与在执行数等瞬时量表）。
func (e *Engine) Snapshot() Metrics {
	m := e.agg.snapshot()
	m.QueueDepth = e.queueDepth.Load()
	m.ActiveWorkers = e.active.Load()
	return m
}

// Results 返回全部终态结果的副本（通常在 Shutdown 后调用）。
// 每个去重后的任务恰好出现一次，且与其完成顺序无关地可复现。
func (e *Engine) Results() []Result { return e.agg.results() }
