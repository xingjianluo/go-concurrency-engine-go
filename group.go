package engine

import (
	"context"
	"sync"
	"sync/atomic"
)

// 本文件实现“按来源分组隔离”的调度器：
//   - 每路来源单独配置同时在跑的上限（groupState.limit），全局并发上限
//     （Workers）同时生效，两者取更紧者；
//   - 调度在单个调度 goroutine 中按分组轮转（round-robin）选择下一个执行任务，
//     持续过载时任一路都不会被无限期饿死；
//   - 受理（入队）也集中在调度器内，全引擎已缓冲任务数严格有界（稳态
//     <= QueueSize；QueueSize=0 时不缓冲、需执行位直接会合），满了就在提交方
//     处阻塞背压，不靠无界堆积换取公平；
//   - 支持只对某一路做取消（CancelGroup）或排空关闭（ShutdownGroup），
//     不影响其它路继续提交与执行。
//
// 调度器是唯一的“队列所有者 + 派发者”：工作协程池只从 dispatchCh 领取已派发
// 的任务并执行，完成后通过 finishCh 归还执行位与缓冲位。

// groupState 是单路来源的运行时状态。
//
// 同步约定：queued/active/draining 等逻辑字段仅由调度器 goroutine 读写；
// queuedN/activeN 是供外部快照读取的原子镜像，由调度器在同一处更新；
// ctx/cancel/done 是可被其它 goroutine 观察/等待的同步原语。
type groupState struct {
	source string
	limit  int // 同时在跑（含退避）上限；<=0 表示本路不单独限流

	ctx    context.Context // 派生给本路任务；本路取消或引擎取消时结束
	cancel context.CancelFunc

	// wg 计本路“已受理但未终态”的任务数，本路排空（ShutdownGroup/CancelGroup
	// 并处理完所有在途）以它归零为准。Add 在调度器 goroutine，Done 在终态处。
	wg sync.WaitGroup

	queued []*inflight // 已受理、等待派发的任务（FIFO）
	active int         // 已派发、正在执行（含退避）的数量（调度器视角）

	queuedN atomic.Int64 // len(queued) 的原子镜像（外部快照用）
	activeN atomic.Int64 // active 的原子镜像（外部快照用）

	draining bool          // 本路已停止受理新提交（排空或取消）
	canceled bool          // 本路是否为取消式关闭
	done     chan struct{} // 本路已受理任务全部终态后关闭
	doneOnce sync.Once
}

// markDone 在本路“停止受理且已受理任务全部终态”时关闭 done，恰好一次。
func (g *groupState) markDone() {
	g.doneOnce.Do(func() { close(g.done) })
}

// admitReq 是提交方发给调度器的受理请求。
type admitReq struct {
	item        *inflight
	source      string
	nonblocking bool

	// aborted 在提交方于背压等待期间放弃（其 ctx 结束）时关闭；调度器在受理
	// 前先检查它，确保已放弃的任务绝不会被受理/执行。
	aborted chan struct{}
	reply   chan admitReply
}

// admitReply 是调度器对受理请求的裁决。ok=true 表示任务已受理（已计入有界缓冲）。
type admitReply struct {
	ok  bool
	err error
}

// groupEvent 是分组生命周期事件：排空关闭（cancel=false）或取消（cancel=true）。
type groupEvent struct {
	source string
	cancel bool
	reply  chan error
}

// scheduler 集中持有分组队列与等待队列，并运行唯一的调度循环。
type scheduler struct {
	e *Engine

	submitCh    chan admitReq
	finishCh    chan *inflight
	eventCh     chan groupEvent
	closeSig    chan bool // 引擎整体关闭信号（唤醒调度器）
	abortWaitCh chan struct{}

	// wantCancel 记录“本次整体关闭应按取消处理”。即使排空(false)信号已占用
	// closeSig 单元素槽、随后的取消(true)信号无法入队，该标记也能让调度器在
	// 消费到排空信号时把本次关闭升级为取消，避免“先 Shutdown 紧接 Cancel”
	// 的窄竞态丢失取消语义。
	wantCancel atomic.Bool

	// gmu 保护 groups 与 groupOrder（调度器写、外部 goroutine 通过
	// knownGroups/lookupGroup 读）。分组内部状态不在此锁保护范围内。
	gmu         sync.RWMutex
	groups      map[string]*groupState
	groupOrder  []string // 分组首次出现的顺序，供轮转与稳定遍历
	waiters     []*admitReq
	queuedTotal int // 所有分组已缓冲任务总数（稳态恒 <= QueueSize）
	rrIdx       int // 分组轮转游标
	closedAll   bool
}

func newScheduler(e *Engine) *scheduler {
	return &scheduler{
		e:           e,
		submitCh:    make(chan admitReq),
		finishCh:    make(chan *inflight, e.cfg.Workers+1),
		eventCh:     make(chan groupEvent, 8),
		closeSig:    make(chan bool, 1),
		abortWaitCh: e.abortWaitCh,
		groups:      make(map[string]*groupState),
	}
}

// ensureDefaultGroups 在启动时预建默认分组与 GroupLimits 中配置的分组，
// 使 GroupSnapshot 对“配置了但还没来活”的分组也可观测。
func (s *scheduler) ensureDefaultGroups() {
	s.getOrCreate("") // 默认分组始终存在（空 Source 的旧用法照常工作）
	for src := range s.e.cfg.GroupLimits {
		if src != "" {
			s.getOrCreate(src)
		}
	}
}

// getOrCreate 取或建分组；仅在调度器 goroutine 中调用。
func (s *scheduler) getOrCreate(source string) *groupState {
	s.gmu.Lock()
	defer s.gmu.Unlock()
	if g, ok := s.groups[source]; ok {
		return g
	}
	ctx, cancel := context.WithCancel(s.e.runCtx)
	g := &groupState{
		source: source,
		limit:  s.e.cfg.GroupLimits[source], // 未配置 => 0（本路不限流，仅受全局上限）
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.groups[source] = g
	s.groupOrder = append(s.groupOrder, source)
	return g
}

// lookupGroup 只读查找分组（外部 goroutine 用）。
func (s *scheduler) lookupGroup(source string) (*groupState, bool) {
	s.gmu.RLock()
	defer s.gmu.RUnlock()
	g, ok := s.groups[source]
	return g, ok
}

// knownGroups 返回已知分组名快照（供 Engine.Groups 合并使用）。
func (s *scheduler) knownGroups() []string {
	s.gmu.RLock()
	defer s.gmu.RUnlock()
	out := make([]string, len(s.groupOrder))
	copy(out, s.groupOrder)
	return out
}

// groupSnapshotGauges 返回某路瞬时量表（队列深度/在执行数）。
func (s *scheduler) groupSnapshotGauges(source string) (queued, active int64, ok bool) {
	g, exists := s.lookupGroup(source)
	if !exists {
		return 0, 0, false
	}
	return g.queuedN.Load(), g.activeN.Load(), true
}

// run 是调度器主循环。所有分组队列、等待队列与并发计数的修改都在此串行进行。
func (s *scheduler) run() {
	for {
		select {
		case <-s.e.stopCh:
			return
		case canceled := <-s.closeSig:
			// 合并“先排空后取消”的升级：槽里是排空信号但 Cancel 已置位时按取消处理。
			s.handleCloseAll(canceled || s.wantCancel.Load())
			s.wantCancel.Store(false)
		case req := <-s.submitCh:
			s.handleSubmit(req)
		case item := <-s.finishCh:
			s.handleFinish(item)
		case ev := <-s.eventCh:
			s.handleGroupEvent(ev)
		case <-s.abortWaitCh:
			// 有提交方在背压等待中放弃：摘除后重新尝试受理/派发。
			s.purgeAbortedWaiters()
			s.pump()
		}
	}
}

// publishGauges 把当前队列深度发布到引擎的原子量表（仅在迭代之间的稳态点调用，
// 快照观测者因此不会看到中间瞬态值）。
func (s *scheduler) publishGauges() {
	s.e.queueDepth.Store(int64(s.queuedTotal))
}

// globalActive 返回当前全局在执行数（含退避）。
func (s *scheduler) globalActive() int {
	n := 0
	for _, g := range s.groups {
		n += g.active
	}
	return n
}

// canRun 判断某路当前是否有资格占用下一个执行位：
// 有排队任务、未触本路并发上限、全局并发上限也未满。
func (s *scheduler) canRun(g *groupState) bool {
	if len(g.queued) == 0 {
		return false
	}
	if g.limit > 0 && g.active >= g.limit {
		return false
	}
	return s.globalActive() < s.e.cfg.Workers
}

// dispatchTarget 描述一次按分组轮转选中的派发动作。
type dispatchTarget struct {
	g    *groupState
	req  *admitReq // 非 nil 表示来自等待队列、需会合直接派发；nil 表示派发本路缓冲队首
	wIdx int       // req 非 nil 时在 waiters 中的下标
}

// nextDispatchTarget 按分组轮转选出下一个可派发的动作。调用前已确认存在全局
// 空闲执行位。轮转规则是公平性的核心：
//
// 从游标 rrIdx 起逐组考察——
//   - 本路已触并发上限：跳过（执行位让给别路，本路排队等待）；
//   - 本路缓冲有任务：派发缓冲队首；
//   - 缓冲为空但本路有等待者：直接会合派发该等待者（因此猛灌的一路即便把
//     缓冲占满，别路等待者在轮到时仍能拿到执行位，不会被饿死）；
//   - 本路既无缓冲也无等待：跳过，但不因此把游标永久停在忙组——找到一个
//     真正可派发的分组后才推进游标到其后一位。
//
// 返回 nil 表示当前没有任何分组能使用该执行位（全部触顶或无活）。
func (s *scheduler) nextDispatchTarget() *dispatchTarget {
	n := len(s.groupOrder)
	for i := 0; i < n; i++ {
		idx := (s.rrIdx + i) % n
		g := s.groups[s.groupOrder[idx]]
		if g.limit > 0 && g.active >= g.limit {
			continue
		}
		if len(g.queued) > 0 {
			return &dispatchTarget{g: g}
		}
		// 缓冲空：找本路第一个仍在等待的提交方。
		for j, req := range s.waiters {
			if req.source == g.source && !isAborted(req) {
				return &dispatchTarget{g: g, req: req, wIdx: j}
			}
		}
	}
	return nil
}

// dispatch 弹出队首并派发给工作协程，并更新本路/全局计数镜像。
// 调用前已确认存在空闲执行位（全局在执行数 < Workers），dispatchCh 容量为
// Workers，故发送不会阻塞（每个完成事件都对应一次先前的接收，槽位严格配对）。
func (s *scheduler) dispatch(g *groupState) {
	item := g.queued[0]
	g.queued = g.queued[1:]
	g.queuedN.Add(-1)
	g.active++
	g.activeN.Add(1)
	s.queuedTotal--
	item.group = g
	s.e.dispatchCh <- item
}

// sendReply 向提交方会合式交付裁决：发送方在提交方真正接收时才完成；若提交方
// 已放弃（aborted 关闭）则立即失败返回 false，调用方必须回滚本次受理。
// 无缓冲通道 + 提交方 select 的接收是原子提交，因此不存在“裁决已受理、提交方
// 却按 ctx 结束返回”的二义性，也不会出现 ok=false 且 err=nil 的半截结论。
// 非阻塞请求（TrySubmit）不设放弃通道（aborted 为 nil，永不关闭），
// 而调用方始终在等待裁决，故发送必然成功。
func sendReply(req *admitReq, rep admitReply) bool {
	select {
	case req.reply <- rep:
		return true
	case <-req.aborted:
		return false
	}
}

// admitQueued 把一个已确认受理的请求放入本路缓冲。会合失败（提交方已放弃）时
// 返回 false，且不产生任何计数/排队副作用。
func (s *scheduler) admitQueued(g *groupState, req *admitReq) bool {
	if !sendReply(req, admitReply{ok: true}) {
		return false
	}
	g.queued = append(g.queued, req.item)
	g.queuedN.Add(1)
	req.item.group = g
	s.queuedTotal++
	g.wg.Add(1)
	s.e.outstanding.add(1) // 受理确认点：先计数（在派发之前），供整体排空等待
	return true
}

// admitAndDispatchDirect 不经过缓冲，把一个等待者直接会合派发给工作协程
// （QueueSize=0，或缓冲已满但恰好腾出执行位时的路径）。会合失败（提交方已
// 放弃）或当前无资格（本路触顶/全局满）时返回 false，且不产生任何副作用。
func (s *scheduler) admitAndDispatchDirect(g *groupState, req *admitReq) bool {
	if s.globalActive() >= s.e.cfg.Workers {
		return false
	}
	if g.limit > 0 && g.active >= g.limit {
		return false
	}
	if !sendReply(req, admitReply{ok: true}) {
		return false // 提交方已放弃：不受理、不派发
	}
	// 队首占位后立即由 dispatch 弹出：queuedTotal/queuedN 净变化为 0，
	// 该任务不占用缓冲额度。
	g.queued = append(g.queued, req.item)
	g.queuedN.Add(1)
	s.queuedTotal++
	g.wg.Add(1)
	s.e.outstanding.add(1) // 受理确认点：直接派发也先计数
	req.item.group = g
	s.dispatch(g)
	return true
}

// removeWaiter 删除指定下标的等待者并返回它。
func (s *scheduler) removeWaiter(idx int) *admitReq {
	req := s.waiters[idx]
	s.waiters = append(s.waiters[:idx], s.waiters[idx+1:]...)
	return req
}

// isAborted 报告等待者的提交方是否已放弃。
func isAborted(req *admitReq) bool {
	select {
	case <-req.aborted:
		return true
	default:
		return false
	}
}

// pump 反复推进“把等待者装入缓冲 / 按分组轮转派发执行”，直到本轮再无进展，
// 然后发布稳态量表。
//
// 每轮：
//  1. 缓冲有空位：FIFO 受理队首等待者入其分组缓冲（提升吞吐：缓冲里的任务
//     不必会合即可派发）；公平性由第 2 步的分组轮转保证，与本步的 FIFO 无关。
//  2. 有全局空闲执行位：按分组轮转选取下一个动作（见 nextDispatchTarget）：
//     缓冲任务直接派发；缓冲为空的分组其等待者会合直接派发。
//
// 这样在持续过载、某一路猛灌时：缓冲里可能全是该路任务，但每轮到别路，
// 只要别路还有等待者，就会直接派发别路的一个任务，别路不会被无限期饿死；
// 某一路配了单独上限触顶后被跳过，执行位留给其它路，本路任务排队等待。
func (s *scheduler) pump() {
	progress := true
	for progress {
		progress = false

		// 1) 缓冲有空位：FIFO 受理队首等待者入缓冲。
		if len(s.waiters) > 0 && s.queuedTotal < s.e.cfg.QueueSize {
			req := s.waiters[0]
			s.waiters = s.waiters[1:]
			if s.admitQueued(s.getOrCreate(req.source), req) {
				progress = true
			}
			// 会合失败仅可能因提交方已放弃：等待者已摘除，继续下一轮。
			continue
		}

		// 2) 有空闲执行位：按分组轮转选择并执行一个派发动作。
		if s.globalActive() >= s.e.cfg.Workers {
			break
		}
		tgt := s.nextDispatchTarget()
		if tgt == nil {
			break // 所有分组都触顶或无活
		}
		n := len(s.groupOrder)
		// 定位被选中分组的下标，以便派发后把游标推进到其后一位。
		chosen := 0
		for k, name := range s.groupOrder {
			if name == tgt.g.source {
				chosen = k
				break
			}
		}
		if tgt.req == nil {
			// 派发本路缓冲队首（此前已受理，直接派发）。
			s.dispatch(tgt.g)
		} else {
			req := s.removeWaiter(tgt.wIdx)
			if !s.admitAndDispatchDirect(tgt.g, req) {
				// 提交方在选中瞬间放弃：摘掉后重新选择，不推进游标、不卡死。
				continue
			}
		}
		s.rrIdx = (chosen + 1) % n
		progress = true
	}
	s.publishGauges()
}

// purgeAbortedWaiters 移除提交方已放弃的等待者（不回复，任务不会被受理）。
func (s *scheduler) purgeAbortedWaiters() {
	kept := s.waiters[:0]
	for _, req := range s.waiters {
		select {
		case <-req.aborted:
			// 已放弃：丢弃。
		default:
			kept = append(kept, req)
		}
	}
	s.waiters = kept
}

// rejectAllWaiters 拒绝全部等待中的受理请求；已自行放弃者静默丢弃。
func (s *scheduler) rejectAllWaiters(err error) {
	for _, req := range s.waiters {
		sendReply(req, admitReply{ok: false, err: err})
	}
	s.waiters = nil
}

// rejectWaitersFor 只拒绝指定分组的等待者（单路关闭用），其它路原样保留。
func (s *scheduler) rejectWaitersFor(source string, err error) {
	kept := s.waiters[:0]
	for _, req := range s.waiters {
		if req.source != source {
			kept = append(kept, req)
			continue
		}
		sendReply(req, admitReply{ok: false, err: err})
	}
	s.waiters = kept
}

// closeReason 返回引擎整体关闭时提交方应拿到的错误。
func (s *scheduler) closeReason() error {
	if s.e.canceledByCaller() {
		return context.Canceled
	}
	return ErrEngineClosed
}

// handleSubmit 处理一次受理请求：
//   - 引擎整体关闭：拒绝（取消式 -> context.Canceled；排空 -> ErrEngineClosed）；
//   - 分组已排空/取消：分别拒绝为 ErrGroupClosed / ErrGroupCanceled；
//   - 缓冲有空位：入本路缓冲（立即在 pump 中参与轮转派发）；
//   - QueueSize=0 或缓冲满，但恰好有全局执行位且本路未触顶：直接会合派发；
//   - 非阻塞且无法立即受理：ErrQueueFull；
//   - 其余：进入 FIFO 等待队列，提交方在原地承受背压。
//
// 对等待者（最终会进入 s.waiters）的所有受理裁决都经 sendReply 会合：提交方
// 一旦放弃，裁决会合失败且不产生任何计数/排队副作用。
func (s *scheduler) handleSubmit(req admitReq) {
	if s.closedAll {
		sendReply(&req, admitReply{ok: false, err: s.closeReason()})
		return
	}
	g := s.getOrCreate(req.source)
	if g.draining {
		err := error(ErrGroupClosed)
		if g.canceled {
			err = ErrGroupCanceled
		}
		sendReply(&req, admitReply{ok: false, err: err})
		return
	}

	// 缓冲有空位：先会合确认受理，再入队并统一受理等待者、轮转派发。
	if s.queuedTotal < s.e.cfg.QueueSize {
		if !s.admitQueued(g, &req) {
			return // 阻塞提交方恰在此时放弃：不受理
		}
		s.pump()
		return
	}

	// 无缓冲（QueueSize=0）或缓冲已满：若有空闲全局执行位且本路未触上限，
	// 直接会合派发（缓冲不留存；QueueSize=0 的正常路径）。
	if s.globalActive() < s.e.cfg.Workers && (g.limit <= 0 || g.active < g.limit) {
		if !sendReply(&req, admitReply{ok: true}) {
			return // 阻塞提交方已放弃：不受理、不派发
		}
		g.queued = append(g.queued, req.item)
		g.queuedN.Add(1)
		s.queuedTotal++
		g.wg.Add(1)
		s.e.outstanding.add(1) // 受理确认点：直接派发也先计数
		req.item.group = g
		s.dispatch(g) // 弹出队首（即该任务）并派发
		s.publishGauges()
		return
	}

	if req.nonblocking {
		// 非阻塞请求的 aborted 为 nil（永不关闭），会合必然成功。
		sendReply(&req, admitReply{ok: false, err: ErrQueueFull})
		return
	}
	s.waiters = append(s.waiters, &req)
	s.publishGauges()
}

// handleFinish 处理任务终态：释放本路执行位，尝试受理等待者并继续轮转派发；
// 若本路在关闭中且已无在途/排队任务，则关闭其 done。
func (s *scheduler) handleFinish(item *inflight) {
	g := item.group
	if g != nil {
		g.active--
		g.activeN.Add(-1)
	}
	s.pump()
	if g != nil {
		s.checkGroupDone(g)
	}
}

// drainQueuedCanceled 把本路全部排队任务给出确定的取消裁决（未执行，
// Attempts=0），并归还分组/全局在途计数。它们此前已计入 Submitted，
// 这里只终态化：不重试、不丢弃、不重复计数。
func (s *scheduler) drainQueuedCanceled(g *groupState) {
	e := s.e
	queued := g.queued
	g.queued = nil
	g.queuedN.Store(0)
	s.queuedTotal -= len(queued)
	for _, item := range queued {
		e.finalize(item, Result{
			TaskID: item.t.ID, Source: g.source, Class: ClassCanceled,
			Attempts: 0, Err: context.Canceled,
		})
		g.wg.Done()
		e.outstanding.add(-1)
	}
}

// checkGroupDone 在本路停止受理且已受理任务全部终态（队列空 + 在执行为 0）时
// 关闭其 done 通道。
func (s *scheduler) checkGroupDone(g *groupState) {
	if g.draining && len(g.queued) == 0 && g.active == 0 {
		g.markDone()
	}
}

// handleGroupEvent 处理单路排空/取消（幂等）。
func (s *scheduler) handleGroupEvent(ev groupEvent) {
	g := s.getOrCreate(ev.source) // 从未出现的分组：预建空分组，语义确定
	if !g.draining {
		g.draining = true
		g.canceled = ev.cancel
		if ev.cancel {
			g.cancel() // 在途任务的 ctx 立即取消；排队任务由下面裁决为取消
		}
		err := error(ErrGroupClosed)
		if ev.cancel {
			err = ErrGroupCanceled
		}
		s.rejectWaitersFor(ev.source, err)
		if ev.cancel {
			s.drainQueuedCanceled(g)
		}
	}
	if ev.reply != nil {
		ev.reply <- nil
	}
	// 排空模式：本路排队任务照常参与轮转执行；取消模式：队列已清空。
	// 其它路不受影响，继续受理与派发。
	s.pump()
	s.checkGroupDone(g)
}

// handleCloseAll 处理引擎整体关闭。
func (s *scheduler) handleCloseAll(canceled bool) {
	s.closedAll = true
	if canceled {
		s.rejectAllWaiters(context.Canceled)
	} else {
		s.rejectAllWaiters(ErrEngineClosed)
	}
	for _, g := range s.groups {
		g.draining = true
		g.canceled = canceled
		if canceled {
			g.cancel()
			s.drainQueuedCanceled(g)
		}
		s.checkGroupDone(g)
	}
	// 排空模式：排队任务仍在，pump 继续派发给工作协程消化；
	// 取消模式：队列已清空，pump 仅发布量表。
	s.pump()
}

// ---- 供 Engine 调用的对外入口（运行在提交方/关闭方 goroutine）----

// requestAdmit 向调度器申请受理一个任务，并阻塞等待裁决（背压在此发生）。
// 阻塞期间提交方 ctx 结束则放弃等待：任务不会被受理（裁决会合未完成），由
// 提交方回滚受理计数。裁决以无缓冲通道会合，接收侧的 select 是原子提交，
// 因此“已受理”与“按 ctx 放弃”二者互斥，不会出现半截或说不清的结论。
func (s *scheduler) requestAdmit(ctx context.Context, item *inflight, nonblocking bool) (bool, error) {
	req := admitReq{
		item:        item,
		source:      item.t.Source,
		nonblocking: nonblocking,
		aborted:     nil,
		reply:       make(chan admitReply), // 无缓冲：裁决会合，避免“已受理却按放弃返回”
	}

	// settled 在裁决有了确定归属后关闭，用于回收放弃 watcher。
	settled := make(chan struct{})
	if nonblocking {
		// 非阻塞请求不与提交方 ctx 挂钩，aborted 保持 nil（永不关闭）：
		// 调用方始终在等待裁决，调度器的会合发送必然成功。
	} else {
		aborted := make(chan struct{})
		req.aborted = aborted
		go func() {
			select {
			case <-ctx.Done():
				close(aborted) // 调度器据此放弃会合，绝不受理本任务
				// 唤醒调度器摘除等待者（合并信号，非阻塞，丢失也无妨）。
				select {
				case s.abortWaitCh <- struct{}{}:
				default:
				}
			case <-settled:
			}
		}()
	}

	// sendRequest 把请求交给调度器；引擎停止时停止交付，按关闭语义返回。
	select {
	case s.submitCh <- req:
	case <-s.e.stopCh:
		close(settled)
		return false, s.closeReason()
	}

	// 接收裁决：reply 无缓冲，与 ctx.Done 的接收选择在运行时是原子提交——
	// 调度器只有在本 select 选中 reply 分支时才会合成功，因此不会出现
	// “任务已受理、提交方却拿到 ctx 错误/空结果”的二义性。
	select {
	case rep := <-req.reply:
		close(settled)
		return rep.ok, rep.err
	case <-ctx.Done():
		close(settled)
		if nonblocking {
			return false, ErrQueueFull
		}
		return false, ctx.Err()
	case <-s.e.stopCh:
		close(settled)
		// 极端竞态：请求已交付而调度器随引擎退出，排空本可能到达的裁决，
		// 否则交给关闭语义（已受理任务一定已纳入整体排空）。
		select {
		case rep := <-req.reply:
			return rep.ok, rep.err
		default:
			return false, s.closeReason()
		}
	}
}

// requestCloseGroup 请求对某一路做排空关闭或取消；重复请求幂等。
func (s *scheduler) requestCloseGroup(source string, cancel bool) error {
	reply := make(chan error, 1)
	select {
	case s.eventCh <- groupEvent{source: source, cancel: cancel, reply: reply}:
		return <-reply
	case <-s.e.stopCh:
		// 引擎整体停止：分组关闭随之完成，等待 done 即可。
		return nil
	}
}

// waitGroupDrained 等待某一路所有已受理任务终态，或 ctx 到期。
func (s *scheduler) waitGroupDrained(ctx context.Context, source string) error {
	g, ok := s.lookupGroup(source)
	if !ok {
		return ErrUnknownGroup
	}
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeAll 通知调度器引擎整体关闭（幂等）。canceled=true 为取消式关闭。
func (s *scheduler) closeAll(canceled bool) {
	if canceled {
		// 先登记“要求取消”：即便槽里已有排空信号、本发送入不了队，调度器
		// 消费该信号时也会按取消处理（见 run）。
		s.wantCancel.Store(true)
	}
	select {
	case s.closeSig <- canceled:
	default:
		// 关闭信号已在排队或已处理：取消意图由 wantCancel 兜底；排空信号幂等。
	}
}
