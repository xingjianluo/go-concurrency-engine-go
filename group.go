package engine

import (
	"context"
	"errors"
)

// ErrGroupClosed 在分组已被关闭受理（DrainGroup）之后向该组提交任务时返回。
// 被取消（CancelGroup）的分组收到的是 context.Canceled 而非本错误。
var ErrGroupClosed = errors.New("engine: group closed")

// GroupMetrics 是单个分组的可观测计数。口径与全局 Metrics 一致：
// 所有分组的各项计数之和恒等于全局汇总（Submitted/终态四类）。
type GroupMetrics struct {
	Submitted         int64 // 该组实际受理的唯一任务数（去重后）
	Succeeded         int64
	PermanentFailures int64
	RetryExhausted    int64
	Canceled          int64
}

// group 是一个来源分组的调度状态。除 ctx/cancel 外所有字段由 Engine.mu 保护。
type group struct {
	name  string
	limit int // 同时在跑的上限；<=0 表示不限制（仅受全局并发约束）

	running     int         // 正在执行（含退避）的该组任务数
	outstanding int         // 已受理未终态的该组任务数（排队 + 在执行）
	queue       []*inflight // 该组的 FIFO 等待队列
	inReady     bool        // 是否在引擎的就绪轮转列表中

	closed   bool // 不再受理新提交（DrainGroup/CancelGroup 后）
	canceled bool // 已取消：在途 ctx 已取消，排队任务已裁决为取消

	ctx    context.Context // 派生自引擎 runCtx；组取消时单独取消
	cancel context.CancelFunc
}

// getOrCreateLocked 返回指定名字的组，不存在则按配置创建。
// 调用方必须持有 e.mu。
func (e *Engine) getOrCreateLocked(name string) *group {
	if g, ok := e.groups[name]; ok {
		return g
	}
	limit := 0
	if e.cfg.GroupLimits != nil {
		limit = e.cfg.GroupLimits[name] // 未配置或 <=0：不单独限流
	}
	ctx, cancel := context.WithCancel(e.runCtx)
	g := &group{name: name, limit: limit, ctx: ctx, cancel: cancel}
	e.groups[name] = g
	return g
}

// CancelGroup 取消指定分组：该组正在执行的任务其 ctx 立即取消（退避也
// 立即结束），已排队未开始的任务获得确定的 ClassCanceled 裁决（不执行、
// 不重试、只统计一次）；此后向该组的提交返回 context.Canceled。
// 其它分组的受理与执行不受影响。对不存在的分组调用会留下取消墓碑：
// 之后该名字的提交同样被拒绝。重复调用幂等。
func (e *Engine) CancelGroup(name string) {
	e.mu.Lock()
	g := e.getOrCreateLocked(name)
	if g.canceled {
		e.mu.Unlock()
		return
	}
	g.canceled = true
	g.closed = true
	g.cancel() // 级联取消该组在执行任务的 ctx（含退避等待）

	// 排队任务立即获得确定的取消裁决：出队、释放全局队列空位。
	queued := g.queue
	g.queue = nil
	e.queuedTotal -= len(queued)
	e.wakeAllWaitersLocked() // 唤醒等待空位/该组状态的提交方
	e.broadcastChangeLocked()
	e.mu.Unlock()

	for _, item := range queued {
		e.finalize(item, Result{
			TaskID: item.t.ID, Group: name, Class: ClassCanceled, Attempts: 0,
			Err: context.Canceled,
		})
		e.mu.Lock()
		g.outstanding--
		e.broadcastChangeLocked()
		e.mu.Unlock()
		e.outstanding.Done()
	}
}

// DrainGroup 排空指定分组：立即停止受理该组的新提交（返回 ErrGroupClosed），
// 并等待该组所有已受理任务到达终态后返回 nil。其它分组不受影响。
// ctx 到期仍未排空时返回 ctx.Err()（组保持关闭受理状态，可再次调用等待）。
// 对不存在的分组调用会留下关闭墓碑：之后该名字的提交被拒绝。
func (e *Engine) DrainGroup(ctx context.Context, name string) error {
	e.mu.Lock()
	g := e.getOrCreateLocked(name)
	g.closed = true
	e.wakeAllWaitersLocked() // 该组阻塞中的提交方需要看到关闭并得到确定拒绝
	for g.outstanding > 0 {
		e.drainWaiters++
		ch := e.changeCh
		e.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			e.mu.Lock()
			e.drainWaiters--
			e.mu.Unlock()
			return ctx.Err()
		}
		e.mu.Lock()
		e.drainWaiters--
	}
	e.mu.Unlock()
	return nil
}

// GroupSnapshot 返回指定分组的一致计数快照；分组不存在时 ok=false。
func (e *Engine) GroupSnapshot(name string) (m GroupMetrics, ok bool) {
	return e.agg.groupSnapshot(name)
}

// GroupSnapshots 返回全部分组（含默认分组 ""）的计数快照副本。
// 所有分组的 Submitted 之和恒等于全局 Snapshot().Submitted，终态四类同理；
// 快照由同一把聚合锁串行读取，多次读取结果一致。
func (e *Engine) GroupSnapshots() map[string]GroupMetrics {
	return e.agg.groupSnapshots()
}
