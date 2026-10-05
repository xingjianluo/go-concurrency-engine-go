package engine

import (
	"sync"
	"time"
)

// Metrics 是引擎可观测指标的一致性快照。
//
// 计数语义：
//   - Submitted 为去重后实际受理的唯一任务数；
//   - DuplicateDeliveries 为被幂等拦截的重复投递次数（不计入 Submitted）；
//   - Succeeded/PermanentFailures/RetryExhausted/Canceled 为四类终态裁决，
//     按任务（而非尝试）计数，因此四类之和恒等于 Submitted；
//   - Retries/Attempts 按执行尝试计数（Attempts = 任务数 + Retries）。
//
// 所有计数由聚合器串行累加，结果与任务完成顺序无关，重复汇总也得到同一结果。
type Metrics struct {
	Submitted           int64
	DuplicateDeliveries int64
	Succeeded           int64
	PermanentFailures   int64
	RetryExhausted      int64 // 临时失败但重试次数已耗尽
	Canceled            int64
	Retries             int64 // 总重试次数（Attempts 中除首次外的部分）
	Attempts            int64 // 总尝试次数（每个任务的首次 + 各次重试）
	TaskTimeTotal       time.Duration
	BackpressureWaits   int64
	BackpressureTime    time.Duration

	// 以下为瞬时量表（仅在 Snapshot 中填充，不参与累计）。
	QueueDepth    int64 // 已入队、等待执行的任务数
	ActiveWorkers int64 // 正在执行（含退避等待）的任务数
}

// aggregator 聚合终态结果。所有方法并发安全；引擎内部仅有单一消费者调用
// recordResult，但 Snapshot/Results 可能被外部任意 goroutine 读取。
type aggregator struct {
	mu     sync.Mutex
	m      Metrics
	groups map[string]*GroupMetrics // 按分组的受理/终态计数，与全局同口径累加
	done   []Result
}

func newAggregator() *aggregator {
	return &aggregator{groups: make(map[string]*GroupMetrics)}
}

// groupLocked 返回指定分组的计数桶，不存在则创建。
func (a *aggregator) groupLocked(name string) *GroupMetrics {
	g, ok := a.groups[name]
	if !ok {
		g = &GroupMetrics{}
		a.groups[name] = g
	}
	return g
}

func (a *aggregator) addSubmitted(group string, delta int64) {
	a.mu.Lock()
	a.m.Submitted += delta
	a.groupLocked(group).Submitted += delta
	a.mu.Unlock()
}

func (a *aggregator) addDuplicate() {
	a.mu.Lock()
	a.m.DuplicateDeliveries++
	a.mu.Unlock()
}

func (a *aggregator) addBackpressure(wait time.Duration) {
	a.mu.Lock()
	a.m.BackpressureWaits++
	a.m.BackpressureTime += wait
	a.mu.Unlock()
}

// recordResult 按终态类别累加计数（全局与所属分组同口径各记一笔）；
// 同一 Result 只会被传入一次（见引擎的单消费者），因此重试不会导致
// 同一任务结果被重复累计。
func (a *aggregator) recordResult(r Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groupLocked(r.Group)
	switch r.Class {
	case ClassSuccess:
		a.m.Succeeded++
		g.Succeeded++
	case ClassPermanent:
		a.m.PermanentFailures++
		g.PermanentFailures++
	case ClassTemporary:
		// 临时失败走到终态只有一种情况：重试耗尽。
		a.m.RetryExhausted++
		g.RetryExhausted++
	case ClassCanceled:
		a.m.Canceled++
		g.Canceled++
	}
	if r.Attempts > 0 {
		a.m.Attempts += int64(r.Attempts)
		if r.Attempts > 1 {
			a.m.Retries += int64(r.Attempts - 1)
		}
	}
	a.m.TaskTimeTotal += r.Duration
	a.done = append(a.done, r)
}

func (a *aggregator) snapshot() Metrics {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m
}

// groupSnapshot 返回单个分组的计数快照。
func (a *aggregator) groupSnapshot(name string) (GroupMetrics, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g, ok := a.groups[name]
	if !ok {
		return GroupMetrics{}, false
	}
	return *g, true
}

// groupSnapshots 返回全部分组计数快照的副本。
func (a *aggregator) groupSnapshots() map[string]GroupMetrics {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]GroupMetrics, len(a.groups))
	for name, g := range a.groups {
		out[name] = *g
	}
	return out
}

// results 返回已记录结果的副本；按完成顺序排列，但计数汇总与顺序无关。
func (a *aggregator) results() []Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Result, len(a.done))
	copy(out, a.done)
	return out
}
