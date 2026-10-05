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
// 全局快照与每个分组快照共用同一套累加代码（见 addCounts），因此对任意一组
// 终态结果恒有：所有分组同名计数之和 == 全局同名计数，与完成顺序无关，
// 重复汇总也得到同一结果。
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
	QueueDepth    int64 // 已受理、等待执行的任务数
	ActiveWorkers int64 // 正在执行（含退避等待）的任务数
}

// aggregator 聚合终态结果。所有方法并发安全；终态结果既计入全局，也按任务
// 所属来源（Result.Source）计入对应分组。
type aggregator struct {
	mu     sync.Mutex
	m      Metrics
	done   []Result
	groups map[string]*Metrics // 不含瞬时量表；键为来源名
}

func newAggregator() *aggregator {
	return &aggregator{groups: make(map[string]*Metrics)}
}

// group 取（必要时创建）某个来源分组的累计计数器；调用方须持有 a.mu。
func (a *aggregator) group(source string) *Metrics {
	gm, ok := a.groups[source]
	if !ok {
		gm = &Metrics{}
		a.groups[source] = gm
	}
	return gm
}

func (a *aggregator) addSubmitted(source string, delta int64) {
	a.mu.Lock()
	a.m.Submitted += delta
	a.group(source).Submitted += delta
	a.mu.Unlock()
}

// addDuplicateFor 记录一次在某路提交时命中的幂等重复投递。重复投递的执行与
// 终态归属任务首次受理的那一路，但“重复投递事件”本身计入发起方所在路，
// 因此各分组 DuplicateDeliveries 之和仍与全局一致。
func (a *aggregator) addDuplicateFor(source string) {
	a.mu.Lock()
	a.m.DuplicateDeliveries++
	a.group(source).DuplicateDeliveries++
	a.mu.Unlock()
}

func (a *aggregator) addBackpressure(source string, wait time.Duration) {
	a.mu.Lock()
	a.m.BackpressureWaits++
	a.m.BackpressureTime += wait
	gm := a.group(source)
	gm.BackpressureWaits++
	gm.BackpressureTime += wait
	a.mu.Unlock()
}

// addCounts 按终态类别与尝试数累加一份计数器（全局与每个分组共用，保证口径一致）。
func addCounts(m *Metrics, r Result) {
	switch r.Class {
	case ClassSuccess:
		m.Succeeded++
	case ClassPermanent:
		m.PermanentFailures++
	case ClassTemporary:
		// 临时失败走到终态只有一种情况：重试耗尽。
		m.RetryExhausted++
	case ClassCanceled:
		m.Canceled++
	}
	if r.Attempts > 0 {
		m.Attempts += int64(r.Attempts)
		if r.Attempts > 1 {
			m.Retries += int64(r.Attempts - 1)
		}
	}
	m.TaskTimeTotal += r.Duration
}

// recordResult 按终态类别累加计数；同一 Result 只会被传入一次（每个任务终态
// 只裁决一次），因此重试不会导致同一任务结果被重复累计。
func (a *aggregator) recordResult(r Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	addCounts(&a.m, r)
	addCounts(a.group(r.Source), r)
	a.done = append(a.done, r)
}

func (a *aggregator) snapshot() Metrics {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m
}

// groupSnapshot 返回某一路的累计计数副本（不含瞬时量表，由调度器另行填充）。
func (a *aggregator) groupSnapshot(source string) (Metrics, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	gm, ok := a.groups[source]
	if !ok {
		return Metrics{}, false
	}
	return *gm, true
}

// groupNames 返回出现过计数的全部分组名（快照副本，供遍历/核查）。
func (a *aggregator) groupNames() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.groups))
	for name := range a.groups {
		out = append(out, name)
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
