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
	mu   sync.Mutex
	m    Metrics
	done []Result
}

func newAggregator() *aggregator {
	return &aggregator{}
}

func (a *aggregator) addSubmitted(delta int64) {
	a.mu.Lock()
	a.m.Submitted += delta
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

// recordResult 按终态类别累加计数；同一 Result 只会被传入一次（见引擎的
// 单消费者），因此重试不会导致同一任务结果被重复累计。
func (a *aggregator) recordResult(r Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch r.Class {
	case ClassSuccess:
		a.m.Succeeded++
	case ClassPermanent:
		a.m.PermanentFailures++
	case ClassTemporary:
		// 临时失败走到终态只有一种情况：重试耗尽。
		a.m.RetryExhausted++
	case ClassCanceled:
		a.m.Canceled++
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

// results 返回已记录结果的副本；按完成顺序排列，但计数汇总与顺序无关。
func (a *aggregator) results() []Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Result, len(a.done))
	copy(out, a.done)
	return out
}
