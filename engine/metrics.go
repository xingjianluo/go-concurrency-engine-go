package engine

import (
	"sync/atomic"
	"time"
)

// Metrics 汇总引擎运行指标，全部通过原子操作更新，可在任意时刻读取快照。
type Metrics struct {
	submitted  atomic.Int64 // 接受的提交数（去重后）
	deduped    atomic.Int64 // 幂等忽略的重复投递数
	succeeded  atomic.Int64
	tempFailed atomic.Int64 // 临时失败（重试耗尽）
	permFailed atomic.Int64
	cancelled  atomic.Int64
	retries    atomic.Int64 // 实际发生的重试次数
	totalNanos atomic.Int64 // 所有完成任务的总耗时（纳秒）
}

// Snapshot 是 Metrics 的一致性快照。
type Snapshot struct {
	Submitted   int64
	Deduped     int64
	Succeeded   int64
	TempFailed  int64
	PermFailed  int64
	Cancelled   int64
	Retries     int64
	TotalTime   time.Duration
	Completed   int64   // Succeeded+TempFailed+PermFailed+Cancelled
	MeanLatency float64 // 平均单任务耗时（纳秒）
}

// Metrics 返回引擎的指标句柄。
func (e *Engine) Metrics() *Metrics {
	return e.metrics
}

// Snapshot 读取一致性快照。计数器为原子读取，
// 汇总字段由读取到的计数即时推导，保证 Completed 与分项自洽。
func (m *Metrics) Snapshot() Snapshot {
	s := Snapshot{
		Submitted:  m.submitted.Load(),
		Deduped:    m.deduped.Load(),
		Succeeded:  m.succeeded.Load(),
		TempFailed: m.tempFailed.Load(),
		PermFailed: m.permFailed.Load(),
		Cancelled:  m.cancelled.Load(),
		Retries:    m.retries.Load(),
		TotalTime:  time.Duration(m.totalNanos.Load()),
	}
	s.Completed = s.Succeeded + s.TempFailed + s.PermFailed + s.Cancelled
	if s.Completed > 0 {
		s.MeanLatency = float64(s.TotalTime) / float64(s.Completed)
	}
	return s
}

// record 记录一个终态结果。
func (m *Metrics) record(r Result) {
	switch r.Kind {
	case KindSuccess:
		m.succeeded.Add(1)
	case KindTemporary:
		m.tempFailed.Add(1)
	case KindPermanent:
		m.permFailed.Add(1)
	case KindCancelled:
		m.cancelled.Add(1)
	}
	m.totalNanos.Add(int64(r.Duration))
	if r.Attempts > 1 {
		m.retries.Add(int64(r.Attempts - 1))
	}
}
