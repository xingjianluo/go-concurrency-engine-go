package engine

import (
	"math"
	"time"
)

// RetryPolicy 描述有限重试策略：上限 + 指数退避。
type RetryPolicy struct {
	// MaxAttempts 为最大尝试次数（含首次）；<=1 表示不重试。
	MaxAttempts int
	// BaseDelay 为第 1 次失败后的退避基准。
	BaseDelay time.Duration
	// MaxDelay 为单次退避上限；<=0 表示不限制。
	MaxDelay time.Duration
	// Multiplier 为退避增长倍数；<=1 时按 1 处理（恒定退避）。
	Multiplier float64
}

// DefaultRetryPolicy 返回保守的默认策略：最多 3 次尝试，5ms 起步指数退避。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    500 * time.Millisecond,
		Multiplier:  2,
	}
}

// NoRetry 表示不进行任何重试。
func NoRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// nextBackoff 返回第 attempt 次失败后、第 attempt+1 次尝试前的退避时长。
// attempt 从 1 开始；采用确定的指数退避（Base * Multiplier^(attempt-1)）并封顶，
// 刻意不引入随机抖动以保证测试可复现；需要抖动时可在 Task.Fn 外部自行处理。
func (p RetryPolicy) nextBackoff(attempt int) time.Duration {
	if p.BaseDelay <= 0 || attempt < 1 {
		return 0
	}
	mult := p.Multiplier
	if mult <= 1 {
		mult = 1
	}
	d := float64(p.BaseDelay) * math.Pow(mult, float64(attempt-1))
	if math.IsInf(d, 0) || d > float64(math.MaxInt64) {
		d = float64(math.MaxInt64)
	}
	dur := time.Duration(d)
	if p.MaxDelay > 0 && dur > p.MaxDelay {
		dur = p.MaxDelay
	}
	return dur
}
