package engine

import "time"

// RetryPolicy 描述有限重试与退避策略。
type RetryPolicy struct {
	MaxAttempts int           // 总尝试次数上限（含首次），<=1 表示不重试
	BaseBackoff time.Duration // 首次退避
	MaxBackoff  time.Duration // 退避上限
}

// backoff 计算第 attempt 次失败（attempt 从 1 开始）后的退避时长：
// 指数退避 BaseBackoff * 2^(attempt-1)，封顶 MaxBackoff。
func (p RetryPolicy) backoff(attempt int) time.Duration {
	d := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}
