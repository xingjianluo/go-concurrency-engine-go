// Package engine 提供单进程本地并发任务执行引擎。
package engine

import (
	"context"
	"errors"
	"time"
)

// Kind 表示任务的终态分类。
type Kind int

const (
	KindSuccess   Kind = iota // 成功
	KindTemporary             // 临时失败（重试耗尽）
	KindPermanent             // 永久失败
	KindCancelled             // 被取消
)

func (k Kind) String() string {
	switch k {
	case KindSuccess:
		return "success"
	case KindTemporary:
		return "temporary_failure"
	case KindPermanent:
		return "permanent_failure"
	case KindCancelled:
		return "cancelled"
	}
	return "unknown"
}

// Permanent 将 err 标记为永久失败（不重试）。
func Permanent(err error) error { return permanentError{err} }

// Temporary 将 err 标记为临时失败（可重试）。
func Temporary(err error) error { return temporaryError{err} }

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

type temporaryError struct{ err error }

func (e temporaryError) Error() string { return e.err.Error() }
func (e temporaryError) Unwrap() error { return e.err }

// IsPermanent 判断错误是否为永久失败。
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// IsTemporary 判断错误是否为临时失败。
func IsTemporary(err error) bool {
	var t temporaryError
	return errors.As(err, &t)
}

// Task 是提交给引擎的工作单元。
type Task struct {
	ID  string // 幂等键：相同 ID 的重复投递只执行一次
	Run func(ctx context.Context) error
}

// Result 是任务的终态结果，恰好产生一次。
type Result struct {
	ID       string
	Kind     Kind
	Err      error
	Attempts int
	Duration time.Duration // 含重试与退避的总耗时
}
