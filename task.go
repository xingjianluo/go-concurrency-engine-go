package engine

import (
	"context"
	"errors"
	"time"
)

// FailureClass 将任务失败划分为可区分的类别，用于重试决策与指标统计。
type FailureClass int

const (
	// ClassUnknown 不应在终态结果中出现，仅作零值兜底。
	ClassUnknown FailureClass = iota
	ClassSuccess
	// ClassTemporary 临时失败：允许按退避策略有限重试。
	ClassTemporary
	// ClassPermanent 永久失败：不重试，立即终态。
	ClassPermanent
	// ClassCanceled 取消（含上下文取消/超时）：不重试，立即终态。
	ClassCanceled
)

func (c FailureClass) String() string {
	switch c {
	case ClassSuccess:
		return "success"
	case ClassTemporary:
		return "temporary"
	case ClassPermanent:
		return "permanent"
	case ClassCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// Task 是引擎调度的最小工作单元。ID 用于幂等去重，Fn 承载实际工作。
// Group 标识任务来源（分组）：同一 Group 的任务共享该组的并发额度，
// 参与跨组的公平调度；空串表示默认分组（旧用法不变）。
type Task struct {
	ID    string
	Group string
	Fn    func(ctx context.Context) error
}

// Result 是一个任务（按 ID 幂等去重后）的最终裁决结果。
// 对同一 ID 的并发/重复投递，所有提交方拿到同一个 Result，且只统计一次。
type Result struct {
	TaskID   string
	Group    string // 任务所属分组（受理时确定，去重共享时与首次投递一致）
	Class    FailureClass
	Attempts int           // 实际尝试次数（含首次）；重试耗尽时等于 MaxAttempts
	Duration time.Duration // 从首次执行开始至终态裁决的总耗时（含退避等待）
	Err      error
}

// classifiedError 是 TempError/PermError 显式分类的包装。
type classifiedError struct {
	cause error
	class FailureClass
}

func (e *classifiedError) Error() string {
	if e.cause == nil {
		return e.class.String()
	}
	return e.class.String() + ": " + e.cause.Error()
}

func (e *classifiedError) Unwrap() error { return e.cause }

// TempError 将 err 标记为临时失败（可重试）。nil 返回 nil。
func TempError(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{cause: err, class: ClassTemporary}
}

// PermError 将 err 标记为永久失败（不可重试）。nil 返回 nil。
func PermError(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{cause: err, class: ClassPermanent}
}

// Classify 判定一个 error 的失败类别：
//   - nil -> ClassSuccess；
//   - context.Canceled / context.DeadlineExceeded（含包装）-> ClassCanceled；
//   - TempError 包装 -> ClassTemporary；PermError 包装 -> ClassPermanent；
//   - 其余普通 error 默认视为 ClassTemporary（保守地允许有限重试）。
func Classify(err error) FailureClass {
	if err == nil {
		return ClassSuccess
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassCanceled
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class
	}
	return ClassTemporary
}
