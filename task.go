package scheduler

import (
	"context"
)

// Task represents a generic task.
type Task interface {
	Do(context.Context) error
}

// TaskFunc is a wrapper for task function.
type TaskFunc func(context.Context) error

// Do is the Task interface implementation for type TaskFunc.
func (t TaskFunc) Do(ctx context.Context) error {
	return t(ctx)
}

type taskWrapper struct {
	task Task
	ctx  context.Context
}

func (t *taskWrapper) Do(ctx context.Context) error {
	return t.task.Do(ctx)
}
