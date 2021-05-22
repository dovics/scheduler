package scheduler

import (
	"context"
	"log"
	"time"
)

// Task represents a generic task.
type Task interface {
	Do(context.Context) error
	WithRetry(times int) Task
	WithTimeout(timeout time.Duration) Task
	BindScheduler(s *Scheduler) Task
	WithCancelFunc(timeout time.Duration) (Task, context.CancelFunc)
	SetContext(context context.Context) Task
}

// TaskFunc is a wrapper for task function.
type TaskFunc func(context.Context) error

// Do is the Task interface implementation for type TaskFunc.
func (t TaskFunc) Do(ctx context.Context) error {
	return t(ctx)
}

func (t TaskFunc) WithRetry(times int) Task {
	task := &task{
		f: t,
	}

	return task.WithRetry(times)
}

func (t TaskFunc) WithTimeout(timeout time.Duration) Task {
	context, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(timeout))
	return &task{
		f:          t,
		ctx:        context,
		cancelFunc: cancelFunc,
	}
}

func (t TaskFunc) WithCancelFunc(timeout time.Duration) (Task, context.CancelFunc) {
	context, cancelFunc := context.WithCancel(context.Background())

	return &task{
		f:          t,
		ctx:        context,
		cancelFunc: cancelFunc,
	}, cancelFunc
}

func (t TaskFunc) BindScheduler(s *Scheduler) Task {
	return &task{
		f:    t,
		sche: s,
	}
}

func (t TaskFunc) SetContext(ctx context.Context) Task {
	return &task{
		f:   t,
		ctx: ctx,
	}
}

type task struct {
	f TaskFunc

	ctx        context.Context
	cancelFunc context.CancelFunc

	sche *Scheduler

	retryTimes int
	timeout    time.Duration
}

func (t *task) Do(ctx context.Context) error {
	return t.f.Do(ctx)
}

func (t *task) WithRetry(times int) Task {
	counter, originFunc := 0, t.f

	t.retryTimes = times
	t.f = TaskFunc(func(ctx context.Context) error {
		err := originFunc.Do(ctx)
		if err == nil {
			return nil
		}

		log.Printf("[Task] error: %s", err)
		if counter < times {
			counter++
			log.Printf("[Task] Retry times: %d", counter)
			t.sche.queue.Add(t)
		}

		return nil
	})

	return t
}

func (t *task) WithTimeout(timeout time.Duration) Task {
	backgroundContext := context.Background()
	if t.ctx != nil {
		backgroundContext = t.ctx
	}

	context, cancelFunc := context.WithDeadline(backgroundContext, time.Now().Add(timeout))

	t.ctx = context
	t.cancelFunc = cancelFunc
	t.timeout = timeout

	return t
}

func (t *task) WithCancelFunc(timeout time.Duration) (Task, context.CancelFunc) {
	backgroundContext := context.Background()
	if t.ctx != nil {
		backgroundContext = t.ctx
	}

	context, cancelFunc := context.WithCancel(backgroundContext)
	t.ctx = context
	t.cancelFunc = cancelFunc
	return t, cancelFunc
}

func (t *task) BindScheduler(s *Scheduler) Task {
	t.sche = s
	return t
}

func (t *task) SetContext(ctx context.Context) Task {
	if t.ctx != nil && t.ctx != ctx {
		log.Printf("[Warning] don't have the same context, use the last")
	}

	t.ctx = ctx
	return t
}
