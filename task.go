package scheduler

import (
	"context"
	"log"
	"time"
)

// Task represents a generic task.
type Task interface {
	Do(context.Context) error
	WithRetry(times uint) Task
	WithTimeout(timeout time.Duration) Task
	WithCancelFunc(timeout time.Duration) (Task, context.CancelFunc)
	WithPriority(int) Task

	BindScheduler(s *Scheduler) Task
	SetContext(context context.Context) Task
}

// TaskFunc is a wrapper for task function.
type TaskFunc func(context.Context) error

var _ Task = TaskFunc(func(context.Context) error { return nil })

// Do is the Task interface implementation for type TaskFunc.
func (t TaskFunc) Do(ctx context.Context) error {
	return t(ctx)
}

// WithRetry set the retry times for this task
func (t TaskFunc) WithRetry(times uint) Task {
	task := &task{
		f: t,
	}

	return task.WithRetry(times)
}

// WithTimeout set the timeout for this task
func (t TaskFunc) WithTimeout(timeout time.Duration) Task {
	context, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(timeout))
	return &task{
		f:          t,
		ctx:        context,
		cancelFunc: cancelFunc,
	}
}

// WithCancelFunc returns the cancel function for this task
func (t TaskFunc) WithCancelFunc(timeout time.Duration) (Task, context.CancelFunc) {
	context, cancelFunc := context.WithCancel(context.Background())

	return &task{
		f:          t,
		ctx:        context,
		cancelFunc: cancelFunc,
	}, cancelFunc
}

// WithPriority set the priority for this task
func (t TaskFunc) WithPriority(priority int) Task {
	return &task{
		f:        t,
		priority: priority,
	}
}

// BindScheduler bind the scheduler with this task, this shouldn't called by user
func (t TaskFunc) BindScheduler(s *Scheduler) Task {
	return &task{
		f:    t,
		sche: s,
	}
}

// SetContext set the context for this task, the context will used when call the internal function
func (t TaskFunc) SetContext(ctx context.Context) Task {
	return &task{
		f:   t,
		ctx: ctx,
	}
}

// task is the implement for Task
type task struct {
	f TaskFunc

	ctx        context.Context
	cancelFunc context.CancelFunc

	sche *Scheduler

	retryTimes uint
	timeout    time.Duration
	deadline   time.Time
	priority   int
}

// NewTask return a task
func NewTask(f TaskFunc) Task {
	return &task{
		f: f,
	}
}

// Do is the Task interface implementation
func (t *task) Do(ctx context.Context) error {
	return t.f.Do(ctx)
}

// WithRetry set the retry times for this task
func (t *task) WithRetry(times uint) Task {
	counter, originFunc := uint(0), t.f

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

// WithTimeout set the timeout for this task
func (t *task) WithTimeout(timeout time.Duration) Task {
	backgroundContext := context.Background()
	if t.ctx != nil {
		backgroundContext = t.ctx
	}
	t.deadline = time.Now().Add(timeout)
	context, cancelFunc := context.WithDeadline(backgroundContext, t.deadline)

	t.ctx = context
	t.cancelFunc = cancelFunc
	t.timeout = timeout

	return t
}

// WithCancelFunc returns the cancel function for this task
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

// WithPriority set the priority for this task
func (t *task) WithPriority(priority int) Task {
	t.priority = priority
	return t
}

// BindScheduler bind the scheduler with this task, this shouldn't called by user
func (t *task) BindScheduler(s *Scheduler) Task {
	t.sche = s
	return t
}

// SetContext set the context for this task, the context will used when call the internal function
func (t *task) SetContext(ctx context.Context) Task {
	if t.ctx != nil && t.ctx != ctx {
		log.Printf("[Warning] don't have the same context, use the lastest")
	}

	t.ctx = ctx
	return t
}
