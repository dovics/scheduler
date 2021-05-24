package scheduler

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

var (
	errSchedulerStop = errors.New("scheduler stopped")
)

// Scheduler caches tasks and schedule tasks to work.
type Scheduler struct {
	queue   Queue
	workers chan chan Task

	shutdown chan struct{}
	stop     sync.Once
}

// New a goroutine Scheduler.
func New() *Scheduler {
	return &Scheduler{
		queue:    NewQueue(),
		workers:  make(chan chan Task),
		shutdown: make(chan struct{}),
	}
}

// Starts the scheduling.
func (s *Scheduler) Start(wsize int) {
	if wsize == 0 {
		wsize = runtime.NumCPU()
	}
	for i := 0; i < wsize; i++ {
		s.startWorker(s.shutdown)
	}

	for {
		select {
		case worker := <-s.workers:
			task := s.queue.Get()
			worker <- task
		case <-s.shutdown:
			return
		}
	}
}

// isShutdown returns whether the schduler has shutdown
func (s *Scheduler) isShutdown() bool {
	select {
	case <-s.shutdown:
		return true
	default:
	}

	return false
}

// SortByPriority uses priority as the comparison factors
func (s *Scheduler) SortByPriority() error {
	if !s.queue.IsEmpty() {
		return errors.New("the scheduler has start, can't set compare function in runtime")
	}

	s.queue.SetCompareFunc(CompareByPriority)
	return nil
}

// SortByPriority uses deadline as the comparison factors
func (s *Scheduler) SortByDeadline() error {
	if !s.queue.IsEmpty() {
		return errors.New("the scheduler has start, can't set compare function in runtime")
	}

	s.queue.SetCompareFunc(CompareByDeadline)
	return nil
}

// Schedule push a task on queue.
func (s *Scheduler) ScheduleWithCtx(ctx context.Context, t Task) error {
	if s.isShutdown() {
		return errSchedulerStop
	}

	task := t.SetContext(ctx).BindScheduler(s)

	s.queue.Add(task)
	return nil
}

// Schedule push a task on queue.
func (s *Scheduler) Schedule(t Task) error {
	if s.isShutdown() {
		return errSchedulerStop
	}

	t = t.BindScheduler(s)

	if t, ok := t.(*task); ok {
		if t.ctx == nil {
			t.SetContext(context.Background())
		}
	}

	s.queue.Add(t)
	return nil
}

// Stop closes the schduler
func (s *Scheduler) Stop() {
	s.stop.Do(func() {
		close(s.shutdown)
	})
}

// Wait waits for all task finished
func (s *Scheduler) Wait() {
	for !s.queue.IsEmpty() {
	}
}
