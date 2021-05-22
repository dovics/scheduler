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
	queue     Queue
	transport Transport

	shutdown chan struct{}
	stop     sync.Once
}

// New a goroutine Scheduler.
func New(wsize int) *Scheduler {
	if wsize == 0 {
		wsize = runtime.NumCPU()
	}

	s := &Scheduler{
		queue:     NewQueue(),
		transport: NewMemoryTransport(),
		shutdown:  make(chan struct{}),
	}

	go s.start()

	for i := 0; i < wsize; i++ {
		s.startWorker(s.shutdown)
	}

	return s
}

// Starts the scheduling.
func (s *Scheduler) start() {
	for {
		select {
		case worker := <-s.transport.Workers():
			task := s.queue.Get()
			worker <- task
		case <-s.shutdown:
			return
		}
	}
}

//
func (s *Scheduler) isShutdown() bool {
	select {
	case <-s.shutdown:
		return true
	default:
	}

	return false
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

func (s *Scheduler) Stop() {
	s.stop.Do(func() {
		close(s.shutdown)
	})
}

func (s *Scheduler) Wait() {
	for !s.queue.IsEmpty() {
	}
}
