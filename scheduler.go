package scheduler

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

var (
	// ErrScheduleTimeout happens when task schedule failed during the specific interval.
	ErrScheduleTimeout = errors.New("schedule not available currently")

	errSchedulerPoolStop = errors.New("pool stopped")
)

// Scheduler caches tasks and schedule tasks to work.
type Scheduler struct {
	queue    Queue
	workers  chan chan Task
	shutdown chan struct{}
	stop     sync.Once
}

// New a goroutine Scheduler.
func New(wsize int) *Scheduler {
	if wsize == 0 {
		wsize = runtime.NumCPU()
	}

	s := &Scheduler{
		queue:    NewQueue(),
		workers:  make(chan chan Task, wsize),
		shutdown: make(chan struct{}),
	}

	go s.start()

	for i := 0; i < wsize; i++ {
		s.startWorker()
	}

	return s
}

// Starts the scheduling.
func (s *Scheduler) start() {
	for {
		select {
		case worker := <-s.workers:
			task := s.queue.Get()
			worker <- task
		case <-s.shutdown:
			s.stopGracefully()
			return
		}
	}
}

func (s *Scheduler) isShutdown() error {
	select {
	case <-s.shutdown:
		return errSchedulerPoolStop
	default:
	}

	return nil
}

// Schedule push a task on queue.
func (s *Scheduler) ScheduleWithCtx(ctx context.Context, t Task) error {
	if err := s.isShutdown(); err != nil {
		return err
	}

	task := t.SetContext(ctx).BindScheduler(s)

	s.queue.Add(task)
	return nil
}

// Schedule push a task on queue.
func (s *Scheduler) Schedule(t Task) error {
	if err := s.isShutdown(); err != nil {
		return err
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
func (s *Scheduler) stopGracefully() {
	// todo
}
