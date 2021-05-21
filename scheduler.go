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
	queue    chan Task
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
		queue:    make(chan Task, 8080),
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
			task := <-s.queue
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
func (s *Scheduler) Schedule(ctx context.Context, task Task) error {
	if err := s.isShutdown(); err != nil {
		return err
	}

	t := &taskWrapper{task, ctx}
	s.queue <- t
	return nil
}

func (s *Scheduler) Stop() {
	s.stop.Do(func() {
		close(s.shutdown)
	})
}

func (s *Scheduler) stopGracefully() {
	// todo
}
