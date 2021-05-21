package scheduler

import "log"

type Work interface {
	Work()
}

// Worker represents a working goroutine.
type goroutineWorker struct {
	sche *Scheduler
	task chan Task
}

// StartWorker create a new worker.
func (s *Scheduler) startWorker() {
	worker := &goroutineWorker{
		sche: s,
		task: make(chan Task),
	}

	go worker.Work()
}

// Worker's main loop.
func (w *goroutineWorker) Work() {
	wrapper := func(task Task) {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()

		t := task.(*taskWrapper)

		select {
		case <-t.ctx.Done():
			return
		default:
		}

		err := task.Do(t.ctx)
		if err != nil {
			log.Println("[Task]", err)
			w.sche.Schedule(t.ctx, t.task)
		}
	}

	w.sche.workers <- w.task

	for t := range w.task {
		wrapper(t)
		w.sche.workers <- w.task
	}
}
