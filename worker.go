package scheduler

type Worker interface {
	Work()
}

// Worker represents a working goroutine.
type goroutineWorker struct {
	sche   *Scheduler
	task   chan Task
	stopCh chan struct{}
}

func NewGoroutineWorker(s *Scheduler, stopCh chan struct{}) Worker {
	return &goroutineWorker{
		sche:   s,
		task:   make(chan Task),
		stopCh: stopCh,
	}
}

// StartWorker create a new worker.
func (s *Scheduler) startWorker(stopCh chan struct{}) {
	worker := NewGoroutineWorker(s, stopCh)

	go worker.Work()
}

// Worker's main loop.
func (w *goroutineWorker) Work() {
	wrapper := func(t Task) {
		realTask := t.(*task)

		defer func() {
			if r := recover(); r != nil {
				realTask.sche.queue.Done(t)
				return
			}
		}()

		select {
		case <-realTask.ctx.Done():
			realTask.sche.queue.Done(t)
			realTask.cancelFunc()
			return
		default:
		}

		realTask.Do(realTask.ctx)
		realTask.sche.queue.Done(t)
	}

	w.sche.transport.Workers() <- w.task

	for {
		select {
		case t := <-w.task:
			wrapper(t)
			w.sche.transport.Workers() <- w.task
		case <-w.stopCh:
			close(w.task)
			return
		}
	}
}
