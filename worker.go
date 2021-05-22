package scheduler

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

	w.sche.workers <- w.task

	for t := range w.task {
		// log.Println("runtask")
		wrapper(t)
		w.sche.workers <- w.task
	}
}
