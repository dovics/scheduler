package scheduler

type Transport interface {
	Workers() chan chan Task
	Close()
}

type memoryTransport struct {
	workers chan chan Task
}

func NewMemoryTransport() Transport {
	return &memoryTransport{
		workers: make(chan chan Task),
	}
}

func (t *memoryTransport) Workers() chan chan Task {
	return t.workers
}

func (t *memoryTransport) Close() {
	close(t.workers)
}
