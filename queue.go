package scheduler

import "sync"

type Queue interface {
	Add(t Task)
	Get() Task
	Done(t Task)
	IsEmpty() bool
}

type chanQueue chan Task

func NewChanQueue() Queue {
	return chanQueue(make(chan Task, 9090))
}
func (q chanQueue) Add(t Task) {
	q <- t
}

func (q chanQueue) Get() Task {
	return <-q
}

func (q chanQueue) Done(t Task) {}

func (q chanQueue) IsEmpty() bool { return false }

type Type struct {
	queue   []Task
	running set
	dirty   set
	cond    *sync.Cond
}

func NewQueue() Queue {
	return &Type{
		running: set{},
		dirty:   set{},
		cond:    sync.NewCond(&sync.Mutex{}),
	}
}

func (q *Type) Add(t Task) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.dirty.has(t) {
		return
	}

	q.dirty.insert(t)
	if q.running.has(t) {
		return
	}

	q.queue = append(q.queue, t)
	q.cond.Signal()
}

func (q *Type) Get() Task {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 {
		q.cond.Wait()
	}

	var t Task
	t, q.queue = q.queue[0], q.queue[1:]

	q.running.insert(t)
	q.dirty.delete(t)

	return t
}

func (q *Type) Done(t Task) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.running.delete(t)
	if q.dirty.has(t) {
		q.queue = append(q.queue, t)
		q.cond.Signal()
	}
}

func (q *Type) IsEmpty() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if len(q.running) == 0 && len(q.queue) == 0 {
		return true
	}

	return false
}

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}
