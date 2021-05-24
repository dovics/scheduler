package scheduler

import (
	"container/heap"
	"sync"
)

type Queue interface {
	Add(t Task)
	Get() Task
	Done(t Task)
	SetCompareFunc(CompareFunc)
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

func (q chanQueue) SetCompareFunc(CompareFunc) {}

type Type struct {
	queue []Task

	running     set
	dirty       set
	cond        *sync.Cond
	compareFunc CompareFunc
}

type CompareFunc func(t1, t2 Task) bool

func NewQueue() Queue {
	q := &Type{
		queue:   []Task{},
		running: set{},
		dirty:   set{},
		cond:    sync.NewCond(&sync.Mutex{}),
	}

	return q
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

	if q.compareFunc == nil {
		q.queue = append(q.queue, t)
	} else {
		heap.Push(q, t)
	}

	q.cond.Signal()
}

func (q *Type) Get() Task {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 {
		q.cond.Wait()
	}

	var t Task
	if q.compareFunc == nil {
		t, q.queue = q.queue[0], q.queue[1:]
	} else {
		t = heap.Pop(q).(Task)
	}

	q.running.insert(t)
	q.dirty.delete(t)

	return t.(Task)
}

func (q *Type) Done(t Task) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.running.delete(t)
	if q.dirty.has(t) {
		if q.compareFunc == nil {
			q.queue = append(q.queue, t)
		} else {
			heap.Push(q, t)
		}
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

func (q *Type) SetCompareFunc(f CompareFunc) {
	q.compareFunc = f
	heap.Init(q)
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

type TaskHeap []Task

func (q *Type) Len() int {
	return len(q.queue)
}

func (q *Type) Less(i, j int) bool {
	if q.compareFunc == nil {
		panic("don't set compare function for Queue")
	}

	return q.compareFunc(q.queue[i], q.queue[j])
}

func (q *Type) Swap(i, j int) {
	q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
}

func (q *Type) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	q.queue = append(q.queue, x.(Task))
}

func (q *Type) Pop() interface{} {
	n := len(q.queue)
	x := q.queue[n-1]
	q.queue = q.queue[0 : n-1]
	return x
}

func CompareByPriority(t1, t2 Task) bool {
	return t1.(*task).priority < t2.(*task).priority
}
