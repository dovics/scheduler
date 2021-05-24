package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	taskNum := 10
	counter := 0
	s := New()
	go s.Start(2)

	for i := 0; i < taskNum; i++ {
		s.Schedule(TaskFunc(func(ctx context.Context) error {
			counter++
			return nil
		}))
	}

	s.Wait()
	s.Stop()

	if counter != taskNum {
		t.Errorf("counter is expected as %d, actually %d", taskNum, counter)
	}
}

func TestTaskCrash(t *testing.T) {
	taskNum := 10
	counter := 0
	s := New()
	go s.Start(2)

	for i := 0; i < taskNum+2; i++ {
		s.Schedule(TaskFunc(func(ctx context.Context) error {
			counter++
			panic("panic")
		}))
	}

	s.Wait()
	s.Stop()

	if counter != taskNum+2 {
		t.Errorf("counter is expected as %d, actually %d", taskNum+2, counter)
	}
}

func TestCancel(t *testing.T) {
	counter := 0
	s := New()
	go s.Start(2)

	f := func(ctx context.Context) error {
		time.Sleep(2000)
		return nil
	}

	for i := 0; i < 2; i++ {
		s.Schedule(TaskFunc(f))
	}

	s.Schedule(TaskFunc(func(ctx context.Context) error {
		select {
		case <-time.After(1000):
			counter++
		case <-ctx.Done():
		}
		return nil
	}).WithTimeout(1000))

	s.Wait()
	s.Stop()

	if counter != 0 {
		t.Errorf("counter is expected as %d, actually %d", 0, counter)
	}
}

func TestRetry(t *testing.T) {
	taskNum := 10
	counter := 0
	retryTimes := uint(10)
	s := New()
	go s.Start(2)
	f := func(ctx context.Context) error {
		counter++
		return errors.New("test retry")
	}

	for i := 0; i < taskNum; i++ {
		s.Schedule(TaskFunc(f).WithRetry(retryTimes))
	}

	s.Wait()
	s.Stop()

	if counter != taskNum*(int(retryTimes)+1) {
		t.Errorf("counter is expected as %d, actually %d", taskNum*(int(retryTimes)+1), counter)
	}
}

func TestDuplicate(t *testing.T) {
	counter := 0
	task := NewTask(TaskFunc(func(ctx context.Context) error {
		counter++
		return nil
	}))

	s := New()
	go s.Start(2)
	for i := 0; i < 10; i++ {
		s.Schedule(task)
	}

	s.Wait()
	s.Stop()
	if counter != 1 {
		t.Errorf("counter is expected as %d, actually %d", 1, counter)
	}
}

func TestPriority(t *testing.T) {
	taskNum := 10
	counter := 0
	priority := 0

	type key string
	var priorityKey key = "priority"
	s := New()
	if err := s.SortByPriority(); err != nil {
		t.Fatal(err)
	}
	go s.Start(1)

	f := func(ctx context.Context) error {
		counter++
		priority := ctx.Value(priorityKey)
		if counter != priority {
			t.Errorf("counter is expected as %d, actually %d", priority, counter)
		}

		return nil
	}

	for i := 0; i < taskNum; i++ {
		priority++
		ctx := context.Background()

		ctx = context.WithValue(ctx, priorityKey, priority)
		s.ScheduleWithCtx(ctx, TaskFunc(f).WithPriority(priority))
	}

	s.Wait()
	s.Stop()
}

func TestFIFO(t *testing.T) {
	taskNum := 10
	counter := 0
	priority := 0

	type key string
	var priorityKey key = "priority"
	s := New()
	go s.Start(1)

	f := func(ctx context.Context) error {
		counter++
		priority := ctx.Value(priorityKey)
		if counter != priority {
			t.Errorf("counter is expected as %d, actually %d", priority, counter)
		}

		return nil
	}

	for i := 0; i < taskNum; i++ {
		priority++
		ctx := context.Background()

		ctx = context.WithValue(ctx, priorityKey, priority)
		s.ScheduleWithCtx(ctx, TaskFunc(f))
	}

	s.Wait()
	s.Stop()
}
