package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

const (
	wSize = 2
)

func TestScheduler(t *testing.T) {
	taskNum := 10
	counter := 0
	s := New(wSize)

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
	s := New(wSize)

	for i := 0; i < taskNum+wSize; i++ {
		s.Schedule(TaskFunc(func(ctx context.Context) error {
			counter++
			panic("panic")
		}))
	}

	s.Wait()
	s.Stop()

	if counter != taskNum+wSize {
		t.Errorf("counter is expected as %d, actually %d", taskNum+wSize, counter)
	}
}

func TestCancel(t *testing.T) {
	counter := 0
	s := New(wSize)

	f := func(ctx context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	}

	for i := 0; i < wSize; i++ {
		s.Schedule(TaskFunc(f))
	}

	s.Schedule(TaskFunc(func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			counter++
		case <-ctx.Done():
		}
		return nil
	}).WithTimeout(1 * time.Second))

	s.Wait()
	s.Stop()

	if counter != 0 {
		t.Errorf("counter is expected as %d, actually %d", 0, counter)
	}
}

func TestRetry(t *testing.T) {
	taskNum := 10
	counter := 0
	retryTimes := 10
	s := New(wSize)
	f := func(ctx context.Context) error {
		counter++
		return errors.New("test retry")
	}

	for i := 0; i < taskNum; i++ {
		s.Schedule(TaskFunc(f).WithRetry(retryTimes))
	}

	s.Wait()
	s.Stop()

	if counter != taskNum*(retryTimes+1) {
		t.Errorf("counter is expected as %d, actually %d", taskNum*(retryTimes+1), counter)
	}
}
