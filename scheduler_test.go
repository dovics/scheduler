package scheduler

import (
	"context"
	"testing"
	"time"
)

const (
	wSize = 2
)

func TestScheduler(t *testing.T) {
	taskNum := 10
	counter := 0
	p := New(wSize)

	for i := 0; i < taskNum; i++ {
		p.Schedule(context.Background(), TaskFunc(func(ctx context.Context) error {
			counter++
			return nil
		}))
	}

	time.Sleep(2 * time.Second)

	p.Stop()

	if counter != taskNum {
		t.Errorf("counter is expected as %d, actually %d", taskNum, counter)
	}
}

func TestTaskCrash(t *testing.T) {
	taskNum := 10
	counter := 0
	p := New(wSize)

	for i := 0; i < taskNum+wSize; i++ {
		p.Schedule(context.Background(), TaskFunc(func(ctx context.Context) error {
			counter++
			panic("panic")
		}))
	}

	time.Sleep(2 * time.Second)

	p.Stop()

	if counter != taskNum+wSize {
		t.Errorf("counter is expected as %d, actually %d", taskNum+wSize, counter)
	}
}

func TestCancel(t *testing.T) {
	taskNum := 10
	counter := 0
	p := New(wSize)

	f := func(ctx context.Context) error {
		time.Sleep(3 * time.Second)
		return nil
	}

	for i := 0; i < taskNum; i++ {
		p.Schedule(context.Background(), TaskFunc(f))
	}

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	p.Schedule(ctx, TaskFunc(func(ctx context.Context) error {
		select {
		case <-time.After(1 * time.Second):
			counter++
		case <-ctx.Done():
		}
		return nil
	}))

	time.Sleep(2 * time.Second)
	p.Stop()

	if counter != 0 {
		t.Errorf("counter is expected as %d, actually %d", 0, counter)
	}
}
