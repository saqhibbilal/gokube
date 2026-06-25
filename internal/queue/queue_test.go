package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gokube/gokube/internal/models"
)

func testJob(id string) *models.Job {
	return &models.Job{
		ID: id,
		Spec: models.JobSpec{
			Name: "job-" + id,
		},
		Status: models.JobStatus{State: models.StateQueued},
	}
}

func TestEnqueueDequeueFIFO(t *testing.T) {
	t.Parallel()

	q := New(4)
	ctx := context.Background()

	if err := q.Enqueue(ctx, testJob("1")); err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	if err := q.Enqueue(ctx, testJob("2")); err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}

	first, err := q.Dequeue(ctx)
	if err != nil || first.ID != "1" {
		t.Fatalf("first dequeue: job=%v err=%v", first, err)
	}
	second, err := q.Dequeue(ctx)
	if err != nil || second.ID != "2" {
		t.Fatalf("second dequeue: job=%v err=%v", second, err)
	}
}

func TestCloseDrainsAndRejectsEnqueue(t *testing.T) {
	t.Parallel()

	q := New(2)
	ctx := context.Background()

	_ = q.Enqueue(ctx, testJob("1"))
	q.Close()

	if err := q.Enqueue(ctx, testJob("2")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on enqueue, got %v", err)
	}

	job, err := q.Dequeue(ctx)
	if err != nil || job.ID != "1" {
		t.Fatalf("drain: job=%v err=%v", job, err)
	}
	if _, err := q.Dequeue(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after drain, got %v", err)
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	t.Parallel()

	const n = 50
	q := New(n)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			if err := q.Enqueue(ctx, testJob(fmtID(i))); err != nil {
				t.Errorf("enqueue %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		seen[job.ID] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique jobs, got %d", n, len(seen))
	}
}

func TestDequeueRespectsContext(t *testing.T) {
	t.Parallel()

	q := New(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func fmtID(i int) string {
	return fmt.Sprintf("job-%d", i)
}
