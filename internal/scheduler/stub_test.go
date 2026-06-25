package scheduler

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

func TestStubSchedulerProcessesJobs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "stub.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	q := queue.New(8)
	sched := NewStub(st, q, 3, slog.Default())
	sched.Start()

	ctx := context.Background()
	spec := models.JobSpec{
		Name:    "train",
		Image:   "python:3.11",
		Command: []string{"echo", "hi"},
		CPU:     "100m",
		Memory:  "128Mi",
	}

	job, err := st.CreateJob(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.UpdateJobState(ctx, job.ID, models.StateQueued); err != nil {
		t.Fatalf("queue transition: %v", err)
	}
	job.Status.State = models.StateQueued

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got.Status.State == models.StateScheduled {
			sched.Stop()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	sched.Stop()
	t.Fatal("job was not transitioned to Scheduled")
}
