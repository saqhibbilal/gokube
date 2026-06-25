package scheduler

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/gokube/gokube/internal/k8s"
	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

type fakeK8s struct {
	capacity k8s.Capacity
}

func (f *fakeK8s) Namespace() string { return "gokube" }

func (f *fakeK8s) ClusterCapacity(context.Context) (k8s.Capacity, error) {
	return f.capacity, nil
}

func (f *fakeK8s) CreateJob(_ context.Context, job *models.Job) (string, error) {
	return k8s.JobName(job.ID), nil
}

func (f *fakeK8s) DeleteJob(context.Context, string) error { return nil }

func TestSchedulerCreatesKubernetesJob(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sched.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	q := queue.New(8)
	client := &fakeK8s{capacity: k8s.Capacity{CPUMillicores: 4000, MemoryBytes: 8 * 1024 * 1024 * 1024}}
	sched := New(st, q, client, Config{Workers: 2, Interval: time.Hour}, slog.Default())
	sched.Start(context.Background())
	t.Cleanup(sched.Stop)

	ctx := context.Background()
	spec := models.JobSpec{
		Name: "train", Image: "python:3.11", Command: []string{"echo", "hi"},
		CPU: "100m", Memory: "128Mi",
	}
	job, err := st.CreateJob(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.UpdateJobState(ctx, job.ID, models.StateQueued); err != nil {
		t.Fatalf("queue: %v", err)
	}
	job.Status.State = models.StateQueued
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status.State == models.StateScheduled && got.Status.K8sJobName != "" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("job was not scheduled to kubernetes")
}
