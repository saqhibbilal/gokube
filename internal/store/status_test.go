package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gokube/gokube/internal/models"
)

func TestApplyJobStatusForwardOnly(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "status.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	job, err := st.CreateJob(ctx, models.JobSpec{
		Name: "x", Image: "img", Command: []string{"echo"}, CPU: "100m", Memory: "128Mi",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = st.MarkJobScheduled(ctx, job.ID, "gokube-abc")

	start := time.Now().UTC()
	if err := st.ApplyJobStatus(ctx, job.ID, models.JobStatusPatch{
		State:     models.StateRunning,
		StartTime: &start,
	}); err != nil {
		t.Fatalf("apply running: %v", err)
	}

	// regress state should be ignored
	if err := st.ApplyJobStatus(ctx, job.ID, models.JobStatusPatch{
		State: models.StateScheduled,
	}); err != nil {
		t.Fatalf("apply scheduled: %v", err)
	}

	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.State != models.StateRunning {
		t.Fatalf("state = %s", got.Status.State)
	}
}

func TestPrepareJobRetry(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "retry.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	job, _ := st.CreateJob(ctx, models.JobSpec{
		Name: "x", Image: "img", Command: []string{"echo"}, CPU: "100m", Memory: "128Mi",
	})
	_ = st.MarkJobScheduled(ctx, job.ID, "gokube-abc")
	_ = st.ApplyJobStatus(ctx, job.ID, models.JobStatusPatch{State: models.StateFailed, SetFailure: true, FailureReason: "err"})

	retried, err := st.PrepareJobRetry(ctx, job.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retried.Status.State != models.StateQueued {
		t.Fatalf("state = %s", retried.Status.State)
	}
	if retried.Status.RestartCount != 1 {
		t.Fatalf("restart_count = %d", retried.Status.RestartCount)
	}
	if retried.Status.K8sJobName != "" {
		t.Fatal("expected cleared k8s job name")
	}
}
