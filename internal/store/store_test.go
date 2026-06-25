package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gokube/gokube/internal/models"
)

func TestStoreUpdateJobState(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	job, err := st.CreateJob(ctx, models.JobSpec{
		Name: "x", Image: "img", Command: []string{"echo"}, CPU: "1", Memory: "1Gi",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := st.UpdateJobState(ctx, job.ID, models.StateQueued); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.State != models.StateQueued {
		t.Fatalf("state = %s, want Queued", got.Status.State)
	}
}

func TestStoreMarkJobScheduled(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	job, err := st.CreateJob(ctx, models.JobSpec{
		Name: "x", Image: "img", Command: []string{"echo"}, CPU: "100m", Memory: "128Mi",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := st.MarkJobScheduled(ctx, job.ID, "gokube-abc12345"); err != nil {
		t.Fatalf("mark scheduled: %v", err)
	}

	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.State != models.StateScheduled {
		t.Fatalf("state = %s", got.Status.State)
	}
	if got.Status.K8sJobName != "gokube-abc12345" {
		t.Fatalf("k8s name = %q", got.Status.K8sJobName)
	}

	used, err := st.SumActiveResourceUsage(ctx)
	if err != nil {
		t.Fatalf("sum usage: %v", err)
	}
	if used.CPUMillicores != 100 {
		t.Fatalf("cpu used = %d", used.CPUMillicores)
	}
}

func TestStoreCRUD(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	spec := models.JobSpec{
		Name:       "job-a",
		Image:      "python:3.11",
		Command:    []string{"python", "-c", "print(1)"},
		CPU:        "250m",
		Memory:     "256Mi",
		Priority:   2,
		MaxRetries: 1,
	}

	created, err := st.CreateJob(ctx, spec)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if created.Status.State != models.StatePending {
		t.Fatalf("expected Pending, got %s", created.Status.State)
	}

	got, err := st.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Spec.Name != spec.Name || len(got.Spec.Command) != len(spec.Command) {
		t.Fatalf("unexpected job: %+v", got)
	}

	jobs, err := st.ListJobs(ctx, string(models.StatePending))
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	if err := st.DeleteJob(ctx, created.ID); err != nil {
		t.Fatalf("delete job: %v", err)
	}
	if _, err := st.GetJob(ctx, created.ID); err != ErrNotFound {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}
