package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gokube/gokube/internal/models"
)

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
