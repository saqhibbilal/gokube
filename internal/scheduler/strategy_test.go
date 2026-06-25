package scheduler

import (
	"testing"
	"time"

	"github.com/gokube/gokube/internal/models"
)

func TestFIFOSelectNext(t *testing.T) {
	t.Parallel()

	jobs := []*models.Job{
		{ID: "1", Spec: models.JobSpec{Priority: 1}, CreatedAt: time.Now()},
		{ID: "2", Spec: models.JobSpec{Priority: 5}, CreatedAt: time.Now()},
	}

	got := FIFO{}.SelectNext(jobs)
	if got == nil || got.ID != "1" {
		t.Fatalf("fifo pick = %v, want id 1", got)
	}
}

func TestPrioritySelectNext(t *testing.T) {
	t.Parallel()

	now := time.Now()
	jobs := []*models.Job{
		{ID: "low", Spec: models.JobSpec{Priority: 1}, CreatedAt: now},
		{ID: "high", Spec: models.JobSpec{Priority: 10}, CreatedAt: now.Add(time.Second)},
		{ID: "mid", Spec: models.JobSpec{Priority: 5}, CreatedAt: now},
	}

	got := Priority{}.SelectNext(jobs)
	if got == nil || got.ID != "high" {
		t.Fatalf("priority pick = %v, want id high", got)
	}
}
