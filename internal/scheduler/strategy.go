package scheduler

import "github.com/gokube/gokube/internal/models"

// Strategy selects the next runnable job from a waiting set.
// Phase 2 uses channel FIFO order directly; this interface is used in Phase 3
// when jobs are buffered before scheduling decisions.
type Strategy interface {
	SelectNext(jobs []*models.Job) *models.Job
	Name() string
}

type FIFO struct{}

func (FIFO) Name() string { return "fifo" }

func (FIFO) SelectNext(jobs []*models.Job) *models.Job {
	if len(jobs) == 0 {
		return nil
	}
	return jobs[0]
}

type Priority struct{}

func (Priority) Name() string { return "priority" }

func (Priority) SelectNext(jobs []*models.Job) *models.Job {
	if len(jobs) == 0 {
		return nil
	}
	best := jobs[0]
	for _, job := range jobs[1:] {
		if job.Spec.Priority > best.Spec.Priority {
			best = job
		} else if job.Spec.Priority == best.Spec.Priority && job.CreatedAt.Before(best.CreatedAt) {
			best = job
		}
	}
	return best
}
