package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

// Stub processes queued jobs without talking to Kubernetes.
// It transitions jobs Queued → Scheduled to prove the pipeline works.
type Stub struct {
	store   *store.Store
	queue   *queue.Queue
	workers int
	logger  *slog.Logger
	wg      sync.WaitGroup
}

func NewStub(st *store.Store, q *queue.Queue, workers int, logger *slog.Logger) *Stub {
	if workers < 1 {
		workers = 3
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Stub{
		store:   st,
		queue:   q,
		workers: workers,
		logger:  logger,
	}
}

func (s *Stub) Start() {
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
	s.logger.Info("stub scheduler started", "workers", s.workers)
}

// Stop closes the queue and waits for workers to drain remaining jobs.
func (s *Stub) Stop() {
	s.queue.Close()
	s.wg.Wait()
	s.logger.Info("stub scheduler stopped")
}

func (s *Stub) worker(id int) {
	defer s.wg.Done()

	for {
		job, err := s.queue.Dequeue(context.Background())
		if errors.Is(err, queue.ErrClosed) {
			return
		}
		if err != nil {
			s.logger.Error("dequeue failed", "worker", id, "error", err)
			continue
		}

		if err := s.store.UpdateJobState(context.Background(), job.ID, models.StateScheduled); err != nil {
			s.logger.Error("update job state failed", "worker", id, "job_id", job.ID, "error", err)
			continue
		}

		s.logger.Info("stub scheduled job",
			"worker", id,
			"job_id", job.ID,
			"name", job.Spec.Name,
		)
	}
}
