package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gokube/gokube/internal/k8s"
	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

// K8sAPI is the subset of Kubernetes operations the scheduler needs.
type K8sAPI interface {
	Namespace() string
	ClusterCapacity(ctx context.Context) (k8s.Capacity, error)
	CreateJob(ctx context.Context, job *models.Job) (string, error)
	DeleteJob(ctx context.Context, name string) error
}

// Scheduler watches the queue, monitors cluster capacity, and creates Kubernetes Jobs.
type Scheduler struct {
	store    *store.Store
	queue    *queue.Queue
	k8s      K8sAPI
	strategy Strategy
	workers  int
	interval time.Duration
	logger   *slog.Logger

	pending *pendingBuffer
	pool    poolSnapshot
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

type poolSnapshot struct {
	mu   sync.RWMutex
	pool k8s.Capacity
}

func (p *poolSnapshot) Set(c k8s.Capacity) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pool = c
}

func (p *poolSnapshot) Get() k8s.Capacity {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pool
}

type Config struct {
	Workers  int
	Interval time.Duration
	Strategy Strategy
}

func New(st *store.Store, q *queue.Queue, client K8sAPI, cfg Config, logger *slog.Logger) *Scheduler {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.Strategy == nil {
		cfg.Strategy = FIFO{}
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Scheduler{
		store:    st,
		queue:    q,
		k8s:      client,
		strategy: cfg.Strategy,
		workers:  cfg.Workers,
		interval: cfg.Interval,
		logger:   logger,
		pending:  newPendingBuffer(),
	}
}

func (s *Scheduler) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	s.wg.Add(1)
	go s.watchQueue()

	s.wg.Add(1)
	go s.monitorResources(ctx)

	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(ctx, i)
	}

	s.logger.Info("scheduler started",
		"workers", s.workers,
		"interval", s.interval,
		"strategy", s.strategy.Name(),
		"namespace", s.k8s.Namespace(),
	)
}

// Stop closes the queue, cancels background loops, and waits for workers to finish.
func (s *Scheduler) Stop() {
	s.queue.Close()
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("scheduler stopped")
}

func (s *Scheduler) watchQueue() {
	defer s.wg.Done()

	for {
		job, err := s.queue.Dequeue(context.Background())
		if errors.Is(err, queue.ErrClosed) {
			return
		}
		if err != nil {
			s.logger.Error("queue dequeue failed", "error", err)
			continue
		}
		s.pending.Add(job)
		s.logger.Debug("job moved to pending buffer", "job_id", job.ID, "name", job.Spec.Name)
	}
}

func (s *Scheduler) monitorResources(ctx context.Context) {
	defer s.wg.Done()

	s.refreshResources(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshResources(ctx)
		}
	}
}

func (s *Scheduler) refreshResources(ctx context.Context) {
	total, err := s.k8s.ClusterCapacity(ctx)
	if err != nil {
		s.logger.Warn("cluster capacity refresh failed", "error", err)
		return
	}

	used, err := s.store.SumActiveResourceUsage(ctx)
	if err != nil {
		s.logger.Warn("active resource usage refresh failed", "error", err)
		return
	}

	available := total.Subtract(used)
	s.pool.Set(available)
	s.logger.Debug("resource pool updated",
		"cpu_millicores", available.CPUMillicores,
		"memory_bytes", available.MemoryBytes,
	)
}

func (s *Scheduler) worker(ctx context.Context, id int) {
	defer s.wg.Done()

	for {
		if ctx.Err() != nil && s.queueDrainedAndEmpty() {
			return
		}

		jobs := s.pending.Snapshot()
		if len(jobs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		pool := s.pool.Get()
		job := s.pickRunnable(pool, jobs)
		if job == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if !s.pending.Remove(job.ID) {
			continue
		}

		k8sName, err := s.k8s.CreateJob(ctx, job)
		if err != nil {
			s.logger.Error("kubernetes job creation failed",
				"worker", id,
				"job_id", job.ID,
				"error", err,
			)
			s.pending.Add(job)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := s.store.MarkJobScheduled(ctx, job.ID, k8sName); err != nil {
			s.logger.Error("mark job scheduled failed",
				"worker", id,
				"job_id", job.ID,
				"k8s_job", k8sName,
				"error", err,
			)
			_ = s.k8s.DeleteJob(ctx, k8sName)
			s.pending.Add(job)
			continue
		}

		s.refreshResources(ctx)
		s.logger.Info("job scheduled to kubernetes",
			"worker", id,
			"job_id", job.ID,
			"name", job.Spec.Name,
			"k8s_job", k8sName,
		)
	}
}

func (s *Scheduler) queueDrainedAndEmpty() bool {
	return s.pending.Len() == 0 && s.queue.Len() == 0
}

func (s *Scheduler) pickRunnable(pool k8s.Capacity, jobs []*models.Job) *models.Job {
	if s.strategy.Name() == (Priority{}).Name() {
		return s.pickPriorityRunnable(pool, jobs)
	}

	candidate := s.strategy.SelectNext(jobs)
	if candidate == nil {
		return nil
	}
	if pool.CanFit(candidate.Spec.CPU, candidate.Spec.Memory) {
		return candidate
	}
	return nil
}

func (s *Scheduler) pickPriorityRunnable(pool k8s.Capacity, jobs []*models.Job) *models.Job {
	remaining := append([]*models.Job(nil), jobs...)
	for len(remaining) > 0 {
		next := (Priority{}).SelectNext(remaining)
		if next == nil {
			return nil
		}
		if pool.CanFit(next.Spec.CPU, next.Spec.Memory) {
			return next
		}
		filtered := remaining[:0]
		for _, job := range remaining {
			if job.ID != next.ID {
				filtered = append(filtered, job)
			}
		}
		remaining = filtered
	}
	return nil
}
