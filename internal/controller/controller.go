package controller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/gokube/gokube/internal/k8s"
	"github.com/gokube/gokube/internal/logs"
	"github.com/gokube/gokube/internal/metrics"
	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

type Event struct {
	Kind string
	Pod  *corev1.Pod
	Job  *batchv1.Job
}

type JobDeleter interface {
	DeleteJob(ctx context.Context, name string) error
}

type Config struct {
	Workers int
	Metrics *metrics.Collector
}

// Controller watches Kubernetes Jobs and Pods and syncs status into SQLite.
type Controller struct {
	store    *store.Store
	queue    *queue.Queue
	k8s      JobDeleter
	clientset kubernetes.Interface
	namespace string
	streamer *logs.Streamer
	events   chan Event
	workers  int
	metrics  *metrics.Collector
	logger   *slog.Logger

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func New(
	st *store.Store,
	q *queue.Queue,
	clientset kubernetes.Interface,
	k8sDeleter JobDeleter,
	namespace string,
	streamer *logs.Streamer,
	cfg Config,
	logger *slog.Logger,
) *Controller {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if logger == nil {
		logger = slog.Default()
	}
	if namespace == "" {
		namespace = "gokube"
	}

	return &Controller{
		store:     st,
		queue:     q,
		k8s:       k8sDeleter,
		clientset: clientset,
		namespace: namespace,
		streamer:  streamer,
		events:    make(chan Event, 256),
		workers:   cfg.Workers,
		metrics:   cfg.Metrics,
		logger:    logger,
		stopCh:    make(chan struct{}),
	}
}

func (c *Controller) Start(ctx context.Context) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		c.clientset,
		30*time.Second,
		informers.WithNamespace(c.namespace),
	)

	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueuePod(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueuePod(obj) },
	})

	jobInformer := factory.Batch().V1().Jobs().Informer()
	jobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueueJob(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueueJob(obj) },
	})

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.eventWorker(ctx, i)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		factory.Start(c.stopCh)
	}()

	c.logger.Info("controller started",
		"namespace", c.namespace,
		"workers", c.workers,
	)
}

func (c *Controller) Stop() {
	close(c.stopCh)
	c.wg.Wait()
	c.logger.Info("controller stopped")
}

func (c *Controller) enqueuePod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	if pod.Labels[k8s.LabelManaged] != k8s.ManagedByValue {
		return
	}
	select {
	case c.events <- Event{Kind: "pod", Pod: pod}:
	default:
		c.logger.Warn("event channel full, dropping pod event", "pod", pod.Name)
	}
}

func (c *Controller) enqueueJob(obj interface{}) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return
	}
	if job.Labels[k8s.LabelManaged] != k8s.ManagedByValue {
		return
	}
	select {
	case c.events <- Event{Kind: "job", Job: job}:
	default:
		c.logger.Warn("event channel full, dropping job event", "job", job.Name)
	}
}

func (c *Controller) eventWorker(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case event, ok := <-c.events:
			if !ok {
				return
			}
			switch event.Kind {
			case "pod":
				if event.Pod != nil {
					c.handlePod(ctx, event.Pod)
				}
			case "job":
				if event.Job != nil {
					c.handleJob(ctx, event.Job)
				}
			}
		}
	}
}

func (c *Controller) handlePod(ctx context.Context, pod *corev1.Pod) {
	jobID := pod.Labels[k8s.LabelJobID]
	if jobID == "" {
		return
	}

	job, err := c.store.GetJob(ctx, jobID)
	if err != nil {
		return
	}
	if models.IsTerminalState(job.Status.State) {
		return
	}

	patch, ok := patchFromPod(pod)
	if !ok {
		return
	}

	if err := c.store.ApplyJobStatus(ctx, jobID, patch); err != nil {
		c.logger.Error("apply pod status failed", "job_id", jobID, "error", err)
		return
	}

	switch patch.State {
	case models.StateRunning:
		c.streamer.EnsureFollowing(ctx, jobID, pod.Name)
	case models.StateSucceeded:
		c.streamer.Stop(jobID)
		if c.metrics != nil {
			c.metrics.RecordSucceeded(time.Duration(patch.DurationMs) * time.Millisecond)
		}
	case models.StateFailed:
		c.streamer.Stop(jobID)
		updated, err := c.store.GetJob(ctx, jobID)
		if err != nil {
			return
		}
		c.handleFailure(ctx, updated)
	}
}

func (c *Controller) handleJob(ctx context.Context, k8sJob *batchv1.Job) {
	jobID := k8sJob.Labels[k8s.LabelJobID]
	if jobID == "" {
		return
	}

	job, err := c.store.GetJob(ctx, jobID)
	if err != nil || models.IsTerminalState(job.Status.State) {
		return
	}

	if k8sJob.Status.Succeeded > 0 {
		_ = c.store.ApplyJobStatus(ctx, jobID, models.JobStatusPatch{State: models.StateSucceeded})
		c.streamer.Stop(jobID)
		if c.metrics != nil {
			c.metrics.RecordSucceeded(0)
		}
		return
	}
	if k8sJob.Status.Failed > 0 {
		_ = c.store.ApplyJobStatus(ctx, jobID, models.JobStatusPatch{
			State:         models.StateFailed,
			FailureReason: "kubernetes job failed",
			SetFailure:    true,
		})
		c.streamer.Stop(jobID)
		updated, err := c.store.GetJob(ctx, jobID)
		if err != nil {
			return
		}
		c.handleFailure(ctx, updated)
	}
}

func (c *Controller) handleFailure(ctx context.Context, job *models.Job) {
	if !ShouldRetry(job) {
		if c.metrics != nil {
			c.metrics.RecordFailed()
		}
		c.logger.Info("job failed permanently",
			"job_id", job.ID,
			"restart_count", job.Status.RestartCount,
			"max_retries", job.Spec.MaxRetries,
		)
		return
	}

	k8sName := job.Status.K8sJobName
	if k8sName != "" && c.k8s != nil {
		if err := c.k8s.DeleteJob(ctx, k8sName); err != nil {
			c.logger.Error("delete kubernetes job for retry failed",
				"job_id", job.ID,
				"k8s_job", k8sName,
				"error", err,
			)
			return
		}
	}

	retried, err := c.store.PrepareJobRetry(ctx, job.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		c.logger.Error("prepare job retry failed", "job_id", job.ID, "error", err)
		return
	}

	if err := c.queue.Enqueue(ctx, retried); err != nil {
		c.logger.Error("re-enqueue failed", "job_id", job.ID, "error", err)
		return
	}
	if c.metrics != nil {
		c.metrics.RecordEnqueued(retried.ID)
	}

	c.logger.Info("job scheduled for retry",
		"job_id", job.ID,
		"restart_count", retried.Status.RestartCount,
	)
}
