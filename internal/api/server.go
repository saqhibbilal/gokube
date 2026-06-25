package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/gokube/gokube/internal/logs"
	"github.com/gokube/gokube/internal/metrics"
	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

// JobDeleter removes a Kubernetes Job by name. Optional for tests.
type JobDeleter interface {
	DeleteJob(ctx context.Context, name string) error
}

type Server struct {
	store      *store.Store
	queue      *queue.Queue
	k8s        JobDeleter
	logs       *logs.Store
	metrics    *metrics.Collector
	queueDepth func() int
	logger     *slog.Logger
	router     chi.Router
}

func NewServer(
	st *store.Store,
	q *queue.Queue,
	k8s JobDeleter,
	logStore *logs.Store,
	m *metrics.Collector,
	queueDepth func() int,
	logger *slog.Logger,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		store:      st,
		queue:      q,
		k8s:        k8s,
		logs:       logStore,
		metrics:    m,
		queueDepth: queueDepth,
		logger:     logger,
	}
	s.router = s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	r.Get("/health", s.handleHealth)
	r.Get("/metrics", s.handleMetrics)

	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", s.handleCreateJob)
		r.Get("/", s.handleListJobs)
		r.Get("/{id}", s.handleGetJob)
		r.Get("/{id}/logs", s.handleGetJobLogs)
		r.Delete("/{id}", s.handleDeleteJob)
	})

	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req models.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}

	if err := models.ValidateCreateRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	job, err := s.store.CreateJob(r.Context(), req.ToSpec())
	if err != nil {
		s.logger.Error("create job failed", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job")
		return
	}

	if err := s.store.UpdateJobState(r.Context(), job.ID, models.StateQueued); err != nil {
		s.logger.Error("enqueue transition failed", "error", err, "job_id", job.ID)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to queue job")
		return
	}
	job.Status.State = models.StateQueued

	if err := s.queue.Enqueue(r.Context(), job); err != nil {
		s.logger.Error("enqueue failed", "error", err, "job_id", job.ID)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to enqueue job")
		return
	}
	if s.metrics != nil {
		s.metrics.RecordEnqueued(job.ID)
	}

	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.CountJobsByState(r.Context())
	if err != nil {
		s.logger.Error("count jobs for metrics failed", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to collect metrics")
		return
	}

	depth := s.queue.Len()
	if s.queueDepth != nil {
		depth = s.queueDepth()
	}

	var snap metrics.Snapshot
	if s.metrics != nil {
		snap = s.metrics.Snapshot(
			depth,
			counts[models.StateQueued],
			counts[models.StateRunning],
			counts[models.StateSucceeded],
			counts[models.StateFailed],
		)
	} else {
		snap = metrics.Snapshot{
			QueueDepth:    depth,
			JobsQueued:    counts[models.StateQueued],
			JobsRunning:   counts[models.StateRunning],
			JobsSucceeded: counts[models.StateSucceeded],
			JobsFailed:    counts[models.StateFailed],
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(metrics.RenderPrometheus(snap)))
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("status")
	if state != "" && !isValidState(state) {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid status filter")
		return
	}

	jobs, err := s.store.ListJobs(r.Context(), state)
	if err != nil {
		s.logger.Error("list jobs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs")
		return
	}

	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	if err != nil {
		s.logger.Error("get job failed", "error", err, "id", id)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := s.store.GetJob(r.Context(), id); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	} else if err != nil {
		s.logger.Error("get job for logs failed", "error", err, "id", id)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get job logs")
		return
	}

	if s.logs == nil {
		writeJSON(w, http.StatusOK, models.JobLogsResponse{JobID: id, Lines: []string{}})
		return
	}

	writeJSON(w, http.StatusOK, s.logs.Get(id))
}

func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	if err != nil {
		s.logger.Error("get job for delete failed", "error", err, "id", id)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete job")
		return
	}

	if job.Status.K8sJobName != "" && s.k8s != nil {
		if err := s.k8s.DeleteJob(r.Context(), job.Status.K8sJobName); err != nil {
			s.logger.Error("kubernetes job delete failed", "error", err, "k8s_job", job.Status.K8sJobName)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete kubernetes job")
			return
		}
	}

	if err := s.store.DeleteJob(r.Context(), id); err != nil {
		s.logger.Error("delete job failed", "error", err, "id", id)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete job")
		return
	}

	if s.logs != nil {
		s.logs.Delete(id)
	}

	w.WriteHeader(http.StatusNoContent)
}

func isValidState(state string) bool {
	switch models.JobState(state) {
	case models.StatePending,
		models.StateQueued,
		models.StateScheduled,
		models.StateRunning,
		models.StateSucceeded,
		models.StateFailed,
		models.StateCancelled:
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, models.ErrorResponse{
		Error: message,
		Code:  code,
	})
}
