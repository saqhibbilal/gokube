package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gokube/gokube/internal/metrics"
	"github.com/gokube/gokube/internal/queue"
)

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	q := queue.New(4)
	m := metrics.New()
	srv := NewServer(st, q, nil, nil, m, func() int { return q.Len() }, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !containsMetric(rec.Body.String(), "gokube_queue_depth") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestCreateJobRecordsMetrics(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	q := queue.New(4)
	m := metrics.New()
	srv := NewServer(st, q, nil, nil, m, nil, slog.Default())

	body := `{"name":"m","image":"python:3.11","command":["echo"],"cpu":"100m","memory":"128Mi"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}

	snap := m.Snapshot(0, 0, 0, 0, 0)
	if snap.JobsSubmittedTotal != 1 {
		t.Fatalf("submitted = %d", snap.JobsSubmittedTotal)
	}
}

func containsMetric(s, sub string) bool {
	return len(s) >= len(sub) && indexOfMetric(s, sub) >= 0
}

func indexOfMetric(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
