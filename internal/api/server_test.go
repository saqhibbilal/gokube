package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gokube/gokube/internal/logs"
	"github.com/gokube/gokube/internal/models"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/store"
)

func TestCreateAndGetJob(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	q := queue.New(8)
	srv := NewServer(st, q, nil, logs.NewStore(100), nil, nil, slog.Default())

	body := `{
		"name": "hello",
		"image": "python:3.11",
		"command": ["python", "-c", "print(1)"],
		"cpu": "250m",
		"memory": "256Mi",
		"priority": 0,
		"max_retries": 0
	}`

	createReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	createRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	var created models.Job
	if err := json.NewDecoder(createRec.Body).Decode(&created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	if created.Status.State != models.StateQueued {
		t.Fatalf("expected Queued, got %s", created.Status.State)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	getRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d", getRec.Code)
	}
}

func TestCreateJobValidationError(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	q := queue.New(8)
	srv := NewServer(st, q, nil, logs.NewStore(100), nil, nil, slog.Default())

	body := `{"name":"","image":"x","command":["echo"],"cpu":"1","memory":"1Gi"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "api-test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}
