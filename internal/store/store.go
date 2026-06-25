package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/gokube/gokube/internal/models"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	image TEXT NOT NULL,
	command TEXT NOT NULL,
	cpu TEXT NOT NULL,
	memory TEXT NOT NULL,
	priority INTEGER NOT NULL DEFAULT 0,
	max_retries INTEGER NOT NULL DEFAULT 0,
	state TEXT NOT NULL,
	k8s_job_name TEXT,
	start_time TEXT,
	completion_time TEXT,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	restart_count INTEGER NOT NULL DEFAULT 0,
	failure_reason TEXT,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
`

var ErrNotFound = errors.New("job not found")

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) CreateJob(ctx context.Context, spec models.JobSpec) (*models.Job, error) {
	now := time.Now().UTC()
	job := &models.Job{
		ID:   uuid.NewString(),
		Spec: spec,
		Status: models.JobStatus{
			State: models.StatePending,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	commandJSON, err := json.Marshal(spec.Command)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobs (
			id, name, image, command, cpu, memory, priority, max_retries,
			state, restart_count, duration_ms, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)`,
		job.ID,
		spec.Name,
		spec.Image,
		string(commandJSON),
		spec.CPU,
		spec.Memory,
		spec.Priority,
		spec.MaxRetries,
		string(models.StatePending),
		formatTime(now),
		formatTime(now),
	)
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}

	return job, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*models.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, image, command, cpu, memory, priority, max_retries,
		       state, k8s_job_name, start_time, completion_time, duration_ms,
		       restart_count, failure_reason, created_at, updated_at
		FROM jobs WHERE id = ?`, id)

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (s *Store) ListJobs(ctx context.Context, stateFilter string) ([]models.Job, error) {
	query := `
		SELECT id, name, image, command, cpu, memory, priority, max_retries,
		       state, k8s_job_name, start_time, completion_time, duration_ms,
		       restart_count, failure_reason, created_at, updated_at
		FROM jobs`

	args := []any{}
	if stateFilter != "" {
		query += " WHERE state = ?"
		args = append(args, stateFilter)
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []models.Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (*models.Job, error) {
	var (
		job           models.Job
		commandJSON   string
		state         string
		k8sJobName    sql.NullString
		startTime     sql.NullString
		completionTime sql.NullString
		failureReason sql.NullString
		createdAt     string
		updatedAt     string
	)

	err := row.Scan(
		&job.ID,
		&job.Spec.Name,
		&job.Spec.Image,
		&commandJSON,
		&job.Spec.CPU,
		&job.Spec.Memory,
		&job.Spec.Priority,
		&job.Spec.MaxRetries,
		&state,
		&k8sJobName,
		&startTime,
		&completionTime,
		&job.Status.DurationMs,
		&job.Status.RestartCount,
		&failureReason,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan job: %w", err)
	}

	if err := json.Unmarshal([]byte(commandJSON), &job.Spec.Command); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}

	job.Status.State = models.JobState(state)
	if k8sJobName.Valid {
		job.Status.K8sJobName = k8sJobName.String
	}
	if startTime.Valid {
		t, err := parseTime(startTime.String)
		if err != nil {
			return nil, err
		}
		job.Status.StartTime = &t
	}
	if completionTime.Valid {
		t, err := parseTime(completionTime.String)
		if err != nil {
			return nil, err
		}
		job.Status.CompletionTime = &t
	}
	if failureReason.Valid {
		job.Status.FailureReason = failureReason.String
	}

	var parseErr error
	job.CreatedAt, parseErr = parseTime(createdAt)
	if parseErr != nil {
		return nil, parseErr
	}
	job.UpdatedAt, parseErr = parseTime(updatedAt)
	if parseErr != nil {
		return nil, parseErr
	}

	return &job, nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time value")
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time %q: %w", value, err)
	}
	return t.UTC(), nil
}
