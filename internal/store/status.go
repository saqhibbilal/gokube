package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gokube/gokube/internal/models"
)

func (s *Store) ApplyJobStatus(ctx context.Context, id string, patch models.JobStatusPatch) error {
	current, err := s.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if models.IsTerminalState(current.Status.State) {
		return nil
	}
	if models.StateRank(patch.State) < models.StateRank(current.Status.State) {
		return nil
	}

	now := time.Now().UTC()
	start := nullTime(current.Status.StartTime)
	if patch.StartTime != nil {
		start = sql.NullString{String: formatTime(*patch.StartTime), Valid: true}
	}

	completion := sql.NullString{}
	if patch.CompletionTime != nil {
		completion = sql.NullString{String: formatTime(*patch.CompletionTime), Valid: true}
	}

	duration := current.Status.DurationMs
	if patch.SetDuration {
		duration = patch.DurationMs
	}

	restart := current.Status.RestartCount
	if patch.SetRestart {
		restart = patch.RestartCount
	}

	failure := sql.NullString{}
	if patch.SetFailure {
		if patch.FailureReason != "" {
			failure = sql.NullString{String: patch.FailureReason, Valid: true}
		}
	} else if current.Status.FailureReason != "" {
		failure = sql.NullString{String: current.Status.FailureReason, Valid: true}
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE jobs SET
			state = ?,
			start_time = ?,
			completion_time = ?,
			duration_ms = ?,
			restart_count = ?,
			failure_reason = ?,
			updated_at = ?
		WHERE id = ?`,
		string(patch.State),
		start,
		completion,
		duration,
		restart,
		failure,
		formatTime(now),
		id,
	)
	if err != nil {
		return fmt.Errorf("apply job status: %w", err)
	}
	return nil
}

// PrepareJobRetry increments restart_count and resets the job to Queued for rescheduling.
func (s *Store) PrepareJobRetry(ctx context.Context, id string) (*models.Job, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET
			state = ?,
			k8s_job_name = NULL,
			start_time = NULL,
			completion_time = NULL,
			duration_ms = 0,
			failure_reason = NULL,
			restart_count = restart_count + 1,
			updated_at = ?
		WHERE id = ? AND state = ?`,
		string(models.StateQueued),
		formatTime(now),
		id,
		string(models.StateFailed),
	)
	if err != nil {
		return nil, fmt.Errorf("prepare job retry: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, ErrNotFound
	}
	return s.GetJob(ctx, id)
}

func nullTime(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: formatTime(*t), Valid: true}
}
