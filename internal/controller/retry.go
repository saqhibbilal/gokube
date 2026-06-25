package controller

import "github.com/gokube/gokube/internal/models"

// ShouldRetry reports whether a failed job should be re-queued.
func ShouldRetry(job *models.Job) bool {
	return job.Status.RestartCount < job.Spec.MaxRetries
}
