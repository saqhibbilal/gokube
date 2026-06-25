package models

import "time"

type JobState string

const (
	StatePending   JobState = "Pending"
	StateQueued    JobState = "Queued"
	StateScheduled JobState = "Scheduled"
	StateRunning   JobState = "Running"
	StateSucceeded JobState = "Succeeded"
	StateFailed    JobState = "Failed"
	StateCancelled JobState = "Cancelled"
)

type JobSpec struct {
	Name       string   `json:"name"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	CPU        string   `json:"cpu"`
	Memory     string   `json:"memory"`
	Priority   int      `json:"priority"`
	MaxRetries int      `json:"max_retries"`
}

type JobStatus struct {
	State         JobState   `json:"state"`
	K8sJobName    string     `json:"k8s_job_name,omitempty"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	CompletionTime *time.Time `json:"completion_time,omitempty"`
	DurationMs    int64      `json:"duration_ms,omitempty"`
	RestartCount  int        `json:"restart_count"`
	FailureReason string     `json:"failure_reason,omitempty"`
}

type Job struct {
	ID        string    `json:"id"`
	Spec      JobSpec   `json:"spec"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateJobRequest struct {
	Name       string   `json:"name"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	CPU        string   `json:"cpu"`
	Memory     string   `json:"memory"`
	Priority   int      `json:"priority"`
	MaxRetries int      `json:"max_retries"`
}

func (r CreateJobRequest) ToSpec() JobSpec {
	return JobSpec{
		Name:       r.Name,
		Image:      r.Image,
		Command:    r.Command,
		CPU:        r.CPU,
		Memory:     r.Memory,
		Priority:   r.Priority,
		MaxRetries: r.MaxRetries,
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
