package models

type JobLogsResponse struct {
	JobID     string   `json:"job_id"`
	Lines     []string `json:"lines"`
	Streaming bool     `json:"streaming"`
}
