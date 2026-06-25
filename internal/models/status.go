package models

import "time"

// JobStatusPatch carries optional fields for a partial job status update.
type JobStatusPatch struct {
	State          JobState
	StartTime      *time.Time
	CompletionTime *time.Time
	DurationMs     int64
	RestartCount   int
	FailureReason  string
	SetFailure     bool
	SetRestart     bool
	SetDuration    bool
}

func IsTerminalState(state JobState) bool {
	switch state {
	case StateSucceeded, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

func StateRank(state JobState) int {
	switch state {
	case StatePending:
		return 0
	case StateQueued:
		return 1
	case StateScheduled:
		return 2
	case StateRunning:
		return 3
	case StateSucceeded, StateFailed:
		return 4
	case StateCancelled:
		return 5
	default:
		return -1
	}
}
