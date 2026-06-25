package logs

import (
	"sync"

	"github.com/gokube/gokube/internal/models"
)

// Store keeps a bounded ring buffer of log lines per job.
type Store struct {
	mu      sync.RWMutex
	maxLines int
	buffers map[string]*buffer
	streaming map[string]bool
}

type buffer struct {
	lines []string
}

func NewStore(maxLines int) *Store {
	if maxLines < 1 {
		maxLines = 500
	}
	return &Store{
		maxLines:  maxLines,
		buffers:   make(map[string]*buffer),
		streaming: make(map[string]bool),
	}
}

func (s *Store) Append(jobID, line string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := s.buffers[jobID]
	if buf == nil {
		buf = &buffer{lines: make([]string, 0, 64)}
		s.buffers[jobID] = buf
	}
	buf.lines = append(buf.lines, line)
	if len(buf.lines) > s.maxLines {
		buf.lines = buf.lines[len(buf.lines)-s.maxLines:]
	}
}

func (s *Store) SetStreaming(jobID string, active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active {
		s.streaming[jobID] = true
	} else {
		delete(s.streaming, jobID)
	}
}

func (s *Store) Get(jobID string) models.JobLogsResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := models.JobLogsResponse{
		JobID:     jobID,
		Lines:     []string{},
		Streaming: s.streaming[jobID],
	}
	if buf := s.buffers[jobID]; buf != nil {
		resp.Lines = append([]string(nil), buf.lines...)
	}
	return resp
}

func (s *Store) Delete(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buffers, jobID)
	delete(s.streaming, jobID)
}
