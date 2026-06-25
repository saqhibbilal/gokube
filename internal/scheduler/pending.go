package scheduler

import (
	"sync"

	"github.com/gokube/gokube/internal/models"
)

type pendingBuffer struct {
	mu   sync.Mutex
	jobs []*models.Job
}

func newPendingBuffer() *pendingBuffer {
	return &pendingBuffer{jobs: []*models.Job{}}
}

func (p *pendingBuffer) Add(job *models.Job) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.jobs {
		if existing.ID == job.ID {
			return
		}
	}
	p.jobs = append(p.jobs, job)
}

func (p *pendingBuffer) Remove(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, job := range p.jobs {
		if job.ID == id {
			p.jobs = append(p.jobs[:i], p.jobs[i+1:]...)
			return true
		}
	}
	return false
}

func (p *pendingBuffer) Snapshot() []*models.Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*models.Job, len(p.jobs))
	copy(out, p.jobs)
	return out
}

func (p *pendingBuffer) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.jobs)
}
