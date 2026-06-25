package queue

import (
	"context"
	"errors"
	"sync"

	"github.com/gokube/gokube/internal/models"
)

var ErrClosed = errors.New("queue is closed")

type Queue struct {
	ch     chan *models.Job
	mu     sync.RWMutex
	closed bool
}

func New(capacity int) *Queue {
	if capacity < 1 {
		capacity = 64
	}
	return &Queue{
		ch: make(chan *models.Job, capacity),
	}
}

func (q *Queue) Enqueue(ctx context.Context, job *models.Job) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return ErrClosed
	}
	q.mu.RUnlock()

	select {
	case q.ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) Dequeue(ctx context.Context) (*models.Job, error) {
	select {
	case job, ok := <-q.ch:
		if !ok {
			return nil, ErrClosed
		}
		return job, nil
	case <-ctx.Done():
		select {
		case job, ok := <-q.ch:
			if !ok {
				return nil, ErrClosed
			}
			return job, nil
		default:
			return nil, ctx.Err()
		}
	}
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.ch)
}

func (q *Queue) Len() int {
	return len(q.ch)
}
