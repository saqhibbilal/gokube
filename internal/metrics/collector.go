package metrics

import (
	"sync"
	"time"
)

// Collector tracks gokube operational metrics.
type Collector struct {
	mu sync.RWMutex

	queuedAt map[string]time.Time

	schedulerLatency rollingAvg
	jobRuntime       rollingAvg

	jobsSubmitted  uint64
	jobsScheduled  uint64
	jobsSucceeded  uint64
	jobsFailed     uint64
}

func New() *Collector {
	return &Collector{
		queuedAt: make(map[string]time.Time),
	}
}

func (c *Collector) RecordEnqueued(jobID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobsSubmitted++
	c.queuedAt[jobID] = time.Now()
}

func (c *Collector) RecordScheduled(jobID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.jobsScheduled++
	if start, ok := c.queuedAt[jobID]; ok {
		c.schedulerLatency.observe(time.Since(start).Seconds())
		delete(c.queuedAt, jobID)
	}
}

func (c *Collector) RecordSucceeded(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobsSucceeded++
	if duration > 0 {
		c.jobRuntime.observe(duration.Seconds())
	}
}

func (c *Collector) RecordFailed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.jobsFailed++
}

type rollingAvg struct {
	sum   float64
	count uint64
}

func (r *rollingAvg) observe(v float64) {
	r.sum += v
	r.count++
}

func (r *rollingAvg) average() float64 {
	if r.count == 0 {
		return 0
	}
	return r.sum / float64(r.count)
}

func (r *rollingAvg) count64() uint64 {
	return r.count
}

// Snapshot holds a point-in-time view for exposition.
type Snapshot struct {
	QueueDepth              int
	JobsQueued              int
	JobsRunning             int
	JobsSucceeded           int
	JobsFailed              int
	JobsSubmittedTotal      uint64
	JobsScheduledTotal      uint64
	JobsSucceededTotal      uint64
	JobsFailedTotal         uint64
	SchedulerLatencySeconds float64
	JobRuntimeSeconds       float64
}

func (c *Collector) Snapshot(queueDepth, jobsQueued, jobsRunning, jobsSucceeded, jobsFailed int) Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return Snapshot{
		QueueDepth:              queueDepth,
		JobsQueued:              jobsQueued,
		JobsRunning:             jobsRunning,
		JobsSucceeded:           jobsSucceeded,
		JobsFailed:              jobsFailed,
		JobsSubmittedTotal:      c.jobsSubmitted,
		JobsScheduledTotal:      c.jobsScheduled,
		JobsSucceededTotal:      c.jobsSucceeded,
		JobsFailedTotal:         c.jobsFailed,
		SchedulerLatencySeconds: c.schedulerLatency.average(),
		JobRuntimeSeconds:       c.jobRuntime.average(),
	}
}
