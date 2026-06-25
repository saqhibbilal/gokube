package metrics

import (
	"testing"
	"time"
)

func TestRecordSchedulerLatency(t *testing.T) {
	t.Parallel()

	c := New()
	c.mu.Lock()
	c.queuedAt["job-1"] = time.Now().Add(-100 * time.Millisecond)
	c.jobsSubmitted = 1
	c.mu.Unlock()

	c.RecordScheduled("job-1")
	snap := c.Snapshot(0, 0, 0, 0, 0)
	if snap.SchedulerLatencySeconds <= 0 {
		t.Fatalf("latency = %f", snap.SchedulerLatencySeconds)
	}
	if snap.JobsScheduledTotal != 1 {
		t.Fatalf("scheduled total = %d", snap.JobsScheduledTotal)
	}
}

func TestRenderPrometheus(t *testing.T) {
	t.Parallel()

	out := RenderPrometheus(Snapshot{
		QueueDepth:  2,
		JobsQueued:  3,
		JobsRunning: 1,
	})
	if !contains(out, "gokube_queue_depth 2") {
		t.Fatalf("missing queue_depth: %s", out)
	}
	if !contains(out, "gokube_jobs_queued 3") {
		t.Fatalf("missing jobs_queued: %s", out)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
