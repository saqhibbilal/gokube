package metrics

import (
	"fmt"
	"strings"
)

// RenderPrometheus returns metrics in Prometheus text exposition format.
func RenderPrometheus(s Snapshot) string {
	var b strings.Builder

	writeGauge := func(name, help string, value float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&b, "# TYPE %s gauge\n", name)
		fmt.Fprintf(&b, "%s %g\n", name, value)
	}
	writeCounter := func(name, help string, value uint64) {
		fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&b, "# TYPE %s counter\n", name)
		fmt.Fprintf(&b, "%s %d\n", name, value)
	}

	writeGauge("gokube_queue_depth", "Jobs waiting in channel and scheduler buffer", float64(s.QueueDepth))
	writeGauge("gokube_jobs_queued", "Jobs in Queued state", float64(s.JobsQueued))
	writeGauge("gokube_jobs_running", "Jobs in Running state", float64(s.JobsRunning))
	writeGauge("gokube_jobs_succeeded", "Jobs in Succeeded state", float64(s.JobsSucceeded))
	writeGauge("gokube_jobs_failed", "Jobs in Failed state", float64(s.JobsFailed))

	writeCounter("gokube_jobs_submitted_total", "Total jobs submitted via API", s.JobsSubmittedTotal)
	writeCounter("gokube_jobs_scheduled_total", "Total jobs scheduled to Kubernetes", s.JobsScheduledTotal)
	writeCounter("gokube_jobs_succeeded_total", "Total jobs completed successfully", s.JobsSucceededTotal)
	writeCounter("gokube_jobs_failed_total", "Total jobs that failed permanently", s.JobsFailedTotal)

	writeGauge("gokube_scheduler_latency_seconds_avg", "Average seconds from enqueue to Kubernetes Job creation", s.SchedulerLatencySeconds)
	writeGauge("gokube_job_runtime_seconds_avg", "Average successful job runtime in seconds", s.JobRuntimeSeconds)

	return b.String()
}
