package controller

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gokube/gokube/internal/models"
)

func patchFromPod(pod *corev1.Pod) (models.JobStatusPatch, bool) {
	_, failureReason := containerStats(pod)

	switch pod.Status.Phase {
	case corev1.PodPending:
		if pod.Status.StartTime != nil {
			return models.JobStatusPatch{
				State:     models.StateRunning,
				StartTime: metaTimePtr(pod.Status.StartTime),
			}, true
		}
		return models.JobStatusPatch{State: models.StateScheduled}, true

	case corev1.PodRunning:
		patch := models.JobStatusPatch{
			State: models.StateRunning,
		}
		if pod.Status.StartTime != nil {
			patch.StartTime = metaTimePtr(pod.Status.StartTime)
		}
		return patch, true

	case corev1.PodSucceeded:
		end := completionTime(pod)
		patch := models.JobStatusPatch{
			State:          models.StateSucceeded,
			CompletionTime: end,
			SetDuration:    true,
		}
		if pod.Status.StartTime != nil {
			start := metaTimePtr(pod.Status.StartTime)
			patch.StartTime = start
			if end != nil && start != nil {
				patch.DurationMs = end.Sub(*start).Milliseconds()
			}
		}
		return patch, true

	case corev1.PodFailed:
		end := completionTime(pod)
		reason := failureReason
		if reason == "" {
			reason = "pod failed"
		}
		patch := models.JobStatusPatch{
			State:          models.StateFailed,
			CompletionTime: end,
			FailureReason:  reason,
			SetFailure:     true,
			SetDuration:    true,
		}
		if pod.Status.StartTime != nil {
			start := metaTimePtr(pod.Status.StartTime)
			patch.StartTime = start
			if end != nil && start != nil {
				patch.DurationMs = end.Sub(*start).Milliseconds()
			}
		}
		return patch, true

	default:
		return models.JobStatusPatch{}, false
	}
}

func containerStats(pod *corev1.Pod) (restartCount int, failureReason string) {
	for _, status := range pod.Status.ContainerStatuses {
		restartCount += int(status.RestartCount)
		if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
			failureReason = terminatedReason(status.State.Terminated)
		}
	}
	if failureReason == "" && pod.Status.Message != "" {
		failureReason = pod.Status.Message
	}
	return restartCount, failureReason
}

func terminatedReason(term *corev1.ContainerStateTerminated) string {
	if term.Reason != "" && term.Message != "" {
		return fmt.Sprintf("%s: %s", term.Reason, term.Message)
	}
	if term.Reason != "" {
		return term.Reason
	}
	if term.Message != "" {
		return term.Message
	}
	return fmt.Sprintf("exit code %d", term.ExitCode)
}

func completionTime(pod *corev1.Pod) *time.Time {
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil && !status.State.Terminated.FinishedAt.IsZero() {
			t := status.State.Terminated.FinishedAt.Time
			return &t
		}
	}
	return nil
}

func metaTimePtr(t *metav1.Time) *time.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	out := t.Time.UTC()
	return &out
}
