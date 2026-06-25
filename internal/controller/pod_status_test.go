package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gokube/gokube/internal/models"
)

func TestPatchFromPodSucceeded(t *testing.T) {
	t.Parallel()

	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now())
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase:     corev1.PodSucceeded,
			StartTime: &start,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode:   0,
							FinishedAt: end,
						},
					},
				},
			},
		},
	}

	patch, ok := patchFromPod(pod)
	if !ok {
		t.Fatal("expected patch")
	}
	if patch.State != models.StateSucceeded {
		t.Fatalf("state = %s", patch.State)
	}
	if patch.DurationMs <= 0 {
		t.Fatalf("duration = %d", patch.DurationMs)
	}
}

func TestPatchFromPodFailed(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
							Reason:   "Error",
							Message:  "boom",
						},
					},
				},
			},
		},
	}

	patch, ok := patchFromPod(pod)
	if !ok || patch.State != models.StateFailed {
		t.Fatalf("patch = %+v ok=%v", patch, ok)
	}
	if patch.FailureReason == "" {
		t.Fatal("expected failure reason")
	}
}

func TestShouldRetry(t *testing.T) {
	t.Parallel()

	job := &models.Job{
		Spec:   models.JobSpec{MaxRetries: 2},
		Status: models.JobStatus{RestartCount: 1},
	}
	if !ShouldRetry(job) {
		t.Fatal("expected retry")
	}
	job.Status.RestartCount = 2
	if ShouldRetry(job) {
		t.Fatal("expected no retry")
	}
}
