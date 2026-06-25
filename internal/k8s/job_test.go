package k8s

import (
	"testing"

	"github.com/gokube/gokube/internal/models"
)

func TestJobName(t *testing.T) {
	t.Parallel()

	name := JobName("d7cf6fe3-b901-41a5-b6a7-7293fe8392b5")
	if name != "gokube-d7cf6fe3" {
		t.Fatalf("job name = %q", name)
	}
}

func TestBuildJob(t *testing.T) {
	t.Parallel()

	job := &models.Job{
		ID: "d7cf6fe3-b901-41a5-b6a7-7293fe8392b5",
		Spec: models.JobSpec{
			Name:       "train",
			Image:      "python:3.11",
			Command:    []string{"python", "-c", "print(1)"},
			CPU:        "250m",
			Memory:     "256Mi",
			MaxRetries: 2,
		},
	}

	k8sJob := BuildJob(job, "gokube")
	if k8sJob.Namespace != "gokube" {
		t.Fatalf("namespace = %q", k8sJob.Namespace)
	}
	if k8sJob.Labels[LabelJobID] != job.ID {
		t.Fatalf("label job id = %q", k8sJob.Labels[LabelJobID])
	}
	if *k8sJob.Spec.BackoffLimit != 2 {
		t.Fatalf("backoff = %d", *k8sJob.Spec.BackoffLimit)
	}

	container := k8sJob.Spec.Template.Spec.Containers[0]
	if container.Image != "python:3.11" {
		t.Fatalf("image = %q", container.Image)
	}
	if container.Resources.Requests.Cpu().MilliValue() != 250 {
		t.Fatalf("cpu request = %d", container.Resources.Requests.Cpu().MilliValue())
	}
}
