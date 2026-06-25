package k8s

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gokube/gokube/internal/models"
)

const (
	LabelJobID  = "gokube.io/job-id"
	LabelManaged = "app.kubernetes.io/managed-by"
	ManagedByValue = "gokube"
)

// JobName returns a DNS-safe Kubernetes Job name for a gokube job.
func JobName(jobID string) string {
	compact := strings.ReplaceAll(jobID, "-", "")
	if len(compact) > 8 {
		compact = compact[:8]
	}
	return fmt.Sprintf("gokube-%s", strings.ToLower(compact))
}

// BuildJob creates a batch/v1.Job for the given gokube job.
func BuildJob(job *models.Job, namespace string) *batchv1.Job {
	name := JobName(job.ID)
	backoff := int32(job.Spec.MaxRetries)
	if backoff < 0 {
		backoff = 0
	}

	cpuQty := resource.MustParse(job.Spec.CPU)
	memQty := resource.MustParse(job.Spec.Memory)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				LabelJobID:     job.ID,
				LabelManaged:   ManagedByValue,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelJobID:   job.ID,
						LabelManaged: ManagedByValue,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "worker",
							Image:   job.Spec.Image,
							Command: job.Spec.Command,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpuQty,
									corev1.ResourceMemory: memQty,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cpuQty,
									corev1.ResourceMemory: memQty,
								},
							},
						},
					},
				},
			},
		},
	}
}
