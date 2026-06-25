package k8s

import (
	"context"
	"fmt"
	"io"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gokube/gokube/internal/models"
)

// Client wraps the Kubernetes API for gokube scheduling.
type Client struct {
	clientset *kubernetes.Clientset
	namespace string
}

func NewClient(namespace, kubeconfig string) (*Client, error) {
	if namespace == "" {
		namespace = "gokube"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		if kubeconfig != "" {
			loadingRules.ExplicitPath = kubeconfig
		}
		config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	return &Client{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

func (c *Client) Namespace() string {
	return c.namespace
}

// ClusterCapacity returns total allocatable CPU and memory across nodes.
func (c *Client) ClusterCapacity(ctx context.Context) (Capacity, error) {
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return Capacity{}, fmt.Errorf("list nodes: %w", err)
	}

	var total Capacity
	for _, node := range nodes.Items {
		cpu := node.Status.Allocatable.Cpu()
		mem := node.Status.Allocatable.Memory()
		if cpu != nil {
			total.CPUMillicores += cpu.MilliValue()
		}
		if mem != nil {
			total.MemoryBytes += mem.Value()
		}
	}
	return total, nil
}

// CreateJob submits a Kubernetes Job for the gokube job and returns the Job name.
func (c *Client) CreateJob(ctx context.Context, job *models.Job) (string, error) {
	k8sJob := BuildJob(job, c.namespace)
	created, err := c.clientset.BatchV1().Jobs(c.namespace).Create(ctx, k8sJob, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create job: %w", err)
	}
	return created.Name, nil
}

// DeleteJob removes a Kubernetes Job by name.
func (c *Client) DeleteJob(ctx context.Context, name string) error {
	propagation := metav1.DeletePropagationBackground
	err := c.clientset.BatchV1().Jobs(c.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete job %q: %w", name, err)
	}
	return nil
}

// Clientset exposes the underlying Kubernetes clientset for informers.
func (c *Client) Clientset() kubernetes.Interface {
	return c.clientset
}

// StreamPodLogs opens a log stream for a pod in the controller namespace.
func (c *Client) StreamPodLogs(ctx context.Context, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	if opts == nil {
		opts = &corev1.PodLogOptions{}
	}
	req := c.clientset.CoreV1().Pods(c.namespace).GetLogs(podName, opts)
	return req.Stream(ctx)
}

// GetJob fetches a Kubernetes Job by name.
func (c *Client) GetJob(ctx context.Context, name string) (*batchv1.Job, error) {
	job, err := c.clientset.BatchV1().Jobs(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get job %q: %w", name, err)
	}
	return job, nil
}
