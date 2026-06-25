package logs

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

// PodLogs streams pod stdout/stderr into the log store.
type PodLogs interface {
	StreamPodLogs(ctx context.Context, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
	Namespace() string
}

// Streamer follows pod logs for running jobs.
type Streamer struct {
	k8s    PodLogs
	store  *Store
	logger *slog.Logger
	active sync.Map
}

func NewStreamer(k8s PodLogs, store *Store, logger *slog.Logger) *Streamer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Streamer{k8s: k8s, store: store, logger: logger}
}

func (s *Streamer) follow(ctx context.Context, jobID, podName string) {
	opts := &corev1.PodLogOptions{Follow: true}
	stream, err := s.k8s.StreamPodLogs(ctx, podName, opts)
	if err != nil {
		s.logger.Warn("open log stream failed", "job_id", jobID, "pod", podName, "error", err)
		return
	}
	defer stream.Close()

	reader := bufio.NewReader(stream)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if line != "" {
				s.store.Append(jobID, line)
			}
		}
		if err != nil {
			if err != io.EOF {
				s.logger.Debug("log stream ended", "job_id", jobID, "error", err)
			}
			return
		}
	}
}
