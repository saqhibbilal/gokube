package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gokube/gokube/internal/models"
)

func main() {
	baseURL := flag.String("url", envString("GOKUBE_URL", "http://localhost:8080"), "gokube API base URL")
	examplesDir := flag.String("examples", "examples", "directory with sample job JSON files")
	pollInterval := flag.Duration("poll", 2*time.Second, "status poll interval")
	timeout := flag.Duration("timeout", 5*time.Minute, "max wait for jobs to finish")
	flag.Parse()

	specs, err := loadExampleJobs(*examplesDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load examples: %v\n", err)
		os.Exit(1)
	}

	// Submit a few duplicates to exercise queueing and concurrency.
	submitList := append([]models.CreateJobRequest{}, specs...)
	submitList = append(submitList, specs...)
	submitList = append(submitList, specs[0]) // extra sleep job

	fmt.Printf("Submitting %d jobs to %s\n", len(submitList), *baseURL)
	ids := make([]string, 0, len(submitList))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, spec := range submitList {
		wg.Add(1)
		go func(i int, spec models.CreateJobRequest) {
			defer wg.Done()
			id, err := submitJob(*baseURL, spec)
			if err != nil {
				fmt.Fprintf(os.Stderr, "submit %d (%s): %v\n", i, spec.Name, err)
				return
			}
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
			fmt.Printf("  submitted %-14s -> %s\n", spec.Name, id)
		}(i, spec)
	}
	wg.Wait()

	if len(ids) == 0 {
		fmt.Fprintln(os.Stderr, "no jobs submitted")
		os.Exit(1)
	}

	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		jobs, err := listJobs(*baseURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "list jobs: %v\n", err)
			os.Exit(1)
		}

		tracked := filterJobs(jobs, ids)
		printTable(tracked)

		if allTerminal(tracked) {
			fmt.Println("\nAll jobs reached a terminal state.")
			break
		}
		time.Sleep(*pollInterval)
	}

	metrics, err := fetchMetrics(*baseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch metrics: %v\n", err)
	} else {
		fmt.Println("\n--- /metrics (summary) ---")
		printMetricSummary(metrics)
	}
}

func loadExampleJobs(dir string) ([]models.CreateJobRequest, error) {
	names := []string{
		"sleep-job.json",
		"python-train.json",
		"failing-job.json",
		"resource-hog.json",
	}
	out := make([]models.CreateJobRequest, 0, len(names))
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var req models.CreateJobRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, req)
	}
	return out, nil
}

func submitJob(baseURL string, spec models.CreateJobRequest) (string, error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	resp, err := http.Post(baseURL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var job models.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return "", err
	}
	return job.ID, nil
}

func fetchMetrics(baseURL string) (string, error) {
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}

func listJobs(baseURL string) ([]models.Job, error) {
	resp, err := http.Get(baseURL + "/jobs")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var jobs []models.Job
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func filterJobs(jobs []models.Job, ids []string) []models.Job {
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	out := make([]models.Job, 0, len(ids))
	for _, job := range jobs {
		if _, ok := want[job.ID]; ok {
			out = append(out, job)
		}
	}
	return out
}

func allTerminal(jobs []models.Job) bool {
	if len(jobs) == 0 {
		return false
	}
	for _, job := range jobs {
		if !models.IsTerminalState(job.Status.State) {
			return false
		}
	}
	return true
}

func printTable(jobs []models.Job) {
	fmt.Printf("\n%-38s %-14s %-10s %8s %s\n", "ID", "NAME", "STATE", "RETRIES", "REASON")
	fmt.Println(strings.Repeat("-", 90))
	for _, job := range jobs {
		reason := job.Status.FailureReason
		if len(reason) > 24 {
			reason = reason[:24] + "..."
		}
		fmt.Printf("%-38s %-14s %-10s %8d %s\n",
			job.ID,
			truncate(job.Spec.Name, 14),
			job.Status.State,
			job.Status.RestartCount,
			reason,
		)
	}
}

func printMetricSummary(raw string) {
	keys := []string{
		"gokube_queue_depth",
		"gokube_jobs_queued",
		"gokube_jobs_running",
		"gokube_jobs_succeeded",
		"gokube_jobs_failed",
		"gokube_scheduler_latency_seconds_avg",
		"gokube_job_runtime_seconds_avg",
	}
	for _, key := range keys {
		if v, ok := parseMetricLine(raw, key); ok {
			fmt.Printf("  %s = %s\n", key, v)
		}
	}
}

func parseMetricLine(raw, key string) (string, bool) {
	prefix := key + " "
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix)), true
		}
	}
	return "", false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
