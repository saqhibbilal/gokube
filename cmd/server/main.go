package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gokube/gokube/internal/api"
	"github.com/gokube/gokube/internal/k8s"
	"github.com/gokube/gokube/internal/queue"
	"github.com/gokube/gokube/internal/scheduler"
	"github.com/gokube/gokube/internal/store"
)

func main() {
	port := flag.Int("port", envInt("GOKUBE_PORT", 8080), "HTTP listen port")
	dbPath := flag.String("db", envString("GOKUBE_DB_PATH", "gokube.db"), "SQLite database path")
	queueSize := flag.Int("queue-size", envInt("GOKUBE_QUEUE_SIZE", 128), "job queue buffer size")
	workers := flag.Int("workers", envInt("GOKUBE_WORKERS", 4), "scheduler worker count")
	namespace := flag.String("namespace", envString("GOKUBE_NAMESPACE", "gokube"), "kubernetes namespace")
	kubeconfig := flag.String("kubeconfig", envString("KUBECONFIG", ""), "path to kubeconfig file")
	interval := flag.Duration("scheduler-interval", envDuration("GOKUBE_SCHEDULER_INTERVAL", 5*time.Second), "cluster resource refresh interval")
	strategyName := flag.String("strategy", envString("GOKUBE_STRATEGY", "fifo"), "scheduling strategy: fifo or priority")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	st, err := store.Open(*dbPath)
	if err != nil {
		logger.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	k8sClient, err := k8s.NewClient(*namespace, *kubeconfig)
	if err != nil {
		logger.Error("failed to create kubernetes client", "error", err)
		os.Exit(1)
	}

	jobQueue := queue.New(*queueSize)
	sched := scheduler.New(st, jobQueue, k8sClient, scheduler.Config{
		Workers:  *workers,
		Interval: *interval,
		Strategy: parseStrategy(*strategyName),
	}, logger)

	ctx := context.Background()
	sched.Start(ctx)

	srv := api.NewServer(st, jobQueue, k8sClient, logger)
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      srv.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("gokube server starting",
			"addr", httpServer.Addr,
			"db", *dbPath,
			"namespace", *namespace,
			"queue_size", *queueSize,
			"workers", *workers,
			"strategy", *strategyName,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		logger.Error("server error", "error", err)
		os.Exit(1)
	case sig := <-stop:
		logger.Info("shutdown signal received", "signal", sig.String())
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("http server stopped")

	sched.Stop()
	logger.Info("gokube stopped")
}

func parseStrategy(name string) scheduler.Strategy {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "priority":
		return scheduler.Priority{}
	default:
		return scheduler.FIFO{}
	}
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}
