# gokube

A learning project: a simplified Kubernetes-based ML job scheduler written in Go.

## Phase 1 (current)

REST API + SQLite persistence for job submission and inspection.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/jobs` | Create a job (state: `Pending`) |
| GET | `/jobs` | List jobs (`?status=Pending` optional) |
| GET | `/jobs/{id}` | Get job by ID |
| DELETE | `/jobs/{id}` | Delete a job |

### Run

```bash
go run ./cmd/server
```

Environment variables:

- `GOKUBE_PORT` (default `8080`)
- `GOKUBE_DB_PATH` (default `gokube.db`)

### Example

```bash
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hello-train",
    "image": "python:3.11",
    "command": ["python", "-c", "print(\"hello\")"],
    "cpu": "250m",
    "memory": "256Mi",
    "priority": 1,
    "max_retries": 2
  }'

curl -s http://localhost:8080/jobs
```
