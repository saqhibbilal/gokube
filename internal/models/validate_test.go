package models

import "testing"

func TestValidateCreateRequest(t *testing.T) {
	valid := CreateJobRequest{
		Name:       "train-1",
		Image:      "python:3.11",
		Command:    []string{"python", "-c", "print(1)"},
		CPU:        "500m",
		Memory:     "512Mi",
		Priority:   1,
		MaxRetries: 2,
	}

	if err := ValidateCreateRequest(valid); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}

	tests := []struct {
		name    string
		req     CreateJobRequest
		wantErr string
	}{
		{
			name:    "missing name",
			req:     CreateJobRequest{Image: "img", Command: []string{"echo"}, CPU: "1", Memory: "1Gi"},
			wantErr: "name is required",
		},
		{
			name:    "invalid cpu",
			req:     CreateJobRequest{Name: "x", Image: "img", Command: []string{"echo"}, CPU: "lots", Memory: "1Gi"},
			wantErr: "cpu must be",
		},
		{
			name:    "negative priority",
			req:     CreateJobRequest{Name: "x", Image: "img", Command: []string{"echo"}, CPU: "1", Memory: "1Gi", Priority: -1},
			wantErr: "priority must be >= 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCreateRequest(tc.req)
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() != tc.wantErr && !contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %q, want containing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
