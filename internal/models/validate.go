package models

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	cpuPattern    = regexp.MustCompile(`^(\d+m|\d+(\.\d+)?)$`)
	memoryPattern = regexp.MustCompile(`^(\d+)(Mi|Gi|M|G)$`)
)

func ValidateCreateRequest(req CreateJobRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if strings.TrimSpace(req.Image) == "" {
		return fmt.Errorf("image is required")
	}
	if len(req.Command) == 0 {
		return fmt.Errorf("command is required")
	}
	for i, arg := range req.Command {
		if strings.TrimSpace(arg) == "" {
			return fmt.Errorf("command[%d] must not be empty", i)
		}
	}

	cpu := strings.TrimSpace(req.CPU)
	if cpu == "" {
		return fmt.Errorf("cpu is required")
	}
	if !cpuPattern.MatchString(cpu) {
		return fmt.Errorf("cpu must be a valid quantity (e.g. 500m, 1, 2.5)")
	}

	memory := strings.TrimSpace(req.Memory)
	if memory == "" {
		return fmt.Errorf("memory is required")
	}
	if !memoryPattern.MatchString(memory) {
		return fmt.Errorf("memory must be a valid quantity (e.g. 256Mi, 1Gi)")
	}

	if req.Priority < 0 {
		return fmt.Errorf("priority must be >= 0")
	}
	if req.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0")
	}

	return nil
}
