package k8s

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Capacity tracks cluster CPU (millicores) and memory (bytes).
type Capacity struct {
	CPUMillicores int64
	MemoryBytes   int64
}

func (c Capacity) CanFit(cpu, memory string) bool {
	reqCPU, err := ParseCPU(cpu)
	if err != nil {
		return false
	}
	reqMem, err := ParseMemory(memory)
	if err != nil {
		return false
	}
	return c.CPUMillicores >= reqCPU && c.MemoryBytes >= reqMem
}

func (c Capacity) Subtract(used Capacity) Capacity {
	return Capacity{
		CPUMillicores: c.CPUMillicores - used.CPUMillicores,
		MemoryBytes:   c.MemoryBytes - used.MemoryBytes,
	}
}

func ParseCPU(value string) (int64, error) {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse cpu %q: %w", value, err)
	}
	return q.MilliValue(), nil
}

func ParseMemory(value string) (int64, error) {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse memory %q: %w", value, err)
	}
	return q.Value(), nil
}

func AddCapacity(a, b Capacity) Capacity {
	return Capacity{
		CPUMillicores: a.CPUMillicores + b.CPUMillicores,
		MemoryBytes:   a.MemoryBytes + b.MemoryBytes,
	}
}
