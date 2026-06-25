package k8s

import "testing"

func TestCapacityCanFit(t *testing.T) {
	t.Parallel()

	pool := Capacity{
		CPUMillicores: 1000,
		MemoryBytes:   512 * 1024 * 1024,
	}

	if !pool.CanFit("500m", "256Mi") {
		t.Fatal("expected job to fit")
	}
	if pool.CanFit("2000m", "256Mi") {
		t.Fatal("expected cpu constraint to block job")
	}
	if pool.CanFit("500m", "1Gi") {
		t.Fatal("expected memory constraint to block job")
	}
}

func TestParseCPUAndMemory(t *testing.T) {
	t.Parallel()

	cpu, err := ParseCPU("500m")
	if err != nil || cpu != 500 {
		t.Fatalf("cpu = %d err = %v", cpu, err)
	}

	mem, err := ParseMemory("256Mi")
	if err != nil || mem != 256*1024*1024 {
		t.Fatalf("mem = %d err = %v", mem, err)
	}
}
