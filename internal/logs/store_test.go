package logs

import "testing"

func TestStoreRingBuffer(t *testing.T) {
	t.Parallel()

	store := NewStore(3)
	store.Append("job-1", "line-1")
	store.Append("job-1", "line-2")
	store.Append("job-1", "line-3")
	store.Append("job-1", "line-4")

	got := store.Get("job-1")
	if len(got.Lines) != 3 {
		t.Fatalf("lines = %d, want 3", len(got.Lines))
	}
	if got.Lines[0] != "line-2" || got.Lines[2] != "line-4" {
		t.Fatalf("unexpected lines: %v", got.Lines)
	}
}

func TestStoreStreamingFlag(t *testing.T) {
	t.Parallel()

	store := NewStore(10)
	store.SetStreaming("job-1", true)
	if !store.Get("job-1").Streaming {
		t.Fatal("expected streaming true")
	}
	store.SetStreaming("job-1", false)
	if store.Get("job-1").Streaming {
		t.Fatal("expected streaming false")
	}
}
