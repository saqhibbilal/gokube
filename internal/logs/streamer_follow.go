package logs

import "context"

func (s *Streamer) EnsureFollowing(parent context.Context, jobID, podName string) {
	if _, loaded := s.active.Load(jobID); loaded {
		return
	}

	ctx, cancel := context.WithCancel(parent)
	if _, loaded := s.active.LoadOrStore(jobID, cancel); loaded {
		cancel()
		return
	}

	go func() {
		defer cancel()
		defer s.active.Delete(jobID)
		defer s.store.SetStreaming(jobID, false)
		s.store.SetStreaming(jobID, true)
		s.follow(ctx, jobID, podName)
	}()
}

func (s *Streamer) Stop(jobID string) {
	if v, ok := s.active.LoadAndDelete(jobID); ok {
		if cancel, ok := v.(context.CancelFunc); ok {
			cancel()
		}
	}
	s.store.SetStreaming(jobID, false)
}
