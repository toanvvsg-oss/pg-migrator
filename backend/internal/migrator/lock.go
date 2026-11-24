package migrator

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrJobLocked is returned when an exclusive operation is already running on a job.
var ErrJobLocked = errors.New("job operation already in progress")

type lockEntry struct {
	op         string
	acquiredAt time.Time
	gen        uint64
}

// JobLocker provides non-blocking per-job mutual exclusion with generation-safe idempotent unlock.
type JobLocker struct {
	mu      sync.Mutex
	nextGen uint64
	active  map[string]lockEntry
}

func NewJobLocker() *JobLocker {
	return &JobLocker{
		active: make(map[string]lockEntry),
	}
}

func (l *JobLocker) TryAcquire(jobID, op string) (func(), error) {
	if jobID == "" {
		return nil, fmt.Errorf("job id is required to acquire lock")
	}

	l.mu.Lock()
	if entry, busy := l.active[jobID]; busy {
		elapsed := time.Since(entry.acquiredAt).Round(time.Millisecond)
		l.mu.Unlock()
		return nil, fmt.Errorf("%w: job %s is currently running %q (for %s)", ErrJobLocked, jobID, entry.op, elapsed)
	}
	l.nextGen++
	gen := l.nextGen
	l.active[jobID] = lockEntry{
		op:         op,
		acquiredAt: time.Now(),
		gen:        gen,
	}
	l.mu.Unlock()

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			l.mu.Lock()
			if cur, ok := l.active[jobID]; ok && cur.gen == gen {
				delete(l.active, jobID)
			}
			l.mu.Unlock()
		})
	}
	return unlock, nil
}
