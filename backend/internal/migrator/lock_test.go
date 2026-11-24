package migrator

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobLockerLifecycleAndConflict(t *testing.T) {
	locker := NewJobLocker()

	if _, err := locker.TryAcquire("", "cutover"); err == nil {
		t.Fatalf("expected error on empty jobID")
	}

	unlock1, err := locker.TryAcquire("job-1", "cutover")
	if err != nil {
		t.Fatalf("first TryAcquire failed: %v", err)
	}

	// Same job conflicts with ErrJobLocked and mentions active op
	_, err = locker.TryAcquire("job-1", "verify")
	if !errors.Is(err, ErrJobLocked) {
		t.Fatalf("expected ErrJobLocked, got %v", err)
	}
	if !strings.Contains(err.Error(), "cutover") {
		t.Fatalf("expected error to mention running op 'cutover', got %v", err)
	}

	// Distinct job ID acquires independently
	unlock2, err := locker.TryAcquire("job-2", "schema")
	if err != nil {
		t.Fatalf("distinct job TryAcquire failed: %v", err)
	}
	unlock2()

	// Unlock and re-acquire
	unlock1()
	unlock3, err := locker.TryAcquire("job-1", "verify")
	if err != nil {
		t.Fatalf("re-acquire after unlock failed: %v", err)
	}

	// Double-unlock from previous holder must NOT evict new holder (generation safety)
	unlock1()
	if _, err := locker.TryAcquire("job-1", "abort"); !errors.Is(err, ErrJobLocked) {
		t.Fatalf("double-unlock evicted active holder; expected ErrJobLocked, got %v", err)
	}

	unlock3()
}

func TestJobLockerConcurrentContention(t *testing.T) {
	locker := NewJobLocker()

	var currentHolders atomic.Int32
	var maxHolders atomic.Int32
	var successes atomic.Int32
	var conflicts atomic.Int32

	const workers = 100
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			unlock, err := locker.TryAcquire("hot-job", "cutover")
			if err != nil {
				if !errors.Is(err, ErrJobLocked) {
					t.Errorf("unexpected error: %v", err)
				}
				conflicts.Add(1)
				return
			}
			cur := currentHolders.Add(1)
			for {
				prev := maxHolders.Load()
				if cur <= prev || maxHolders.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(200 * time.Microsecond)
			currentHolders.Add(-1)
			successes.Add(1)
			unlock()
		}()
	}
	wg.Wait()

	if maxHolders.Load() != 1 {
		t.Fatalf("max concurrent holders = %d, want 1", maxHolders.Load())
	}
	if successes.Load()+conflicts.Load() != workers {
		t.Fatalf("total outcomes = %d, want %d", successes.Load()+conflicts.Load(), workers)
	}
}
