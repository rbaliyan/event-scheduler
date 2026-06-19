package scheduler

import (
	"sync/atomic"
	"testing"
	"time"
)

// --- adaptivePollState.adjust math ---

func TestAdaptivePollState_DecreasesWhenBusy(t *testing.T) {
	t.Parallel()
	s := newAdaptivePollState(100*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	got := s.adjust(5) // messages processed -> decrease by 25%
	if got != 75*time.Millisecond {
		t.Errorf("expected 75ms after busy adjust, got %v", got)
	}
}

func TestAdaptivePollState_IncreasesWhenIdle(t *testing.T) {
	t.Parallel()
	s := newAdaptivePollState(100*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	got := s.adjust(0) // no messages -> increase by 50%
	if got != 150*time.Millisecond {
		t.Errorf("expected 150ms after idle adjust, got %v", got)
	}
}

func TestAdaptivePollState_ClampsToMin(t *testing.T) {
	t.Parallel()
	s := newAdaptivePollState(12*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	// 12ms * 3/4 = 9ms, below the 10ms floor.
	if got := s.adjust(1); got != 10*time.Millisecond {
		t.Errorf("expected clamp to min 10ms, got %v", got)
	}
}

func TestAdaptivePollState_ClampsToMax(t *testing.T) {
	t.Parallel()
	s := newAdaptivePollState(4*time.Second, 10*time.Millisecond, 5*time.Second)
	// 4s * 3/2 = 6s, above the 5s ceiling.
	if got := s.adjust(0); got != 5*time.Second {
		t.Errorf("expected clamp to max 5s, got %v", got)
	}
}

// --- runSchedulerLoop orchestration ---

// startLoop runs runSchedulerLoop in a goroutine with the given hooks and
// returns the stop channel plus a func that blocks until the loop exits.
func startLoop(opts *options, processFn func() int, recoverFn func(), notifyCh <-chan struct{}) (stop chan struct{}, wait func()) {
	stop = make(chan struct{})
	done := make(chan struct{})
	go func() {
		runSchedulerLoop(
			discardLogger(),
			opts,
			make(chan struct{}), // doneCh: never fires in these tests
			stop,
			processFn,
			recoverFn,
			notifyCh,
			func() { close(done) },
		)
	}()
	return stop, func() { <-done }
}

func TestRunSchedulerLoop_ProcessesOnTick(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.pollInterval = 2 * time.Millisecond

	// Signal after the 3rd tick instead of racing a fixed wall-clock window.
	const target = 3
	done := make(chan struct{})
	var calls atomic.Int64
	stop, wait := startLoop(opts, func() int {
		if calls.Add(1) == target {
			close(done)
		}
		return 0
	}, nil, nil)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("processFn fired only %d times, want >= %d", calls.Load(), target)
	}
	close(stop)
	wait()
}

func TestRunSchedulerLoop_StopInvokesOnExit(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.pollInterval = time.Hour // ensure the ticker never fires during the test

	stop, wait := startLoop(opts, func() int { return 0 }, nil, nil)
	close(stop)

	// wait() returning proves onExit ran and the loop exited promptly.
	doneCh := make(chan struct{})
	go func() { wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("loop did not exit and call onExit after Stop")
	}
}

func TestRunSchedulerLoop_RecoversAtStartup(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.pollInterval = time.Hour

	// recoverFn runs once synchronously at startup; signal it deterministically.
	ran := make(chan struct{}, 1)
	var recovered atomic.Int64
	stop, wait := startLoop(opts, func() int { return 0 }, func() {
		recovered.Add(1)
		select {
		case ran <- struct{}{}:
		default:
		}
	}, nil)

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("recoverFn did not run at startup")
	}
	close(stop)
	wait()

	if got := recovered.Load(); got < 1 {
		t.Errorf("expected recoverFn to run at startup, got %d calls", got)
	}
}

func TestRunSchedulerLoop_NotifyTriggersProcess(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.pollInterval = time.Hour // isolate: only the notify can drive processing

	processed := make(chan struct{}, 4)
	notify := make(chan struct{}, 1)

	stop, wait := startLoop(opts, func() int {
		select {
		case processed <- struct{}{}:
		default:
		}
		return 1
	}, nil, notify)

	notify <- struct{}{}
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("notify did not trigger a process cycle")
	}

	close(stop)
	wait()
}
