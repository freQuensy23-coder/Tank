package budget

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

type pacingWait struct {
	delay time.Duration
	wake  chan struct{}
}

type pacingResult struct {
	release func()
	err     error
}

func controlledPacingGate() (*Gate, *clock, <-chan pacingWait) {
	c := &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	g := NewGate(NewMemoryLedger(c.now), "pacing-regression")
	g.now = c.now
	waits := make(chan pacingWait, HostInFlight)
	g.sleep = func(ctx context.Context, delay time.Duration) error {
		wait := pacingWait{delay: delay, wake: make(chan struct{})}
		select {
		case waits <- wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-wait.wake:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g, c, waits
}

func pacingEnter(ctx context.Context, gate *Gate) <-chan pacingResult {
	result := make(chan pacingResult, 1)
	go func() {
		release, err := gate.Enter(ctx, "api.bybit.com", 0)
		result <- pacingResult{release: release, err: err}
	}()
	return result
}

func pacingWaitFor(t *testing.T, ctx context.Context, waits <-chan pacingWait, entered <-chan pacingResult) pacingWait {
	t.Helper()
	select {
	case wait := <-waits:
		return wait
	case result := <-entered:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("request admitted without required start spacing: %v", result.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return pacingWait{}
}

func pacingStarted(t *testing.T, ctx context.Context, entered <-chan pacingResult) func() {
	t.Helper()
	select {
	case result := <-entered:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.release
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil
}

func TestOverdueStartSpacesTheNextActualAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, c, waits := controlledPacingGate()
	first, err := g.Enter(ctx, "api.bybit.com", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	secondResult := pacingEnter(ctx, g)
	secondWait := pacingWaitFor(t, ctx, waits, secondResult)
	if secondWait.delay != 80*time.Millisecond {
		t.Fatalf("second delay %v", secondWait.delay)
	}
	// The timer wakes late; the request starts at t+500ms, not its reserved t+80ms.
	c.advance(500 * time.Millisecond)
	close(secondWait.wake)
	second := pacingStarted(t, ctx, secondResult)
	defer second()

	thirdResult := pacingEnter(ctx, g)
	thirdWait := pacingWaitFor(t, ctx, waits, thirdResult)
	if thirdWait.delay != 80*time.Millisecond {
		t.Fatalf("next delay %v, want 80ms from the actual late start", thirdWait.delay)
	}
	c.advance(thirdWait.delay)
	close(thirdWait.wake)
	third := pacingStarted(t, ctx, thirdResult)
	defer third()

	h := g.host("api.bybit.com")
	h.slots.mu.Lock()
	active := h.slots.active
	h.slots.mu.Unlock()
	if active != 3 {
		t.Fatalf("only %d holders: admission must not hold the network slot serially", active)
	}
}

func TestCancelledPacingWaitDoesNotReserveTheNextStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, c, waits := controlledPacingGate()
	first, err := g.Enter(ctx, "api.bybit.com", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	waitCtx, stopWaiting := context.WithCancel(ctx)
	waiting := pacingEnter(waitCtx, g)
	wait := pacingWaitFor(t, ctx, waits, waiting)
	if wait.delay != 80*time.Millisecond {
		t.Fatalf("cancelled request delay %v", wait.delay)
	}
	stopWaiting()
	select {
	case result := <-waiting:
		if !errors.Is(result.err, context.Canceled) {
			if result.release != nil {
				result.release()
			}
			t.Fatalf("cancelled request: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	following := pacingEnter(ctx, g)
	next := pacingWaitFor(t, ctx, waits, following)
	if next.delay != 80*time.Millisecond {
		t.Fatalf("cancelled request burned a future slot: next delay %v, want 80ms", next.delay)
	}
	c.advance(next.delay)
	close(next.wake)
	release := pacingStarted(t, ctx, following)
	release()
}

func TestResponseCompletionDoesNotRestartStartSpacing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, c, waits := controlledPacingGate()
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	first, err := g.Enter(ctx, "api.bybit.com", 0)
	if err != nil {
		t.Fatal(err)
	}
	releases = append(releases, first)
	for i := 1; i < HostInFlight; i++ {
		pending := pacingEnter(ctx, g)
		wait := pacingWaitFor(t, ctx, waits, pending)
		c.advance(wait.delay)
		close(wait.wake)
		releases = append(releases, pacingStarted(t, ctx, pending))
	}
	pending := pacingEnter(ctx, g)
	h := g.host("api.bybit.com")
	for {
		h.slots.mu.Lock()
		queued := h.slots.waiters.Len()
		h.slots.mu.Unlock()
		if queued == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			runtime.Gosched()
		}
	}
	// Four requests remain in flight until this delayed response completes.
	c.advance(2 * time.Second)
	if retry, err := g.Observe(ctx, "api.bybit.com", 200, nil); retry || err != nil {
		t.Fatalf("successful response: retry=%v err=%v", retry, err)
	}
	first()
	select {
	case result := <-pending:
		if result.err != nil {
			t.Fatal(result.err)
		}
		defer result.release()
		h.slots.mu.Lock()
		active := h.slots.active
		h.slots.mu.Unlock()
		if active != HostInFlight {
			t.Fatalf("active %d: the other three requests must still hold their permits", active)
		}
	case wait := <-waits:
		t.Fatalf("successful completion added an artificial %v start delay", wait.delay)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
