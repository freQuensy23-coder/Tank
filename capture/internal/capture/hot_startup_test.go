package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type controlledAnchorStore struct {
	Store
	fail    atomic.Bool
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	entered sync.Once
}

func (s *controlledAnchorStore) BackfillAnchors(ctx context.Context, scopes []hotstate.Scope) ([]hotstate.AnchorRecord, error) {
	s.calls.Add(1)
	if s.fail.Load() {
		return nil, errors.New("anchor store temporarily unavailable")
	}
	if s.release != nil {
		s.entered.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.BackfillAnchors(ctx, scopes)
}

func TestStartupAnchorFailureRefusesEffectsAndRetriesOnceWithOriginalBound(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bound := r.base.Add(3 * time.Minute)
	missing := r.base.Add(time.Minute)
	rows := map[string]map[string]any{}
	for at := r.base.Add(-WindowBefore); at.Before(bound); at = at.Add(time.Minute) {
		if at.Equal(missing) {
			continue
		}
		rows[r.inst.ID+":"+pytime.ISO(at)] = map[string]any{"instrument_id": r.inst.ID, "minute_start": pytime.ISO(at), "complete": true}
	}
	r.mem.rows["mkt.flow_bars_1m"] = rows
	r.prints(1, r.base.Add(-time.Minute), r.base.Add(5*time.Minute), 30*time.Second)
	store := &controlledAnchorStore{Store: r.mem, started: make(chan struct{}), release: make(chan struct{})}
	store.fail.Store(true)
	r.s.store = store
	hot := []hotstate.Record{{StorageKey: "hot-startup", TokenID: r.inst.TokenID, EventID: "event-startup", Since: &r.base, SincePrec: requestText("EXACT")}}
	r.s.startStartupRepairs(ctx, hot, bound)
	if store.calls.Load() != 1 || r.v.calls != 0 || r.bar(missing) != nil {
		t.Fatal("Failed anchor lookup fell back to since and performed repair effects")
	}
	r.s.mu.Lock()
	pending := r.s.startupRetry
	queued := pending != nil && pending.bound.Equal(bound) && !pending.running
	r.s.mu.Unlock()
	if !queued {
		t.Fatal("Transient startup refusal lost its bounded retry state")
	}
	store.fail.Store(false)
	r.clock.set(r.base.Add(2 * time.Hour))
	r.s.retryStartupRepairs(ctx, hot)
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("Queued startup pass was not retried")
	}
	var callers sync.WaitGroup
	for range 8 {
		callers.Add(1)
		go func() { defer callers.Done(); r.s.retryStartupRepairs(ctx, hot) }()
	}
	callers.Wait()
	if store.calls.Load() != 2 {
		t.Fatal("Concurrent refreshes duplicated the active startup lookup")
	}
	close(store.release)
	r.s.wg.Wait()
	if store.calls.Load() != 2 || r.v.calls == 0 || r.bar(missing) == nil || r.bar(missing)["complete"] != true {
		t.Fatalf("Recovered startup pass did not repair the observed missing minute: lookups=%d reads=%d row=%v", store.calls.Load(), r.v.calls, r.bar(missing))
	}
	if r.bar(r.base.Add(4*time.Minute)) != nil {
		t.Fatal("Retry expanded its bound to the later processing clock")
	}
	r.s.mu.Lock()
	remains := r.s.startupRetry != nil
	r.s.mu.Unlock()
	if remains {
		t.Fatal("Successful startup retry remained queued")
	}
	r.s.retryStartupRepairs(ctx, hot)
	r.s.wg.Wait()
	if store.calls.Load() != 2 {
		t.Fatal("Completed startup pass was repeated")
	}
}

func TestStoredHotMembershipDoesNotInventExpiryDeletionAuthority(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.opt.Order = nil
	expired := r.base.Add(-time.Minute)
	hot := hotstate.Record{StorageKey: "hot-retained", TokenID: r.inst.TokenID, EventID: "event-retained", Since: &r.base, ExpiresAt: &expired}
	r.s.syncHot(context.Background(), []hotstate.Record{hot})
	since, present := r.s.hot(r.inst.TokenID)
	if !present || !since.Equal(r.base) {
		t.Fatal("Finite expiry metadata silently deleted stored HOT membership")
	}
	hot.Since = nil
	r.s.syncHot(context.Background(), []hotstate.Record{hot})
	if _, present := r.s.hot(r.inst.TokenID); present {
		t.Fatal("Missing recorded since was replaced by a guessed clock")
	}
}

func TestExistingHotLoopRecoversQueuedStartupLookup(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.s.ctx = ctx
	r.s.opt.Order = nil
	r.mem.hot = []hotstate.Record{{StorageKey: "loop-hot", TokenID: r.inst.TokenID, EventID: "event-loop", Since: &r.base}}
	store := &controlledAnchorStore{Store: r.mem}
	store.fail.Store(true)
	r.s.store = store
	r.s.startStartupRepairs(ctx, r.mem.hot, r.base.Add(time.Minute))
	store.fail.Store(false)
	r.s.spawn(r.s.hotLoop)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		r.s.mu.Lock()
		done := r.s.startupRetry == nil
		r.s.mu.Unlock()
		if done {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("Existing HOT loop did not recover the queued startup lookup")
		}
	}
	cancel()
	r.s.wg.Wait()
	if store.calls.Load() != 2 || r.v.calls != 0 {
		t.Fatalf("HOT loop retry was lost or duplicated: lookups=%d reads=%d", store.calls.Load(), r.v.calls)
	}
}

func TestMalformedMatchingAnchorRefusesStartupWithoutSinceFallback(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.mem.rows["mkt.backfill_runs"] = map[string]map[string]any{
		"actual-stored-key": {"backfill_id": "bad-anchor", "token_id": r.inst.TokenID, "event_id": "event-bad", "status": "RUNNING", "anchor": nil},
	}
	hot := []hotstate.Record{{StorageKey: "bad-window-hot", TokenID: r.inst.TokenID, EventID: "event-bad", Since: &r.base}}
	r.s.startStartupRepairs(context.Background(), hot, r.base.Add(time.Minute))
	r.s.mu.Lock()
	queued := r.s.startupRetry != nil
	r.s.mu.Unlock()
	if !queued || r.v.calls != 0 {
		t.Fatal("Malformed matching evidence was treated as absent and repaired from since")
	}
}

func TestStartupLogPreservesRecordedClockUncertainty(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	r.s.opt.Order = nil
	var log bytes.Buffer
	r.s.log = slog.New(slog.NewJSONHandler(&log, nil))
	hot := []hotstate.Record{{StorageKey: "hot-uncertain", TokenID: r.inst.TokenID, EventID: "event-uncertain", Since: &r.base, SinceReason: requestText("LEGACY_PRECISION_UNREPORTED")}}
	if err := r.s.startupRepairs(context.Background(), hot, r.base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var record struct {
		Sources struct {
			Source struct {
				TokenID string `json:"token_id"`
				Origin  struct {
					Kind  string `json:"kind"`
					Clock struct {
						At               string  `json:"at"`
						PrecisionPresent *bool   `json:"precision_present"`
						ReasonPresent    *bool   `json:"reason_present"`
						Precision        *string `json:"precision"`
						Reason           *string `json:"reason"`
					} `json:"clock"`
					BackfillPresent *bool `json:"backfill_id_present"`
				} `json:"origin"`
			} `json:"source_0"`
		} `json:"recorded_window_sources"`
	}
	if err := json.Unmarshal(log.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	source := record.Sources.Source
	clock := source.Origin.Clock
	if source.TokenID != r.inst.TokenID || source.Origin.Kind != "RECORDED_HOT_SINCE" || clock.At != pytime.ISO(r.base) ||
		clock.PrecisionPresent == nil || *clock.PrecisionPresent || clock.Precision != nil || clock.ReasonPresent == nil ||
		!*clock.ReasonPresent || clock.Reason == nil || *clock.Reason != "LEGACY_PRECISION_UNREPORTED" ||
		source.Origin.BackfillPresent == nil || *source.Origin.BackfillPresent {
		t.Fatalf("Startup diagnostics inferred verification or discarded uncertainty: %s", log.Bytes())
	}
}
