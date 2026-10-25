package capture

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type freshMembershipStore struct {
	Store
	fail  atomic.Bool
	reads atomic.Int32
}

func (s *freshMembershipStore) HotTokens(ctx context.Context) ([]hotstate.Record, error) {
	s.reads.Add(1)
	if s.fail.Load() {
		return nil, errors.New("authoritative membership read failed")
	}
	return s.Store.HotTokens(ctx)
}

type startupVenueSet map[string]*fakeVenue

func (s startupVenueSet) Venue(inst *venue.Instrument, _ int) Venue {
	result, found := s[inst.ID]
	if !found {
		panic("startup fixture requested an undeclared instrument")
	}
	return result
}
func (startupVenueSet) FXRate(context.Context, string) (string, error) {
	return "", errors.New("startup fixture does not declare foreign currency conversion")
}

func TestLeadershipPendingMembershipChangeCannotRepairStaleToken(t *testing.T) {
	for _, action := range []string{"remove", "replace", "refresh-error"} {
		t.Run(action, func(t *testing.T) {
			r := newRESTRig(t, "coinbase")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r.s.ctx = ctx
			defer func() { cancel(); r.s.wg.Wait() }()
			bound := r.base.Add(3 * time.Minute)
			r.clock.set(bound)
			missing := r.base.Add(time.Minute)
			r.prints(1, r.base.Add(-time.Minute), bound, 30*time.Second)
			replacement := *r.inst
			replacement.ID = "coinbase:spot:REPLACEMENT"
			replacement.TokenID = "test:REPLACEMENT"
			replacement.Symbol = "REPLACEMENT-USD"
			nextVenue := &fakeVenue{inst: &replacement, trades: append([]fakeTrade(nil), r.v.trades...)}
			r.s.opt.Instruments[replacement.ID] = &replacement
			r.s.venues = startupVenueSet{r.inst.ID: r.v, replacement.ID: nextVenue}
			r.s.dial = pendingStartupDial
			history := map[string]map[string]any{}
			for _, iid := range []string{r.inst.ID, replacement.ID} {
				for at := r.base.Add(-WindowBefore); at.Before(bound); at = at.Add(time.Minute) {
					if at.Equal(missing) {
						continue
					}
					history[iid+":"+pytime.ISO(at)] = map[string]any{"instrument_id": iid, "minute_start": pytime.ISO(at), "complete": true}
				}
			}
			r.mem.rows["mkt.flow_bars_1m"] = history
			old := hotstate.Record{StorageKey: "old-hot", TokenID: r.inst.TokenID, EventID: "old-event", Since: &r.base, SincePrec: requestText("EXACT")}
			fresh := hotstate.Record{StorageKey: "replacement-hot", TokenID: replacement.TokenID, EventID: "replacement-event", Since: &r.base, SincePrec: requestText("EXACT")}
			r.mem.hot = []hotstate.Record{old}
			store := &freshMembershipStore{Store: r.mem}
			r.s.store = store
			snapshot, err := store.HotTokens(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r.s.opt.Order = nil
			r.s.syncHot(ctx, snapshot)
			r.s.opt.Order = []string{r.inst.ID, replacement.ID}
			if _, present := r.s.hot(r.inst.TokenID); !present {
				t.Fatal("Fixture did not establish the pre-leadership stored membership")
			}
			leading := make(chan struct{})
			r.s.opt.Leading = leading
			entered, done := make(chan struct{}), make(chan struct{})
			r.s.spawn(func(ctx context.Context) {
				close(entered)
				r.s.startupAfterLeading(ctx, r.base)
				close(done)
			})
			<-entered
			r.mem.mu.Lock()
			if action == "remove" {
				r.mem.hot = nil
			} else {
				r.mem.hot = []hotstate.Record{fresh}
			}
			r.mem.mu.Unlock()
			if action == "refresh-error" {
				store.fail.Store(true)
			}
			select {
			case <-done:
				t.Fatal("Startup repair bypassed pending leadership")
			default:
			}
			close(leading)
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("Post-leadership startup pass did not finish")
			}
			if store.reads.Load() != 2 {
				t.Fatalf("Startup did not refresh the pre-leadership input: reads=%d", store.reads.Load())
			}
			if r.v.calls != 0 || r.bar(missing) != nil {
				t.Fatal("Removed pre-leadership token produced stale repair effects")
			}
			if action == "refresh-error" {
				if nextVenue.calls != 0 {
					t.Fatal("Failed authoritative read was interpreted as usable membership")
				}
				r.s.mu.Lock()
				pending := r.s.startupRetry != nil && r.s.startupRetry.bound.Equal(bound)
				r.s.mu.Unlock()
				if !pending {
					t.Fatal("Failed fresh read lost its original bounded retry")
				}
				store.fail.Store(false)
				freshRows, err := store.HotTokens(ctx)
				if err != nil {
					t.Fatal(err)
				}
				r.clock.set(r.base.Add(2 * time.Hour))
				r.s.retryStartupRepairs(ctx, freshRows)
				waitStartupCleared(t, r.s, ctx)
			}
			if action != "remove" {
				if nextVenue.calls == 0 {
					t.Fatal("Current replacement membership was not repaired")
				}
				bars := r.mem.bars(replacement.ID)
				repaired := bars[pytime.ISO(missing)]
				if repaired == nil || repaired["complete"] != true {
					t.Fatalf("Replacement missing minute was not actually repaired: %v", repaired)
				}
				if bars[pytime.ISO(r.base.Add(4*time.Minute))] != nil {
					t.Fatal("Fresh-read recovery expanded the original repair bound")
				}
			} else if nextVenue.calls != 0 {
				t.Fatal("Known empty membership produced replacement repair effects")
			}
			if _, present := r.s.hot(r.inst.TokenID); present {
				t.Fatal("Successful authoritative refresh retained removed membership")
			}
		})
	}
}
