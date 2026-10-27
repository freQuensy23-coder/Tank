package capture

import (
	"context"
	"errors"
	"github.com/freQuensy23-coder/Tank/capture/internal/captureparams"
	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/parity"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

type lockedClock struct {
	mu sync.Mutex
	t  time.Time
	t0 time.Time
}

func (c *lockedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *lockedClock) Monotonic() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t.Sub(c.t0)
}

func (c *lockedClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.t) {
		c.t = t
	}
}

type replayConn struct {
	s        *Service
	endpoint string
	frames   []parity.Frame
	clock    *lockedClock
	i, sent  int
	done     chan struct{}
	closed   chan struct{}
	once     sync.Once
	stop     sync.Once
}

func (c *replayConn) processed() int {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.s.frames[c.endpoint]
}

func (c *replayConn) preparing() bool {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	for _, states := range []map[string]*memberState{c.s.members, c.s.depth} {
		for _, m := range states {
			if m.endpoint == c.endpoint && m.state == "PREPARING" {
				return true
			}
		}
	}
	return false
}

func (c *replayConn) ReadMessage() (int, []byte, error) {
	for c.i < len(c.frames) {
		f := c.frames[c.i]
		c.i++
		if f.REST != "" || f.Text == "" || (f.Endpoint != "" && f.Endpoint != c.endpoint) {
			continue
		}

		for c.processed() < c.sent || (c.sent >= 50 && c.preparing()) {
			select {
			case <-c.closed:
				return 0, nil, errors.New("closed")
			case <-time.After(50 * time.Microsecond):
			}
		}
		c.clock.set(pytime.MustParse(f.T))
		c.sent++
		return textMessage, []byte(f.Text), nil
	}
	c.once.Do(func() { close(c.done) })
	<-c.closed
	return 0, nil, errors.New("closed")
}

func (c *replayConn) WriteMessage(int, []byte) error            { return nil }
func (c *replayConn) WriteControl(int, []byte, time.Time) error { return nil }
func (c *replayConn) SetReadDeadline(time.Time) error           { return nil }
func (c *replayConn) SetPongHandler(func(string) error)         {}
func (c *replayConn) Close() error                              { c.stop.Do(func() { close(c.closed) }); return nil }

type replayVenue struct {
	fakeVenue
	snapshot map[string]any
	md       map[string]any
}

func (v *replayVenue) Discover(context.Context) (map[string]any, error) { return v.md, nil }
func (v *replayVenue) DepthSnapshot(context.Context) (map[string]any, error) {
	if v.snapshot == nil {
		return nil, errors.New("no recorded snapshot")
	}
	return v.snapshot, nil
}

type replayVenues map[string]*replayVenue

func (r replayVenues) Venue(inst *venue.Instrument, _ int) Venue { return r[inst.ID] }
func (r replayVenues) FXRate(context.Context, string) (string, error) {
	return parity.USDTRate, nil
}

func TestReplayRecordedVenueStreams(t *testing.T) {
	for _, name := range []string{"coinbase-spot", "kraken-spot", "okx-spot", "okx-swap", "bybit-spot", "bybit-linear", "hyperliquid-perp", "binance-spot"} {
		t.Run(name, func(t *testing.T) { replayFixture(t, name) })
	}
}

func replayFixture(t *testing.T, name string) {
	fx, err := parity.Load(testutil.Path("frames", name+".jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	start := pytime.MustParse(fx.Frames[0].T)
	end := pytime.MustParse(fx.Frames[len(fx.Frames)-1].T)
	clock := &lockedClock{t: start, t0: start}
	instruments := map[string]*venue.Instrument{}
	var order []string
	venues := replayVenues{}
	mem := newMemStore()
	for _, raw := range pyval.List(fx.Meta["instruments"]) {
		m := pyval.Map(raw)
		inst := venue.FromRegistry(m)
		if inst.Quote() == "USDT" {
			inst.QuoteUSDRate = parity.USDTRate
		}
		instruments[inst.ID], order = inst, append(order, inst.ID)
		md := map[string]any{}
		for _, k := range []string{"contract_multiplier", "tick_size", "funding_interval_h"} {
			if m[k] != nil {
				md[k] = m[k]
			}
		}
		if inst.MarketType != "SPOT" && md["funding_interval_h"] == nil {
			md["funding_interval_h"] = "8"
		}
		venues[inst.ID] = &replayVenue{fakeVenue: fakeVenue{inst: inst}, md: md}
		since := end.Add(time.Hour)
		mem.hot = append(mem.hot, hotstate.Record{StorageKey: "replay-hot-" + inst.ID, TokenID: inst.TokenID, EventID: "EVT-REPLAY", Since: &since})
	}
	for _, f := range fx.Frames {
		if f.REST == "depth_snapshot" {
			snap, err := pyval.Decode([]byte(f.Text))
			if err != nil {
				t.Fatal(err)
			}
			venues[f.InstrumentID].snapshot = pyval.Map(snap)
		}
	}
	var mu sync.Mutex
	var conns []*replayConn
	var s *Service
	dial := func(_ context.Context, url string) (Conn, error) {
		c := &replayConn{s: s, endpoint: url, frames: fx.Frames, clock: clock, done: make(chan struct{}), closed: make(chan struct{})}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		return c, nil
	}
	s, err = New(Options{Parameters: captureparams.Defaults(), Instruments: instruments, Order: order, Writer: "test"}, mem, mem, venues, dial, clock)
	if err != nil {
		t.Fatal(err)
	}
	s.fx.Remember("USDT", map[time.Time]string{start.Add(-time.Hour): parity.USDTRate})
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.syncHot(ctx, mem.hot)
	deadline := time.After(60 * time.Second)
	for {
		mu.Lock()
		n, finished := len(conns), 0
		for _, c := range conns {
			select {
			case <-c.done:
				if c.processed() >= c.sent {
					finished++
				}
			default:
			}
		}
		mu.Unlock()
		if n > 0 && finished == n {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("replay did not finish: %d connections", n)
		case <-time.After(10 * time.Millisecond):
		}
	}

	now := clock.Now()
	for _, check := range s.seal.Tick(now) {
		s.seal.HeadResult(check, nil, now, false)
	}
	s.seal.Tick(now)
	s.flush(ctx)
	cancel()
	s.wg.Wait()

	for kind, rows := range map[string]int{"STREAM": 0, "ORDER_BOOK_STREAM": 0} {
		for _, gap := range mem.rows["mkt.capture_gaps"] {
			if gap["kind"] == kind {
				rows++
				t.Errorf("unexpected %s gap: %v", kind, gap["reason"])
			}
		}
	}
	summaries := map[string]int{}
	for _, row := range mem.rows["mkt.book_summaries"] {
		if row["integrity_ok"] != true {
			t.Errorf("book summary without integrity: %v", row["gap_reason"])
		}
		summaries[text(row["instrument_id"])]++
	}

	first := pytime.MinuteFloor(start).Add(time.Minute)
	if !first.Add(time.Minute + 45*time.Second).Before(end) {
		t.Fatalf("fixture too short: %v..%v", start, end)
	}
	for _, iid := range order {
		row := mem.bars(iid)[pytime.ISO(first)]
		if row == nil || row["bar_version"] != int64(1) || row["complete"] != true {
			t.Errorf("%s %s sealed complete: %v", iid, pytime.ISO(first), row)
		}
		if summaries[iid] == 0 {
			t.Errorf("%s has no book summary", iid)
		}
	}
}
