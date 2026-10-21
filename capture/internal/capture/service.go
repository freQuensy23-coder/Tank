package capture

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/books"
	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pipeline"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/stream"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// Service is the capture runtime.
type Service struct {
	opt    Options
	store  Store
	writer Writer
	venues Venues
	dial   Dialer
	clock  Clock
	log    *slog.Logger

	fx     *normalize.FX
	points *points.Points
	pipe   *pipeline.Pipeline
	seal   *Sealer
	marks  *stream.Watermarks

	ctx     context.Context
	wg      sync.WaitGroup
	connSeq atomic.Int64
	wake    chan struct{}
	slots   chan struct{}

	mu           sync.Mutex
	hotSince     map[string]time.Time // hot token -> earliest since
	groups       map[string]*group
	members      map[string]*memberState // instrument -> main socket member
	depth        map[string]*memberState // instrument -> Binance USD-M depth member
	books        map[string]*books.Book
	metadata     map[string]map[string]any // discovered instrument metadata
	klinesPolled map[string][]string
	obliged      map[string]bool
	flowLocks    map[string]*sync.Mutex
	ctxLocks     map[string]*sync.Mutex
	late         map[flow.Key]bool
	repairs      map[string]*repairQueue
	fxCache      map[string][2]string // quote -> (rate, iso ts)
	history      map[[2]string]time.Time
	reads        map[string][2]int
	frames       map[string]int

	flushMu   sync.Mutex
	lastFlush atomic.Int64 // unix nanoseconds of the last successful flush
	flushErr  atomic.Value
}

// memberState is one instrument on one socket.
type memberState struct {
	member    *stream.Member
	conn      int64
	endpoint  string
	state     string
	connected time.Time
	message   time.Time
}

// New is a service; Run starts it.
func New(opt Options, store Store, writer Writer, venues Venues, dial Dialer, clock Clock) *Service {
	if opt.Log == nil {
		opt.Log = slog.Default()
	}
	s := &Service{
		opt: opt, store: store, writer: writer, venues: venues, dial: dial, clock: clock, log: opt.Log,
		fx: normalize.NewFX(), marks: stream.NewWatermarks(),
		wake: make(chan struct{}, 1), slots: make(chan struct{}, RepairSlots),
		hotSince: map[string]time.Time{}, groups: map[string]*group{}, members: map[string]*memberState{},
		depth: map[string]*memberState{}, books: map[string]*books.Book{}, metadata: map[string]map[string]any{},
		klinesPolled: map[string][]string{}, obliged: map[string]bool{}, flowLocks: map[string]*sync.Mutex{},
		ctxLocks: map[string]*sync.Mutex{}, late: map[flow.Key]bool{}, repairs: map[string]*repairQueue{},
		fxCache: map[string][2]string{}, history: map[[2]string]time.Time{}, reads: map[string][2]int{},
		frames: map[string]int{},
	}
	s.points = points.New(clock, writer.Sink, opt.ConfigVersion, opt.CodeVersion, s.fx)
	headDelay := time.Duration(opt.Threshold("head_check_delay_s", 45) * float64(time.Second))
	s.seal = NewSealer(clock, s.points.Put, headDelay)
	s.seal.Repair = func(inst *venue.Instrument, start, end time.Time, reason string) {
		s.queueRepair(inst, span{start, end}, reason, nil)
	}
	s.seal.Gap = func(inst *venue.Instrument, kind, reason string, start, end time.Time) {
		s.gap(inst, kind, reason, &start, &end)
	}
	s.seal.Late = s.lateMinute
	s.pipe = pipeline.New(s.points, s.fx, clock, func(inst *venue.Instrument, t *normalize.Trade) {
		s.raw(t)
		s.seal.Trade(inst, t, s.mainConn(inst.ID))
	}, s.confirm)
	s.pipe.BookSaveS = opt.Threshold("book_save_interval_s", pipeline.BookSaveS)
	s.pipe.Async = func(f func()) { go f() }
	return s
}

// leading reports whether this process holds the writer lock (Options.Leading).
func (s *Service) leading() bool {
	if s.opt.Leading == nil {
		return true
	}
	select {
	case <-s.opt.Leading:
		return true
	default:
		return false
	}
}

// waitLeading blocks until this process holds the writer lock; false if ctx ends first.
func (s *Service) waitLeading(ctx context.Context) bool {
	if s.opt.Leading == nil {
		return true
	}
	select {
	case <-s.opt.Leading:
		return true
	case <-ctx.Done():
		return false
	}
}

// Ready reports that every stream member is connected (a standby can take over).
func (s *Service) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.members) == 0 {
		return false
	}
	for _, m := range s.members {
		if m.state != "CONNECTED" {
			return false
		}
	}
	return true
}

// Wake makes the hot set, obligations and requests be read now (a tank_capture notification).
func (s *Service) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// LastFlush is when typed rows were last written successfully.
func (s *Service) LastFlush() time.Time {
	if n := s.lastFlush.Load(); n != 0 {
		return time.Unix(0, n).UTC()
	}
	return time.Time{}
}

// Status is the health summary of the runtime.
func (s *Service) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	connected := 0
	for _, m := range s.members {
		if m.state == "CONNECTED" {
			connected++
		}
	}
	hot := make([]string, 0, len(s.hotSince))
	for t := range s.hotSince {
		hot = append(hot, t)
	}
	sort.Strings(hot)
	status := map[string]any{
		"hot_tokens": hot, "stream_groups": len(s.groups), "members": len(s.members),
		"members_connected": connected, "last_flush": pytime.ISO(s.LastFlush()),
	}
	if err, ok := s.flushErr.Load().(string); ok && err != "" {
		status["last_flush_error"] = err
	}
	return status
}

// Run captures until ctx ends, then writes what it holds.
func (s *Service) Run(ctx context.Context) error {
	s.ctx = ctx
	started := s.clock.Now()
	if err := s.restore(ctx); err != nil {
		return err
	}
	s.discoverAll(ctx)
	hot, err := s.store.HotTokens(ctx)
	if err != nil {
		return err
	}
	s.syncHot(ctx, hot)
	s.spawn(func(ctx context.Context) {
		if !s.waitLeading(ctx) {
			return
		}
		// Write what the standby captured first: the gap left to repair is then only what
		// no process saw.
		s.flush(ctx)
		s.startupRepairs(ctx, hot, maxTime(started, s.clock.Now()))
		s.sweep(ctx)
	})
	s.loop(ctx, time.Second, s.tick)
	s.loop(ctx, streamStateEvery, func(context.Context) { s.publishStreams() })
	s.loop(ctx, rawFlushEvery, func(context.Context) { s.flushRaw() })
	s.loop(ctx, fetchPruneEvery, s.pruneFetches)
	s.loop(ctx, sweepEvery, s.sweep)
	s.spawn(s.hotLoop)
	for _, iid := range s.opt.Order {
		inst := s.opt.Instruments[iid]
		if inst == nil || !inst.Active {
			continue
		}
		s.spawn(func(ctx context.Context) { s.instrumentLoop(ctx, inst) })
		if !inst.Spot() {
			interval := s.opt.Threshold("hot_poll_interval_s", HotPollS)
			s.spawn(func(ctx context.Context) {
				s.hotGrid(ctx, inst, interval, interval/2, pollKinds{derivatives: true})
			})
		}
		interval := s.opt.Threshold("hot_ticker_interval_s", HotTickerS)
		s.spawn(func(ctx context.Context) { s.hotGrid(ctx, inst, interval, 0, pollKinds{ticker: true}) })
	}
	s.spawn(func(ctx context.Context) {
		if s.waitLeading(ctx) {
			s.oiHistoryLoop(ctx)
		}
	})
	<-ctx.Done()
	s.mu.Lock()
	for _, g := range s.groups {
		g.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
	final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	s.seal.Tick(s.clock.Now())
	s.publishStreamsStopped()
	s.flush(final)
	s.flushRaw()
	return nil
}

// fetchPruneEvery and fetchKeep bound finished HTTP_FETCH rows (their bodies are one-shot).
const fetchPruneEvery, fetchKeep = 10 * time.Minute, time.Hour

func (s *Service) pruneFetches(ctx context.Context) {
	if s.opt.CanWrite != nil && !s.opt.CanWrite() {
		return
	}
	if _, err := s.store.PruneFetches(ctx, s.clock.Now().Add(-fetchKeep)); err != nil {
		s.log.Warn("fetch prune failed", "error", err)
	}
}

// raw keeps a print for the hourly trade files.
func (s *Service) raw(t *normalize.Trade) {
	if s.opt.Raw != nil {
		s.opt.Raw.Add(t)
	}
}

// flushRaw writes the buffered prints to their hour journals and seals finished hours
// (only while holding the writer lock: the previous process still appends until then).
func (s *Service) flushRaw() {
	if s.opt.Raw == nil || (s.opt.CanWrite != nil && !s.opt.CanWrite()) {
		return
	}
	if err := s.opt.Raw.Flush(s.clock.Now()); err != nil {
		s.log.Warn("raw trade flush failed", "error", err.Error())
	}
}

// spawn runs f until the service stops.
func (s *Service) spawn(f func(ctx context.Context)) {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("capture task panicked", "panic", r)
			}
		}()
		f(ctx)
	}()
}

// after runs f once d has passed, unless the service stops first.
func (s *Service) after(d time.Duration, f func(ctx context.Context)) {
	s.spawn(func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-time.After(d):
			f(ctx)
		}
	})
}

// loop runs f every interval.
func (s *Service) loop(ctx context.Context, interval time.Duration, f func(ctx context.Context)) {
	s.spawn(func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				f(ctx)
			}
		}
	})
}

// tick seals what can be sealed, runs the head checks due and writes the queued rows.
func (s *Service) tick(ctx context.Context) {
	for _, check := range s.seal.Tick(s.clock.Now()) {
		check := check
		s.spawn(func(ctx context.Context) { s.headCheck(ctx, check) })
	}
	s.flush(ctx)
}

// flush writes the queued typed rows (one writer at a time). While the writer lock is
// being re-acquired the rows stay queued: nothing is written without it, nothing dropped.
func (s *Service) flush(ctx context.Context) {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.opt.CanWrite != nil && !s.opt.CanWrite() {
		s.flushErr.Store("writer lock not held: rows kept in memory")
		return
	}
	if err := s.writer.Flush(ctx); err != nil {
		s.flushErr.Store(err.Error())
		s.log.Warn("market flush failed", "error", err)
		return
	}
	s.flushErr.Store("")
	s.lastFlush.Store(s.clock.Now().UnixNano())
}

func (s *Service) flowLock(iid string) func() {
	s.mu.Lock()
	l := s.flowLocks[iid]
	if l == nil {
		l = &sync.Mutex{}
		s.flowLocks[iid] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (s *Service) contextLock(iid string) func() {
	s.mu.Lock()
	l := s.ctxLocks[iid]
	if l == nil {
		l = &sync.Mutex{}
		s.ctxLocks[iid] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// repairSlot is one of the shared repair slots (_repair_slots): repair work never takes
// every REST slot from the reference venue.
func (s *Service) repairSlot(ctx context.Context) (func(), error) {
	select {
	case s.slots <- struct{}{}:
		return func() { <-s.slots }, nil
	case <-ctx.Done():
		return nil, ErrStopped
	}
}

// restore is _ensure_market_restore without the raw journals: the newest ticker and
// funding points seed repeat suppression, the stored stablecoin USD samples value the
// prints of a restart's repairs as of their own time, and the newest stored print of each
// instrument is the cursor its first connection resumes from (03 §5.2).
func (s *Service) restore(ctx context.Context) error {
	ids := make([]string, 0, len(s.opt.Order))
	for _, iid := range s.opt.Order {
		if inst := s.opt.Instruments[iid]; inst != nil && inst.Active {
			ids = append(ids, iid)
		}
	}
	for _, spec := range []struct {
		collection string
		groups     []string
		field      string
	}{{"mkt.tickers_24h", []string{"instrument_id"}, "ts"}, {"mkt.funding_obs", []string{"instrument_id", "kind"}, "as_of"}} {
		rows, err := s.store.LatestRecords(ctx, spec.collection, spec.groups, spec.field, ids)
		if err != nil {
			return err
		}
		for _, row := range rows {
			key := text(row["instrument_id"]) + ":" + text(row["ts"])
			if spec.collection == "mkt.funding_obs" {
				key = text(row["instrument_id"]) + ":" + text(row["kind"]) + ":" + text(row["as_of"])
			}
			s.points.Restore(spec.collection, key, row)
		}
	}
	now := s.clock.Now()
	for _, quote := range []string{"USDT", "USDC"} {
		var quoted []string
		for _, iid := range ids {
			inst := s.opt.Instruments[iid]
			if inst.Quote() == quote && inst.Spot() {
				quoted = append(quoted, iid)
			}
		}
		sort.Strings(quoted)
		if len(quoted) > 2 {
			quoted = quoted[:2]
		}
		if len(quoted) == 0 {
			continue
		}
		rows, err := s.store.Projection(ctx, "mkt.klines_1m", []string{"quote_usd_rate", "quote_usd_rate_as_of"}, "open_time", now.Add(-normalize.QuoteUSDReach), now, quoted)
		if err != nil {
			return err
		}
		samples := map[time.Time]string{}
		for _, row := range rows {
			at, err := pytime.Parse(row["quote_usd_rate_as_of"])
			if row["quote_usd_rate"] != nil && err == nil {
				samples[at] = text(row["quote_usd_rate"])
			}
		}
		s.fx.Remember(quote, samples)
	}
	cursors, err := s.store.LatestTrades(ctx, ids, now.Add(-CursorLookback))
	if err != nil {
		return err
	}
	for iid, last := range cursors {
		if inst := s.opt.Instruments[iid]; inst != nil && last != nil {
			s.seal.Anchor(inst, last)
		}
	}
	return nil
}

// hotLoop reads the hot set, the obligations and the requests every few seconds and on
// every tank_capture notification.
func (s *Service) hotLoop(ctx context.Context) {
	refresh := time.NewTicker(5 * time.Second)
	defer refresh.Stop()
	for {
		hot, err := s.store.HotTokens(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("hot set unreadable", "error", err)
		} else {
			s.syncHot(ctx, hot)
		}
		if s.leading() {
			s.obligations(ctx)
			s.requests(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-refresh.C:
		case <-s.wake:
		}
	}
}

// syncHot is _sync_streams: one stream group per endpoint for every active instrument
// of a hot token; groups whose members changed get their new membership.
func (s *Service) syncHot(ctx context.Context, hot []map[string]any) {
	since := map[string]time.Time{}
	for _, row := range hot {
		token := text(row["token_id"])
		at, err := pytime.Parse(row["since"])
		if err != nil {
			continue
		}
		if old, ok := since[token]; !ok || at.Before(old) {
			since[token] = at
		}
	}
	desired := map[string]map[string]bool{}
	for _, iid := range s.opt.Order {
		inst := s.opt.Instruments[iid]
		if inst == nil || !inst.Active {
			continue
		}
		if _, ok := since[inst.TokenID]; !ok {
			continue
		}
		for _, endpoint := range stream.EndpointKeys(inst) {
			if desired[endpoint] == nil {
				desired[endpoint] = map[string]bool{}
			}
			desired[endpoint][iid] = true
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hotSince = since
	for endpoint, g := range s.groups {
		if desired[endpoint] == nil {
			g.cancel()
			delete(s.groups, endpoint)
		}
	}
	for endpoint, members := range desired {
		if g := s.groups[endpoint]; g != nil && !g.finished() {
			g.update(members)
			continue
		}
		s.groups[endpoint] = s.startGroup(ctx, endpoint, members)
	}
}

// hot reports whether a token is in the hot set, and its earliest since.
func (s *Service) hot(token string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.hotSince[token]
	return at, ok
}

func (s *Service) mainConn(iid string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.members[iid]; m != nil && m.state == "CONNECTED" {
		return m.conn
	}
	return 0
}

// streamRows are the current stream states.
func (s *Service) streamRows() []streamRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []streamRow
	for _, iid := range s.opt.Order {
		inst := s.opt.Instruments[iid]
		if inst == nil {
			continue
		}
		_, hot := s.hotSince[inst.TokenID]
		if m := s.members[iid]; m != nil || hot {
			r := streamRow{inst: inst, stream: "WS", state: "DISCONNECTED"}
			if m != nil {
				r.endpoint, r.state, r.connected, r.message = m.endpoint, m.state, m.connected, m.message
				if m.state != "CONNECTED" {
					r.connected = time.Time{}
				}
			}
			if w, ok := s.marks.Get(iid); ok {
				r.watermark = w.TS
			}
			rows = append(rows, r)
		}
		if m := s.depth[iid]; m != nil {
			rows = append(rows, streamRow{inst: inst, stream: "DEPTH", endpoint: m.endpoint, state: m.state, connected: m.connected, message: m.message})
		}
		if polls := s.klinesPolled[iid]; len(polls) > 0 {
			rows = append(rows, streamRow{inst: inst, stream: "KLINES_POLL", state: "POLLING", polls: append([]string(nil), polls...)})
		}
	}
	return rows
}

// publishStreamsStopped marks every socket of this writer disconnected at shutdown.
func (s *Service) publishStreamsStopped() {
	s.mu.Lock()
	for _, m := range s.members {
		m.state = "DISCONNECTED"
	}
	for _, m := range s.depth {
		m.state = "DISCONNECTED"
	}
	s.mu.Unlock()
	s.publishStreams()
}

// latePrintRevision handles a late print for a complete bar of a venue without REST trade
// history, where the minute cannot be rebuilt. A print at or before the bar's last
// counted print is a re-delivery (a reconnect replays recent prints) and changes nothing.
// A newer one revises the bar: bar_version + 1, revised, still complete, with the print
// recorded in capture_proof.revision.late_print. A complete bar is never downgraded.
func (s *Service) latePrintRevision(key flow.Key, stored map[string]any, t *normalize.Trade, now time.Time) {
	if last, err := pytime.Parse(pyval.Map(stored["last_trade"])["ts"]); err == nil && !t.TS.After(last) {
		return
	}
	version := int64(1)
	if v, err := pyval.Int(stored["bar_version"]); err == nil && v > 0 {
		version = v
	}
	if known, ok := s.seal.Version(key); ok && known > version {
		version = known
	}
	next := version + 1
	row := copyRow(stored)
	delete(row, "semantic_fingerprint")
	row["bar_version"], row["revised"] = next, true
	late := map[string]any{"trade_id": t.TradeID, "ts": pytime.ISO(t.TS), "price": t.Price.String(), "qty": t.Qty.String(), "side": t.Side}
	row["capture_proof"] = map[string]any{
		"sealed_by": text(pyval.Map(stored["capture_proof"])["sealed_by"]),
		"revision": map[string]any{
			"version": next, "reason": "LATE_PRINT_AFTER_SEAL", "observed_at": pytime.ISO(now),
			"previous": flowSummary(stored), "late_print": late,
		},
	}
	s.seal.Forget(key, next)
	s.points.Put("mkt.flow_bars_1m", key.IID+":"+key.MinuteISO(), row)
}

// confirm is the second book path (03 §5.9) of a listed venue.
func (s *Service) confirm(inst *venue.Instrument, row map[string]any) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(s.context(), 30*time.Second)
	defer cancel()
	return s.venues.Venue(inst, 2).IndependentBook(ctx, row)
}

func (s *Service) context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// lateMinute is a print for a minute the sealer no longer holds (or one before this
// process's first connection): the minute is rebuilt over REST against its stored row,
// once for a burst of late prints. Without REST trade history (Hyperliquid) a complete
// bar is never downgraded: see latePrintRevision.
func (s *Service) lateMinute(inst *venue.Instrument, t *normalize.Trade) {
	key := flow.KeyOf(inst.ID, t.TS)
	s.mu.Lock()
	if s.late[key] {
		s.mu.Unlock()
		return
	}
	s.late[key] = true
	s.mu.Unlock()
	minute := key.MinuteTime()
	s.after(5*time.Second, func(ctx context.Context) {
		defer func() {
			s.mu.Lock()
			delete(s.late, key)
			s.mu.Unlock()
		}()
		if _, none := NoTradeHistory[inst.Venue]; none {
			stored, err := s.store.Get(ctx, "mkt.flow_bars_1m", inst.ID+":"+key.MinuteISO())
			if err != nil || stored == nil || stored["complete"] != true {
				return
			}
			s.latePrintRevision(key, stored, t, s.clock.Now())
			return
		}
		s.queueRepair(inst, span{minute, minute.Add(time.Minute)}, "LATE_PRINT", nil)
	})
}
