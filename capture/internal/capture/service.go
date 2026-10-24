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

type Service struct {
	opt    Options
	store  Store
	writer Writer
	venues Venues
	dial   Dialer
	clock  Clock
	log    *slog.Logger

	repairGate     chan struct{}
	repairGateOnce sync.Once

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
	hotSince     map[string]time.Time
	groups       map[string]*group
	members      map[string]*memberState
	depth        map[string]*memberState
	books        map[string]*books.Book
	metadata     map[string]map[string]any
	klinesPolled map[string][]string
	obliged      map[string]bool
	flowLocks    map[string]*sync.Mutex
	ctxLocks     map[string]*sync.Mutex
	late         map[flow.Key]bool
	repairs      map[string]*repairQueue
	fxCache      map[string][2]string
	history      map[[2]string]time.Time
	reads        map[string][2]int
	frames       map[string]int

	flushMu   sync.Mutex
	lastFlush atomic.Int64
	flushErr  atomic.Value
}

type memberState struct {
	member    *stream.Member
	conn      int64
	endpoint  string
	state     string
	connected time.Time
	message   time.Time
}

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

func (s *Service) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) LastFlush() time.Time {
	if n := s.lastFlush.Load(); n != 0 {
		return time.Unix(0, n).UTC()
	}
	return time.Time{}
}

type Status struct {
	HotTokens        []string `json:"hot_tokens"`
	StreamGroups     int      `json:"stream_groups"`
	Members          int      `json:"members"`
	MembersConnected int      `json:"members_connected"`
	LastFlush        string   `json:"last_flush"`
	LastFlushError   string   `json:"last_flush_error,omitempty"`
}

func (s *Service) Status() Status {
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
	status := Status{
		HotTokens: hot, StreamGroups: len(s.groups), Members: len(s.members),
		MembersConnected: connected, LastFlush: pytime.ISO(s.LastFlush()),
	}
	if err, ok := s.flushErr.Load().(string); ok && err != "" {
		status.LastFlushError = err
	}
	return status
}

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

		s.flush(ctx)

		s.sweep(ctx)
		s.startupRepairs(ctx, hot, maxTime(started, s.clock.Now()))
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

const fetchPruneEvery, fetchKeep = 10 * time.Minute, time.Hour

func (s *Service) pruneFetches(ctx context.Context) {
	if s.opt.CanWrite != nil && !s.opt.CanWrite() {
		return
	}
	if _, err := s.store.PruneFetches(ctx, s.clock.Now().Add(-fetchKeep)); err != nil {
		s.log.Warn("fetch prune failed", "error", err)
	}
}

func (s *Service) raw(t *normalize.Trade) {
	if s.opt.Raw != nil {
		s.opt.Raw.Add(t)
	}
}

func (s *Service) flushRaw() {
	if s.opt.Raw == nil || (s.opt.CanWrite != nil && !s.opt.CanWrite()) {
		return
	}
	if err := s.opt.Raw.Flush(s.clock.Now()); err != nil {
		s.log.Warn("raw trade flush failed", "error", err.Error())
	}
}

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

func (s *Service) after(d time.Duration, f func(ctx context.Context)) {
	s.spawn(func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-time.After(d):
			f(ctx)
		}
	})
}

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

func (s *Service) tick(ctx context.Context) {
	for _, check := range s.seal.Tick(s.clock.Now()) {
		check := check
		s.spawn(func(ctx context.Context) { s.headCheck(ctx, check) })
	}
	s.flush(ctx)
}

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

func (s *Service) repairSlot(ctx context.Context) (func(), error) {
	select {
	case s.slots <- struct{}{}:
		return func() { <-s.slots }, nil
	case <-ctx.Done():
		return nil, ErrStopped
	}
}

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
