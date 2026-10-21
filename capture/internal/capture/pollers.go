package capture

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// repairQueue serializes the REST repairs of one instrument (one backfill at a time,
// queued ranges merged, the earliest start kept).
type repairQueue struct {
	running bool
	pending []*repairJob
}

type repairJob struct {
	start, end time.Time
	reason     string
	cursor     any
	spans      []span // sealer ranges the job closes
}

// queueRepair asks for a REST repair of [sp.start, sp.end) of an instrument.
func (s *Service) queueRepair(inst *venue.Instrument, sp span, reason string, cursor any) {
	s.mu.Lock()
	q := s.repairs[inst.ID]
	if q == nil {
		q = &repairQueue{}
		s.repairs[inst.ID] = q
	}
	merged := false
	for _, job := range q.pending {
		if !sp.start.After(job.end) && !sp.end.Before(job.start) {
			job.start, job.end = minTime(job.start, sp.start), maxTime(job.end, sp.end)
			job.spans = append(job.spans, sp)
			if job.cursor == nil || (cursor != nil && !sp.end.Before(job.end)) {
				job.cursor = cursor
			}
			merged = true
			break
		}
	}
	if !merged {
		q.pending = append(q.pending, &repairJob{start: sp.start, end: sp.end, reason: reason, cursor: cursor, spans: []span{sp}})
	}
	start := !q.running
	q.running = true
	s.mu.Unlock()
	if start {
		s.spawn(func(ctx context.Context) { s.runRepairs(ctx, inst) })
	}
}

func (s *Service) runRepairs(ctx context.Context, inst *venue.Instrument) {
	for {
		s.mu.Lock()
		q := s.repairs[inst.ID]
		if len(q.pending) == 0 || ctx.Err() != nil {
			q.running = false
			s.mu.Unlock()
			return
		}
		sort.Slice(q.pending, func(i, j int) bool { return q.pending[i].start.Before(q.pending[j].start) })
		job := q.pending[0]
		q.pending = q.pending[1:]
		s.mu.Unlock()
		s.runRepair(ctx, inst, job)
		for _, sp := range job.spans {
			s.seal.RepairDone(inst.ID, sp.start, sp.end)
		}
	}
}

// runRepair repairs one range from the last proven print before it (the checkpoint of
// _repair_instrument).
func (s *Service) runRepair(ctx context.Context, inst *venue.Instrument, job *repairJob) {
	if _, none := NoTradeHistory[inst.Venue]; none {
		return
	}
	if inst.Venue == "bybit" && job.end.Before(s.clock.Now().Add(-BybitRecent)) {
		return
	}
	start := pytime.MinuteFloor(job.start)
	cp := &checkpoint{lastTS: start, endCursor: job.cursor}
	previous := flow.KeyOf(inst.ID, start.Add(-time.Minute))
	var last map[string]any
	if held := s.seal.Hold(previous); held.Sealed != nil && held.Sealed["complete"] == true {
		last = pyval.Map(held.Sealed["last_trade"])
	} else if stored, err := s.store.Get(ctx, "mkt.flow_bars_1m", inst.ID+":"+previous.MinuteISO()); err == nil && stored != nil && stored["complete"] == true {
		last = pyval.Map(stored["last_trade"])
	}
	if text(last["trade_id"]) != "" {
		cp.lastTradeID, cp.lastTrade = text(last["trade_id"]), last
	} else if inst.Venue == "kraken" {
		if p := s.krakenPredecessor(ctx, inst, start); p != nil {
			cp.lastTradeID, cp.lastTrade = text(p["last_trade_id"]), pyval.Map(p["last_trade"])
		}
	}
	end := job.end
	if now := s.clock.Now(); end.After(now) {
		end = now
	}
	if err := s.backfill(ctx, inst, end, cp); err != nil && err != ErrStopped {
		s.log.Warn("repair failed", "instrument", inst.ID, "reason", job.reason, "error", err)
	}
	s.flush(ctx)
}

// headCheck asks REST for the newest prints of an instrument whose chain has gone quiet.
func (s *Service) headCheck(ctx context.Context, check HeadCheck) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := s.venues.Venue(check.Inst, 0).RecentTrades(ctx, headPrints)
	observed := s.clock.Now()
	if err != nil {
		s.seal.HeadResult(check, nil, observed, true)
		return
	}
	prints, err := s.normalizeREST(check.Inst, rows)
	s.seal.HeadResult(check, prints, observed, err != nil)
}

type pollKinds struct{ derivatives, klines, ticker bool }

// instrument is the instrument with its discovered metadata and the current stablecoin
// USD sample (MarketService mutates its registry dicts; the runtime works on copies).
func (s *Service) instrument(iid string) *venue.Instrument {
	inst := s.opt.Instruments[iid].Clone()
	s.mu.Lock()
	md := s.metadata[iid]
	s.mu.Unlock()
	applyMetadata(inst, md)
	return inst
}

func applyMetadata(inst *venue.Instrument, md map[string]any) {
	if md == nil {
		return
	}
	if v, ok := md["contract_multiplier"]; ok {
		inst.ContractMultiplier = v
	}
	if v, ok := md["tick_size"]; ok {
		inst.TickSize = v
	}
	if v, ok := md["funding_interval_h"]; ok && v != nil {
		inst.FundingIntervalH = v
	}
	if v, ok := md["active"].(bool); ok && !v {
		inst.Active = false
	}
}

// discover is MarketService.discover for one instrument.
func (s *Service) discover(ctx context.Context, inst *venue.Instrument) error {
	md, err := s.venues.Venue(inst, 0).Discover(ctx)
	if err != nil {
		return err
	}
	md["resolved_at"] = pytime.ISO(s.clock.Now())
	s.mu.Lock()
	s.metadata[inst.ID] = md
	s.mu.Unlock()
	return nil
}

func (s *Service) discovered(iid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metadata[iid] != nil
}

// discoverAll reads the listed metadata of every instrument once at start (OKX ctVal,
// Hyperliquid meta; the others come from the semantics registry without a request).
func (s *Service) discoverAll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	// In parallel (the venue gates still pace each host): one by one it took ~40 s and
	// held back the streams a standby process connects before a roll hands over.
	sem := make(chan struct{}, discoverParallel)
	var wg sync.WaitGroup
	for _, iid := range s.opt.Order {
		inst := s.opt.Instruments[iid]
		if inst == nil || !inst.Active {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(inst *venue.Instrument) {
			defer func() { <-sem; wg.Done() }()
			if err := s.discover(ctx, inst); err != nil && ctx.Err() == nil {
				s.gap(inst, "METADATA", err.Error(), nil, nil)
			}
		}(inst)
	}
	wg.Wait()
}

// discoverParallel bounds the instruments discovered at once at start.
const discoverParallel = 16

// fxRate is MarketService._fx_rate: the stablecoin USD sample of an instrument's quote,
// fetched at most every 300 s.
func (s *Service) fxRate(ctx context.Context, inst *venue.Instrument) error {
	quote := inst.Quote()
	if quote == "USD" {
		inst.QuoteUSDRate = "1"
		return nil
	}
	now := s.clock.Now()
	s.mu.Lock()
	cached, ok := s.fxCache[quote]
	s.mu.Unlock()
	if ok {
		if at, err := pytime.Parse(cached[1]); err == nil && now.Sub(at) < 300*time.Second {
			inst.QuoteUSDRate, inst.QuoteUSDRateAsOf = cached[0], cached[1]
			return nil
		}
	}
	if quote != "USDT" && quote != "USDC" {
		return nil
	}
	rate, err := s.venues.FXRate(ctx, quote)
	if err != nil {
		return err
	}
	ts := pytime.ISO(s.clock.Now())
	s.mu.Lock()
	s.fxCache[quote] = [2]string{rate, ts}
	s.mu.Unlock()
	s.fx.Remember(quote, map[time.Time]string{pytime.MustParse(ts): rate})
	inst.QuoteUSDRate, inst.QuoteUSDRateAsOf = rate, ts
	return nil
}

// poll is MarketService.poll_once.
func (s *Service) poll(ctx context.Context, iid string, kinds pollKinds, priority int) {
	if !s.discovered(iid) {
		if err := s.discover(ctx, s.opt.Instruments[iid]); err != nil {
			s.gap(s.opt.Instruments[iid], "METADATA", err.Error(), nil, nil)
		}
	}
	inst := s.instrument(iid)
	if err := s.fxRate(ctx, inst); err != nil {
		s.gap(inst, "QUOTE_FX", err.Error(), nil, nil)
	}
	v := s.venues.Venue(inst, priority)
	// OI and funding first: a slow host must not push the HOT derivatives cadence behind
	// klines and tickers (03 §5.1).
	if kinds.derivatives && !inst.Spot() {
		if err := s.pollDerivatives(ctx, inst, v); err != nil {
			s.gap(inst, "DERIVATIVES", err.Error(), nil, nil)
		}
	}
	if kinds.klines {
		rows, err := v.Klines(ctx, nil, nil)
		polled := pytime.ISO(s.clock.Now())
		if err == nil {
			for _, raw := range rows {
				row, kerr := normalize.KlineRecord(inst, raw, s.clock.Now(), s.fx)
				if kerr != nil {
					err = kerr
					break
				}
				s.points.StoreKline(inst, row)
			}
		}
		if err != nil {
			s.gap(inst, "KLINES", err.Error(), nil, nil)
		} else {
			s.mu.Lock()
			polls := append(s.klinesPolled[iid], polled)
			if len(polls) > 5 {
				polls = polls[len(polls)-5:]
			}
			s.klinesPolled[iid] = polls
			s.mu.Unlock()
		}
	}
	if kinds.ticker {
		raw, err := v.Ticker(ctx)
		if err == nil {
			err = s.points.Ticker(inst, raw)
		}
		if err != nil {
			s.gap(inst, "TICKER", err.Error(), nil, nil)
		}
	}
}

// fresh is MarketService._fresh: the points of a repeated REST history newer than those
// processed before, and the time of the newest older one.
func (s *Service) fresh(iid, kind string, rows []any, stamp func(map[string]any) any) (*time.Time, []any) {
	key := [2]string{iid, kind}
	s.mu.Lock()
	last, seen := s.history[key]
	type timed struct {
		at  time.Time
		row any
	}
	var all []timed
	var newest time.Time
	for _, r := range rows {
		at, err := pytime.Parse(stamp(pyval.Map(r)))
		if err != nil {
			continue
		}
		all = append(all, timed{at, r})
		if at.After(newest) {
			newest = at
		}
	}
	if len(all) > 0 {
		if !seen || newest.After(last) {
			s.history[key] = newest
		}
	}
	s.mu.Unlock()
	if !seen {
		return nil, rows
	}
	var older *time.Time
	var out []any
	for _, t := range all {
		if !t.at.After(last) {
			if older == nil || t.at.After(*older) {
				at := t.at
				older = &at
			}
		} else {
			out = append(out, t.row)
		}
	}
	return older, out
}

func settledStamp(r map[string]any) any {
	for _, k := range []string{"fundingTime", "fundingRateTimestamp", "time"} {
		if v, ok := r[k]; ok {
			return v
		}
	}
	return nil
}

// pollDerivatives is the derivatives branch of poll_once.
func (s *Service) pollDerivatives(ctx context.Context, inst *venue.Instrument, v Venue) error {
	d, err := v.Derivatives(ctx)
	if err != nil {
		return err
	}
	iid := inst.ID
	if d.Interval != nil && text(inst.FundingIntervalH) != text(d.Interval) {
		s.mu.Lock()
		md := s.metadata[iid]
		if md == nil {
			md = map[string]any{}
			s.metadata[iid] = md
		}
		md["funding_interval_h"] = d.Interval
		s.mu.Unlock()
		inst.FundingIntervalH = d.Interval
	}
	var histories, current []any
	for _, r := range d.OI {
		m := pyval.Map(r)
		if m["timestamp"] != nil && !pyval.Has(m, "time") {
			histories = append(histories, r)
		} else {
			current = append(current, r)
		}
	}
	_, fresh := s.fresh(iid, "oi", histories, func(m map[string]any) any { return m["timestamp"] })
	now := pytime.ISO(s.clock.Now())
	for _, r := range append(current, fresh...) {
		raw := pyval.Map(r)
		observed := pyval.GetOr(raw, "_capture_observed_at", now)
		row, err := normalize.OIRecord(inst, raw, normalize.OIOptions{Mark: d.Mark, MarkTS: observed, MarkTSPrec: pytime.CollectionOnly}, s.clock.Now())
		if err != nil {
			return err
		}
		if row["mark_price"] != nil {
			var index any
			if d.Current != nil {
				index = pyval.GetOr(d.Current, "indexPrice", d.Current["oraclePx"])
			}
			if index != nil {
				row["index_price"] = text(index)
				for k, v := range pytime.Fields("index_as_of", observed, pytime.CollectionOnly) {
					row[k] = v
				}
			} else {
				row["index_price"] = nil
				for k, v := range pytime.Fields("index_as_of", nil, "") {
					row[k] = v
				}
			}
		}
		s.points.Put("mkt.oi_obs", iid+":"+text(row["metric_key"])+":"+text(row["ts"]), row)
	}
	if d.Current != nil && d.Mark != nil {
		observed := pyval.GetOr(d.Current, "_capture_observed_at", now)
		index := pyval.GetOr(d.Current, "indexPrice", d.Current["oraclePx"])
		indexTS := pyval.GetOr(d.Current, "index_time", observed)
		basis := "COLLECTION_TIME"
		if pyval.Truthy(d.Current["index_time"]) {
			basis = "SOURCE_TIME"
		}
		s.points.SaveMark(inst, points.MarkInput{
			Price: d.Mark, TS: observed, Index: index, IndexTS: indexTS, Source: "PUBLIC_REST",
			Received: d.Current["_capture_observed_at"], IndexClockBasis: basis, Persist: true,
		})
	}
	if d.Current != nil {
		if err := s.points.Funding(inst, d.Current, "CURRENT_ESTIMATE", d.Interval, "PUBLIC_REST"); err != nil {
			return err
		}
	}
	predecessor, settled := s.fresh(iid, "settled", d.Settled, settledStamp)
	sort.SliceStable(settled, func(i, j int) bool {
		a, _ := pytime.Parse(settledStamp(pyval.Map(settled[i])))
		b, _ := pytime.Parse(settledStamp(pyval.Map(settled[j])))
		return a.Before(b)
	})
	previous := predecessor
	for _, r := range settled {
		raw := pyval.Map(r)
		stamp, err := pytime.Parse(settledStamp(raw))
		if err != nil {
			return err
		}
		var interval any
		switch {
		case previous != nil:
			interval = hoursBetween(*previous, stamp)
		case inst.Key() == "hyperliquid.perp":
			interval = pydec.FromInt(1)
		}
		at := stamp
		previous = &at
		if interval == nil {
			s.gap(inst, "FUNDING_INTERVAL", "Oldest settled funding interval unobserved", &stamp, &stamp)
			continue
		}
		if err := s.points.Funding(inst, raw, "SETTLED", interval, "PUBLIC_REST"); err != nil {
			return err
		}
	}
	return nil
}

// hoursBetween is Decimal(str((stamp - previous).total_seconds())) / 3600.
func hoursBetween(previous, stamp time.Time) pydec.Dec {
	seconds := stamp.Sub(previous).Seconds()
	text, _ := floatText(seconds)
	return pydec.MustDiv(pydec.MustParse(text), pydec.FromInt(3600), pydec.DefaultPrec)
}

func floatText(f float64) (string, error) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "0", nil
	}
	return pyjson.FloatRepr(f)
}

// instrumentLoop is _instrument_loop: klines every base_kline_interval_s, BASE tickers
// every base_ticker_interval_s and derivatives (HOT ones run on their own grids), and the hourly kline audit of the
// trailing 25 h, which fetches only the minutes the database lacks.
func (s *Service) instrumentLoop(ctx context.Context, inst *venue.Instrument) {
	iid := inst.ID
	var derivDue, tickerDue time.Duration
	auditDue := s.clock.Monotonic() + 60*time.Second
	for ctx.Err() == nil {
		started := s.clock.Monotonic()
		_, hot := s.hot(inst.TokenID)
		doDeriv := started >= derivDue && !hot
		// A 24 h ticker every base_ticker_interval_s, not every kline minute (r135: 181
		// instruments x 60/h were most of mkt.tickers_24h).
		doTicker := started >= tickerDue && !hot
		s.poll(ctx, iid, pollKinds{derivatives: doDeriv, klines: true, ticker: doTicker}, 0)
		if s.clock.Monotonic() >= auditDue {
			auditDue = s.clock.Monotonic() + time.Duration(s.opt.Threshold("kline_audit_interval_s", 3600)*float64(time.Second))
			now := pytime.MinuteFloor(s.clock.Now())
			if _, err := s.backfillKlines(ctx, s.instrument(iid), now.Add(-25*time.Hour), now, false, "KLINES_GAP"); err != nil && err != ErrStopped {
				s.gap(inst, "KLINES_GAP", err.Error(), nil, nil)
			}
		}
		if doDeriv {
			derivDue = s.clock.Monotonic() + time.Duration(s.opt.Threshold("base_oi_funding_interval_s", 300)*float64(time.Second))
		}
		if doTicker {
			tickerDue = s.clock.Monotonic() + time.Duration(s.opt.Threshold("base_ticker_interval_s", 300)*float64(time.Second))
		}
		wait := time.Duration(s.opt.Threshold("base_kline_interval_s", 60)*float64(time.Second)) - (s.clock.Monotonic() - started)
		if wait < time.Second {
			wait = time.Second
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// hotGrid is _hot_grid: poll a HOT instrument's kinds at every point of a wall-clock grid
// (offset into each interval) ahead of other requests to its host; a request stuck past
// the next point is cut and recorded, so a missed point never doubles an age.
func (s *Service) hotGrid(ctx context.Context, inst *venue.Instrument, interval, offset float64, kinds pollKinds) {
	for ctx.Err() == nil {
		if _, hot := s.hot(inst.TokenID); hot {
			pollCtx, cancel := context.WithTimeout(ctx, time.Duration(interval*float64(time.Second)))
			s.poll(pollCtx, inst.ID, kinds, -1)
			cancel()
		}
		now := float64(s.clock.Now().UnixMicro()) / 1e6
		wait := interval - math.Mod(now-offset, interval)
		if wait < 0.01 {
			wait = 0.01
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(wait * float64(time.Second))):
		}
	}
}

// oiHistoryLoop is the daily _oi_history_pass: Bybit 5-min OI history back
// historical_lookback_days, older than the first stored sample (02 §4.6).
func (s *Service) oiHistoryLoop(ctx context.Context) {
	days := int(s.opt.Threshold("historical_lookback_days", 90))
	for ctx.Err() == nil {
		s.oiHistoryPass(ctx, days)
		select {
		case <-ctx.Done():
		case <-time.After(24 * time.Hour):
		}
	}
}

func (s *Service) oiHistoryPass(ctx context.Context, days int) {
	end := s.clock.Now()
	start := end.Add(-time.Duration(days) * 24 * time.Hour)
	for _, iid := range s.opt.Order {
		inst := s.opt.Instruments[iid]
		if inst == nil || inst.Key() != "bybit.linear" || !inst.Active || ctx.Err() != nil {
			continue
		}
		first, found, err := s.store.Earliest(ctx, "mkt.oi_obs", "ts", start, end, map[string]any{"instrument_id": iid, "metric_key": "bybit.linear.open-interest"})
		if err != nil {
			continue
		}
		upto := end
		if found {
			upto = first.Add(-time.Second)
		}
		if !upto.After(start) {
			continue
		}
		v := s.venues.Venue(inst, 2)
		cursor := ""
		for page := 0; page < 1000; page++ {
			rows, next, err := v.OIHistory(ctx, start, upto, cursor)
			if err != nil {
				s.gap(inst, "HISTORICAL_OI", err.Error(), &start, &upto)
				break
			}
			for _, raw := range rows {
				row, err := normalize.OIRecord(inst, pyval.Map(raw), normalize.OIOptions{}, s.clock.Now())
				if err != nil {
					s.gap(inst, "HISTORICAL_OI", err.Error(), &start, &upto)
					break
				}
				s.points.Put("mkt.oi_obs", iid+":"+text(row["metric_key"])+":"+text(row["ts"]), row)
			}
			s.flush(ctx)
			if len(rows) == 0 || next == "" {
				break
			}
			cursor = next
		}
	}
}
