package capture

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// checkpoint is a reconnect or repair checkpoint: REST pages from the last proven print.
type checkpoint struct {
	lastTS       time.Time
	lastTradeID  string
	lastTrade    map[string]any
	endCursor    any
	handoffRetry int
}

// gapPair is a trade-ID hole a REST rebuild found: the prints before and after it.
type gapPair struct{ before, after *normalize.Trade }

// minuteBuild is one minute rebuilt from REST prints (and the prints the sealer holds).
type minuteBuild struct {
	key    flow.Key
	bar    *flow.Bar
	first  *normalize.Trade // lowest print in continuity order
	last   *normalize.Trade
	gaps   []gapPair
	held   bool
	sealed bool
}

// restFlow pages one backfill's prints and rebuilds the minutes of [lo, end) as the
// paging leaves them behind, so a day of prints is never held at once.
type restFlow struct {
	s        *Service
	inst     *venue.Instrument
	lo, end  time.Time
	ids      bool
	pending  map[int64][]*normalize.Trade // minute -> prints of a minute not yet built
	seen     map[string]bool
	built    map[int64]*minuteBuild
	previous *normalize.Trade // newest print before the earliest pending minute (forward paging)
	err      error
}

func newRESTFlow(s *Service, inst *venue.Instrument, lo, end time.Time) *restFlow {
	return &restFlow{
		s: s, inst: inst, lo: pytime.MinuteFloor(lo), end: end, ids: ConsecutiveIDs(inst),
		pending: map[int64][]*normalize.Trade{}, seen: map[string]bool{}, built: map[int64]*minuteBuild{},
	}
}

// add collects a print for the rebuild range (ingest_trade(backfill=True)).
func (r *restFlow) add(t *normalize.Trade) {
	if t.TS.Before(r.lo) || !t.TS.Before(r.end) {
		if t.TS.Before(r.lo) && (r.previous == nil || lessPrint(r.previous, t, r.ids)) {
			r.previous = t
		}
		return
	}
	if r.seen[t.TradeID] {
		return
	}
	r.seen[t.TradeID] = true
	t.SourcePayload = nil // a rebuilt bar needs the print, not its frame
	m := pytime.MinuteFloor(t.TS).Unix()
	if r.built[m] != nil {
		r.err = fmt.Errorf("print %s after its minute was built", t.TradeID)
		return
	}
	r.pending[m] = append(r.pending[m], t)
}

// closeBefore builds every pending minute before `before` (forward paging passed them).
func (r *restFlow) closeBefore(before time.Time) {
	var minutes []int64
	for m := range r.pending {
		if time.Unix(m, 0).UTC().Add(time.Minute).Compare(before) <= 0 {
			minutes = append(minutes, m)
		}
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i] < minutes[j] })
	for _, m := range minutes {
		r.build(m)
	}
}

// closeAfter builds every pending minute that starts after `after` (backward paging
// reached a print older than them).
func (r *restFlow) closeAfter(after time.Time) {
	for m := range r.pending {
		if time.Unix(m, 0).UTC().After(after) {
			r.build(m)
		}
	}
}

func (r *restFlow) build(m int64) {
	minute := time.Unix(m, 0).UTC()
	key := flow.Key{IID: r.inst.ID, Minute: m}
	prints := r.pending[m]
	delete(r.pending, m)
	held := r.s.seal.Hold(key)
	for _, t := range held.Trades {
		if !r.seen[t.TradeID] {
			prints = append(prints, t)
		}
	}
	sort.SliceStable(prints, func(i, j int) bool { return lessPrint(prints[i], prints[j], r.ids) })
	acc := flow.New()
	var prior *normalize.Trade
	for _, t := range r.previousOf(minute) {
		prior = t
	}
	if prior != nil {
		acc.Last[r.inst.ID] = prior
	}
	acc.EmptyMinute(r.inst, minute)
	b := &minuteBuild{key: key, held: held.Live || held.Sealed != nil, sealed: held.Sealed != nil}
	var previous *normalize.Trade
	for _, t := range prints {
		if previous != nil && r.ids && t.TradeID != previous.TradeID {
			cur, ok1 := intID(t.TradeID)
			prev, ok2 := intID(previous.TradeID)
			if ok1 && ok2 && cur != prev+1 {
				b.gaps = append(b.gaps, gapPair{previous, t})
			}
		}
		acc.Add(t, false)
		previous = t
	}
	if len(prints) > 0 {
		b.first, b.last = prints[0], prints[len(prints)-1]
	}
	b.bar = acc.Bars[key]
	b.bar.Coverage = nil
	if _, boundary := r.s.boundaryMinute(key); !boundary && !b.held {
		b.bar.Trades = nil // Record walks prints only at window boundaries
	}
	r.built[m] = b
}

// previousOf is the newest collected print before minute (for the bar's prior trade).
func (r *restFlow) previousOf(minute time.Time) []*normalize.Trade {
	var best *normalize.Trade
	consider := func(t *normalize.Trade) {
		if t.TS.Before(minute) && (best == nil || lessPrint(best, t, r.ids)) {
			best = t
		}
	}
	if r.previous != nil {
		consider(r.previous)
	}
	for m, prints := range r.pending {
		if time.Unix(m, 0).UTC().Before(minute) {
			for _, t := range prints {
				consider(t)
			}
		}
	}
	for m, b := range r.built {
		if time.Unix(m, 0).UTC().Before(minute) && b.last != nil {
			consider(b.last)
		}
	}
	if best == nil {
		return nil
	}
	return []*normalize.Trade{best}
}

// finish builds what is left and every empty minute of the range.
func (r *restFlow) finish() {
	r.closeBefore(r.end.Add(time.Minute))
	for m := r.lo; m.Before(r.end); m = m.Add(time.Minute) {
		if r.built[m.Unix()] == nil {
			r.pending[m.Unix()] = nil
			r.build(m.Unix())
		}
	}
}

// pagingInfo is what _backfill learns while paging.
type pagingInfo struct {
	proven      []span
	firstWindow *normalize.Trade // lowest-ID print of [start, end) (Binance, Kraken)
	firstAfter  *normalize.Trade // lowest-ID print at or after start (Kraken)
	reference   *normalize.Trade // newest print before start
	restLast    int64
	restLastSet bool
	minTime     time.Time
	krakenFwd   []span
	err         error
}

func (s *Service) normalizeREST(inst *venue.Instrument, rows []any) ([]*normalize.Trade, error) {
	now := s.clock.Now()
	out := make([]*normalize.Trade, 0, len(rows))
	for _, raw := range rows {
		t, err := normalize.NormalizeTrade(inst, raw, now, "REST", s.fx)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TS.Before(out[j].TS) })
	return out, nil
}

// page is the paging loop of _backfill over its slices.
func (s *Service) page(ctx context.Context, inst *venue.Instrument, v Venue, rf *restFlow, start, end, captureStart time.Time, cp *checkpoint) pagingInfo {
	key := inst.Key()
	binance := strings.HasPrefix(key, "binance")
	kraken := strings.HasPrefix(key, "kraken")
	consecutive := binance || strings.HasPrefix(key, "coinbase") || kraken
	var slices []span
	if binance {
		n := int(end.Sub(start).Seconds()/3600) + 1
		for i := 0; i < max(1, n); i++ {
			lo := start.Add(time.Duration(i) * time.Hour)
			if lo.Before(end) {
				slices = append(slices, span{lo, minTime(lo.Add(time.Hour), end)})
			}
		}
	} else {
		slices = []span{{captureStart, end}}
	}
	var info pagingInfo
	seen := map[string]bool{}
	for index, sl := range slices {
		lo, hi := sl.start, sl.end
		var cursor any
		switch {
		case cp != nil && cp.lastTradeID != "" && binance && index == 0:
			id, err := pyval.Int(cp.lastTradeID)
			if err == nil {
				cursor = id + 1
			}
		case cp != nil && !binance:
			cursor = cp.endCursor
		}
		exhausted := false
		for p := 0; p < 10000; p++ {
			if ctx.Err() != nil {
				info.err = ctx.Err()
				return info
			}
			last := hi.Add(-time.Millisecond)
			rows, next, err := v.Trades(ctx, &lo, &last, cursor)
			if err != nil {
				info.err = err
				return info
			}
			if len(rows) == 0 {
				if binance {
					exhausted = true
				}
				break
			}
			normalized, err := s.normalizeREST(inst, rows)
			if err != nil {
				info.err = err
				return info
			}
			for _, t := range normalized {
				ts := t.TS
				if ts.Before(start) && (info.reference == nil || ts.After(info.reference.TS)) {
					info.reference = t
				}
				if (binance || kraken) && !ts.Before(start) && ts.Before(end) {
					if info.firstWindow == nil || idLess(t.TradeID, info.firstWindow.TradeID) {
						info.firstWindow = t
					}
				}
				if kraken && !ts.Before(start) && (info.firstAfter == nil || idLess(t.TradeID, info.firstAfter.TradeID)) {
					info.firstAfter = t
				}
				if ts.Before(end) && (cp == nil || !ts.Before(captureStart)) && (!binance || (!ts.Before(lo) && ts.Before(hi))) {
					if info.minTime.IsZero() || ts.Before(info.minTime) {
						info.minTime = ts
					}
					if consecutive {
						if id, ok := intID(t.TradeID); ok && (!info.restLastSet || id > info.restLast) {
							info.restLast, info.restLastSet = id, true
						}
					}
					rf.add(t)
				} else if ts.Before(start) {
					rf.add(t) // a reference candidate belongs to its minute's bar too
				}
			}
			newest, oldest := normalized[len(normalized)-1], normalized[0]
			if binance || kraken {
				rf.closeBefore(pytime.MinuteFloor(newest.TS))
			} else {
				rf.closeAfter(pytime.MinuteFloor(oldest.TS))
			}
			if binance {
				if len(rows) < 1000 || !newest.TS.Before(hi) {
					exhausted = true
					break
				}
				id, _ := intID(newest.TradeID)
				next = id + 1
			} else if kraken {
				if !newest.TS.Before(end) {
					exhausted = !info.minTime.IsZero() && !info.minTime.After(start)
					if !exhausted {
						info.krakenFwd = append(info.krakenFwd, span{lo, minTime(hi, s.clock.Now())})
					}
					break
				}
			} else if !oldest.TS.After(start) {
				exhausted = true
				break
			}
			if next == nil || seen[text(next)] {
				break
			}
			seen[text(next)] = true
			cursor = next
		}
		if exhausted {
			info.proven = append(info.proven, span{lo, minTime(hi, s.clock.Now())})
		}
	}
	return info
}

func idLess(a, b string) bool {
	x, ok1 := intID(a)
	y, ok2 := intID(b)
	if ok1 && ok2 {
		return x < y
	}
	return a < b
}

// rangesCover is _ranges_cover.
func rangesCover(ranges []span, start, end time.Time) bool {
	sorted := append([]span(nil), ranges...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].start.Equal(sorted[j].start) {
			return sorted[i].start.Before(sorted[j].start)
		}
		return sorted[i].end.Before(sorted[j].end)
	})
	cursor := start
	for _, r := range sorted {
		if r.start.After(cursor) {
			return false
		}
		cursor = maxTime(cursor, r.end)
		if !cursor.Before(end) {
			return true
		}
	}
	return !cursor.Before(end)
}

// backfill is MarketService.backfill/_backfill: page immutable prints over REST and
// rebuild the minutes they prove. With a checkpoint it repairs [checkpoint, anchor); without
// one it captures the 24 h before anchor and the reference print before them. Unproven
// endpoints and retention stay explicit gaps.
func (s *Service) backfill(ctx context.Context, inst *venue.Instrument, anchor time.Time, cp *checkpoint) error {
	unlock := s.flowLock(inst.ID)
	defer unlock()
	key := inst.Key()
	var start time.Time
	if cp != nil {
		start = cp.lastTS
	} else {
		start = anchor.Add(-24 * time.Hour)
	}
	end := anchor
	captureStart := start
	if cp == nil {
		captureStart = start.Add(-time.Minute)
	}
	s.registerBoundaries([]string{inst.ID}, end, &start)
	priority := 1
	if cp != nil {
		priority = 0 // a reconnect repair completes live minutes
	}
	v := s.venues.Venue(inst, priority)
	rf := newRESTFlow(s, inst, start, end)
	info := s.page(ctx, inst, v, rf, start, end, captureStart, cp)
	if info.err != nil {
		if ctx.Err() != nil {
			return ErrStopped
		}
		s.gap(inst, "PRE_EVENT_FLOW", info.err.Error(), &start, &end)
	}
	boundary := start
	if cp == nil && info.reference != nil && !strings.HasPrefix(key, "binance") {
		s.recordReference(inst, boundary, info.reference, "REST_ENUMERATION")
	}
	if cp == nil && key == "binance.spot" && info.firstWindow == nil {
		s.binanceEndBound(ctx, inst, v, rf, start)
	}
	if cp == nil && strings.HasPrefix(key, "binance") && info.firstWindow != nil {
		s.binancePredecessor(ctx, inst, v, rf, start, info.firstWindow)
	}
	if cp == nil && key == "kraken.spot" && s.seal.ReferenceAt(inst.ID, start) == nil {
		s.krakenReference(ctx, inst, v, rf, start)
	}
	if len(info.krakenFwd) > 0 && (info.firstWindow != nil || info.firstAfter != nil) {
		previousID := ""
		if cp != nil {
			previousID = cp.lastTradeID
		} else if ref := s.seal.ReferenceAt(inst.ID, start); ref != nil {
			previousID = text(pyval.Map(ref["last_trade"])["trade_id"])
		}
		// Consecutive per-pair IDs: the first print of the window (or, in a window without
		// prints, the first one after it) following the print before it proves the window.
		next := info.firstWindow
		if next == nil {
			next = info.firstAfter
		}
		first, ok1 := intID(next.TradeID)
		prev, ok2 := intID(previousID)
		if previousID != "" && ok1 && ok2 && first == prev+1 {
			info.proven = append(info.proven, info.krakenFwd...)
		}
	}
	if cp != nil && !rangesCover(info.proven, start, end) {
		s.gap(inst, "RECONNECT", "CHECKPOINT_RANGE_NOT_EXHAUSTED", &start, &end)
	}
	if cp == nil {
		limit := ""
		if strings.HasPrefix(key, "bybit") {
			limit = " (" + BybitLimit + ")"
		}
		for _, hours := range []int{24, 4, 1} {
			lo := end.Add(-time.Duration(hours) * time.Hour)
			if !rangesCover(info.proven, lo, end) {
				s.gap(inst, "PRE_EVENT_FLOW", fmt.Sprintf("%dh window not fully covered by retained REST trades%s", hours, limit), &lo, &end)
			}
		}
		if s.seal.ReferenceAt(inst.ID, start) == nil {
			s.gap(inst, "PRE24_REFERENCE", "NO_RETAINED_TRADE_BEFORE_PRE24_START", &captureStart, &start)
		}
	}
	rf.finish()
	if rf.err != nil {
		s.gap(inst, "PRE_EVENT_FLOW", rf.err.Error(), &start, &end)
	}
	gaps := s.persistRESTFlow(ctx, inst, rf, start, end, info.proven, cp)
	var handoff []gapPair
	for _, g := range gaps {
		if id, ok := intID(g.after.TradeID); !info.restLastSet || (ok && id > info.restLast) {
			handoff = append(handoff, g)
		}
	}
	retries := 0
	if cp != nil {
		retries = cp.handoffRetry
	}
	if cp != nil && len(handoff) > 0 && retries < HandoffRetries {
		// REST trails the stream: ask again from the last print it had.
		before := handoff[0].before
		for _, g := range handoff[1:] {
			if idLess(g.before.TradeID, before.TradeID) {
				before = g.before
			}
		}
		retry := &checkpoint{lastTS: pytime.MinuteFloor(before.TS), lastTradeID: before.TradeID, handoffRetry: retries + 1}
		s.after(HandoffRetry, func(ctx context.Context) {
			if err := s.backfill(ctx, inst, end, retry); err != nil && err != ErrStopped {
				s.log.Warn("handoff retry failed", "instrument", inst.ID, "error", err)
			}
		})
	}
	_, err := s.backfillKlines(ctx, inst, start, end, cp == nil, "PRE_EVENT_KLINES")
	return err
}

// persistRESTFlow is _persist_rest_flow_ranges: every minute of [floor(start), end) is
// written from its prints and proofs, a complete stored bar the rebuild reproduces stands
// as written, and the trade-ID gaps found are recorded and returned.
func (s *Service) persistRESTFlow(ctx context.Context, inst *venue.Instrument, rf *restFlow, start, end time.Time, proven []span, cp *checkpoint) []gapPair {
	if len(proven) == 0 {
		return nil // nothing proven: the stream's bars and the stored ones stand
	}
	now := s.clock.Now()
	minutes := make([]int64, 0, len(rf.built))
	for m := range rf.built {
		minutes = append(minutes, m)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i] < minutes[j] })
	stored := map[string]map[string]any{}
	if len(minutes) > 0 {
		rows, err := s.store.Range(ctx, "mkt.flow_bars_1m", "minute_start", rf.lo, end, map[string]any{"instrument_id": inst.ID})
		if err != nil {
			s.gap(inst, "PRE_EVENT_FLOW", "stored bars unreadable: "+err.Error(), &start, &end)
			return nil // never write over bars this rebuild could not compare
		}
		for _, row := range rows {
			stored[text(row["minute_start"])] = row
		}
	}
	var proofs []flow.Proof
	for _, p := range proven {
		proofs = append(proofs, flow.Proof{Start: p.start, End: p.end, Complete: true, Source: sourceREST, Observed: now})
	}
	// The print before the range: an explicit reference, else none (a checkpoint repair
	// starts at a proven print or a minute start).
	var previous *normalize.Trade
	if ref := s.seal.ReferenceAt(inst.ID, start); ref != nil && rf.ids {
		if lt := pyval.Map(ref["last_trade"]); lt != nil {
			if ts, err := pytime.Parse(lt["ts"]); err == nil {
				previous = &normalize.Trade{TradeID: text(lt["trade_id"]), TS: ts}
			}
		}
	}
	if cp != nil && cp.lastTradeID != "" && rf.ids {
		ts := cp.lastTS
		if lt := cp.lastTrade; lt != nil {
			if parsed, err := pytime.Parse(lt["ts"]); err == nil {
				ts = parsed
			}
		}
		previous = &normalize.Trade{TradeID: cp.lastTradeID, TS: ts}
	}
	var found []gapPair
	for _, m := range minutes {
		b := rf.built[m]
		if previous != nil && b.first != nil && rf.ids && b.first.TradeID != previous.TradeID {
			cur, ok1 := intID(b.first.TradeID)
			prev, ok2 := intID(previous.TradeID)
			if ok1 && ok2 && cur != prev+1 {
				b.gaps = append([]gapPair{{previous, b.first}}, b.gaps...)
			}
		}
		if b.last != nil {
			previous = b.last
		}
		found = append(found, b.gaps...)
	}
	var holes []flow.Proof
	for _, g := range found {
		holes = append(holes, flow.Proof{Start: g.before.TS, End: g.after.TS.Add(time.Microsecond), Complete: false, Source: "REST", Reason: "REST_TRADE_ID_GAP", Observed: now})
		lo, hi := g.before.TS, g.after.TS
		s.gap(inst, "TRADE_INTEGRITY", "REST_TRADE_ID_GAP", &lo, &hi)
	}
	for _, m := range minutes {
		if ctx.Err() != nil {
			return found
		}
		b := rf.built[m]
		minute := b.key.MinuteTime()
		var mine []flow.Proof
		for _, p := range append(append([]flow.Proof(nil), proofs...), holes...) {
			if p.Start.Before(minute.Add(time.Minute)) && p.End.After(minute) {
				mine = append(mine, p)
			}
		}
		if len(b.gaps) > 0 {
			b.bar.GapReason = "REST_TRADE_ID_GAP"
		}
		if s.seal.Rebuilt(b.key, b.bar, mine, now) {
			continue
		}
		b.bar.Coverage = mine
		complete := flow.Covered(minute, minute.Add(time.Minute), mine) && b.bar.GapReason == ""
		row := s.recordBuilt(b, complete, now)
		s.writeStored(inst, b.key, row, stored[b.key.MinuteISO()], "REST_REPAIR", now)
	}
	return found
}

// recordBuilt is the typed row of a rebuilt minute the sealer does not hold.
func (s *Service) recordBuilt(b *minuteBuild, complete bool, now time.Time) map[string]any {
	acc := flow.New()
	acc.Bars[b.key] = b.bar
	refs, points := s.seal.References(b.key)
	acc.References = refs
	if points != nil {
		acc.Boundaries[b.key] = points
	}
	row := acc.Record(b.key, &complete, true, now)
	if !complete && row["gap_reason"] == nil {
		row["gap_reason"] = unprovenReason(b.bar.Coverage)
	}
	return row
}

// writeStored writes a minute the sealer does not hold against its stored row: a missing
// or provisional row is sealed, a complete row is never replaced by an incomplete one nor
// rewritten when the rebuild reproduces it, any other change is a revision.
func (s *Service) writeStored(inst *venue.Instrument, key flow.Key, row, stored map[string]any, reason string, now time.Time) {
	s.writeRevised(inst, key, row, stored, reason, now, false)
}

// writeRevised is writeStored; with invalidate a complete stored bar may be revised to
// incomplete (a print it lacks is known but cannot be rebuilt).
func (s *Service) writeRevised(inst *venue.Instrument, key flow.Key, row, stored map[string]any, reason string, now time.Time, invalidate bool) {
	version := int64(0)
	if stored != nil {
		if v, err := pyval.Int(stored["bar_version"]); err == nil {
			version = v
		} else {
			version = 1 // written by the in-process capture: final as written
		}
	}
	if known, ok := s.seal.Version(key); ok && known > version {
		version = known
	}
	if stored != nil && version >= 1 {
		storedComplete := stored["complete"] == true
		if storedComplete && row["complete"] != true && !invalidate {
			return
		}
		if SameFlow(stored, row) && (stored["complete"] == true) == (row["complete"] == true) &&
			text(stored["gap_reason"]) == text(row["gap_reason"]) {
			return
		}
	}
	next := version + 1
	row["bar_version"], row["revised"] = next, next > 1
	method := sourceREST
	if row["complete"] != true {
		method = sealedFinal
	}
	proof := map[string]any{"sealed_by": method, "observed_at": pytime.ISO(now)}
	if next > 1 {
		proof["revision"] = map[string]any{"version": next, "reason": reason, "observed_at": pytime.ISO(now), "previous": flowSummary(stored)}
	}
	row["capture_proof"] = proof
	s.seal.Forget(key, next)
	s.points.Put("mkt.flow_bars_1m", key.IID+":"+key.MinuteISO(), row)
}

// recordReference is _record_reference: an explicit print strictly before a boundary.
func (s *Service) recordReference(inst *venue.Instrument, boundary time.Time, t *normalize.Trade, source string) {
	if !t.TS.Before(boundary) {
		return
	}
	point := map[string]any{
		"complete": true, "last_trade": flow.PointOf(t).Map(), "coverage_reason": nil, "source": source,
	}
	for k, v := range pytime.Fields("boundary", pytime.ISO(boundary), pytime.Exact) {
		point[k] = v
	}
	pytime.CollectionClock(s.clock.Now()).Into(point, "observed_at")
	s.seal.Reference(inst.ID, boundary, point)
}

// binanceEndBound is the binance.spot PRE24 reference of a wholly quiet window: endTime
// alone returns the newest records up to that bound.
func (s *Service) binanceEndBound(ctx context.Context, inst *venue.Instrument, v Venue, rf *restFlow, start time.Time) {
	bound := start.Add(-time.Millisecond)
	rows, _, err := v.Trades(ctx, nil, &bound, nil)
	if err != nil {
		s.gap(inst, "PRE24_REFERENCE", err.Error(), nil, &start)
		return
	}
	prints, err := s.normalizeREST(inst, rows)
	if err != nil {
		s.gap(inst, "PRE24_REFERENCE", err.Error(), nil, &start)
		return
	}
	var best *normalize.Trade
	for _, t := range prints {
		if t.TS.Before(start) && (best == nil || idLess(best.TradeID, t.TradeID)) {
			best = t
		}
	}
	if best != nil {
		rf.add(best)
		s.recordReference(inst, start, best, "BINANCE_END_BOUND")
	}
}

// binancePredecessor walks back by aggregate ID to the actual print before the window.
func (s *Service) binancePredecessor(ctx context.Context, inst *venue.Instrument, v Venue, rf *restFlow, start time.Time, first *normalize.Trade) {
	before, _ := intID(first.TradeID)
	for i := 0; i < 10; i++ {
		if before <= 0 {
			return
		}
		lower := max(int64(0), before-1000)
		rows, _, err := v.Trades(ctx, nil, nil, lower)
		if err != nil {
			s.gap(inst, "PRE24_REFERENCE", err.Error(), nil, &start)
			return
		}
		prints, err := s.normalizeREST(inst, rows)
		if err != nil {
			s.gap(inst, "PRE24_REFERENCE", err.Error(), nil, &start)
			return
		}
		var best *normalize.Trade
		for _, t := range prints {
			id, _ := intID(t.TradeID)
			if id < before && t.TS.Before(start) && (best == nil || idLess(best.TradeID, t.TradeID)) {
				best = t
			}
		}
		if best != nil {
			rf.add(best)
			s.recordReference(inst, start, best, "BINANCE_ID_PREDECESSOR")
			return
		}
		before = lower
	}
}

// krakenReference follows Kraken's forward `since` cursor, widening the search only when
// a quiet interval has no print, until a page crosses the boundary.
func (s *Service) krakenReference(ctx context.Context, inst *venue.Instrument, v Venue, rf *restFlow, start time.Time) {
	span := time.Minute
	var cursor any
	var candidate *normalize.Trade
	query := start.Add(-span)
	epoch := time.Unix(0, 0).UTC()
	for attempt := 0; attempt < 32; attempt++ {
		rows, next, err := v.Trades(ctx, &query, nil, cursor)
		if err != nil {
			s.gap(inst, "PRE24_REFERENCE", err.Error(), &query, &start)
			return
		}
		prints, err := s.normalizeREST(inst, rows)
		if err != nil {
			s.gap(inst, "PRE24_REFERENCE", err.Error(), &query, &start)
			return
		}
		crossed := false
		for _, t := range prints {
			if t.TS.Before(start) && (candidate == nil || t.TS.After(candidate.TS)) {
				candidate = t
			}
			if !t.TS.Before(start) {
				crossed = true
			}
		}
		if candidate != nil && crossed {
			rf.add(candidate)
			s.recordReference(inst, start, candidate, "KRAKEN_SINCE_CURSOR")
			return
		}
		if candidate != nil && next != nil && text(next) != text(cursor) {
			cursor = next
		} else {
			cursor = nil
			span *= 2
			query = start.Add(-minDuration(span, start.Sub(epoch)))
		}
	}
	s.gap(inst, "PRE24_REFERENCE", "PREDECESSOR_SEARCH_REQUEST_BUDGET_EXHAUSTED", &query, &start)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// krakenPredecessor is _kraken_predecessor: the last Kraken print before start, found by
// a `since` page that crosses start.
func (s *Service) krakenPredecessor(ctx context.Context, inst *venue.Instrument, start time.Time) map[string]any {
	v := s.venues.Venue(inst, 0)
	for _, minutes := range []int{2, 10, 60, 240} {
		from := start.Add(-time.Duration(minutes) * time.Minute)
		rows, _, err := v.Trades(ctx, &from, nil, nil)
		if err != nil {
			return nil
		}
		prints, err := s.normalizeREST(inst, rows)
		if err != nil {
			return nil
		}
		var best *normalize.Trade
		crossed := false
		for _, t := range prints {
			if t.TS.Before(start) && (best == nil || idLess(best.TradeID, t.TradeID)) {
				best = t
			}
			if !t.TS.Before(start) {
				crossed = true
			}
		}
		if best != nil && crossed {
			return map[string]any{"last_trade_id": best.TradeID, "last_trade": flow.PointOf(best).Map()}
		}
	}
	return nil
}

// registerBoundaries is _register_capture_boundaries.
func (s *Service) registerBoundaries(ids []string, cutoff time.Time, anchor *time.Time) {
	points := []time.Time{cutoff}
	for _, minutes := range []int{15, 60, 240, 1440} {
		d := time.Duration(minutes) * time.Minute
		points = append(points, cutoff.Add(-d), cutoff.Add(-2*d))
	}
	if anchor != nil {
		points = append(points, *anchor, anchor.Add(-cutoff.Sub(*anchor)))
	}
	for _, iid := range ids {
		s.seal.Boundaries(iid, points)
	}
}

// boundaryMinute reports whether a minute has registered window boundaries.
func (s *Service) boundaryMinute(key flow.Key) (map[int64]time.Time, bool) {
	_, points := s.seal.References(key)
	return points, len(points) > 0
}
