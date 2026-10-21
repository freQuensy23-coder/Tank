package capture

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// obligations runs the pre-event capture obligations core wrote (mkt.backfill_runs):
// the PENDING ones, and RUNNING ones a stopped writer left.
func (s *Service) obligations(ctx context.Context) {
	rows, err := s.store.BackfillRuns(ctx, []string{"PENDING", "RUNNING"})
	if err != nil {
		s.log.Warn("backfill runs unreadable", "error", err)
		return
	}
	for _, row := range rows {
		id := text(row["backfill_id"])
		s.mu.Lock()
		started := s.obliged[id]
		s.mu.Unlock()
		if started || id == "" {
			continue
		}
		s.mu.Lock()
		s.obliged[id] = true
		s.mu.Unlock()
		row := row
		s.spawn(func(ctx context.Context) { s.runObligation(ctx, row) })
	}
}

// putRun writes a backfill run with MarketService._put's run clocks.
func (s *Service) putRun(ctx context.Context, row map[string]any) {
	row = copyRow(row)
	for _, field := range []string{"started_at", "finished_at"} {
		if row[field] != nil && row[field] != "" {
			for k, v := range pytime.Fields(field, row[field], pytime.CollectionOnly) {
				row[k] = v
			}
			continue
		}
		reason := "INCOMPLETE"
		if st := text(row["status"]); st == "PENDING" || st == "RUNNING" {
			reason = "PENDING"
		}
		row[field], row[field+"_prec"], row[field+"_prec_reason"] = nil, nil, reason
	}
	delete(row, "recorded_at")
	delete(row, "recorded_at_prec")
	delete(row, "recorded_at_prec_reason")
	s.points.Put("mkt.backfill_runs", text(row["backfill_id"]), row)
	s.flush(ctx)
}

func copyRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

// runObligation is the capture task of _schedule_event_backfill.
func (s *Service) runObligation(ctx context.Context, row map[string]any) {
	row = copyRow(row)
	for _, k := range []string{"semantic_fingerprint", "config_version", "code_version"} {
		delete(row, k)
	}
	fail := func(reason string) {
		row["status"], row["finished_at"], row["reason"] = "FAILED", pytime.ISO(s.clock.Now()), reason
		s.putRun(context.WithoutCancel(ctx), row)
	}
	defer func() {
		if r := recover(); r != nil {
			fail(fmt.Sprint(r))
			s.log.Error("capture obligation panicked", "backfill_id", row["backfill_id"], "panic", r)
		}
	}()
	anchor, err := pytime.Parse(row["anchor"])
	if err != nil {
		fail("anchor unreadable: " + err.Error())
		return
	}
	token := text(row["token_id"])
	row["status"] = "RUNNING"
	s.putRun(ctx, row)
	instruments := s.tokenInstruments(token)
	ids := make([]string, len(instruments))
	for i, inst := range instruments {
		ids[i] = inst.ID
	}
	for _, offset := range []int{-86400, -14400, -3600, 900, 3600, 14400, 86400, 259200} {
		cutoff, begin := anchor, anchor
		if offset < 0 {
			begin = anchor.Add(time.Duration(offset) * time.Second)
		} else {
			cutoff = anchor.Add(time.Duration(offset) * time.Second)
		}
		s.registerBoundaries(ids, cutoff, &begin)
	}
	// Benchmark closes first: two cheap kline reads that market context needs.
	benchmarks := s.backfillBenchmarkKlines(ctx, anchor)
	var wg sync.WaitGroup
	for _, inst := range instruments {
		wg.Add(1)
		go func(inst *venue.Instrument) {
			defer wg.Done()
			release, err := s.repairSlot(ctx)
			if err != nil {
				return
			}
			defer release()
			if err := s.backfill(ctx, inst, anchor, nil); err != nil && err != ErrStopped {
				s.log.Warn("pre-event backfill failed", "instrument", inst.ID, "error", err)
			}
		}(inst)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return // a RUNNING obligation is resumed by the next writer
	}
	end := minTime(s.clock.Now(), anchor.Add(WindowAfter))
	s.repairWindows(ctx, token, anchor.Add(-WindowBefore), end, 0)
	coverage := make([]map[string]any, len(instruments))
	for i, inst := range instruments {
		coverage[i] = s.captureWindowCoverage(ctx, inst, anchor)
	}
	ready := len(coverage) > 0
	for _, c := range coverage {
		ready = ready && c["complete"] == true
	}
	for _, b := range benchmarks {
		ready = ready && b["complete"] == true
	}
	row["status"], row["finished_at"], row["reason"] = "PARTIAL", pytime.ISO(s.clock.Now()), "INCOMPLETE_VENUE_WINDOWS_EXCLUDED"
	if ready {
		row["status"], row["reason"] = "COMPLETE", nil
	}
	instrumentsResult := make([]any, len(coverage))
	for i, c := range coverage {
		instrumentsResult[i] = c
	}
	benchResult := make([]any, len(benchmarks))
	for i, b := range benchmarks {
		benchResult[i] = b
	}
	row["result"] = map[string]any{"instruments": instrumentsResult, "benchmark_klines": benchResult}
	s.putRun(ctx, row)
}

// tokenInstruments are a token's active instruments in registry order.
func (s *Service) tokenInstruments(token string) []*venue.Instrument {
	var out []*venue.Instrument
	for _, iid := range s.opt.Order {
		if inst := s.opt.Instruments[iid]; inst != nil && inst.TokenID == token && inst.Active {
			out = append(out, inst)
		}
	}
	return out
}

// requests runs the window repairs core asks for (mkt.capture_requests).
func (s *Service) requests(ctx context.Context) {
	for ctx.Err() == nil {
		row, err := s.store.ClaimRequest(ctx, s.opt.Writer, s.clock.Now())
		if err != nil {
			s.log.Warn("capture request claim failed", "error", err)
			return
		}
		if row == nil {
			return
		}
		s.spawn(func(ctx context.Context) { s.runRequest(ctx, row) })
	}
}

func (s *Service) runRequest(ctx context.Context, row map[string]any) {
	row = copyRow(row)
	finish := func(status string, reason any, result any) {
		row["status"], row["reason"], row["result"] = status, reason, result
		for k, v := range pytime.Fields("finished_at", pytime.ISO(s.clock.Now()), pytime.CollectionOnly) {
			row[k] = v
		}
		if err := s.store.FinishRequest(context.WithoutCancel(ctx), row); err != nil {
			s.log.Warn("capture request not finished", "request_id", row["request_id"], "error", err)
		}
	}
	start, err1 := pytime.Parse(row["start"])
	end, err2 := pytime.Parse(row["end"])
	if err1 != nil || err2 != nil || text(row["kind"]) != "REPAIR_WINDOWS" {
		finish("FAILED", "UNSUPPORTED_CAPTURE_REQUEST", nil)
		return
	}
	budget := time.Duration(0)
	if deadline, err := pytime.Parse(row["deadline"]); err == nil {
		budget = deadline.Sub(s.clock.Now())
		if budget <= 0 {
			finish("EXPIRED", "REQUEST_DEADLINE_PASSED", nil)
			return
		}
	}
	results := s.repairWindows(ctx, text(row["token_id"]), start, end, budget)
	out := map[string]any{}
	for iid, r := range results {
		out[iid] = r
	}
	finish("COMPLETE", nil, map[string]any{"instruments": out})
}

// repairWindows is repair_windows: every incomplete or missing persisted minute of
// [start, end) for a token's instruments is repaired over REST from the last proven print
// (03 §5.1 fill order, §5.2 step 2) within budget (0 = none).
func (s *Service) repairWindows(ctx context.Context, token string, start, end time.Time, budget time.Duration) map[string]map[string]any {
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	lo := pytime.MinuteFloor(start)
	hi := pytime.MinuteFloor(minTime(end, s.clock.Now().Add(-5*time.Second)))
	instruments := s.tokenInstruments(token)
	var spot, perp []*venue.Instrument
	for _, inst := range instruments {
		if inst.Spot() {
			spot = append(spot, inst)
		} else {
			perp = append(perp, inst)
		}
	}
	recent := maxTime(lo, hi.Add(-ReferenceSpan))
	rank := func(inst *venue.Instrument) int {
		for i, v := range ReferenceVenues {
			if inst.Venue == v {
				return i
			}
		}
		return 99
	}
	ordered := append([]*venue.Instrument(nil), spot...)
	sort.SliceStable(ordered, func(i, j int) bool { return rank(ordered[i]) < rank(ordered[j]) })
	var reference, others []*venue.Instrument
	if len(ordered) > 0 {
		reference, others = ordered[:1], ordered[1:]
	}
	othersSpot := make([]*venue.Instrument, 0, len(spot))
	for _, inst := range spot {
		if len(reference) == 0 || inst != reference[0] {
			othersSpot = append(othersSpot, inst)
		}
	}
	_ = others
	type phase struct {
		group    []*venue.Instrument
		a, b     time.Time
		reserved bool
	}
	phases := []phase{{reference, recent, hi, true}, {othersSpot, recent, hi, false}, {perp, recent, hi, false}, {instruments, lo, recent, false}}
	results := map[string]map[string]any{}
	var mu sync.Mutex
	for _, ph := range phases {
		var wg sync.WaitGroup
		for _, inst := range ph.group {
			wg.Add(1)
			go func(inst *venue.Instrument) {
				defer wg.Done()
				before, after := s.repairInstrument(ctx, inst, ph.a, ph.b, ph.reserved)
				mu.Lock()
				total := results[inst.ID]
				if total == nil {
					total = map[string]any{"incomplete_before": int64(0), "incomplete_after": int64(0)}
					results[inst.ID] = total
				}
				total["incomplete_before"] = total["incomplete_before"].(int64) + before
				total["incomplete_after"] = total["incomplete_after"].(int64) + after
				mu.Unlock()
			}(inst)
		}
		wg.Wait()
	}
	return results
}

// incompleteRuns is _incomplete_runs: contiguous [start, end) runs of minutes without a
// complete persisted bar, and the bars of [lo - 1 min, hi) by minute.
func (s *Service) incompleteRuns(ctx context.Context, iid string, lo, hi time.Time) ([]span, map[int64]map[string]any, error) {
	rows, err := s.store.Range(ctx, "mkt.flow_bars_1m", "minute_start", lo.Add(-time.Minute), hi, map[string]any{"instrument_id": iid})
	if err != nil {
		return nil, nil, err
	}
	bars := map[int64]map[string]any{}
	for _, r := range rows {
		if t, err := pytime.Parse(r["minute_start"]); err == nil {
			bars[t.Unix()] = r
		}
	}
	var runs []span
	for m := lo; m.Before(hi); m = m.Add(time.Minute) {
		if b := bars[m.Unix()]; b == nil || b["complete"] != true {
			if n := len(runs); n > 0 && runs[n-1].end.Equal(m) {
				runs[n-1].end = m.Add(time.Minute)
			} else {
				runs = append(runs, span{m, m.Add(time.Minute)})
			}
		}
	}
	return runs, bars, nil
}

func minutesIn(runs []span) int64 {
	var n int64
	for _, r := range runs {
		n += int64(r.end.Sub(r.start) / time.Minute)
	}
	return n
}

// repairInstrument is _repair_instrument: newest first, at most an hour per backfill, so
// the event windows come before older history and every finished hour is persisted.
func (s *Service) repairInstrument(ctx context.Context, inst *venue.Instrument, lo, hi time.Time, reserved bool) (int64, int64) {
	if !hi.After(lo) {
		return 0, 0
	}
	iid := inst.ID
	runs, bars, err := s.incompleteRuns(ctx, iid, lo, hi)
	if err != nil {
		s.log.Warn("repair read failed", "instrument", iid, "error", err)
		return 0, 0
	}
	before := minutesIn(runs)
	var chunks []span
	for i := len(runs) - 1; i >= 0; i-- {
		r := runs[i]
		n := int((r.end.Sub(r.start) + RepairChunk - 1) / RepairChunk)
		for k := 0; k < n; k++ {
			end := r.end.Add(-RepairChunk * time.Duration(k))
			chunks = append(chunks, span{maxTime(r.start, end.Add(-RepairChunk)), end})
		}
	}
	for _, ch := range chunks {
		if _, none := NoTradeHistory[inst.Venue]; none || (inst.Venue == "bybit" && ch.end.Before(s.clock.Now().Add(-BybitRecent))) {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		cp := &checkpoint{lastTS: ch.start}
		previous := bars[ch.start.Add(-time.Minute).Unix()]
		if lt := pyval.Map(previous["last_trade"]); previous != nil && previous["complete"] == true && text(lt["trade_id"]) != "" {
			cp.lastTradeID, cp.lastTrade = text(lt["trade_id"]), lt
		} else if inst.Venue == "kraken" {
			if p := s.krakenPredecessor(ctx, inst, ch.start); p != nil {
				cp.lastTradeID, cp.lastTrade = text(p["last_trade_id"]), pyval.Map(p["last_trade"])
			}
		}
		// Paging venues continue below the first print of the newer minute (just repaired)
		// instead of walking back from now for every chunk.
		if inst.Venue == "coinbase" || inst.Venue == "okx" {
			if following, err := s.store.Get(ctx, "mkt.flow_bars_1m", iid+":"+pytime.ISO(ch.end)); err == nil && following != nil {
				if ft := pyval.Map(following["first_trade"]); text(ft["trade_id"]) != "" {
					cp.endCursor = text(ft["trade_id"])
				}
			}
		}
		run := func() error { return s.backfill(ctx, inst, ch.end, cp) }
		if !reserved {
			release, err := s.repairSlot(ctx)
			if err != nil {
				break
			}
			err = run()
			release()
			if err == ErrStopped {
				break
			}
		} else if err := run(); err == ErrStopped {
			break
		}
		if done, _, err := s.incompleteRuns(context.WithoutCancel(ctx), iid, ch.start, ch.end); err == nil {
			total := int64(ch.end.Sub(ch.start) / time.Minute)
			s.log.Info("repair", "instrument", iid, "start", pytime.ISO(ch.start), "end", pytime.ISO(ch.end),
				"complete_minutes", total-minutesIn(done), "minutes", total)
		}
	}
	runs, bars, err = s.incompleteRuns(context.WithoutCancel(ctx), iid, lo, hi)
	if err != nil {
		return before, before
	}
	reason := NoTradeHistory[inst.Venue]
	if reason == "" {
		reason = "REST_REPAIR_INCOMPLETE"
		if inst.Venue == "bybit" {
			reason = BybitLimit
		}
	}
	now := s.clock.Now()
	for _, r := range runs {
		for m := r.start; m.Before(r.end); m = m.Add(time.Minute) {
			if bar := bars[m.Unix()]; bar != nil && bar["gap_reason"] == nil {
				// An unproven minute always says why (never a null reason).
				row := copyRow(bar)
				row["gap_reason"] = reason
				s.writeStored(inst, flow.Key{IID: iid, Minute: m.Unix()}, row, bar, "REASON_RECORDED", now)
			}
		}
	}
	s.flush(context.WithoutCancel(ctx))
	return before, minutesIn(runs)
}

// backfillKlines is _backfill_klines: actual closed candles, including the close at or
// before a return anchor.
func (s *Service) backfillKlines(ctx context.Context, inst *venue.Instrument, start, end time.Time, includeReference bool, kind string) (map[string]any, error) {
	first, last := start, pytime.MinuteFloor(end)
	if includeReference {
		first = pytime.MinuteFloor(start).Add(-time.Minute)
	}
	iid := inst.ID
	expected := int(last.Sub(first) / time.Minute)
	if expected < 0 {
		expected = 0
	}
	observed := func() (map[int64]bool, error) {
		rows, err := s.store.Range(ctx, "mkt.klines_1m", "open_time", first, last, map[string]any{"instrument_id": iid})
		if err != nil {
			return nil, err
		}
		out := map[int64]bool{}
		for _, r := range rows {
			t, err := pytime.Parse(r["open_time"])
			if err == nil && r["complete"] == true && !t.Add(time.Minute).After(end) {
				out[t.Unix()] = true
			}
		}
		return out, nil
	}
	points, err := observed()
	if err != nil {
		return nil, err
	}
	v := s.venues.Venue(inst, 1)
	for offset := 0; offset < expected; offset += 240 {
		lo := first.Add(time.Duration(offset) * time.Minute)
		hi := minTime(lo.Add(240*time.Minute), last)
		have := true
		for t := lo; t.Before(hi); t = t.Add(time.Minute) {
			if !points[t.Unix()] {
				have = false
				break
			}
		}
		if have {
			continue
		}
		rows, err := v.Klines(ctx, &lo, &hi)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ErrStopped
			}
			s.gap(inst, kind, err.Error(), &lo, &hi)
			continue
		}
		for _, raw := range rows {
			row, err := normalize.KlineRecord(inst, raw, s.clock.Now(), s.fx)
			if err != nil {
				s.gap(inst, kind, err.Error(), &lo, &hi)
				break
			}
			if t, err := pytime.Parse(row["open_time"]); err == nil && !t.Before(first) && t.Before(last) {
				s.points.StoreKline(inst, row)
			}
		}
	}
	s.flush(ctx)
	points, err = observed()
	if err != nil {
		return nil, err
	}
	complete := 0
	all := expected > 0
	for n := 0; n < expected; n++ {
		if points[first.Add(time.Duration(n)*time.Minute).Unix()] {
			complete++
		} else {
			all = false
		}
	}
	return map[string]any{
		"instrument_id": iid, "start": pytime.ISO(first), "end": pytime.ISO(last),
		"expected_minutes": int64(expected), "complete_minutes": int64(complete),
		"complete": all, "includes_anchor_close": includeReference,
	}, nil
}

// backfillBenchmarkKlines is _backfill_benchmark_klines: the BTC and ETH closes of the
// day before an anchor.
func (s *Service) backfillBenchmarkKlines(ctx context.Context, anchor time.Time) []map[string]any {
	var result []map[string]any
	for _, symbol := range []string{"BTC", "ETH"} {
		tokens := map[string]bool{}
		for id, token := range s.opt.Tokens {
			if text(token["symbol"]) == symbol {
				tokens[id] = true
			}
		}
		var best *venue.Instrument
		for _, iid := range s.opt.Order {
			inst := s.opt.Instruments[iid]
			if inst == nil || !tokens[inst.TokenID] || !inst.Active || !inst.Spot() {
				continue
			}
			if best == nil || benchmarkLess(inst, best) {
				best = inst
			}
		}
		if best == nil {
			continue
		}
		unlock := s.contextLock(best.ID)
		row, err := s.backfillKlines(ctx, best, anchor.Add(-24*time.Hour), anchor, true, "PRE_EVENT_KLINES")
		unlock()
		if err != nil {
			continue
		}
		result = append(result, row)
	}
	return result
}

func benchmarkLess(a, b *venue.Instrument) bool {
	ka, kb := a.Key() != "binance.spot", b.Key() != "binance.spot"
	if ka != kb {
		return !ka
	}
	return a.ID < b.ID
}

// captureWindowCoverage is _capture_window_coverage: the 1, 4 and 24 h windows before an
// anchor, from the stored bars and klines, and the PRE24 reference print.
func (s *Service) captureWindowCoverage(ctx context.Context, inst *venue.Instrument, anchor time.Time) map[string]any {
	end := anchor
	windows := map[string]any{}
	allComplete := true
	for _, hours := range []int{1, 4, 24} {
		start := end.Add(-time.Duration(hours) * time.Hour)
		lo := pytime.MinuteFloor(start)
		if start.After(lo) {
			lo = lo.Add(time.Minute)
		}
		hi := pytime.MinuteFloor(end)
		expected := int64(hi.Sub(lo) / time.Minute)
		if expected < 0 {
			expected = 0
		}
		metrics := map[string]any{}
		windowComplete := true
		for _, spec := range [][3]string{{"flow", "mkt.flow_bars_1m", "minute_start"}, {"klines", "mkt.klines_1m", "open_time"}} {
			rows, err := s.store.Range(ctx, spec[1], spec[2], lo, hi, map[string]any{"instrument_id": inst.ID})
			var complete int64
			if err == nil {
				for _, r := range rows {
					if pyval.Truthy(r["complete"]) {
						complete++
					}
				}
			}
			ok := complete == expected && expected > 0
			windowComplete = windowComplete && ok
			metrics[spec[0]] = map[string]any{
				"expected_minutes": expected, "complete_minutes": complete,
				"missing_or_incomplete_minutes": max(int64(0), expected-complete), "complete": ok,
			}
		}
		unresolved := lo.Sub(start).Seconds() + end.Sub(hi).Seconds()
		w := map[string]any{
			"start": pytime.ISO(start), "end": pytime.ISO(end), "observed_bucket_start": pytime.ISO(lo),
			"observed_bucket_end": pytime.ISO(hi), "timestamp_precision": "BUCKET",
			"unresolved_boundary_seconds": unresolved,
		}
		for k, v := range metrics {
			w[k] = v
		}
		windows[fmt.Sprintf("W%dH", hours)] = w
		allComplete = allComplete && windowComplete && unresolved == 0
	}
	result := map[string]any{"instrument_id": inst.ID, "windows": windows, "complete": allComplete}
	if inst.Spot() {
		boundary := end.Add(-24 * time.Hour)
		bars, _ := s.store.Range(ctx, "mkt.flow_bars_1m", "minute_start", pytime.MinuteFloor(boundary).Add(-time.Minute), pytime.MinuteFloor(boundary).Add(time.Minute), map[string]any{"instrument_id": inst.ID})
		var reference map[string]any
		var referenceTS time.Time
		for _, bar := range bars {
			for _, p := range pyval.List(bar["reference_points"]) {
				point := pyval.Map(p)
				at, err := pytime.Parse(point["boundary"])
				if point["complete"] != true || err != nil || !at.Equal(boundary) {
					continue
				}
				lt := pyval.Map(point["last_trade"])
				ts, err := pytime.Parse(lt["ts"])
				if err == nil && (reference == nil || ts.After(referenceTS)) {
					reference, referenceTS = lt, ts
				}
			}
		}
		pre := map[string]any{"available": reference != nil, "trade_id": nil, "ts": nil, "ts_prec": nil, "reason": "INCOMPLETE"}
		if reference != nil {
			pre["trade_id"], pre["ts"], pre["ts_prec"], pre["reason"] = reference["trade_id"], reference["ts"], reference["ts_prec"], nil
		}
		result["pre24_reference"] = pre
		result["complete"] = allComplete && reference != nil
	}
	return result
}

// startupRepairs is _repair_on_start: a restart is a reconnect (03 §5.2 step 2); each hot
// token's windows are repaired from the bars stored up to this start.
func (s *Service) startupRepairs(ctx context.Context, hot []map[string]any, started time.Time) {
	anchors := s.hotAnchors(ctx, hot)
	type window struct{ start, end time.Time }
	spans := map[string]*window{}
	var order []string
	for _, row := range hot {
		token := text(row["token_id"])
		at, ok := anchors[token+"|"+text(row["event_id"])]
		if !ok {
			continue
		}
		w := spans[token]
		if w == nil {
			w = &window{at.Add(-WindowBefore), at.Add(WindowAfter)}
			spans[token] = w
			order = append(order, token)
		}
		w.start = minTime(w.start, at.Add(-WindowBefore))
		w.end = maxTime(w.end, at.Add(WindowAfter))
	}
	for _, token := range order {
		w := spans[token]
		if ctx.Err() != nil {
			return
		}
		s.log.Info("startup repair", "token", token, "start", pytime.ISO(w.start), "end", pytime.ISO(minTime(started, w.end)))
		s.repairWindows(ctx, token, w.start, minTime(started, w.end), 0)
	}
}

// hotAnchors is the anchor of every hot (token, event): the newest obligation's anchor,
// else the hot row's since.
func (s *Service) hotAnchors(ctx context.Context, hot []map[string]any) map[string]time.Time {
	out := map[string]time.Time{}
	runs, err := s.store.BackfillRuns(ctx, []string{"PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED"})
	newest := map[string]time.Time{}
	if err == nil {
		for _, r := range runs {
			k := text(r["token_id"]) + "|" + text(r["event_id"])
			at, err1 := pytime.Parse(r["anchor"])
			started, err2 := pytime.Parse(r["started_at"])
			if err1 == nil && (err2 != nil || started.After(newest[k]) || newest[k].IsZero()) {
				out[k] = at
				if err2 == nil {
					newest[k] = started
				}
			}
		}
	}
	for _, row := range hot {
		k := text(row["token_id"]) + "|" + text(row["event_id"])
		if _, ok := out[k]; !ok {
			if at, err := pytime.Parse(row["since"]); err == nil {
				out[k] = at
			}
		}
	}
	return out
}

func hasPrefix(s string, prefixes ...string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
