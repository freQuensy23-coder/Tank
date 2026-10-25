package capture

import (
	"context"
	"errors"
	"fmt"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"sort"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

func (s *Service) obligations(ctx context.Context) {
	rows, err := s.store.BackfillRuns(ctx, []string{"PENDING", "RUNNING"})
	if err != nil {
		s.log.Warn("backfill runs unreadable", "error", err)
		return
	}
	for _, row := range rows {
		id, validID := row["backfill_id"].(string)
		if !validID || id == "" {
			s.log.Error("capture obligation has no backfill identity")
			continue
		}
		s.mu.Lock()
		if _, active := s.activeObligations[id]; active {
			s.mu.Unlock()
			continue
		}
		s.activeObligations[id] = struct{}{}
		s.mu.Unlock()
		s.spawn(func(ctx context.Context) {
			defer func() {
				s.mu.Lock()
				delete(s.activeObligations, id)
				s.mu.Unlock()
			}()
			s.runObligation(ctx, row)
		})
	}
}

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
	row["status"], row["started_at"] = "RUNNING", pytime.ISO(s.clock.Now())
	s.putRun(ctx, row)
	instruments := s.tokenInstruments(ctx, token)
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

	benchmarks, err := s.backfillBenchmarkKlines(ctx, anchor)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.log.Warn("benchmark capture unavailable", "backfill_id", row["backfill_id"], "error", err)
		fail("BENCHMARK_CAPTURE_FAILED")
		return
	}
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
		return
	}
	end := minTime(s.clock.Now(), anchor.Add(WindowAfter))
	if _, err := s.repairWindows(ctx, token, anchor.Add(-WindowBefore), end, 0); err != nil {
		s.log.Warn("pre-event repair coverage unavailable", "backfill_id", row["backfill_id"], "error", err)
		fail("REPAIR_FAILED")
		return
	}
	coverage := make([]InstrumentCaptureCoverage, len(instruments))
	for i, inst := range instruments {
		coverage[i], err = s.captureWindowCoverage(ctx, inst, anchor)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("capture coverage unavailable", "backfill_id", row["backfill_id"], "instrument", inst.ID, "error", err)
			fail("COVERAGE_READ_FAILED")
			return
		}
	}
	ready := len(coverage) > 0
	for _, c := range coverage {
		ready = ready && c.Complete
	}
	for _, b := range benchmarks {
		ready = ready && b.Complete
	}
	row["status"], row["finished_at"], row["reason"] = "PARTIAL", pytime.ISO(s.clock.Now()), "INCOMPLETE_VENUE_WINDOWS_EXCLUDED"
	if ready {
		row["status"], row["reason"] = "COMPLETE", nil
	}
	instrumentsResult := make([]any, len(coverage))
	for i, c := range coverage {
		instrumentsResult[i] = c.storageValue()
	}
	benchResult := make([]any, len(benchmarks))
	for i, b := range benchmarks {
		benchResult[i] = b.storageValue()
	}
	row["result"] = map[string]any{"instruments": instrumentsResult, "benchmark_klines": benchResult}
	s.putRun(ctx, row)
}

func (s *Service) tokenInstruments(ctx context.Context, token string) []*venue.Instrument {
	var out []*venue.Instrument
	for _, iid := range s.opt.Order {
		if inst := s.opt.Instruments[iid]; inst != nil && inst.TokenID == token && inst.Active {
			out = append(out, s.resolved(ctx, iid))
		}
	}
	return out
}

func (s *Service) resolved(ctx context.Context, iid string) *venue.Instrument {
	if !s.discovered(iid) {
		if err := s.discover(ctx, s.opt.Instruments[iid]); err != nil && ctx.Err() == nil {
			s.gap(s.opt.Instruments[iid], "METADATA", err.Error(), nil, nil)
		}
	}
	return s.instrument(iid)
}

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
		s.spawn(func(ctx context.Context) { s.runRequest(ctx, *row) })
	}
}

func (s *Service) runRequest(ctx context.Context, row requeststate.Claimed) {
	finish := func(outcome requeststate.Outcome) {
		completion := requeststate.Completion{ID: row.ID, Outcome: outcome, FinishedAt: s.clock.Now()}
		if err := s.store.FinishRequest(context.WithoutCancel(ctx), completion); err != nil {
			s.log.Warn("capture request not finished", "request_id", row.ID, "error", err)
		}
	}
	limits, invalid := requeststate.DecodeLimits(row.Deadline, row.BudgetS, s.clock.Now())
	if invalid != nil {
		finish(requeststate.Failure(invalid.Reason, nil))
		return
	}
	if limits.Budget <= 0 || !limits.Deadline.After(s.clock.Now()) {
		finish(requeststate.Expiry())
		return
	}
	if row.Kind != nil && *row.Kind == "HTTP_FETCH" {
		finish(s.runFetch(ctx, row.Request, limits.Budget))
		return
	}
	if row.Start == nil || row.End == nil || !row.End.After(*row.Start) || row.Kind == nil || *row.Kind != "REPAIR_WINDOWS" {
		finish(requeststate.Failure(requeststate.Unsupported, nil))
		return
	}
	if row.TokenID == nil || *row.TokenID == "" {
		finish(requeststate.Failure(requeststate.TokenInvalid, nil))
		return
	}
	results, err := s.repairWindows(ctx, *row.TokenID, *row.Start, *row.End, limits.Budget)
	result := requeststate.RepairResult{Instruments: results}
	if err != nil {
		s.log.Warn("capture request repair coverage unavailable", "request_id", row.ID, "error", err)
		finish(requeststate.Failure(requeststate.RepairFailed, result))
		return
	}
	finish(requeststate.Completed(result))
}

type RepairCoverage = requeststate.RepairCoverage

func (s *Service) repairWindows(ctx context.Context, token string, start, end time.Time, budget time.Duration) (map[string]RepairCoverage, error) {
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	lo := pytime.MinuteFloor(start)
	hi := pytime.MinuteFloor(minTime(end, s.clock.Now().Add(-5*time.Second)))
	instruments := s.tokenInstruments(ctx, token)
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
	results := map[string]RepairCoverage{}
	var repairErrors []error
	var mu sync.Mutex
	for _, ph := range phases {
		if !ph.b.After(ph.a) {
			continue
		}
		var wg sync.WaitGroup
		for _, inst := range ph.group {
			wg.Add(1)
			go func(inst *venue.Instrument) {
				defer wg.Done()
				coverage, err := s.repairInstrument(ctx, inst, ph.a, ph.b, ph.reserved)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					repairErrors = append(repairErrors, fmt.Errorf("%s: %w", inst.ID, err))
					return
				}
				total := results[inst.ID]
				total.IncompleteBefore += coverage.IncompleteBefore
				total.IncompleteAfter += coverage.IncompleteAfter
				results[inst.ID] = total
			}(inst)
		}
		wg.Wait()
	}
	return results, errors.Join(repairErrors...)
}

func (s *Service) incompleteRuns(ctx context.Context, iid string, lo, hi time.Time) ([]span, map[int64]map[string]any, error) {
	rows, err := s.store.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start", "complete", "last_trade", "gap_reason"}, "minute_start", lo.Add(-time.Minute), hi, []string{iid})
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

func (s *Service) repairInstrument(ctx context.Context, inst *venue.Instrument, lo, hi time.Time, reserved bool) (RepairCoverage, error) {
	if !hi.After(lo) {
		return RepairCoverage{}, nil
	}
	iid := inst.ID
	runs, bars, err := s.incompleteRuns(ctx, iid, lo, hi)
	if err != nil {
		return RepairCoverage{}, fmt.Errorf("initial repair coverage unavailable: %w", err)
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
		return RepairCoverage{}, fmt.Errorf("final repair coverage unavailable: %w", err)
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

				bar, err := s.store.Get(context.WithoutCancel(ctx), "mkt.flow_bars_1m", iid+":"+pytime.ISO(m))
				if err != nil || bar == nil {
					continue
				}
				row := copyRow(bar)
				row["gap_reason"] = reason
				s.writeStored(inst, flow.Key{IID: iid, Minute: m.Unix()}, row, bar, "REASON_RECORDED", now)
			}
		}
	}
	s.flush(context.WithoutCancel(ctx))
	return RepairCoverage{IncompleteBefore: before, IncompleteAfter: minutesIn(runs)}, nil
}

type KlineCoverage struct {
	InstrumentID        string
	Start               string
	End                 string
	ExpectedMinutes     int64
	CompleteMinutes     int64
	Complete            bool
	IncludesAnchorClose bool
}

func (r KlineCoverage) storageValue() map[string]any {
	return map[string]any{
		"instrument_id": r.InstrumentID, "start": r.Start, "end": r.End,
		"expected_minutes": r.ExpectedMinutes, "complete_minutes": r.CompleteMinutes,
		"complete": r.Complete, "includes_anchor_close": r.IncludesAnchorClose,
	}
}

func (s *Service) backfillKlines(ctx context.Context, inst *venue.Instrument, start, end time.Time, includeReference bool, kind string) (KlineCoverage, error) {
	first, last := start, pytime.MinuteFloor(end)
	if includeReference {
		first = pytime.MinuteFloor(start).Add(-time.Minute)
	}
	iid := inst.ID
	expected := int(last.Sub(first) / time.Minute)
	if expected < 0 || end.Before(start) {
		return KlineCoverage{}, errors.New("capture kline interval end precedes start")
	}
	observed := func() (map[int64]bool, error) {
		rows, err := s.store.Projection(ctx, "mkt.klines_1m", []string{"open_time", "complete"}, "open_time", first, last, []string{iid})
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
		return KlineCoverage{}, err
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
				return KlineCoverage{}, ErrStopped
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
		return KlineCoverage{}, err
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
	return KlineCoverage{
		InstrumentID: iid, Start: pytime.ISO(first), End: pytime.ISO(last),
		ExpectedMinutes: int64(expected), CompleteMinutes: int64(complete),
		Complete: all, IncludesAnchorClose: includeReference,
	}, nil
}

func (s *Service) backfillBenchmarkKlines(ctx context.Context, anchor time.Time) ([]KlineCoverage, error) {
	var result []KlineCoverage
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
			return nil, fmt.Errorf("%s benchmark capture unavailable: %w", symbol, err)
		}
		result = append(result, row)
	}
	return result, nil
}

func benchmarkLess(a, b *venue.Instrument) bool {
	ka, kb := a.Key() != "binance.spot", b.Key() != "binance.spot"
	if ka != kb {
		return !ka
	}
	return a.ID < b.ID
}

func (s *Service) captureWindowCoverage(ctx context.Context, inst *venue.Instrument, anchor time.Time) (InstrumentCaptureCoverage, error) {
	end := anchor
	var windows [3]CaptureWindowCoverage
	allComplete := true
	for index, hours := range []int{1, 4, 24} {
		start := end.Add(-time.Duration(hours) * time.Hour)
		lo := pytime.MinuteFloor(start)
		if start.After(lo) {
			lo = lo.Add(time.Minute)
		}
		hi := pytime.MinuteFloor(end)
		expected := int64(hi.Sub(lo) / time.Minute)
		if expected < 0 {
			return InstrumentCaptureCoverage{}, errors.New("capture coverage interval end precedes start")
		}
		var metrics [2]CoverageMinutes
		windowComplete := true
		for metricIndex, spec := range [][3]string{{"flow", "mkt.flow_bars_1m", "minute_start"}, {"klines", "mkt.klines_1m", "open_time"}} {
			rows, err := s.store.Range(ctx, spec[1], spec[2], lo, hi, map[string]any{"instrument_id": inst.ID})
			if err != nil {
				return InstrumentCaptureCoverage{}, fmt.Errorf("%s %s coverage unavailable: %w", inst.ID, spec[0], err)
			}
			metric, err := decodeCoverageMinutes(rows, spec[2], lo, hi)
			if err != nil {
				return InstrumentCaptureCoverage{}, err
			}
			windowComplete = windowComplete && metric.Complete
			metrics[metricIndex] = metric
		}
		unresolved := lo.Sub(start).Seconds() + end.Sub(hi).Seconds()
		windows[index] = CaptureWindowCoverage{
			Start: pytime.ISO(start), End: pytime.ISO(end), ObservedBucketStart: pytime.ISO(lo),
			ObservedBucketEnd: pytime.ISO(hi), UnresolvedBoundarySeconds: unresolved,
			Flow: metrics[0], Klines: metrics[1],
		}
		allComplete = allComplete && windowComplete && unresolved == 0
	}
	result := InstrumentCaptureCoverage{
		InstrumentID: inst.ID, Windows: CaptureWindows{W1H: windows[0], W4H: windows[1], W24H: windows[2]}, Complete: allComplete,
	}
	if inst.Spot() {
		boundary := end.Add(-24 * time.Hour)
		bars, err := s.store.Range(ctx, "mkt.flow_bars_1m", "minute_start", pytime.MinuteFloor(boundary).Add(-time.Minute), pytime.MinuteFloor(boundary).Add(time.Minute), map[string]any{"instrument_id": inst.ID})
		if err != nil {
			return InstrumentCaptureCoverage{}, fmt.Errorf("%s pre24 reference unavailable: %w", inst.ID, err)
		}
		var reference map[string]any
		var referenceTS time.Time
		for _, bar := range bars {
			for _, raw := range pyval.List(bar["reference_points"]) {
				point := pyval.Map(raw)
				at, err := pytime.Parse(point["boundary"])
				if point["complete"] != true || err != nil || !at.Equal(boundary) {
					continue
				}
				trade := pyval.Map(point["last_trade"])
				ts, err := pytime.Parse(trade["ts"])
				if err == nil && (reference == nil || ts.After(referenceTS)) {
					reference, referenceTS = trade, ts
				}
			}
		}
		reason := "INCOMPLETE"
		pre := Pre24Reference{Reason: &reason}
		if reference != nil {
			identity, err := decodeReferenceTradeIdentity(reference["trade_id"])
			if err != nil {
				return InstrumentCaptureCoverage{}, err
			}
			ts, ok := reference["ts"].(string)
			if !ok {
				return InstrumentCaptureCoverage{}, errors.New("reference trade requires a JSON timestamp string")
			}
			var precision *string
			if raw := reference["ts_prec"]; raw != nil {
				value, ok := raw.(string)
				if !ok {
					return InstrumentCaptureCoverage{}, errors.New("reference trade precision requires text or null")
				}
				precision = &value
			}
			pre = Pre24Reference{Available: true, TradeID: &identity, TS: &ts, Precision: precision}
		}
		result.Pre24Reference = &pre
		result.Complete = allComplete && pre.Available
	}
	return result, nil
}

func (s *Service) unrepaired(ctx context.Context, iid string, lo, hi time.Time) []span {
	if !hi.After(lo) {
		return nil
	}
	runs, bars, err := s.incompleteRuns(ctx, iid, lo, hi)
	if err != nil {
		s.log.Warn("repair read failed", "instrument", iid, "error", err)
		return nil
	}
	var out []span
	for _, r := range runs {
		for m := r.start; m.Before(r.end); m = m.Add(time.Minute) {
			if b := bars[m.Unix()]; b != nil && sourceLimit(text(b["gap_reason"])) {
				continue
			}
			if n := len(out); n > 0 && out[n-1].end.Equal(m) {
				out[n-1].end = m.Add(time.Minute)
			} else {
				out = append(out, span{m, m.Add(time.Minute)})
			}
		}
	}
	return out
}

func sourceLimit(reason string) bool {
	if reason == BybitLimit {
		return true
	}
	for _, r := range NoTradeHistory {
		if reason == r {
			return true
		}
	}
	return false
}

func (s *Service) repairUnrepaired(ctx context.Context, token string, lo, hi time.Time, reserved bool) {
	s.repairGateOnce.Do(func() { s.repairGate = make(chan struct{}, repairParallel) })
	sem := s.repairGate
	var wg sync.WaitGroup
	for _, inst := range s.tokenInstruments(ctx, token) {
		gaps := s.unrepaired(ctx, inst.ID, lo, hi)
		if len(gaps) == 0 {
			continue
		}
		wg.Add(1)
		go func(inst *venue.Instrument, gaps []span) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			for i := len(gaps) - 1; i >= 0 && ctx.Err() == nil; i-- {
				s.log.Info("gap repair", "instrument", inst.ID, "start", pytime.ISO(gaps[i].start), "end", pytime.ISO(gaps[i].end))
				if _, err := s.repairInstrument(ctx, inst, gaps[i].start, gaps[i].end, reserved); err != nil {
					s.log.Warn("gap repair coverage unavailable", "instrument", inst.ID, "error", err)
					return
				}
			}
		}(inst, gaps)
	}
	wg.Wait()
}

const repairParallel = 8

const sweepEvery, sweepSpan, sweepLag = 2 * time.Minute, time.Hour, 2 * time.Minute

func (s *Service) sweep(ctx context.Context) {
	s.sweepAt(ctx, s.clock.Now())
}
func (s *Service) sweepAt(ctx context.Context, observed time.Time) {
	if !s.leading() {
		return
	}
	s.mu.Lock()
	tokens := make([]string, 0, len(s.hotSince))
	for t := range s.hotSince {
		tokens = append(tokens, t)
	}
	s.mu.Unlock()
	sort.Strings(tokens)
	hi := pytime.MinuteFloor(observed.Add(-sweepLag))
	var wg sync.WaitGroup
	for _, token := range tokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			s.repairUnrepaired(ctx, token, hi.Add(-sweepSpan), hi, true)
		}(token)
	}
	wg.Wait()
}
