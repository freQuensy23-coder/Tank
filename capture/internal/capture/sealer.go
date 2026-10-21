package capture

import (
	"sort"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const Unproven = "STREAM_COVERAGE_UNPROVEN"

const (
	sourceStream    = "CONTINUOUS_STREAM"
	sourceHead      = "REST_HEAD_CHECK"
	sourceWatermark = "STREAM_WATERMARK"
	sourceREST      = "REST_RANGE_EXHAUSTED"
	sealedFinal     = "UNPROVEN_FINAL"
)

const (
	headInterval  = 60 * time.Second
	provisionalS  = 5 * time.Second
	sealRetain    = 15 * time.Minute
	holdUnsealed  = 30 * time.Minute
	headPrints    = 100
	revisionsKeep = 200000
)

func ConsecutiveIDs(inst *venue.Instrument) bool {
	switch inst.VenueName() {
	case "binance", "coinbase", "kraken":
		return true
	}
	return false
}

func intID(id string) (int64, bool) {
	v, err := pyval.Int(id)
	return v, err == nil
}

type span struct{ start, end time.Time }

func (s span) overlaps(lo, hi time.Time) bool { return s.start.Before(hi) && s.end.After(lo) }

type chain struct {
	inst      *venue.Instrument
	ids       bool
	conn      int64
	first     time.Time
	start     time.Time
	proved    time.Time
	frontier  time.Time
	last      *normalize.Trade
	lastConn  int64
	anchor    *flow.TradePoint
	resume    time.Time
	repairs   []span
	headNext  time.Time
	heading   bool
	watermark time.Time
}

func (c *chain) pending(lo, hi time.Time) bool {
	for _, r := range c.repairs {
		if r.overlaps(lo, hi) {
			return true
		}
	}
	return false
}

type seal struct {
	version int64
	at      time.Time
	record  map[string]any
}

type Sealer struct {
	mu        sync.Mutex
	acc       *flow.Accumulator
	chains    map[string]*chain
	seals     map[flow.Key]*seal
	versions  map[flow.Key]int64
	ids       map[flow.Key]map[string]bool
	wrote     map[flow.Key]time.Time
	clock     Clock
	put       func(collection, key string, row map[string]any) map[string]any
	headDelay time.Duration

	Repair func(inst *venue.Instrument, start, end time.Time, reason string)
	Gap    func(inst *venue.Instrument, kind, reason string, start, end time.Time)

	Late func(inst *venue.Instrument, t *normalize.Trade)
}

func NewSealer(clock Clock, put func(collection, key string, row map[string]any) map[string]any, headDelay time.Duration) *Sealer {
	if headDelay < 30*time.Second {
		headDelay = 30 * time.Second
	}
	if headDelay > 60*time.Second {
		headDelay = 60 * time.Second
	}
	return &Sealer{
		acc: flow.New(), chains: map[string]*chain{}, seals: map[flow.Key]*seal{},
		versions: map[flow.Key]int64{}, ids: map[flow.Key]map[string]bool{}, wrote: map[flow.Key]time.Time{},
		clock: clock, put: put, headDelay: headDelay,
		Repair: func(*venue.Instrument, time.Time, time.Time, string) {},
		Gap:    func(*venue.Instrument, string, string, time.Time, time.Time) {},
		Late:   func(*venue.Instrument, *normalize.Trade) {},
	}
}

func (s *Sealer) chain(inst *venue.Instrument) *chain {
	c := s.chains[inst.ID]
	if c == nil {
		c = &chain{inst: inst, ids: ConsecutiveIDs(inst)}
		s.chains[inst.ID] = c
	}
	return c
}

func (s *Sealer) Anchor(inst *venue.Instrument, point map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, err := pytime.Parse(point["ts"])
	if err != nil {
		return
	}
	c := s.chain(inst)
	c.anchor = &flow.TradePoint{TradeID: text(point["trade_id"]), Price: text(point["price"]), TS: ts}
	if p := text(point["ts_prec"]); p != "" {
		c.anchor.TSPrec = p
	}
}

func (s *Sealer) Boundaries(iid string, points []time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acc.RegisterBoundaries(iid, points)
}

func (s *Sealer) Reference(iid string, boundary time.Time, point map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acc.RegisterBoundaries(iid, []time.Time{boundary})
	s.acc.References[flow.RefKey(iid, boundary)] = point
}

func (s *Sealer) References(key flow.Key) (map[string]map[string]any, map[int64]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make(map[string]map[string]any, len(s.acc.References))
	for k, v := range s.acc.References {
		refs[k] = v
	}
	var points map[int64]time.Time
	if b := s.acc.Boundaries[key]; b != nil {
		points = make(map[int64]time.Time, len(b))
		for k, v := range b {
			points[k] = v
		}
	}
	return refs, points
}

func (s *Sealer) ReferenceAt(iid string, boundary time.Time) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acc.References[flow.RefKey(iid, boundary)]
}

func (s *Sealer) Connected(inst *venue.Instrument, conn int64, at time.Time, hotSince *time.Time) *span {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chain(inst)
	c.conn, c.start, c.proved, c.frontier = conn, at, at, at
	if c.first.IsZero() {
		c.first = at
	}
	c.headNext, c.heading, c.watermark = at, false, time.Time{}
	var lo time.Time
	switch {
	case c.anchor != nil:
		lo = pytime.MinuteFloor(c.anchor.TS)
		if c.resume.After(lo) {
			lo = c.resume
		}
		s.Gap(inst, "RECONNECT", "REST reconciliation pending", c.anchor.TS, at)
	case hotSince != nil && hotSince.Before(at):

		lo = *hotSince
		if lo.Before(at.Add(-24 * time.Hour)) {
			lo = at.Add(-24 * time.Hour)
		}
		lo = pytime.MinuteFloor(lo)
	default:
		c.repairs = dropOpen(c.repairs)
		return nil
	}
	c.repairs = dropOpen(c.repairs)
	bridge := span{lo, at}
	c.repairs = append(c.repairs, bridge)
	return &bridge
}

func dropOpen(repairs []span) []span {
	kept := repairs[:0]
	for _, r := range repairs {
		if r.end.Year() < 9999 {
			kept = append(kept, r)
		}
	}
	return kept
}

func (s *Sealer) Disconnected(inst *venue.Instrument, conn int64, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chain(inst)
	if c.conn != conn || conn == 0 {
		return
	}
	now := s.clock.Now()
	s.proveTo(c, minTime(c.frontier, now), sourceStream, now)
	if now.After(c.proved) {
		s.acc.ProveRange(inst.ID, c.proved, now, false, "STREAM", reason, now)
	}
	if c.last != nil {
		p := flow.PointOf(c.last)
		c.anchor = p
	} else if c.anchor == nil {
		c.anchor = &flow.TradePoint{TS: c.proved}
	}
	if c.anchor.TS.After(c.proved) {
		c.anchor.TS = c.proved
	}
	c.resume = pytime.MinuteFloor(c.proved)
	c.conn = 0
	c.repairs = append(dropOpen(c.repairs), span{pytime.MinuteFloor(c.proved), time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)})
}

func (s *Sealer) RepairDone(iid string, start, end time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chains[iid]
	if c == nil {
		return
	}
	for i, r := range c.repairs {
		if r.start.Equal(start) && r.end.Equal(end) {
			c.repairs = append(c.repairs[:i], c.repairs[i+1:]...)
			return
		}
	}
}

func (s *Sealer) RepairQueued(inst *venue.Instrument, start, end time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chain(inst)
	c.repairs = append(c.repairs, span{start, end})
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (s *Sealer) holds(key flow.Key, id string) bool { return s.ids[key][id] }

func (s *Sealer) remember(key flow.Key, id string) {
	set := s.ids[key]
	if set == nil {
		set = map[string]bool{}
		s.ids[key] = set
	}
	set[id] = true
}

func (s *Sealer) Trade(inst *venue.Instrument, t *normalize.Trade, conn int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := flow.KeyOf(inst.ID, t.TS)
	if s.holds(key, t.TradeID) {
		return
	}
	if s.seals[key] != nil {
		s.lateHeld(inst, key, t)
		return
	}
	if _, gone := s.versions[key]; gone {
		s.Late(inst, t)
		return
	}
	c := s.chain(inst)
	if !c.first.IsZero() && t.TS.Before(pytime.MinuteFloor(c.first)) {

		s.Late(inst, t)
		return
	}
	live := conn != 0 && conn == c.conn
	prior := c.last
	hole := false
	newer := true
	if c.ids && live && prior != nil && c.lastConn == conn {
		cur, ok1 := intID(t.TradeID)
		prev, ok2 := intID(prior.TradeID)
		if ok1 && ok2 {
			newer = cur > prev
			hole = cur > prev+1
		}
	}
	before := ""
	if b := s.acc.Bars[key]; b != nil {
		before = b.GapReason
	}
	s.acc.Add(t, live)
	s.remember(key, t.TradeID)
	if !live {
		return
	}
	if hole {

		s.proveTo(c, minTime(prior.TS, s.clock.Now()), sourceStream, s.clock.Now())
		s.acc.ProveRange(inst.ID, prior.TS, t.TS, false, "STREAM", "TRADE_ID_GAP", s.clock.Now())
		c.proved = maxTime(c.proved, t.TS)
		lo, hi := pytime.MinuteFloor(prior.TS), t.TS.Add(time.Millisecond)
		c.repairs = append(c.repairs, span{lo, hi})
		s.Repair(inst, lo, hi, "TRADE_ID_GAP")
	} else if b := s.acc.Bars[key]; b != nil && b.GapReason != "" && b.GapReason != before && !c.ids {

		lo, hi := pytime.MinuteFloor(t.TS), pytime.MinuteFloor(t.TS).Add(time.Minute)
		c.repairs = append(c.repairs, span{lo, hi})
		s.Repair(inst, lo, hi, b.GapReason)
	}
	if newer || prior == nil {
		c.last, c.lastConn = t, conn
	}
	if t.TS.After(c.frontier) {
		c.frontier = t.TS
	}
}

func (s *Sealer) lateHeld(inst *venue.Instrument, key flow.Key, t *normalize.Trade) {
	if s.acc.Bars[key] == nil {
		s.Late(inst, t)
		return
	}
	s.acc.Add(t, false)
	s.remember(key, t.TradeID)
	s.revise(key, "LATE_PRINT", s.clock.Now())
}

func (s *Sealer) Watermark(iid string, conn int64, ts time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.chains[iid]; c != nil && c.conn == conn && ts.After(c.watermark) {
		c.watermark = ts
	}
}

func (s *Sealer) proveTo(c *chain, until time.Time, source string, now time.Time) {
	for lo := c.proved; lo.Before(until); {
		minute := pytime.MinuteFloor(lo)
		hi := minTime(minute.Add(time.Minute), until)
		key := flow.KeyOf(c.inst.ID, minute)
		if _, ok := s.acc.Bars[key]; !ok && s.seals[key] == nil {
			if _, gone := s.versions[key]; !gone {
				s.acc.EmptyMinute(c.inst, minute)
			}
		}
		s.acc.ProveRange(c.inst.ID, lo, hi, true, source, "", now)
		lo = hi
	}
	if until.After(c.proved) {
		c.proved = until
	}
}

type HeadCheck struct {
	Inst *venue.Instrument
	Conn int64
	Last *normalize.Trade
}

func (s *Sealer) Tick(now time.Time) []HeadCheck {
	s.mu.Lock()
	defer s.mu.Unlock()
	var checks []HeadCheck
	for _, c := range s.chains {
		if c.conn == 0 {
			continue
		}
		s.proveTo(c, minTime(c.frontier, now), sourceStream, now)
		if c.inst.VenueName() == "hyperliquid" && !c.watermark.IsZero() {
			s.proveTo(c, minTime(c.watermark.Add(-s.headDelay), now), sourceWatermark, now)
		}
		if c.heading || now.Before(c.headNext) || c.inst.VenueName() == "hyperliquid" {
			continue
		}

		open := pytime.MinuteFloor(c.proved).Add(time.Minute)
		if !open.Add(s.headDelay).After(now) && !c.pending(pytime.MinuteFloor(c.proved), open) {
			c.heading = true
			checks = append(checks, HeadCheck{Inst: c.inst, Conn: c.conn, Last: c.last})
		}
	}
	keys := make([]flow.Key, 0, len(s.acc.Bars))
	for key := range s.acc.Bars {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Minute != keys[j].Minute {
			return keys[i].Minute < keys[j].Minute
		}
		return keys[i].IID < keys[j].IID
	})
	for _, key := range keys {
		bar := s.acc.Bars[key]
		minute := key.MinuteTime()
		end := minute.Add(time.Minute)
		if sl := s.seals[key]; sl != nil {
			if sl.at.Add(sealRetain).Before(now) && end.Add(sealRetain).Before(now) {
				s.release(key)
			}
			continue
		}
		c := s.chains[key.IID]
		if end.After(now) || c == nil {
			s.provisional(key, now)
			continue
		}
		covered := flow.Covered(minute, end, bar.Coverage)
		switch {
		case covered && bar.GapReason == "":
			s.seal(key, true, proofSource(bar.Coverage, end), now)
		case c.pending(minute, end):
			s.provisional(key, now)
		case !c.proved.Before(end), !end.After(c.start), c.conn == 0, end.Add(holdUnsealed).Before(now):

			s.seal(key, false, sealedFinal, now)
		default:
			s.provisional(key, now)
		}
	}
	return checks
}

func proofSource(proofs []flow.Proof, end time.Time) string {
	source := sourceStream
	for _, p := range proofs {
		if p.Complete && p.Start.Before(end) && !p.End.Before(end) && p.Source != "" {
			source = p.Source
		}
	}
	return source
}

func (s *Sealer) HeadResult(check HeadCheck, prints []*normalize.Trade, observed time.Time, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chain(check.Inst)
	c.heading = false
	c.headNext = observed.Add(headInterval)
	if failed || c.conn != check.Conn || c.conn == 0 {
		return
	}
	limit := observed.Add(-s.headDelay)
	sort.Slice(prints, func(i, j int) bool { return lessPrint(prints[i], prints[j], c.ids) })
	var newer []*normalize.Trade
	for _, t := range prints {
		if c.last == nil {
			if !t.TS.Before(c.start) {
				newer = append(newer, t)
			}
			continue
		}
		if c.ids {
			cur, ok1 := intID(t.TradeID)
			prev, ok2 := intID(c.last.TradeID)
			if ok1 && ok2 && cur > prev {
				newer = append(newer, t)
			}
		} else if t.TS.After(c.last.TS) && !s.holds(flow.KeyOf(c.inst.ID, t.TS), t.TradeID) && !t.TS.After(limit) {
			newer = append(newer, t)
		}
	}
	now := s.clock.Now()
	if len(newer) == 0 {

		if len(prints) > 0 && c.last != nil && prints[0].TS.After(c.last.TS) && len(prints) >= headPrints {
			return
		}
		if limit.After(c.frontier) {
			c.frontier = limit
			s.proveTo(c, minTime(limit, now), sourceHead, now)
		}
		return
	}
	if c.ids && c.last != nil {
		first, _ := intID(newer[0].TradeID)
		prev, _ := intID(c.last.TradeID)
		if first == prev+1 {

			run := 0
			for i, t := range newer {
				id, _ := intID(t.TradeID)
				if id != prev+1+int64(i) {
					break
				}
				run = i + 1
			}
			for _, t := range newer[:run] {
				key := flow.KeyOf(c.inst.ID, t.TS)
				if s.seals[key] != nil {
					s.lateHeld(c.inst, key, t)
					continue
				}
				s.acc.Add(t, false)
				s.remember(key, t.TradeID)
			}
			last := newer[run-1]
			c.last = last
			c.frontier = maxTime(c.frontier, last.TS)
			if run == len(newer) && limit.After(c.frontier) {
				c.frontier = limit
			}
			s.proveTo(c, minTime(c.frontier, now), sourceHead, now)
			if run == len(newer) {
				return
			}
			newer = newer[run:]
		}
		if newer[0].TS.After(limit) {
			return
		}
	}

	var lo time.Time
	if c.last != nil {
		lo = c.last.TS
	} else {
		lo = c.start
	}
	hi := newer[len(newer)-1].TS.Add(time.Millisecond)
	s.proveTo(c, minTime(lo, now), sourceStream, now)
	s.acc.ProveRange(c.inst.ID, lo, hi, false, "STREAM", "STREAM_MISSED_PRINTS", now)
	c.proved = maxTime(c.proved, hi)
	c.frontier = maxTime(c.frontier, hi)
	repair := span{pytime.MinuteFloor(lo), hi}
	c.repairs = append(c.repairs, repair)
	s.Repair(c.inst, repair.start, repair.end, "STREAM_MISSED_PRINTS")
}

func lessPrint(a, b *normalize.Trade, ids bool) bool {
	if ids {
		x, ok1 := intID(a.TradeID)
		y, ok2 := intID(b.TradeID)
		if ok1 && ok2 {
			return x < y
		}
	}
	if !a.TS.Equal(b.TS) {
		return a.TS.Before(b.TS)
	}
	return a.TradeID < b.TradeID
}

func (s *Sealer) record(key flow.Key, complete bool, now time.Time) map[string]any {
	bar := s.acc.Bars[key]
	row := s.acc.Record(key, &complete, true, now)
	if !complete && row["gap_reason"] == nil {
		row["gap_reason"] = unprovenReason(bar.Coverage)
	}
	return row
}

func unprovenReason(proofs []flow.Proof) string {
	for i := len(proofs) - 1; i >= 0; i-- {
		if !proofs[i].Complete && proofs[i].Reason != "" {
			return proofs[i].Reason
		}
	}
	return Unproven
}

func (s *Sealer) provisional(key flow.Key, now time.Time) {
	if !s.acc.Dirty[key] {
		return
	}
	if at, ok := s.wrote[key]; ok && now.Sub(at) < provisionalS {
		return
	}
	delete(s.acc.Dirty, key)
	s.wrote[key] = now
	row := s.record(key, false, now)
	if row["gap_reason"] == nil {
		row["gap_reason"] = Unproven
	}
	row["bar_version"], row["revised"] = int64(0), false
	s.put("mkt.flow_bars_1m", key.IID+":"+key.MinuteISO(), row)
}

func (s *Sealer) seal(key flow.Key, complete bool, method string, now time.Time) {
	row := s.record(key, complete, now)
	version := s.versions[key] + 1
	row["bar_version"], row["revised"] = version, version > 1
	proof := map[string]any{"sealed_by": method, "observed_at": pytime.ISO(now)}
	if version > 1 {
		proof["revision"] = map[string]any{"version": version, "reason": "RESEALED", "observed_at": pytime.ISO(now)}
	}
	row["capture_proof"] = proof
	delete(s.acc.Dirty, key)
	delete(s.wrote, key)
	s.seals[key] = &seal{version: version, at: now, record: row}
	s.put("mkt.flow_bars_1m", key.IID+":"+key.MinuteISO(), row)
}

func (s *Sealer) revise(key flow.Key, reason string, now time.Time) {
	sl := s.seals[key]
	bar := s.acc.Bars[key]
	complete := flow.Covered(key.MinuteTime(), key.MinuteTime().Add(time.Minute), bar.Coverage) && bar.GapReason == ""
	row := s.record(key, complete, now)
	if SameFlow(sl.record, row) && sl.record["complete"] == row["complete"] {
		return
	}
	s.writeRevision(key, sl, row, reason, now)
}

func (s *Sealer) writeRevision(key flow.Key, sl *seal, row map[string]any, reason string, now time.Time) {
	version := sl.version + 1
	row["bar_version"], row["revised"] = version, true
	row["capture_proof"] = map[string]any{
		"sealed_by": text(pyval.Map(sl.record["capture_proof"])["sealed_by"]),
		"revision": map[string]any{
			"version": version, "reason": reason, "observed_at": pytime.ISO(now),
			"previous": flowSummary(sl.record),
		},
	}
	sl.version, sl.record = version, row
	s.put("mkt.flow_bars_1m", key.IID+":"+key.MinuteISO(), row)
}

func flowSummary(row map[string]any) map[string]any {
	out := map[string]any{}
	for _, f := range []string{"n_buy", "n_sell", "buy_qty", "sell_qty", "complete", "gap_reason", "last_trade_id"} {
		out[f] = row[f]
	}
	return out
}

func (s *Sealer) release(key flow.Key) {
	if sl := s.seals[key]; sl != nil {
		s.versions[key] = sl.version
		if len(s.versions) > revisionsKeep {
			for k := range s.versions {
				delete(s.versions, k)
				break
			}
		}
	}
	delete(s.seals, key)
	delete(s.acc.Bars, key)
	delete(s.acc.Dirty, key)
	delete(s.acc.Boundaries, key)
	delete(s.ids, key)
	delete(s.wrote, key)
}

var RESTSameFlow = []string{
	"n_buy", "n_sell", "buy_qty", "sell_qty", "sum_sq_qty", "buy_notional", "sell_notional",
	"open", "high", "low", "close", "first_trade_id", "last_trade_id",
}

func SameFlow(stored, audited map[string]any) bool {
	value := func(row map[string]any, field string) (string, bool) {
		item, ok := row[field]
		if !ok && field == "buy_notional" {
			item = row["buy_usd"]
		} else if !ok && field == "sell_notional" {
			item = row["sell_usd"]
		} else if !ok {
			switch field {
			case "open":
				item = row["o"]
			case "high":
				item = row["h"]
			case "low":
				item = row["l"]
			case "close":
				item = row["c"]
			}
		}
		if item == nil {
			return "", false
		}
		if field == "first_trade_id" || field == "last_trade_id" {
			return text(item), true
		}
		return text(item), true
	}
	for _, field := range RESTSameFlow {
		a, okA := value(stored, field)
		b, okB := value(audited, field)
		if okA != okB {
			return false
		}
		if a == b || field == "first_trade_id" || field == "last_trade_id" {
			if a != b {
				return false
			}
			continue
		}
		x, e1 := pydec.Parse(a)
		y, e2 := pydec.Parse(b)
		if e1 != nil || e2 != nil || !x.Equal(y) {
			return false
		}
	}
	return true
}

type Held struct {
	Trades   []*normalize.Trade
	Coverage []flow.Proof
	Sealed   map[string]any
	Version  int64
	Live     bool
}

func (s *Sealer) Hold(key flow.Key) Held {
	s.mu.Lock()
	defer s.mu.Unlock()
	var h Held
	if bar := s.acc.Bars[key]; bar != nil {
		h.Trades = append(h.Trades, bar.Trades...)
		h.Coverage = append(h.Coverage, bar.Coverage...)
		h.Live = s.seals[key] == nil
	}
	if sl := s.seals[key]; sl != nil {
		h.Sealed, h.Version = sl.record, sl.version
	} else if v, ok := s.versions[key]; ok {
		h.Version = v
	}
	return h
}

func (s *Sealer) Rebuilt(key flow.Key, bar *flow.Bar, proofs []flow.Proof, now time.Time) (held bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.acc.Bars[key]
	minute := key.MinuteTime()
	if live == nil {

		c := s.chains[key.IID]
		if c == nil || c.conn == 0 || c.first.IsZero() || minute.Before(pytime.MinuteFloor(c.first)) ||
			!minute.Add(time.Minute).After(now.Add(-3*time.Minute)) {
			return false
		}
		live = &flow.Bar{}
	}
	for _, t := range live.Trades {
		if !containsTrade(bar.Trades, t.TradeID) {
			addToBar(bar, t)
		}
	}
	bar.Coverage = append(append(append([]flow.Proof(nil), live.Coverage...), proofs...), bar.Coverage...)
	complete := flow.Covered(minute, minute.Add(time.Minute), bar.Coverage) && bar.GapReason == ""
	if sl := s.seals[key]; sl != nil {
		s.acc.Bars[key] = bar
		row := s.record(key, complete, now)
		storedComplete := sl.record["complete"] == true
		if storedComplete && (row["complete"] != true || SameFlow(sl.record, row)) {
			s.acc.Bars[key] = live
			return true
		}
		for _, t := range bar.Trades {
			s.remember(key, t.TradeID)
		}
		s.writeRevision(key, sl, row, "REST_REPAIR", now)
		return true
	}
	s.acc.Bars[key] = bar
	s.acc.Dirty[key] = true
	for _, t := range bar.Trades {
		s.remember(key, t.TradeID)
	}
	if complete && !minute.Add(time.Minute).After(now) {
		s.seal(key, true, sourceREST, now)
	}
	return true
}

func containsTrade(trades []*normalize.Trade, id string) bool {
	for _, t := range trades {
		if t.TradeID == id {
			return true
		}
	}
	return false
}

func addToBar(bar *flow.Bar, t *normalize.Trade) {
	acc := flow.New()
	key := flow.KeyOf(bar.InstrumentID, t.TS)
	acc.Bars[key] = bar
	acc.Add(t, false)
}

func (s *Sealer) Version(key flow.Key) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sl := s.seals[key]; sl != nil {
		return sl.version, true
	}
	v, ok := s.versions[key]
	return v, ok
}

func (s *Sealer) Forget(key flow.Key, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seals[key] == nil {
		s.versions[key] = version
	}
}

type ChainState struct {
	Conn    int64
	Start   time.Time
	Proved  time.Time
	Last    *normalize.Trade
	Repairs int
	Sealed  time.Time
}

func (s *Sealer) State(iid string) (ChainState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chains[iid]
	if c == nil {
		return ChainState{}, false
	}
	st := ChainState{Conn: c.conn, Start: c.start, Proved: c.proved, Last: c.last, Repairs: len(c.repairs)}
	oldest := time.Time{}
	for key := range s.acc.Bars {
		if key.IID == iid && s.seals[key] == nil {
			if m := key.MinuteTime(); oldest.IsZero() || m.Before(oldest) {
				oldest = m
			}
		}
	}
	if oldest.IsZero() {
		oldest = pytime.MinuteFloor(c.proved)
	}
	st.Sealed = oldest
	return st, true
}

func (s *Sealer) Drop(iid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	for key := range s.acc.Bars {
		if key.IID == iid && s.seals[key] == nil {
			s.seal(key, false, sealedFinal, now)
		}
	}
	for key := range s.acc.Bars {
		if key.IID == iid {
			s.release(key)
		}
	}
	if c := s.chains[iid]; c != nil {
		c.conn, c.repairs = 0, nil
	}
}
