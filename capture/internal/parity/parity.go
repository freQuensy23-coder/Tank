package parity

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/books"
	"github.com/freQuensy23-coder/Tank/capture/internal/flow"
	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pipeline"
	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/freQuensy23-coder/Tank/capture/internal/stream"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const (
	ConfigVersion = "parity-config"
	CodeVersion   = "parity-code"
	USDTRate      = "0.99950"
)

type Frame struct {
	T            string `json:"t"`
	Endpoint     string `json:"endpoint"`
	Text         string `json:"text"`
	REST         string `json:"rest"`
	InstrumentID string `json:"instrument_id"`
}

type Fixture struct {
	Meta   map[string]any
	Frames []Frame
}

func Load(path string) (*Fixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	fx := &Fixture{}
	for first := true; scanner.Scan(); first = false {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if first {
			meta, err := pyval.Decode(line)
			if err != nil {
				return nil, err
			}
			fx.Meta = pyval.Map(meta)
			continue
		}
		var frame Frame
		if err := json.Unmarshal(line, &frame); err != nil {
			return nil, err
		}
		fx.Frames = append(fx.Frames, frame)
	}
	return fx, scanner.Err()
}

func Instruments(meta map[string]any) []*venue.Instrument {
	var out []*venue.Instrument
	for _, raw := range pyval.List(meta["instruments"]) {
		m := pyval.Map(raw)
		inst := venue.FromRegistry(m)
		if inst.QuoteAsset == "USDT" {
			inst.QuoteUSDRate, inst.QuoteUSDRateAsOf = USDTRate, meta["recorded_at"]
		}
		if inst.MarketType != "SPOT" && inst.FundingIntervalH == nil {
			inst.FundingIntervalH = "8"
		}
		out = append(out, inst)
	}
	return out
}

type FrameClock struct{ T0, T time.Time }

func (c *FrameClock) Now() time.Time            { return c.T }
func (c *FrameClock) Monotonic() time.Duration  { return c.T.Sub(c.T0) }
func (c *FrameClock) set(value string)          { c.T = pytime.MustParse(value) }
func (c *FrameClock) monotonicSeconds() float64 { return pipeline.Seconds(c.Monotonic()) }

type RowOut struct {
	Collection string         `json:"collection"`
	Key        string         `json:"key"`
	Row        map[string]any `json:"row"`
}

type Output struct {
	Trades       []map[string]any  `json:"trades"`
	Rows         []RowOut          `json:"rows"`
	Bars         []map[string]any  `json:"bars"`
	Fingerprints map[string]string `json:"fingerprints"`
	Watermarks   map[string]any    `json:"watermarks"`
	Errors       [][]any           `json:"errors"`
}

func Run(fx *Fixture) (*Output, error) {
	instruments := Instruments(fx.Meta)
	if len(instruments) == 0 {
		return nil, errors.New("fixture without instruments")
	}
	recordedAt := fx.Meta["recorded_at"].(string)
	clock := &FrameClock{T0: pytime.MustParse(recordedAt), T: pytime.MustParse(recordedAt)}
	out := &Output{Fingerprints: map[string]string{}, Watermarks: map[string]any{}, Trades: []map[string]any{}, Rows: []RowOut{}, Errors: [][]any{}}
	fxHistory := normalize.NewFX()
	fxHistory.Remember("USDT", map[time.Time]string{pytime.MustParse(recordedAt): USDTRate})
	pts := points.New(clock, func(r points.Row) {
		out.Rows = append(out.Rows, RowOut{r.Collection, r.Key, r.Record})
	}, ConfigVersion, CodeVersion, fxHistory)
	acc := flow.New()
	var deferred []func()
	pipe := pipeline.New(pts, fxHistory, clock, func(inst *venue.Instrument, t *normalize.Trade) {
		out.Trades = append(out.Trades, t.Record())
		acc.Add(t, true)
	}, func(*venue.Instrument, map[string]any) (map[string]any, error) {
		return nil, errors.New("PARITY_NO_SECOND_PATH")
	})
	pipe.Async = func(f func()) { deferred = append(deferred, f) }
	venueName := instruments[0].VenueName()
	index := map[string]*venue.Instrument{}
	members := map[string]*stream.Member{}
	byID := map[string]*venue.Instrument{}
	newMember := func(inst *venue.Instrument) *stream.Member {
		return &stream.Member{Inst: inst, Book: books.New(inst, clock, clock.Now), Bridging: true}
	}
	waiting := map[string][]queued{}
	type sides struct{ bids, asks *orderedLevels }
	merged := map[string]*sides{}
	for _, inst := range instruments {
		index[inst.RoutingSymbol()] = inst
		byID[inst.ID] = inst
		members[inst.ID] = newMember(inst)
		if venueName == "binance" && inst.MarketType == "SPOT" {
			waiting[inst.ID] = []queued{}
		} else if venueName == "binance" {
			merged[inst.ID] = &sides{newOrderedLevels(), newOrderedLevels()}
		}
	}
	marks := stream.NewWatermarks()
	process := func(number int, inst *venue.Instrument, payload any) {
		mem := members[inst.ID]
		err := stream.Process(pipe, mem, payload)
		switch {
		case errors.Is(err, stream.ErrSkip):
		case err != nil:
			name := "Exception"
			var se *pyval.SemanticError
			if errors.As(err, &se) {
				name = "SemanticError: " + se.Msg
			}
			out.Errors = append(out.Errors, []any{int64(number), inst.ID, name})
			members[inst.ID] = newMember(inst)
		default:
			marks.Record(inst, payload, clock.monotonicSeconds(), clock.Now())
		}
		for len(deferred) > 0 {
			f := deferred[0]
			deferred = deferred[1:]
			f()
		}
	}
	for number, frame := range fx.Frames {
		clock.set(frame.T)
		if frame.REST == "depth_snapshot" {
			inst := byID[frame.InstrumentID]
			snap, err := pyval.Decode([]byte(frame.Text))
			if err != nil {
				return nil, err
			}
			sm := pyval.Map(snap)
			prec := pytime.CollectionOnly
			if sm["E"] != nil {
				prec = pytime.Exact
			}
			if err := members[inst.ID].Book.Snapshot(pyval.List(sm["bids"]), pyval.List(sm["asks"]), sm["lastUpdateId"], sm["E"], nil, prec); err != nil {
				return nil, err
			}
			queue := waiting[inst.ID]
			delete(waiting, inst.ID)
			for _, q := range queue {
				process(q.number, inst, q.payload)
			}
			continue
		}
		if frame.Text == "pong" {
			continue
		}
		raw, err := pyval.Decode([]byte(frame.Text))
		if err != nil {
			return nil, err
		}
		rm := pyval.Map(raw)
		if rm == nil || rm["event"] == "error" || rm["success"] == false {
			continue
		}
		for _, routed := range stream.RouteIndexed(index, venueName, raw) {
			inst := routed.Inst
			if q, ok := waiting[inst.ID]; ok {
				waiting[inst.ID] = append(q, queued{number, routed.Payload})
				continue
			}
			data := pyval.Map(pyval.GetOr(pyval.Map(routed.Payload), "data", routed.Payload))
			if m, ok := merged[inst.ID]; ok && data != nil && data["e"] == "depthUpdate" {
				for _, pair := range []struct {
					side *orderedLevels
					rows []any
				}{{m.bids, pyval.List(data["b"])}, {m.asks, pyval.List(data["a"])}} {
					for _, r := range pair.rows {
						row := pyval.List(r)
						price, qty := pyval.Str(row[0]), pyval.Str(row[1])
						d, err := pyval.Decimal(qty)
						if err != nil {
							return nil, err
						}
						if d.IsZero() {
							pair.side.remove(price)
						} else {
							pair.side.set(price, qty)
						}
					}
				}
				if m.bids.len() == 0 || m.asks.len() == 0 {
					continue
				}
				if err := members[inst.ID].Book.Snapshot(m.bids.levels(), m.asks.levels(), data["u"], data["E"], nil, ""); err != nil {
					return nil, err
				}
				delete(merged, inst.ID)
			}
			process(number, inst, routed.Payload)
		}
	}
	if n := len(fx.Frames); n > 0 {
		clock.set(fx.Frames[n-1].T)
	}
	ids := make([]string, 0, len(marks.Marks))
	for iid := range marks.Marks {
		ids = append(ids, iid)
	}
	sort.Strings(ids)
	for _, iid := range ids {
		m := marks.Marks[iid]
		out.Watermarks[iid] = map[string]any{
			"ts": pytime.ISO(m.TS), "received_ts": pytime.ISO(m.Received), "source": "OBSERVED_SHARED_PUBLIC_SOCKET_EXCHANGE_TIME",
		}
	}
	out.Bars = Bars(acc, instruments, clock.Now())
	for _, r := range out.Rows {
		fp, err := fingerprint(r.Collection, r.Row)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", r.Collection, r.Key, err)
		}
		out.Fingerprints[r.Collection+"|"+r.Key] = fp
	}
	barPoints := points.New(clock, func(points.Row) {}, ConfigVersion, CodeVersion, fxHistory)
	for _, bar := range out.Bars {
		key := bar["instrument_id"].(string) + ":" + bar["minute_start"].(string)
		stored := barPoints.Put("mkt.flow_bars_1m", key, bar)
		fp, err := fingerprint("mkt.flow_bars_1m", stored)
		if err != nil {
			return nil, err
		}
		out.Fingerprints["mkt.flow_bars_1m|"+key] = fp
	}
	return out, nil
}

type queued struct {
	number  int
	payload any
}

type orderedLevels struct {
	order []string
	qty   map[string]string
}

func newOrderedLevels() *orderedLevels { return &orderedLevels{qty: map[string]string{}} }

func (o *orderedLevels) set(price, qty string) {
	if _, ok := o.qty[price]; !ok {
		o.order = append(o.order, price)
	}
	o.qty[price] = qty
}

func (o *orderedLevels) remove(price string) {
	if _, ok := o.qty[price]; !ok {
		return
	}
	delete(o.qty, price)
	for i, p := range o.order {
		if p == price {
			o.order = append(o.order[:i], o.order[i+1:]...)
			return
		}
	}
}

func (o *orderedLevels) len() int { return len(o.qty) }

func (o *orderedLevels) levels() []any {
	out := make([]any, 0, len(o.order))
	for _, p := range o.order {
		out = append(out, []any{p, o.qty[p]})
	}
	return out
}

func fingerprint(collection string, row map[string]any) (string, error) {
	encoded, err := schema.Encode(collection, row)
	if err != nil {
		return "", err
	}
	return schema.Fingerprint(encoded, collection), nil
}

func Bars(acc *flow.Accumulator, instruments []*venue.Instrument, now time.Time) []map[string]any {
	var out []map[string]any
	for _, inst := range instruments {
		var minutes []flow.Key
		for key := range acc.Bars {
			if key.IID == inst.ID {
				minutes = append(minutes, key)
			}
		}
		if len(minutes) == 0 {
			continue
		}
		sort.Slice(minutes, func(i, j int) bool { return minutes[i].Minute < minutes[j].Minute })
		first, last := minutes[0].MinuteTime(), minutes[len(minutes)-1].MinuteTime()
		acc.RegisterBoundaries(inst.ID, []time.Time{
			first.Add(37*time.Second + 500*time.Millisecond),
			first.Add(time.Minute),
			last.Add(12*time.Second + 345678*time.Microsecond),
		})
		acc.ProveRange(inst.ID, first.Add(15*time.Second), last.Add(time.Minute), true, "PARITY", "", now)
		middle := first.Add(time.Duration(len(minutes)/2) * time.Minute)
		acc.ProveRange(inst.ID, middle.Add(20*time.Second), middle.Add(30*time.Second), false, "PARITY", "STREAM_MEMBER_GAP", now)
		for cursor := first; !cursor.After(last); cursor = cursor.Add(time.Minute) {
			acc.EmptyMinute(inst, cursor)
		}
		var keys []flow.Key
		for key := range acc.Bars {
			if key.IID == inst.ID {
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Minute < keys[j].Minute })
		for _, key := range keys {
			minute := key.MinuteTime()
			complete := flow.Covered(minute, minute.Add(time.Minute), acc.Bars[key].Coverage)
			out = append(out, acc.Record(key, &complete, true, now))
		}
	}
	return out
}

func Digests(o *Output) map[string]any {
	trades := make([]any, len(o.Trades))
	for i, t := range o.Trades {
		trades[i] = pyjson.SHA256(t)
	}
	rows := make([]any, len(o.Rows))
	for i, r := range o.Rows {
		rows[i] = map[string]any{"collection": r.Collection, "key": r.Key, "sha256": pyjson.SHA256(r.Row)}
	}
	bars := make([]any, len(o.Bars))
	for i, b := range o.Bars {
		bars[i] = pyjson.SHA256(b)
	}
	fps := map[string]any{}
	for k, v := range o.Fingerprints {
		fps[k] = v
	}
	errs := make([]any, len(o.Errors))
	for i, e := range o.Errors {
		errs[i] = e
	}
	return map[string]any{
		"trades": trades, "rows": rows, "bars": bars, "fingerprints": fps,
		"watermarks": o.Watermarks, "errors": errs,
	}
}
