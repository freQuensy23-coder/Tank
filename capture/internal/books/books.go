// Package books is tank.market.books.OrderBook: stateful level-2 books with venue
// continuity rules (Binance and OKX sequence chains, OKX and Kraken CRC32 checksums)
// and the 10 s summary row.
package books

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const prec = pydec.Precise

// VerifyInterval is OrderBook.VERIFY_S.
const VerifyInterval = time.Second

// DeprecatedChecksum is the integrity method once OKX publishes checksum 0.
const DeprecatedChecksum = "SEQ_ID_PREV_SEQ_ID_CHECKSUM_DEPRECATED_20260623"

// Monotonic is the clock of checksum verification (time.monotonic()).
type Monotonic interface {
	Monotonic() time.Duration
}

type level struct {
	price      pydec.Dec // the key as first inserted (Python keeps the original key object)
	size       pydec.Dec
	text, qtxt string // the venue's original strings, for checksums
}

type side struct {
	levels map[string]*level // normalized price -> level
	prices []pydec.Dec       // ascending, the stored keys
}

func newSide() *side { return &side{levels: map[string]*level{}} }

func canon(d pydec.Dec) string {
	// Equal decimals are one dict key in Python: key by the value, not the text.
	s := d.FormatF()
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	if s == "" || s == "-0" {
		s = "0"
	}
	return s
}

func (s *side) search(p pydec.Dec) int {
	return sort.Search(len(s.prices), func(i int) bool { return s.prices[i].Cmp(p) >= 0 })
}

func (s *side) clear() {
	s.levels = map[string]*level{}
	s.prices = s.prices[:0]
}

// Book is OrderBook.
type Book struct {
	Inst            *venue.Instrument
	key             string
	bids, asks      *side
	Sequence        any // the venue's raw value (int or string), nil before a snapshot
	IntegrityOK     bool
	GapReason       string
	ExchangeTS      string
	ExchangeTSPrec  string
	Verified        bool
	nextVerify      float64 // monotonic float seconds, as Python's time.monotonic()
	IntegrityMethod string
	clock           Monotonic
	Now             func() time.Time // collection clock for iso() of missing venue clocks
}

// New is OrderBook(instrument).
func New(inst *venue.Instrument, clock Monotonic, now func() time.Time) *Book {
	key := inst.Key()
	method := "DOCUMENTED_LEVEL2_DELIVERY"
	switch {
	case strings.HasPrefix(key, "binance") || strings.HasPrefix(key, "okx"):
		method = "SEQUENCE"
	case strings.HasPrefix(key, "kraken"):
		method = "CRC32"
	}
	return &Book{
		Inst: inst, key: key, bids: newSide(), asks: newSide(), GapReason: "NO_SNAPSHOT",
		Verified: true, IntegrityMethod: method, clock: clock, Now: now,
	}
}

func (b *Book) sideOf(name string) *side {
	if name == "bid" {
		return b.bids
	}
	return b.asks
}

// levelStrings is the (price, qty) text of one venue level: dict levels format JSON
// decimals with format(d, "f") (integers and strings with str()), list levels use str().
func levelStrings(lv any) (string, string, error) {
	if m := pyval.Map(lv); m != nil {
		var out [2]string
		for i, k := range []string{"price", "qty"} {
			v, err := pyval.Need(m, k)
			if err != nil {
				return "", "", err
			}
			switch x := v.(type) {
			case json.Number:
				if pyval.IsInt(x) {
					out[i] = pyval.Str(x)
					continue
				}
				d, err := pydec.Parse(string(x))
				if err != nil {
					return "", "", err
				}
				out[i] = d.FormatF()
			case pydec.Dec:
				out[i] = x.FormatF()
			default:
				out[i] = pyval.Str(v)
			}
		}
		return out[0], out[1], nil
	}
	l := pyval.List(lv)
	if len(l) < 2 {
		return "", "", fmt.Errorf("book level %v", lv)
	}
	return pyval.Str(l[0]), pyval.Str(l[1]), nil
}

func (b *Book) levels(name string, levels []any, snapshot bool) error {
	s := b.sideOf(name)
	if snapshot {
		s.clear()
	}
	for _, lv := range levels {
		p, q, err := levelStrings(lv)
		if err != nil {
			return err
		}
		price, err := pyval.Decimal(p)
		if err != nil {
			return err
		}
		size, err := pyval.Decimal(q)
		if err != nil {
			return err
		}
		if price.Sign() <= 0 || size.Sign() < 0 {
			return pyval.Semantic("invalid book level")
		}
		k := canon(price)
		if size.IsZero() {
			if _, ok := s.levels[k]; ok {
				delete(s.levels, k)
				if !snapshot {
					i := s.search(price)
					s.prices = append(s.prices[:i], s.prices[i+1:]...)
				}
			}
			continue
		}
		if existing, ok := s.levels[k]; ok {
			existing.size, existing.text, existing.qtxt = size, p, q
			continue
		}
		s.levels[k] = &level{price: price, size: size, text: p, qtxt: q}
		if !snapshot {
			i := s.search(price)
			s.prices = append(s.prices, pydec.Dec{})
			copy(s.prices[i+1:], s.prices[i:])
			s.prices[i] = price
		}
	}
	if snapshot {
		s.prices = s.prices[:0]
		for _, lv := range s.levels {
			s.prices = append(s.prices, lv.price)
		}
		sort.Slice(s.prices, func(i, j int) bool { return s.prices[i].Cmp(s.prices[j]) < 0 })
	}
	return nil
}

// ApplyLevels applies levels without a snapshot or sequence (the Binance bridge event).
func (b *Book) ApplyLevels(bids, asks []any) error {
	if err := b.levels("bid", bids, false); err != nil {
		return err
	}
	return b.levels("ask", asks, false)
}

func (b *Book) isoOf(ts any) (string, error) {
	if ts == nil {
		return pytime.ISO(b.Now()), nil
	}
	return pytime.ISOOf(ts)
}

// Snapshot is OrderBook.snapshot. tsPrec "" means EXACT.
func (b *Book) Snapshot(bids, asks []any, sequence, ts, checksum any, tsPrec string) error {
	if tsPrec == "" {
		tsPrec = pytime.Exact
	}
	if err := b.levels("bid", bids, true); err != nil {
		return err
	}
	if err := b.levels("ask", asks, true); err != nil {
		return err
	}
	b.Sequence = sequence
	iso, err := b.isoOf(ts)
	if err != nil {
		return err
	}
	b.ExchangeTS = iso
	b.ExchangeTSPrec = tsPrec
	if ts == nil {
		b.ExchangeTSPrec = pytime.CollectionOnly
	}
	b.IntegrityOK = len(b.bids.levels) > 0 && len(b.asks.levels) > 0
	b.GapReason = ""
	if !b.IntegrityOK {
		b.GapReason = "EMPTY_BOOK"
	}
	if checksum != nil {
		if strings.HasPrefix(b.key, "okx") && isZero(checksum) {
			b.IntegrityMethod = DeprecatedChecksum
		} else if err := b.Verify(checksum); err != nil {
			return err
		}
	}
	return nil
}

func seconds(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

func isZero(v any) bool {
	i, err := pyval.Int(v)
	return err == nil && i == 0
}

func intOf(v any) (int64, error) { return pyval.Int(v) }

// Update is OrderBook.update; false means a stale, already applied event.
func (b *Book) Update(bids, asks []any, sequence, previous, first, ts, checksum any, verify bool) (bool, error) {
	if b.Sequence == nil || !b.IntegrityOK {
		return false, pyval.Semantic("book requires fresh snapshot")
	}
	if strings.HasPrefix(b.key, "okx") {
		if pyval.Equal(sequence, b.Sequence) && pyval.Equal(previous, b.Sequence) {
			return false, nil
		}
	} else if sequence != nil {
		cur, err := intOf(sequence)
		if err != nil {
			return false, err
		}
		have, err := intOf(b.Sequence)
		if err != nil {
			return false, err
		}
		if cur <= have {
			return false, nil
		}
	}
	valid := true
	switch {
	case strings.HasPrefix(b.key, "binance"):
		have, err := intOf(b.Sequence)
		if err != nil {
			return false, err
		}
		if previous != nil {
			prev, err := intOf(previous)
			if err != nil {
				return false, err
			}
			valid = prev == have
		} else {
			valid = false
			if first != nil {
				f, err := intOf(first)
				if err != nil {
					return false, err
				}
				s, err := intOf(sequence)
				if err != nil {
					return false, err
				}
				valid = f <= have+1 && have+1 <= s
			}
		}
	case strings.HasPrefix(b.key, "okx"):
		valid = false
		if previous != nil {
			prev, err := intOf(previous)
			if err != nil {
				return false, err
			}
			have, err := intOf(b.Sequence)
			if err != nil {
				return false, err
			}
			valid = prev == have
		}
	}
	if !valid {
		b.IntegrityOK, b.GapReason = false, "SEQUENCE_GAP"
		return false, pyval.Semantic("book sequence gap")
	}
	if err := b.levels("bid", bids, false); err != nil {
		return false, err
	}
	if err := b.levels("ask", asks, false); err != nil {
		return false, err
	}
	if sequence != nil {
		b.Sequence = sequence
	}
	iso, err := b.isoOf(ts)
	if err != nil {
		return false, err
	}
	b.ExchangeTS = iso
	b.ExchangeTSPrec = pytime.Exact
	if ts == nil {
		b.ExchangeTSPrec = pytime.CollectionOnly
	}
	if strings.HasPrefix(b.key, "kraken") {
		for _, name := range []string{"bid", "ask"} {
			s := b.sideOf(name)
			excess := len(s.levels) - 1000
			if excess <= 0 {
				continue
			}
			var far []pydec.Dec
			if name == "bid" {
				far = s.prices[:excess]
			} else {
				far = s.prices[len(s.prices)-excess:]
			}
			for _, p := range far {
				delete(s.levels, canon(p))
			}
			if name == "bid" {
				s.prices = append(s.prices[:0], s.prices[excess:]...)
			} else {
				s.prices = s.prices[:len(s.prices)-excess]
			}
		}
	}
	if checksum != nil {
		if strings.HasPrefix(b.key, "okx") && isZero(checksum) {
			b.IntegrityMethod = DeprecatedChecksum
			b.Verified = true
		} else if verify || seconds(b.clock.Monotonic()) >= b.nextVerify {
			if err := b.Verify(checksum); err != nil {
				return false, err
			}
			b.Verified = true
			b.nextVerify = seconds(b.clock.Monotonic()) + VerifyInterval.Seconds()
		} else {
			// A wrong state persists, so the next verification still catches it;
			// nothing is summarized from an unverified state.
			b.Verified = false
		}
	}
	return true, nil
}

// Verify is OrderBook.verify: OKX (top 25 interleaved, signed CRC32) or Kraken (top 10
// asks then bids, decimal points and leading zeros removed).
func (b *Book) Verify(expected any) error {
	bids := b.bids.prices
	if len(bids) > 25 {
		bids = bids[len(bids)-25:]
	}
	rev := make([]pydec.Dec, len(bids))
	for i := range bids {
		rev[i] = bids[len(bids)-1-i]
	}
	asks := b.asks.prices
	if len(asks) > 25 {
		asks = asks[:25]
	}
	var crc int64
	switch {
	case strings.HasPrefix(b.key, "okx"):
		var parts []string
		for i := 0; i < 25; i++ {
			if i < len(rev) {
				lv := b.bids.levels[canon(rev[i])]
				parts = append(parts, lv.text, lv.qtxt)
			}
			if i < len(asks) {
				lv := b.asks.levels[canon(asks[i])]
				parts = append(parts, lv.text, lv.qtxt)
			}
		}
		v := int64(crc32.ChecksumIEEE([]byte(strings.Join(parts, ":"))))
		if v >= 1<<31 {
			v -= 1 << 32
		}
		crc = v
	case strings.HasPrefix(b.key, "kraken"):
		var sb strings.Builder
		for _, pass := range []struct {
			s      *side
			prices []pydec.Dec
		}{{b.asks, asks[:min(10, len(asks))]}, {b.bids, rev[:min(10, len(rev))]}} {
			for _, p := range pass.prices {
				lv := pass.s.levels[canon(p)]
				for _, value := range []string{lv.text, lv.qtxt} {
					sb.WriteString(strings.TrimLeft(strings.ReplaceAll(value, ".", ""), "0"))
				}
			}
		}
		crc = int64(crc32.ChecksumIEEE([]byte(sb.String())))
	default:
		return pyval.Semantic("checksum not defined for venue")
	}
	want, err := intOf(expected)
	if err != nil {
		return err
	}
	b.IntegrityOK = crc == want
	b.GapReason = ""
	if !b.IntegrityOK {
		b.GapReason = "CHECKSUM_MISMATCH"
		return pyval.Semantic("book checksum mismatch")
	}
	return nil
}

// Levels is the number of levels held per side.
func (b *Book) Levels() (int, int) { return len(b.bids.levels), len(b.asks.levels) }

// Empty reports a book without levels on a side.
func (b *Book) Empty() bool { return len(b.bids.levels) == 0 || len(b.asks.levels) == 0 }

// Frozen is a private copy of the current state (OrderBook.frozen).
func (b *Book) Frozen() *Book {
	c := *b
	for _, pair := range []struct {
		dst **side
		src *side
	}{{&c.bids, b.bids}, {&c.asks, b.asks}} {
		s := &side{levels: make(map[string]*level, len(pair.src.levels)), prices: append([]pydec.Dec(nil), pair.src.prices...)}
		for k, lv := range pair.src.levels {
			copyLevel := *lv
			s.levels[k] = &copyLevel
		}
		*pair.dst = s
	}
	return &c
}

var (
	half      = pydec.MustParse(".5")
	onePct    = pydec.FromInt(1)
	twoPct    = pydec.FromInt(2)
	hundred   = pydec.FromInt(100)
	tenK      = pydec.FromInt(10000)
	two       = pydec.FromInt(2)
	nearLower = pydec.Sub(pydec.FromInt(1), pydec.MustParse(".02"), prec)
	nearUpper = pydec.Add(pydec.FromInt(1), pydec.MustParse(".02"), prec)
)

// Summary is OrderBook.summary(received_ts=received). A crossed book clears
// IntegrityOK on this (frozen) copy, as in Python.
func (b *Book) Summary(received time.Time) (map[string]any, error) {
	if len(b.bids.levels) == 0 || len(b.asks.levels) == 0 {
		return nil, pyval.Semantic("empty order book")
	}
	bid, ask := b.bids.prices[len(b.bids.prices)-1], b.asks.prices[0]
	if ask.Cmp(bid) <= 0 {
		b.IntegrityOK, b.GapReason = false, "CROSSED_BOOK"
	}
	mid := pydec.MustDiv(pydec.Add(ask, bid, prec), two, prec)
	multiplier := pydec.FromInt(1)
	if b.key == "okx.swap" {
		if b.Inst.ContractMultiplier == nil {
			return nil, pyval.Semantic("OKX multiplier absent")
		}
		m, err := pyval.Decimal(b.Inst.ContractMultiplier)
		if err != nil {
			return nil, err
		}
		multiplier = m
	}
	var fx *pydec.Dec
	if b.Inst.QuoteValue() == "USD" {
		one := pydec.FromInt(1)
		fx = &one
	} else if b.Inst.QuoteUSDRate != nil {
		r, err := pyval.Decimal(b.Inst.QuoteUSDRate)
		if err != nil {
			return nil, err
		}
		fx = &r
	}
	bands := []struct {
		name string
		pct  pydec.Dec
	}{{"p0_5", half}, {"p1", onePct}, {"p2", twoPct}}
	depth := map[string]any{}
	ranges := map[string]any{}
	levelsOut := map[string][]any{}
	for _, name := range []string{"bid", "ask"} {
		s := b.sideOf(name)
		far := s.prices[0]
		if name == "ask" {
			far = s.prices[len(s.prices)-1]
		}
		covered := pydec.Mul(pydec.Sub(pydec.MustDiv(far, mid, prec), pydec.FromInt(1), prec).Abs(prec), hundred, prec)
		ranges[name] = covered.String()
		var near []pydec.Dec
		if name == "bid" {
			lo := pydec.Mul(mid, nearLower, prec)
			i := sort.Search(len(s.prices), func(i int) bool { return s.prices[i].Cmp(lo) >= 0 })
			for j := len(s.prices) - 1; j >= i; j-- {
				near = append(near, s.prices[j])
			}
		} else {
			hi := pydec.Mul(mid, nearUpper, prec)
			i := sort.Search(len(s.prices), func(i int) bool { return s.prices[i].Cmp(hi) > 0 })
			near = append(near, s.prices[:i]...)
		}
		type pq struct{ p, q pydec.Dec }
		var held []pq
		for _, p := range near {
			held = append(held, pq{p, pydec.Mul(s.levels[canon(p)].size, multiplier, prec)})
		}
		sideDepth := map[string]any{}
		for _, band := range bands {
			if covered.Cmp(band.pct) < 0 {
				sideDepth[band.name] = nil
				continue
			}
			native, usd := pydec.Zero, pydec.Zero
			for _, x := range held {
				dist := pydec.Mul(pydec.Sub(pydec.MustDiv(x.p, mid, prec), pydec.FromInt(1), prec).Abs(prec), hundred, prec)
				if dist.Cmp(band.pct) <= 0 {
					native = pydec.Add(native, x.q, prec)
					if fx != nil {
						usd = pydec.Add(usd, pydec.Mul(pydec.Mul(x.p, x.q, prec), *fx, prec), prec)
					}
				}
			}
			entry := map[string]any{"native": native.String(), "usd": nil}
			if fx != nil {
				entry["usd"] = usd.String()
			}
			sideDepth[band.name] = entry
		}
		depth[name] = sideDepth
		rows := make([]any, 0, len(held))
		for _, x := range held {
			rows = append(rows, []any{x.p.String(), x.q.String()})
		}
		levelsOut[name] = rows
	}
	spread := pydec.Mul(pydec.MustDiv(pydec.Sub(ask, bid, prec), mid, prec), tenK, prec)
	row := map[string]any{
		"instrument_id":        b.Inst.ID,
		"token_id":             b.Inst.TokenID,
		"mid":                  mid.String(),
		"best_bid":             bid.String(),
		"best_ask":             ask.String(),
		"spread_bps":           spread.String(),
		"depth":                depth,
		"raw_levels_range_pct": "2",
		"raw_levels_scope":     "WITHIN_TWO_PERCENT_OF_MID",
		"quote_asset":          b.Inst.QuoteValue(),
		"quote_usd_rate":       nil,
		"range_covered_pct":    ranges,
		"levels_received":      int64(len(b.bids.levels) + len(b.asks.levels)),
		"integrity_ok":         b.IntegrityOK,
		"integrity_method":     b.IntegrityMethod,
		"gap_reason":           nilIf(b.GapReason),
		"bids":                 levelsOut["bid"],
		"asks":                 levelsOut["ask"],
	}
	if fx != nil {
		row["quote_usd_rate"] = fx.String()
	}
	for k, v := range pytime.Fields("exchange_ts", b.ExchangeTS, b.ExchangeTSPrec) {
		row[k] = v
	}
	pytime.CollectionClock(received).Into(row, "received_ts")
	return row, nil
}

func nilIf(s string) any {
	if s == "" {
		return nil
	}
	return s
}
