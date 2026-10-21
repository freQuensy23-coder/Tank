package normalize

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// QuoteUSDReach and QuoteUSDMaxAge are QUOTE_USD_REACH and QUOTE_USD_MAX_AGE.
const (
	QuoteUSDReach  = 4 * 24 * time.Hour
	QuoteUSDMaxAge = 6 * time.Hour
)

type sample struct {
	at   time.Time
	rate string
}

// FX is QUOTE_USD_HISTORY: per quote asset, the stablecoin USD samples the capture
// fetched or restored, ascending. A print is valued with the latest sample at or before
// its own time, never a later one.
type FX struct {
	mu      sync.RWMutex
	history map[string][]sample
}

// NewFX is an empty history.
func NewFX() *FX { return &FX{history: map[string][]sample{}} }

// Remember is remember_quote_usd: add (as_of, rate) samples; samples more than
// QuoteUSDReach older than the newest are dropped.
func (f *FX) Remember(quote string, samples map[time.Time]string) {
	if len(samples) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	merged := map[time.Time]string{}
	for _, s := range f.history[quote] {
		merged[s.at] = s.rate
	}
	for at, rate := range samples {
		merged[at.UTC()] = rate
	}
	var newest time.Time
	for at := range merged {
		if at.After(newest) {
			newest = at
		}
	}
	kept := make([]sample, 0, len(merged))
	for at, rate := range merged {
		if !at.Before(newest.Add(-QuoteUSDReach)) {
			kept = append(kept, sample{at, rate})
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].at.Before(kept[j].at) })
	f.history[quote] = kept
}

// Has reports whether any sample of quote is held.
func (f *FX) Has(quote string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.history[quote]) > 0
}

// Rate is quote_usd_rate(instrument, at): (rate, as_of) or ok=false. USD quotes are "1"
// with no as_of; with a history only a sample at or before `at` and at most
// QuoteUSDMaxAge old values it; without one the instrument's live fields serve when not
// newer than `at`.
func (f *FX) Rate(inst *venue.Instrument, at time.Time) (rate string, asOf string, ok bool) {
	quote := strings.ToUpper(inst.Quote())
	if quote == "USD" {
		return "1", "", true
	}
	f.mu.RLock()
	history := f.history[quote]
	f.mu.RUnlock()
	if len(history) > 0 {
		i := sort.Search(len(history), func(i int) bool { return history[i].at.After(at) })
		if i > 0 && at.Sub(history[i-1].at) <= QuoteUSDMaxAge {
			return history[i-1].rate, pytime.ISO(history[i-1].at), true
		}
		return "", "", false
	}
	if inst.QuoteUSDRate == nil {
		return "", "", false
	}
	asOfValue := ""
	if inst.QuoteUSDRateAsOf != nil {
		t, err := pytime.Parse(inst.QuoteUSDRateAsOf)
		if err != nil || t.After(at) {
			return "", "", false
		}
		asOfValue = pytime.ISO(t)
		if s, isStr := inst.QuoteUSDRateAsOf.(string); isStr {
			asOfValue = s
		}
	}
	return pyStr(inst.QuoteUSDRate), asOfValue, true
}
