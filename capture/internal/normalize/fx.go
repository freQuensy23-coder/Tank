package normalize

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const (
	QuoteUSDReach  = 4 * 24 * time.Hour
	QuoteUSDMaxAge = 6 * time.Hour
)

type sample struct {
	at   time.Time
	rate string
}

type FX struct {
	mu      sync.RWMutex
	history map[string][]sample
}

func NewFX() *FX { return &FX{history: map[string][]sample{}} }

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

func (f *FX) Has(quote string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.history[quote]) > 0
}

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
