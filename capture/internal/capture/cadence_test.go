package capture

import (
	"testing"
	"time"
)

func TestBaseTickerKeepsItsDeclaredCadenceAcrossKlineTicks(t *testing.T) {
	const kline = 60 * time.Second
	var due time.Duration
	var polls []time.Duration
	for started := time.Duration(0); started < 20*time.Minute; started += kline {
		if started < due {
			continue
		}
		polls = append(polls, started)
		due = cadenceDue(started, 300)
	}
	want := []time.Duration{0, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute}
	if len(polls) != len(want) {
		t.Fatalf("ticker polls %v, want %v", polls, want)
	}
	for i := range want {
		if polls[i] != want[i] {
			t.Fatalf("ticker polls %v, want %v", polls, want)
		}
	}
}
