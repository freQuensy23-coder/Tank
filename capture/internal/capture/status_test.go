package capture

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatusReportsLiveStateWithoutExposingInternalMaps(t *testing.T) {
	s := &Service{
		hotSince: map[string]time.Time{"Z": {}, "A": {}},
		groups:   map[string]*group{"stream": {}},
		members: map[string]*memberState{
			"connected": {state: "CONNECTED"},
			"pending":   {state: "CONNECTING"},
		},
	}
	s.lastFlush.Store(time.Date(2026, 10, 24, 1, 2, 3, 0, time.UTC).UnixNano())
	status := s.Status()
	if len(status.HotTokens) != 2 || status.HotTokens[0] != "A" || status.HotTokens[1] != "Z" {
		t.Fatalf("status did not sort live hot tokens: %+v", status)
	}
	if status.StreamGroups != 1 || status.Members != 2 || status.MembersConnected != 1 {
		t.Fatalf("status did not count current stream state: %+v", status)
	}
	status.HotTokens[0] = "mutated"
	if s.Status().HotTokens[0] != "A" {
		t.Fatal("status leaked mutable internal state")
	}
	body, err := json.Marshal(s.Status())
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire) != 5 || wire["last_flush_error"] != nil || string(wire["last_flush"]) != "\"2026-10-24T01:02:03Z\"" {
		t.Fatalf("status changed the successful wire contract: %s", body)
	}
	s.flushErr.Store("write unavailable")
	body, err = json.Marshal(s.Status())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire["last_flush_error"]) != "\"write unavailable\"" {
		t.Fatalf("status lost the actual flush failure: %s", body)
	}
}
