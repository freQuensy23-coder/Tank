package store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClickHouseGetRefusesMalformedNarrowingBeforeAnotherQuery(t *testing.T) {
	for name, response := range map[string]string{
		"missing instrument": `{"lo":"2026-10-25 00:00:00.000000","hi":"2026-10-26 00:00:00.000000"}`,
		"numeric instrument": `{"instrument_id":17,"lo":"2026-10-25 00:00:00.000000","hi":"2026-10-26 00:00:00.000000"}`,
		"blank lower":        `{"instrument_id":"captured-spot","lo":" ","hi":"2026-10-26 00:00:00.000000"}`,
		"null upper":         `{"instrument_id":"captured-spot","lo":"2026-10-25 00:00:00.000000","hi":null}`,
		"later malformed":    `{"instrument_id":"captured-spot","lo":"2026-10-25 00:00:00.000000","hi":"2026-10-26 00:00:00.000000"}` + "\n" + `{"instrument_id":"second","lo":null,"hi":"2026-10-26 00:00:00.000000"}`,
		"invalid calendar":   `{"instrument_id":"captured-spot","lo":"2026-02-30 00:00:00.000000","hi":"2026-10-26 00:00:00.000000"}`,
		"numeric date":       `{"instrument_id":"captured-spot","lo":"1700000000","hi":"2026-10-26 00:00:00.000000"}`,
		"reversed bounds":    `{"instrument_id":"captured-spot","lo":"2026-10-26 00:00:00.000000","hi":"2026-10-25 00:00:00.000000"}`,
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = fmt.Fprintln(w, response)
			}))
			defer server.Close()
			client, err := NewClickHouse(server.URL, "")
			if err != nil {
				t.Fatal(err)
			}
			store := &Store{CH: client}
			row, err := store.Get(context.Background(), "mkt.flow_bars_1m", "captured-spot:key")
			if err == nil || !strings.Contains(err.Error(), "invalid ClickHouse storage-key metadata") || row != nil {
				t.Fatalf("malformed narrowing = %v, %v", row, err)
			}
			if observed := requests.Load(); observed != 1 {
				t.Fatalf("malformed metadata triggered %d queries", observed)
			}
		})
	}
}

func TestClickHouseGetUsesValidatedUTCSpanWithoutBroadeningLatestRead(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		number := requests.Add(1)
		if number == 1 {
			_, _ = fmt.Fprintln(w, `{"instrument_id":"captured-spot","lo":"2026-10-25 00:00:00.000001","hi":"2026-10-26 00:00:00.000002"}`)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		query := string(body)
		parameters := r.URL.Query()
		if !strings.Contains(query, " FINAL WHERE ") || !strings.Contains(query, "instrument_id IN") {
			t.Errorf("latest query lost physical latest or scope qualification: %s", query)
		}
		values := map[string]bool{}
		for name, entries := range parameters {
			if strings.HasPrefix(name, "param_") {
				for _, value := range entries {
					values[value] = true
				}
			}
		}
		if !values["captured-spot:key"] || !values["['captured-spot']"] || !values["2026-10-25 00:00:00.000001"] || !values["2026-10-26 00:00:00.000002"] {
			t.Error("latest query did not preserve exact captured key, instrument and microsecond bounds")
		}
	}))
	defer server.Close()
	client, err := NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{CH: client}
	row, err := store.Get(context.Background(), "mkt.flow_bars_1m", "captured-spot:key")
	if err != nil || row != nil || requests.Load() != 2 {
		t.Fatalf("validated narrowing and empty latest read = %v, %v, %d", row, err, requests.Load())
	}
}
