package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pre24ref"
)

func pre24Query(t *testing.T) pre24ref.Query {
	t.Helper()
	q, err := pre24ref.NewQuery("synthetic:pre24", time.Date(2026, 10, 27, 0, 0, 3, 123456000, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func pre24HTTPStore(t *testing.T, body string, status int) (*Store, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	client, err := NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	return &Store{CH: client}, requests
}

func pre24Wire(points string) string {
	wire, err := json.Marshal(struct {
		Instrument string `json:"instrument_id"`
		Minute     string `json:"minute_start"`
		Points     string `json:"reference_points"`
	}{"synthetic:pre24", "2026-10-27T00:00:00Z", points})
	if err != nil {
		panic(err)
	}
	return string(wire)
}

func TestPre24ProjectionPreservesMissingNullAndMalformedProblems(t *testing.T) {
	for _, test := range []struct {
		name, body string
		problem    Pre24ProjectionProblem
	}{
		{"missing instrument", `{"minute_start":"2026-10-27T00:00:00Z","reference_points":null}`, MissingPre24Instrument},
		{"null instrument", `{"instrument_id":null,"minute_start":"2026-10-27T00:00:00Z","reference_points":null}`, NullPre24Instrument},
		{"malformed instrument", `{"instrument_id":7,"minute_start":"2026-10-27T00:00:00Z","reference_points":null}`, MalformedPre24Instrument},
		{"foreign instrument", `{"instrument_id":"other","minute_start":"2026-10-27T00:00:00Z","reference_points":null}`, ForeignPre24Instrument},
		{"missing minute", `{"instrument_id":"synthetic:pre24","reference_points":null}`, MissingPre24Minute},
		{"null minute", `{"instrument_id":"synthetic:pre24","minute_start":null,"reference_points":null}`, NullPre24Minute},
		{"numeric minute", `{"instrument_id":"synthetic:pre24","minute_start":1793059200000,"reference_points":null}`, MalformedPre24Minute},
		{"unaligned minute", `{"instrument_id":"synthetic:pre24","minute_start":"2026-10-27T00:00:00.000001Z","reference_points":null}`, MalformedPre24Minute},
		{"end boundary minute", `{"instrument_id":"synthetic:pre24","minute_start":"2026-10-27T00:01:00Z","reference_points":null}`, MalformedPre24Minute},
		{"missing points", `{"instrument_id":"synthetic:pre24","minute_start":"2026-10-27T00:00:00Z"}`, MissingPre24Points},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, requests := pre24HTTPStore(t, test.body, http.StatusOK)
			q := pre24Query(t)
			observation, err := s.Pre24References(context.Background(), q)
			var problem *Pre24ProjectionError
			if !errors.As(err, &problem) || problem.Problem() != test.problem || requests.Load() != 1 {
				t.Fatalf("missing/NULL/type/scope failure collapsed: %v", err)
			}
			if _, err := observation.Reference(q); !errors.Is(err, pre24ref.ErrObservation) {
				t.Fatal("failed read acquired an observed empty result")
			}
		})
	}
}

func TestPre24ReadUsesOnlyActualScopeAndThreeRequestedColumns(t *testing.T) {
	q := pre24Query(t)
	var requests atomic.Int32
	points := `[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"trade_id":123456789012345678901234567890,"ts":"2026-10-27T01:00:02.123456+01:00","ts_prec":null,"price":"unconsumed","source_payload_json":"not parsed as embedded JSON"}}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		statement := string(body)
		if !strings.HasPrefix(statement, "SELECT instrument_id, minute_start, reference_points FROM mkt.flow_bars_1m FINAL WHERE ") || strings.Contains(statement, "SELECT *") || !strings.Contains(statement, "instrument_id IN") || !strings.Contains(statement, "ORDER BY minute_start, storage_key") {
			t.Errorf("query lost exact scope/projection: %s", statement)
		}
		values := map[string]bool{}
		for name, entries := range r.URL.Query() {
			if strings.HasPrefix(name, "param_") {
				for _, value := range entries {
					values[value] = true
				}
			}
		}
		if !values["['synthetic:pre24']"] || !values["2026-10-26 23:59:00.000000"] || !values["2026-10-27 00:01:00.000000"] {
			t.Error("query lost actual neighboring minute bounds")
		}
		_, _ = fmt.Fprintln(w, pre24Wire(points))
	}))
	defer server.Close()
	client, err := NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (&Store{CH: client}).Pre24References(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := observation.Reference(q)
	wire, encodeErr := json.Marshal(reference)
	if err != nil || encodeErr != nil || requests.Load() != 1 || !strings.Contains(string(wire), `"trade_id":123456789012345678901234567890`) || !strings.Contains(string(wire), `"ts":"2026-10-27T01:00:02.123456+01:00"`) || !strings.Contains(string(wire), `"ts_prec":null`) {
		t.Fatal("actual source identity/offset/unknown precision changed or unrelated fields were required")
	}
}

func TestPre24PhysicalNullJSONNullAndEmptyAreObservedUnavailable(t *testing.T) {
	for _, body := range []string{"", `{"instrument_id":"synthetic:pre24","minute_start":"2026-10-27T00:00:00Z","reference_points":null}`, pre24Wire("null"), pre24Wire("[]"), pre24Wire(`[{"complete":null},{"complete":false,"boundary":"unconsumed"}]`)} {
		s, requests := pre24HTTPStore(t, body, http.StatusOK)
		q := pre24Query(t)
		observation, err := s.Pre24References(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := observation.Reference(q)
		wire, encodeErr := json.Marshal(reference)
		if err != nil || encodeErr != nil || requests.Load() != 1 || string(wire) != `{"available":false,"trade_id":null,"ts":null,"ts_prec":null,"reason":"INCOMPLETE"}` {
			t.Fatal("an actual unreported/empty read acquired a witness or a guessed clock")
		}
	}
}

func TestPre24WireRefusesNestedDuplicatesAndMalformedConsumedFacts(t *testing.T) {
	for _, body := range []string{
		`{"instrument_id":"synthetic:pre24","instrument_id":"other","minute_start":"2026-10-27T00:00:00Z","reference_points":null}`,
		`{"instrument_id":"synthetic:pre24","minute_start":"2026-10-27T00:00:00Z","reference_points":null,"unused":null}`,
		pre24Wire(`[{"complete":false,"complete":true}]`),
		pre24Wire(`[{"complete":false,"\u0063omplete":true}]`),
		pre24Wire(`[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"trade_id":"a","trade_id":"b","ts":"2026-10-27T00:00:02Z"}}]`),
		pre24Wire(`[{"complete":"true"}]`),
		pre24Wire(`[{"complete":1}]`),
		pre24Wire(`[{"complete":true,"boundary":"missing-offset"}]`),
		pre24Wire(`[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":"not an object"}]`),
		pre24Wire(`[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"ts":"2026-10-27T00:00:03.123456Z","trade_id":"future"}}]`),
		pre24Wire(`[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"ts":"2026-10-27T00:00:02Z","trade_id":"\ud800"}}]`),
		pre24Wire(`[null]`), pre24Wire(`"not an array"`), pre24Wire(`[] {}`), pre24Wire(`NaN`), `[]`, `null`,
	} {
		s, requests := pre24HTTPStore(t, body, http.StatusOK)
		if _, err := s.Pre24References(context.Background(), pre24Query(t)); err == nil || requests.Load() != 1 {
			t.Fatalf("ambiguous/malformed consumed wire admitted: %s", body)
		}
	}
}

func TestPre24LatestAdmissionDefersOnlyActualOlderUnusedFacts(t *testing.T) {
	q := pre24Query(t)
	older := `{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"ts":"2026-10-27T00:00:01Z","trade_id":false,"ts_prec":7}}`
	latest := `{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"ts":"2026-10-27T00:00:02Z","trade_id":"actual-latest","ts_prec":null}}`
	for _, points := range []string{"[" + older + "," + latest + "]", "[" + latest + "," + older + "]"} {
		s, _ := pre24HTTPStore(t, pre24Wire(points), http.StatusOK)
		observation, err := s.Pre24References(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := observation.Reference(q)
		wire, encodeErr := json.Marshal(reference)
		if err != nil || encodeErr != nil || !strings.Contains(string(wire), `"trade_id":"actual-latest"`) {
			t.Fatal("unused older identity/precision prevented actual latest selection")
		}
	}
	s, _ := pre24HTTPStore(t, pre24Wire("["+older+"]"), http.StatusOK)
	observation, err := s.Pre24References(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Reference(q); err == nil || !strings.Contains(err.Error(), "requires text or an integer") {
		t.Fatal("invalid latest candidate became a missing or available witness")
	}
}

func TestPre24BackendFailureAndInvalidScopeNeverBecomeEmptySuccess(t *testing.T) {
	s, requests := pre24HTTPStore(t, "owned read failure", http.StatusServiceUnavailable)
	q := pre24Query(t)
	observation, err := s.Pre24References(context.Background(), q)
	var backend pre24ref.ReadError
	if !errors.As(err, &backend) || !strings.Contains(err.Error(), "clickhouse 503") || requests.Load() != 1 {
		t.Fatal("actual backend failure lost its phase or became ordinary no-data")
	}
	if _, err := observation.Reference(q); !errors.Is(err, pre24ref.ErrObservation) {
		t.Fatal("failed backend created an observed result")
	}
	if _, err := s.Pre24References(context.Background(), pre24ref.Query{}); !errors.Is(err, pre24ref.ErrScope) || requests.Load() != 1 {
		t.Fatal("invalid scope reached actual HTTP before admission")
	}
}

func TestPre24PhysicalReadPreservesRawEvidenceAndActualLatestClock(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	q := pre24Query(t)
	points := `[{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"trade_id":false,"ts":"2026-10-27T00:00:01Z","ts_prec":7}},{"complete":true,"boundary":"2026-10-27T00:00:03.123456Z","last_trade":{"trade_id":123456789012345678901234567890,"ts":"2026-10-27T01:00:02.123456+01:00","ts_prec":null}}]`
	_, err := s.CH.post(ctx, "INSERT INTO mkt.flow_bars_1m (storage_key,instrument_id,minute_start,reference_points,first_trade) VALUES ({key:String},{iid:String},{at:DateTime64(6, 'UTC')},{points:String},{raw:String})", map[string]string{"key": q.InstrumentID() + ":minute", "iid": q.InstrumentID(), "at": "2026-10-27 00:00:00.000000", "points": points, "raw": "{invalid-unselected-json"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := s.Pre24References(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := observation.Reference(q)
	wire, encodeErr := json.Marshal(reference)
	if err != nil || encodeErr != nil || !strings.Contains(string(wire), `"trade_id":123456789012345678901234567890`) || !strings.Contains(string(wire), `"ts":"2026-10-27T01:00:02.123456+01:00"`) || !strings.Contains(string(wire), `"ts_prec":null`) {
		t.Fatal("actual scoped physical read lost latest identity, clock bytes or unknown precision")
	}
	physical, err := s.CH.Rows(ctx, "SELECT first_trade,reference_points FROM mkt.flow_bars_1m FINAL WHERE instrument_id={iid:String}", map[string]string{"iid": q.InstrumentID()})
	if err != nil || len(physical) != 1 || physical[0]["first_trade"] != "{invalid-unselected-json" || physical[0]["reference_points"] != points {
		t.Fatal("reference read sanitized an unrequested field or rewrote selected raw evidence")
	}
}

func TestPre24PhysicalNullAndUnselectedScopeRemainUnreported(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	q := pre24Query(t)
	for _, spec := range []struct {
		key, iid, at string
		points       *string
	}{
		{"actual-null", q.InstrumentID(), "2026-10-27 00:00:00.000000", nil},
		{"unrequested-instrument", "other-instrument", "2026-10-27 00:00:00.000000", stringAddress("{malformed-unrequested-json")},
		{"unrequested-minute", q.InstrumentID(), "2026-10-27 00:01:00.000000", stringAddress("{malformed-unrequested-json")},
	} {
		statement := "INSERT INTO mkt.flow_bars_1m (storage_key,instrument_id,minute_start,reference_points) VALUES ({key:String},{iid:String},{at:DateTime64(6, 'UTC')},"
		parameters := map[string]string{"key": spec.key, "iid": spec.iid, "at": spec.at}
		if spec.points == nil {
			statement += "NULL)"
		} else {
			statement += "{points:String})"
			parameters["points"] = *spec.points
		}
		if _, err := s.CH.post(ctx, statement, parameters, nil); err != nil {
			t.Fatal(err)
		}
	}
	observation, err := s.Pre24References(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := observation.Reference(q)
	wire, encodeErr := json.Marshal(reference)
	if err != nil || encodeErr != nil || string(wire) != `{"available":false,"trade_id":null,"ts":null,"ts_prec":null,"reason":"INCOMPLETE"}` {
		t.Fatal("SQL NULL or unrequested rows became actual source witnesses")
	}
	physical, err := s.CH.Rows(ctx, "SELECT reference_points FROM mkt.flow_bars_1m FINAL WHERE storage_key={key:String}", map[string]string{"key": "actual-null"})
	if err != nil || len(physical) != 1 || physical[0]["reference_points"] != nil {
		t.Fatal("actual SQL NULL was replaced during read")
	}
}

func stringAddress(value string) *string { return &value }
