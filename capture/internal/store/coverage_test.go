package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/coverage"
)

func coverageQuery(t *testing.T, series coverage.Series, iid string) coverage.Query {
	t.Helper()
	at := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	q, err := coverage.NewQuery(series, iid, at, at.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func coverageHTTPStore(t *testing.T, body string, status int) (*Store, *atomic.Int32) {
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

func TestCoverageProjectionSeparatesMissingNullAndMalformedFacts(t *testing.T) {
	for _, test := range []struct {
		name, body string
		problem    CoverageProjectionProblem
	}{
		{"missing instrument", `{"minute_start":"2026-10-27T00:00:00Z","complete":true}`, MissingCoverageInstrument},
		{"null instrument", `{"instrument_id":null,"minute_start":"2026-10-27T00:00:00Z","complete":true}`, NullCoverageInstrument},
		{"malformed instrument", `{"instrument_id":7,"minute_start":"2026-10-27T00:00:00Z","complete":true}`, MalformedCoverageInstrument},
		{"missing bucket", `{"instrument_id":"synthetic:flow","complete":true}`, MissingCoverageBucket},
		{"null bucket", `{"instrument_id":"synthetic:flow","minute_start":null,"complete":true}`, NullCoverageBucket},
		{"malformed bucket", `{"instrument_id":"synthetic:flow","minute_start":17,"complete":true}`, MalformedCoverageBucket},
		{"missing completion", `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z"}`, MissingCoverageCompletion},
		{"null completion", `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":null}`, NullCoverageCompletion},
		{"numeric completion", `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":1}`, MalformedCoverageCompletion},
		{"text completion", `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":"true"}`, MalformedCoverageCompletion},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, requests := coverageHTTPStore(t, test.body, http.StatusOK)
			rows, err := s.CoverageMinutes(context.Background(), coverageQuery(t, coverage.Flow, "synthetic:flow"))
			var problem *CoverageProjectionError
			if rows != nil || !errors.As(err, &problem) || problem.Problem() != test.problem || requests.Load() != 1 {
				t.Fatalf("projection problem collapsed: rows=%v problem=%v requests=%d", rows, err, requests.Load())
			}
		})
	}
}

func TestCoverageWireRefusesAmbiguousAndUnscopedRows(t *testing.T) {
	for _, body := range []string{
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true,"complete":false}`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true,"\u0063omplete":true}`,
		`{"instrument_id":"foreign","instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true}`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true,"unused":null}`,
		`{"instrument_id":"foreign","minute_start":"2026-10-27T00:00:00Z","complete":true}`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00.000000001Z","complete":true}`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:03:00Z","complete":true}`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true} {}`,
		`null`, `[]`, `true`,
		`{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true}` + "\n" + `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:01:00Z"}`,
	} {
		s, requests := coverageHTTPStore(t, body, http.StatusOK)
		if rows, err := s.CoverageMinutes(context.Background(), coverageQuery(t, coverage.Flow, "synthetic:flow")); err == nil || rows != nil || requests.Load() != 1 {
			t.Fatalf("invalid wire rows admitted: %v %v", rows, err)
		}
	}
}

func TestCoverageUsesExactThreeColumnQueryAndCountsActualFalse(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		query := string(body)
		if !strings.HasPrefix(query, "SELECT instrument_id, `minute_start`, complete FROM mkt.flow_bars_1m FINAL WHERE ") || strings.Contains(query, "SELECT *") || !strings.Contains(query, "instrument_id IN") || !strings.Contains(query, "ORDER BY `minute_start`, storage_key") {
			t.Errorf("query lost physical scope/projection: %s", query)
		}
		values := map[string]bool{}
		for name, entries := range r.URL.Query() {
			if strings.HasPrefix(name, "param_") {
				for _, value := range entries {
					values[value] = true
				}
			}
		}
		if !values["['synthetic:flow']"] || !values["2026-10-27 00:00:00.000000"] || !values["2026-10-27 00:03:00.000000"] {
			t.Error("query lost actual scope/bounds")
		}
		_, _ = fmt.Fprintln(w, `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:00:00Z","complete":true}`)
		_, _ = fmt.Fprintln(w, `{"instrument_id":"synthetic:flow","minute_start":"2026-10-27T00:01:00Z","complete":false}`)
	}))
	defer server.Close()
	client, err := NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	q := coverageQuery(t, coverage.Flow, "synthetic:flow")
	rows, err := (&Store{CH: client}).CoverageMinutes(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	result, err := coverage.Count(q, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || requests.Load() != 1 || result.ExpectedMinutes() != 3 || result.CompleteMinutes() != 1 || result.MissingMinutes() != 2 {
		t.Fatalf("actual false/missing evidence changed: %+v", result)
	}
}

func TestCoverageUnicodeAdmissionDoesNotRepairIdentityBytes(t *testing.T) {
	for _, test := range []struct {
		iid, wire string
		valid     bool
	}{
		{"synthetic:😀", `"synthetic:\ud83d\ude00"`, true},
		{"synthetic:\\ud800", `"synthetic:\\ud800"`, true},
		{"synthetic:�", `"synthetic:\ud800"`, false},
		{"synthetic:�", `"synthetic:\udfff"`, false},
		{"synthetic:�", string([]byte{'"', 's', 'y', 'n', 't', 'h', 'e', 't', 'i', 'c', ':', 255, '"'}), false},
	} {
		body := `{"instrument_id":` + test.wire + `,"minute_start":"2026-10-27T00:00:00Z","complete":true}`
		s, _ := coverageHTTPStore(t, body, http.StatusOK)
		rows, err := s.CoverageMinutes(context.Background(), coverageQuery(t, coverage.Flow, test.iid))
		if test.valid {
			if err != nil || len(rows) != 1 || rows[0].InstrumentID() != test.iid {
				t.Fatalf("valid scalar identity lost: %v %v", rows, err)
			}
		} else if err == nil || rows != nil {
			t.Fatal("invalid Unicode identity was repaired into admitted scope")
		}
	}
}

func TestCoverageInvalidQueryAndReadFailureCannotBecomeEmptySuccess(t *testing.T) {
	s, requests := coverageHTTPStore(t, "", http.StatusOK)
	if rows, err := s.CoverageMinutes(context.Background(), coverage.Query{}); err == nil || rows != nil || requests.Load() != 0 {
		t.Fatal("missing query reached HTTP or fabricated empty coverage")
	}
	q := coverageQuery(t, coverage.Flow, "synthetic:flow")
	rows, err := s.CoverageMinutes(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	result, err := coverage.Count(q, rows)
	if err != nil || result.CompleteMinutes() != 0 || result.MissingMinutes() != 3 {
		t.Fatal("successful absent rows were not preserved")
	}
	failed, _ := coverageHTTPStore(t, "unavailable", http.StatusServiceUnavailable)
	if rows, err := failed.CoverageMinutes(context.Background(), q); err == nil || rows != nil {
		t.Fatal("failed read became empty successful observation")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := s.CoverageMinutes(ctx, q); !errors.Is(err, context.Canceled) || rows != nil || requests.Load() != 1 {
		t.Fatal("cancelled read dispatched or fabricated observation")
	}
}

func TestCoveragePhysicalReadPreservesScopeAndIgnoresUnselectedRawJSON(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:coverage-raw"
	_, err := s.CH.post(ctx, "INSERT INTO mkt.flow_bars_1m (storage_key,instrument_id,minute_start,complete,first_trade) VALUES ({key:String},{iid:String},{at:DateTime64(6, 'UTC')},true,{raw:String})", map[string]string{"key": iid + ":minute", "iid": iid, "at": "2026-10-27 00:00:00.000000", "raw": "{invalid-retained-json"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := coverageQuery(t, coverage.Flow, iid)
	rows, err := s.CoverageMinutes(ctx, q)
	if err != nil || len(rows) != 1 || !rows[0].Complete() {
		t.Fatalf("unused raw field blocked exact coverage facts: %v %v", rows, err)
	}
	physical, err := s.CH.Rows(ctx, "SELECT first_trade FROM mkt.flow_bars_1m FINAL WHERE instrument_id={iid:String}", map[string]string{"iid": iid})
	if err != nil || len(physical) != 1 || physical[0]["first_trade"] != "{invalid-retained-json" {
		t.Fatal("coverage read sanitized raw retained JSON")
	}
}

func TestCoveragePhysicalNullCompletionIsUnreportedAndRemainsNull(t *testing.T) {
	s := chStore(t)
	ctx := context.Background()
	iid := "storetest:coverage-null"
	_, err := s.CH.post(ctx, "INSERT INTO mkt.klines_1m (storage_key,instrument_id,open_time,complete) VALUES ({key:String},{iid:String},{at:DateTime64(6, 'UTC')},NULL)", map[string]string{"key": iid + ":minute", "iid": iid, "at": "2026-10-27 00:00:00.000000"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.CoverageMinutes(ctx, coverageQuery(t, coverage.Klines, iid))
	var problem *CoverageProjectionError
	if rows != nil || !errors.As(err, &problem) || problem.Problem() != NullCoverageCompletion {
		t.Fatalf("SQL NULL completion became known false: %v %v", rows, err)
	}
	physical, err := s.CH.Rows(ctx, "SELECT complete FROM mkt.klines_1m FINAL WHERE instrument_id={iid:String}", map[string]string{"iid": iid})
	if err != nil || len(physical) != 1 || physical[0]["complete"] != nil {
		t.Fatal("coverage read changed unreported SQL completion")
	}
}
