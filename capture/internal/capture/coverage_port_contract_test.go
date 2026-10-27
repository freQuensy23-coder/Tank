package capture

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/coverage"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/store"
)

type physicalCoverageReader struct {
	Store
	reader *store.Store
}

func (s *physicalCoverageReader) CoverageMinutes(ctx context.Context, query coverage.Query) ([]coverage.Minute, error) {
	return s.reader.CoverageMinutes(ctx, query)
}

func TestPhysicalCoverageRefusalKeepsOriginalErrorAndHasNoCaptureEffects(t *testing.T) {
	for _, test := range []struct {
		name, field string
		problem     store.CoverageProjectionProblem
	}{
		{"missing", "", store.MissingCoverageCompletion},
		{"null", `,"complete":null`, store.NullCoverageCompletion},
		{"text", `,"complete":"true"`, store.MalformedCoverageCompletion},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := newRESTRig(t, "coinbase")
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads.Add(1)
				_, _ = fmt.Fprintf(w, `{"instrument_id":%q,"minute_start":%q%s}`, r.inst.ID, pytime.ISO(r.base.Add(-time.Minute)), test.field)
			}))
			defer server.Close()
			client, err := store.NewClickHouse(server.URL, "")
			if err != nil {
				t.Fatal(err)
			}
			r.s.store = &physicalCoverageReader{Store: r.mem, reader: &store.Store{CH: client}}
			_, err = r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
			var projection *store.CoverageProjectionError
			if err == nil || err.Error() != "capture coverage requires boolean completion flags" || !errors.As(err, &projection) || projection.Problem() != test.problem {
				t.Fatalf("original refusal/absence distinction changed: %v", err)
			}
			if reads.Load() != 1 || len(r.mem.writes) != 0 || r.v.klineCalls.Load() != 0 {
				t.Fatal("invalid observed facts triggered capture effects or further reads")
			}
		})
	}
}

func TestPhysicalCoverageUnavailableCannotBecomeKnownEmptyCounts(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := store.NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	r.s.store = &physicalCoverageReader{Store: r.mem, reader: &store.Store{CH: client}}
	_, err = r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	if err == nil || !strings.Contains(err.Error(), "flow coverage unavailable") || reads.Load() != 1 || len(r.mem.writes) != 0 {
		t.Fatalf("I/O failure became a successful zero observation: %v", err)
	}
}

func TestPhysicalCoverageInvalidFactsCannotFinalizeAnObligationResult(t *testing.T) {
	r := newRESTRig(t, "hyperliquid")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"instrument_id":%q,"minute_start":%q,"complete":null}`, r.inst.ID, pytime.ISO(r.base.Add(-time.Minute)))
	}))
	defer server.Close()
	client, err := store.NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	r.s.store = &physicalCoverageReader{Store: r.mem, reader: &store.Store{CH: client}}
	obligation := queuedBackfill(t, r.mem, "BF-physical-invalid", map[string]any{"backfill_id": "BF-physical-invalid", "event_id": "EVT-physical-invalid", "token_id": r.inst.TokenID, "anchor": pytime.ISO(r.base), "status": "PENDING"})
	r.s.runObligation(context.Background(), obligation)
	row := r.mem.rows["mkt.backfill_runs"]["BF-physical-invalid"]
	if row["status"] != "FAILED" || row["reason"] != "COVERAGE_READ_FAILED" || row["result"] != nil || row["finished_at"] == nil {
		t.Fatalf("unavailable completion facts became invented persisted counts: %v", row)
	}
}
