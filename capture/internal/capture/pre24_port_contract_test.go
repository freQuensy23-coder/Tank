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

	"github.com/freQuensy23-coder/Tank/capture/internal/pre24ref"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/store"
)

type physicalReferenceReader struct {
	Store
	reader *store.Store
}

func (s *physicalReferenceReader) Pre24References(ctx context.Context, q pre24ref.Query) (pre24ref.Observation, error) {
	return s.reader.Pre24References(ctx, q)
}

func TestPre24PhysicalProjectionRefusesWithoutCaptureEffects(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	var reads atomic.Int32
	boundary := r.base.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		_, _ = fmt.Fprintf(w, `{"instrument_id":%q,"minute_start":%q}`, r.inst.ID, pytime.ISO(pytime.MinuteFloor(boundary)))
	}))
	defer server.Close()
	client, err := store.NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	r.s.store = &physicalReferenceReader{Store: r.mem, reader: &store.Store{CH: client}}
	_, err = r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	var projection *store.Pre24ProjectionError
	if !errors.As(err, &projection) || projection.Problem() != store.MissingPre24Points {
		t.Fatalf("missing projection became ordinary missing source evidence: %v", err)
	}
	if reads.Load() != 1 || len(r.mem.writes) != 0 || r.v.klineCalls.Load() != 0 {
		t.Fatal("refused reference caused capture writes/fetches or a second read")
	}
}

func TestPre24BackendFailurePreservesUnavailablePublicPhase(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "owned backend unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := store.NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	r.s.store = &physicalReferenceReader{Store: r.mem, reader: &store.Store{CH: client}}
	_, err = r.s.captureWindowCoverage(context.Background(), r.inst, r.base)
	if err == nil || !strings.Contains(err.Error(), r.inst.ID+" pre24 reference unavailable: mkt.flow_bars_1m: clickhouse 503") || len(r.mem.writes) != 0 || r.v.klineCalls.Load() != 0 {
		t.Fatalf("backend error became a source observation or changed public phase: %v", err)
	}
}

func TestPre24MalformedSelectedWitnessCannotFinalizeCoverage(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	if !r.inst.Spot() {
		t.Fatal("the fixture must exercise actual spot pre24 reference consumption")
	}
	var reads atomic.Int32
	boundary := r.base.Add(-24 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		points := fmt.Sprintf(`[{"complete":true,"boundary":%q,"last_trade":{"trade_id":false,"ts":%q}}]`, pytime.ISO(boundary), pytime.ISO(boundary.Add(-time.Second)))
		_, _ = fmt.Fprintf(w, `{"instrument_id":%q,"minute_start":%q,"reference_points":%q}`, r.inst.ID, pytime.ISO(pytime.MinuteFloor(boundary)), points)
	}))
	defer server.Close()
	client, err := store.NewClickHouse(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	r.s.store = &physicalReferenceReader{Store: r.mem, reader: &store.Store{CH: client}}
	obligation := queuedBackfill(t, r.mem, "BF-pre24-invalid", map[string]any{"backfill_id": "BF-pre24-invalid", "event_id": "EVT-pre24-invalid", "token_id": r.inst.TokenID, "anchor": pytime.ISO(r.base), "status": "PENDING"})
	r.s.runObligation(context.Background(), obligation)
	row := r.mem.rows["mkt.backfill_runs"]["BF-pre24-invalid"]
	if row["status"] != "FAILED" || row["reason"] != "COVERAGE_READ_FAILED" || row["result"] != nil || row["finished_at"] == nil || reads.Load() != 1 {
		t.Fatalf("malformed selected witness acquired a terminal result: %v", row)
	}
}
