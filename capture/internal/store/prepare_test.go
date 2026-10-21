package store

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

// prepareVectors are records folded by tank.market_storage._prepare_records (encode,
// identity de-duplication, merge_point, semantic_fingerprint). Regenerate the expected
// side with TANK_UPDATE_VECTORS=1 go test ./internal/store/ -run TestPrepareRecords.
const vectorsFile = "testdata/prepare_vectors.json"

type prepareCase struct {
	Name       string           `json:"name"`
	Collection string           `json:"collection"`
	Records    []map[string]any `json:"records"`
	Keys       []string         `json:"keys"`
	Expected   any              `json:"expected"`
}

func prepareCases() []prepareCase {
	base := map[string]any{
		"instrument_id": "batch:BTC", "token_id": "BTC", "kind": "CURRENT_ESTIMATE",
		"as_of": "2026-10-18T00:00:00Z", "metric_key": "funding", "interval_h": "8",
	}
	with := func(m map[string]any, kv ...any) map[string]any {
		out := map[string]any{}
		for k, v := range m {
			out[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			out[kv[i].(string)] = kv[i+1]
		}
		return out
	}
	rest := with(base, "rate", "0.02", "rate_8h_eq", "0.02", "capture_source", "PUBLIC_REST", "source_path", "rest", "received_ts", "2026-10-18T00:00:01Z")
	ws := with(base, "rate", "0.01", "rate_8h_eq", "0.01", "capture_source", "WEBSOCKET", "source_path", "ws", "received_ts", "2026-10-18T00:00:02Z")
	restPacket := with(rest, "source_observations", map[string]any{"PUBLIC_REST": rest})
	wsPacket := with(ws, "source_observations", map[string]any{"WEBSOCKET": ws})
	settled := with(rest, "kind", "SETTLED", "funding_time", "2026-10-18T00:00:00Z", "funding_time_prec", "EXACT")
	settledWS := with(ws, "kind", "SETTLED", "funding_time", "2026-10-18T00:00:00Z", "funding_time_prec", "EXACT")
	other := with(ws, "capture_source", "PUBLIC_REST_BACKUP", "rate", "0.03", "rate_8h_eq", "0.03")
	tickA := map[string]any{
		"instrument_id": "batch:BTC", "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "10",
		"field_sources":        map[string]any{"mark_price": map[string]any{"as_of": "2026-10-18T00:00:02Z", "capture_source": "WEBSOCKET", "source_path": "ws"}},
		"unclocked_components": map[string]any{"volume": "NO_CLOCK"},
	}
	tickB := map[string]any{
		"instrument_id": "batch:BTC", "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "9", "index_price": "11",
		"field_sources": map[string]any{
			"mark_price":  map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
			"index_price": map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
		},
		"unclocked_components": map[string]any{"open_interest": "NO_CLOCK"},
	}
	flow := map[string]any{
		"instrument_id": "test:BTC", "token_id": "BTC", "minute_start": "2026-10-18T00:00:00.250000Z",
		"buy_qty": "1.00000000000000000001", "sell_qty": "0", "buy_usd": "10", "sell_usd": "0.00000001",
		"size_buckets": map[string]any{"B1": map[string]any{"n": 1, "buy_qty": "1"}}, "n_buy": 1, "n_sell": 0,
		"sum_sq_qty": "1E+2", "complete": true, "recorded_at": "2026-10-18T00:01:00Z", "recorded_at_prec": "COLLECTION_ONLY",
		"first_trade": map[string]any{"price": "123.456789012345678901", "ratio": 0.5},
	}
	return []prepareCase{
		{Name: "funding rest then ws", Collection: "mkt.funding_obs", Records: []map[string]any{restPacket, wsPacket}, Keys: []string{"one", "two"}},
		{Name: "funding ws then rest", Collection: "mkt.funding_obs", Records: []map[string]any{wsPacket, restPacket}, Keys: []string{"one", "two"}},
		{Name: "funding settled prefers rest", Collection: "mkt.funding_obs", Records: []map[string]any{
			with(settledWS, "source_observations", map[string]any{"WEBSOCKET": settledWS}),
			with(settled, "source_observations", map[string]any{"PUBLIC_REST": settled}),
		}, Keys: []string{"a", "b"}},
		{Name: "funding unknown preferred source", Collection: "mkt.funding_obs", Records: []map[string]any{
			with(other, "source_observations", map[string]any{"PUBLIC_REST_BACKUP": other}),
			with(other, "rate", "0.04", "rate_8h_eq", "0.04", "source_observations", map[string]any{"PUBLIC_REST_BACKUP": with(other, "rate", "0.04")}),
		}, Keys: []string{"a", "a"}},
		{Name: "tickers a then b", Collection: "mkt.tickers_24h", Records: []map[string]any{tickA, tickB}, Keys: []string{"ticker_one", "ticker_two"}},
		{Name: "tickers b then a", Collection: "mkt.tickers_24h", Records: []map[string]any{tickB, tickA}, Keys: []string{"ticker_one", "ticker_two"}},
		{Name: "flow duplicates later key wins", Collection: "mkt.flow_bars_1m", Records: []map[string]any{flow, with(flow, "sell_qty", "2")}, Keys: []string{"legacy", "canonical"}},
		{Name: "flow default keys", Collection: "mkt.flow_bars_1m", Records: []map[string]any{flow, with(flow, "minute_start", "2026-10-18T00:01:00Z")}},
		{Name: "gap identity", Collection: "mkt.capture_gaps", Records: []map[string]any{{
			"gap_id": "GAP-1", "instrument_id": "test:BTC", "kind": "STREAM", "reason": "r",
			"start": "2026-10-18T00:00:00Z", "start_prec": "EXACT", "end": "2026-10-18T00:01:00Z", "end_prec": "COLLECTION_ONLY",
			"observed_at": "2026-10-18T00:01:00Z", "observed_at_prec": "COLLECTION_ONLY", "status": "OPEN",
		}}},
	}
}

const pythonPrepare = `import json, sys
from tank.market_schema import MARKET_COLUMNS
from tank.market_storage import _prepare_records
from tank.util import jsonable
cases = json.load(sys.stdin)
for case in cases:
    unique = _prepare_records(case["collection"], case["records"], case["keys"], MARKET_COLUMNS[case["collection"]], False)
    case["expected"] = [[key, jsonable(record)] for key, record in unique.items()]
print(json.dumps(cases, indent=1, sort_keys=True))
`

func storeDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func TestPrepareRecordsMatchesPython(t *testing.T) {
	path := filepath.Join(storeDir(), vectorsFile)
	if os.Getenv("TANK_UPDATE_VECTORS") != "" {
		input, err := json.Marshal(prepareCases())
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("uv", "run", "--no-sync", "python", "-c", pythonPrepare)
		cmd.Dir = filepath.Join(storeDir(), "..", "..", "..")
		cmd.Stdin = bytesReader(input)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("python: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := loadJSON(string(data))
	if err != nil {
		t.Fatal(err)
	}
	cases, _ := decoded.([]any)
	if len(cases) != len(prepareCases()) {
		t.Fatalf("%d vectors for %d cases: regenerate with TANK_UPDATE_VECTORS=1", len(cases), len(prepareCases()))
	}
	for _, item := range cases {
		c := item.(map[string]any)
		var records []map[string]any
		for _, r := range c["records"].([]any) {
			records = append(records, r.(map[string]any))
		}
		var keys []string
		if list, ok := c["keys"].([]any); ok {
			for _, k := range list {
				keys = append(keys, k.(string))
			}
		}
		order, unique, err := prepareRecords(c["collection"].(string), records, keys)
		if err != nil {
			t.Fatalf("%s: %v", c["name"], err)
		}
		var got []any
		for _, key := range order {
			got = append(got, []any{key, unique[key]})
		}
		if g, w := pyjson.Canonical(got), pyjson.Canonical(c["expected"]); g != w {
			t.Errorf("%s:\n got  %s\n want %s", c["name"], g, w)
		}
	}
}

func TestPrepareRecordsErrors(t *testing.T) {
	row := map[string]any{"instrument_id": "", "minute_start": "2026-10-18T00:00:00Z", "buy_qty": "1", "sell_qty": "0", "n_buy": 1, "n_sell": 0, "sum_sq_qty": "1", "complete": true}
	if _, _, err := prepareRecords("mkt.flow_bars_1m", []map[string]any{row}, []string{"k"}); err == nil {
		t.Fatal("an empty identity must fail")
	}
	row["instrument_id"] = "x"
	row["unregistered"] = "lost"
	if _, _, err := prepareRecords("mkt.flow_bars_1m", []map[string]any{row}, []string{"k"}); err == nil {
		t.Fatal("an unknown field must fail")
	}
	delete(row, "unregistered")
	if _, _, err := prepareRecords("mkt.flow_bars_1m", []map[string]any{row}, []string{"a", "b"}); err == nil {
		t.Fatal("keys and records must have equal lengths")
	}
}

func TestDecodeLikePsycopg(t *testing.T) {
	for in, want := range map[string]string{
		"0.00000001": "1E-8", "1.50": "1.50", "100": "100", "-0.0001": "-0.0001", "NaN": "NaN",
		"1234567890.123456789": "1234567890.123456789", "0.000001": "0.000001", "0.0000001": "1E-7",
	} {
		if got := decodeNumeric(in); got != want {
			t.Errorf("str(Decimal(%q)) = %q, want %q", in, got, want)
		}
	}
	v, err := loadJSON(`{"a":1,"b":1.5,"c":[1e2,"x",null,true],"d":123456789012345678901234567890}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := pyjson.Canonical(v); got != `{"a":1,"b":1.5,"c":[100.0,"x",null,true],"d":123456789012345678901234567890}` {
		t.Errorf("json.loads semantics: %s", got)
	}
	at := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	if pyStr(at) != "2026-10-18 00:00:00+00:00" || pyStr(at.Add(1500*time.Microsecond)) != "2026-10-18 00:00:00.001500+00:00" {
		t.Error("str(datetime)")
	}
	// int.from_bytes(sha256(key).digest()[:8], "big", signed=True), as tank.storage.
	if LockID("tank:migrations") != 2453682814151299647 || LockID("tank:capture:writer") != -6613590839062989762 {
		t.Errorf("lock ids %d %d", LockID("tank:migrations"), LockID("tank:capture:writer"))
	}
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
