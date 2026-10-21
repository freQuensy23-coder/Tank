package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/points"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

// The PostgreSQL tests run against TANK_TEST_DATABASE_URL, a database already migrated
// with `tank db migrate` (CI migrates it once). They write only rows of their own random
// instruments, tokens, requests and buckets, and private shadow schemas, and remove them.

func databaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TANK_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TANK_TEST_DATABASE_URL required (a database migrated with `tank db migrate`)")
	}
	return url
}

func suffix() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func open(t *testing.T, schema string) *Store {
	t.Helper()
	url := databaseURL(t)
	ctx := context.Background()
	s, err := Open(ctx, url, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if schema != "mkt" {
			s.Pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quote(schema)+" CASCADE")
		}
		s.Close()
	})
	return s
}

// cleanup deletes the live rows a test wrote for its instruments.
func cleanup(t *testing.T, s *Store, column, value string, collections ...string) {
	t.Cleanup(func() {
		for _, c := range collections {
			s.Pool.Exec(context.Background(), "DELETE FROM "+liveTable(c)+" WHERE "+quote(column)+" LIKE $1", value)
		}
	})
}

// python runs a script with the tank package against the test database; the script
// reads JSON from stdin and prints JSON.
func python(t *testing.T, script string, input any) any {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("uv", "run", "--no-sync", "python", "-c", script)
	cmd.Dir = filepath.Join(storeDir(), "..", "..", "..")
	cmd.Env = append(os.Environ(), "TANK_TEST_URL="+databaseURL(t))
	cmd.Stdin = bytesReader(payload)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Skipf("tank Python environment unavailable: %v", err)
		}
		t.Fatalf("python: %v\n%s", err, stderr.String())
	}
	v, err := loadJSON(string(out))
	if err != nil {
		t.Fatalf("python output: %v: %s", err, out)
	}
	return v
}

func flowRow(iid, at string) map[string]any {
	return map[string]any{
		"instrument_id": iid, "token_id": "BTC", "minute_start": at,
		"buy_qty": "1.00000000000000000001", "sell_qty": "0", "buy_usd": "10", "sell_usd": "0",
		"size_buckets": map[string]any{}, "n_buy": 1, "n_sell": 0, "sum_sq_qty": "1", "complete": true,
		"recorded_at": "2026-10-18T00:01:00Z", "recorded_at_prec": "COLLECTION_ONLY",
		"first_trade": map[string]any{"price": "123.456789012345678901"},
	}
}

func with(m map[string]any, kv ...any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

func TestPutMarketBatchRoundTrip(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	iid := "storetest-" + suffix() + ":BTC"
	cleanup(t, s, "instrument_id", iid, "mkt.flow_bars_1m")
	row := flowRow(iid, "2026-10-18T00:00:00Z")
	result, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{row, row}, []string{iid + ":legacy", iid + ":canonical"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["buy_qty"] != row["buy_qty"] || pyjson.Canonical(result[0]["first_trade"]) != pyjson.Canonical(row["first_trade"]) {
		t.Fatalf("written %v", result)
	}
	if result[0]["minute_start_prec"] != nil || result[0]["close"] != nil || result[0]["minute_start"] != "2026-10-18T00:00:00Z" {
		t.Fatalf("decode: %v", result[0])
	}
	again, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{row}, []string{iid + ":canonical"})
	if err != nil || len(again) != 0 {
		t.Fatalf("an equal fingerprint is a no-op: %v %v", again, err)
	}
	stored, err := s.Get(ctx, "mkt.flow_bars_1m", iid+":canonical")
	if err != nil || pyjson.Canonical(stored) != pyjson.Canonical(result[0]) {
		t.Fatalf("get %v %v", stored, err)
	}
	if missing, err := s.Get(ctx, "mkt.flow_bars_1m", iid+":legacy"); err != nil || missing != nil {
		t.Fatalf("the later key replaced the earlier one: %v %v", missing, err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(row, "instrument_id", nil)}, nil); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("identity: %v", err)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(row, "unregistered", "lost")}, nil); err == nil || !strings.Contains(err.Error(), "nknown typed") {
		t.Fatalf("unknown field: %v", err)
	}
	// Python reads the same row.
	read := python(t, `import asyncio, json, os, sys
from tank.storage import PostgresRepository
key = json.load(sys.stdin)
async def main():
    repo = await PostgresRepository(os.environ["TANK_TEST_URL"]).open()
    try:
        print(json.dumps(await repo.get("mkt.flow_bars_1m", key)))
    finally:
        await repo.close()
asyncio.run(main())`, iid+":canonical")
	if pyjson.Canonical(read) != pyjson.Canonical(stored) {
		t.Fatalf("Python reads\n %s\nGo reads\n %s", pyjson.Canonical(read), pyjson.Canonical(stored))
	}
}

func TestUnprovenFlowBarNeverReplacesComplete(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	iid := "storetest-" + suffix() + ":BTC"
	cleanup(t, s, "instrument_id", iid, "mkt.flow_bars_1m")
	proven := flowRow(iid, "2026-10-18T00:00:00Z")
	key := []string{iid + ":bar"}
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{proven}, key); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	for _, reason := range []any{nil, Unproven} {
		out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{with(proven, "complete", false, "gap_reason", reason)}, key)
		if err != nil || len(out) != 0 {
			t.Fatalf("an unproven bar replaced a complete one: %v %v", out, err)
		}
	}
	lost := with(proven, "complete", false, "gap_reason", "STREAM_DISCONNECTED: keepalive ping timeout")
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{lost}, key); err != nil || len(out) != 1 {
		t.Fatalf("evidence of loss invalidates: %v %v", out, err)
	}
	stored, _ := s.Get(ctx, "mkt.flow_bars_1m", key[0])
	if stored["complete"] != false || !strings.HasPrefix(stored["gap_reason"].(string), "STREAM_DISCONNECTED") {
		t.Fatalf("stored %v", stored)
	}
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{proven}, key); err != nil || len(out) != 1 {
		t.Fatalf("an incomplete bar may become complete: %v %v", out, err)
	}
}

// pointPackets are the funding and ticker packets of tank's merge tests.
func pointPackets(iid string) map[string][]map[string]any {
	base := map[string]any{"instrument_id": iid, "token_id": "BTC", "kind": "CURRENT_ESTIMATE", "as_of": "2026-10-18T00:00:00Z", "metric_key": "funding", "interval_h": "8"}
	rest := with(base, "rate", "0.02", "rate_8h_eq", "0.02", "capture_source", "PUBLIC_REST", "source_path", "rest", "received_ts", "2026-10-18T00:00:01Z")
	ws := with(base, "rate", "0.01", "rate_8h_eq", "0.01", "capture_source", "WEBSOCKET", "source_path", "ws", "received_ts", "2026-10-18T00:00:02Z")
	a := map[string]any{"instrument_id": iid, "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "10",
		"field_sources": map[string]any{"mark_price": map[string]any{"as_of": "2026-10-18T00:00:02Z", "capture_source": "WEBSOCKET", "source_path": "ws"}}}
	b := map[string]any{"instrument_id": iid, "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "mark_price": "9", "index_price": "11",
		"field_sources": map[string]any{
			"mark_price":  map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
			"index_price": map[string]any{"as_of": "2026-10-18T00:00:01Z", "capture_source": "PUBLIC_REST", "source_path": "rest"},
		}}
	return map[string][]map[string]any{
		"mkt.funding_obs": {with(rest, "source_observations", map[string]any{"PUBLIC_REST": rest}), with(ws, "source_observations", map[string]any{"WEBSOCKET": ws})},
		"mkt.tickers_24h": {a, b},
	}
}

// The server-side point merge (sequential writes) and the batch merge give Go and
// Python the same stored rows.
func TestPointMergeEqualsPython(t *testing.T) {
	shadow := "mkt_shadow_" + suffix()
	s := open(t, shadow)
	ctx := context.Background()
	iid := "storetest-" + suffix() + ":BTC"
	cleanup(t, s, "instrument_id", iid, "mkt.funding_obs", "mkt.tickers_24h")
	packets := pointPackets(iid)
	want := python(t, `import asyncio, json, os, sys
from tank.storage import PostgresRepository
data = json.load(sys.stdin)
prefix = data["prefix"]
async def main():
    repo = await PostgresRepository(os.environ["TANK_TEST_URL"]).open()
    out = {}
    try:
        for collection, rows in data["packets"].items():
            for i, row in enumerate(rows):
                await repo.put_market_batch(collection, [row], keys=[prefix + ":seq" + str(i)])
            out[collection + ":seq"] = await repo.get(collection, prefix + ":seq" + str(len(rows) - 1))
            batched = await repo.put_market_batch(collection, list(reversed(rows)), keys=[prefix + ":batch0", prefix + ":batch1"])
            out[collection + ":batch"] = batched[0]
    finally:
        await repo.close()
    print(json.dumps(out, sort_keys=True))
asyncio.run(main())`, map[string]any{"prefix": iid, "packets": packets}).(map[string]any)
	for collection, rows := range packets {
		for i, row := range rows {
			if _, err := s.PutMarketBatch(ctx, collection, []map[string]any{row}, []string{iid + ":seq" + string(rune('0'+i))}); err != nil {
				t.Fatal(err)
			}
		}
		seq, err := s.Get(ctx, collection, iid+":seq"+string(rune('0'+len(rows)-1)))
		if err != nil {
			t.Fatal(err)
		}
		reversed := []map[string]any{rows[1], rows[0]}
		batched, err := s.PutMarketBatch(ctx, collection, reversed, []string{iid + ":batch0", iid + ":batch1"})
		if err != nil || len(batched) != 1 {
			t.Fatalf("%s batch: %v %v", collection, batched, err)
		}
		for name, got := range map[string]map[string]any{"seq": seq, "batch": batched[0]} {
			if g, w := pyjson.Canonical(got), pyjson.Canonical(want[collection+":"+name]); g != w {
				t.Errorf("%s %s:\n Go     %s\n Python %s", collection, name, g, w)
			}
		}
	}
	// A current estimate prefers the WebSocket source; both sources stay, disagreeing.
	if seq := want["mkt.funding_obs:seq"].(map[string]any); seq["rate"] != "0.01" || seq["capture_source"] != "WEBSOCKET" || seq["source_disagreement"].(map[string]any)["present"] != true {
		t.Fatalf("funding merge: %v", seq)
	}
}

func TestShadowSchema(t *testing.T) {
	shadow := "mkt_shadow_" + suffix()
	s := open(t, shadow)
	ctx := context.Background()
	if s.Table("mkt.flow_bars_1m") != `"`+shadow+`"."flow_bars_1m"` || s.Table("mkt.hot_tokens") != `"mkt"."hot_tokens"` || s.Table("mkt.capture_requests") != `"mkt"."capture_requests"` {
		t.Fatal("written collections resolve to the shadow schema, inputs to mkt")
	}
	iid := "storetest-" + suffix() + ":BTC"
	if out, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", []map[string]any{flowRow(iid, "2026-10-18T00:00:00Z")}, []string{iid}); err != nil || len(out) != 1 {
		t.Fatal(out, err)
	}
	var live int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM mkt.flow_bars_1m WHERE instrument_id=$1", iid).Scan(&live)
	if live != 0 {
		t.Fatal("a shadow write reached mkt")
	}
	liq := map[string]any{"instrument_id": iid, "token_id": "BTC", "ts": "2026-10-18T00:00:00Z", "side": "LONG_LIQ", "qty_native": "1", "notional_usd": "10", "coverage": "COMPLETE"}
	out, err := s.PutMarketBatch(ctx, "mkt.liq_obs", []map[string]any{liq}, []string{iid + ":liq"})
	if err != nil || len(out) != 1 || out[0]["id"] == nil {
		t.Fatalf("liq id from the shadow sequence: %v %v", out, err)
	}
	// A column a later migration adds to mkt reaches an existing copy at the next Open.
	if _, err := s.Pool.Exec(ctx, "ALTER TABLE "+quote(shadow)+".capture_streams DROP COLUMN recent_polls"); err != nil {
		t.Fatal(err)
	}
	again, err := Open(ctx, databaseURL(t), shadow)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	var restored bool
	again.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=$1 AND table_name='capture_streams' AND column_name='recent_polls')", shadow).Scan(&restored)
	if !restored {
		t.Fatal("missing shadow column not restored")
	}
	if _, err := Open(ctx, databaseURL(t), "bad-name"); err == nil {
		t.Fatal("an invalid schema name must fail")
	}
}

func TestLockExclusion(t *testing.T) {
	a, b := open(t, "mkt"), open(t, "mkt")
	ctx := context.Background()
	key := "tank:capture:test:" + suffix()
	previous := lockPing
	lockPing = 200 * time.Millisecond
	defer func() { lockPing = previous }()
	first, err := a.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	if _, err := b.Lock(waiting, key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a held lock must exclude: %v", err)
	}
	first.Release()
	second, err := b.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// A lost lock connection is reported, and the lock is free again.
	var pid int
	if err := b.Pool.QueryRow(ctx, "SELECT pid FROM pg_locks WHERE locktype='advisory' AND ((classid::bigint<<32)|objid::bigint)=$1 AND granted", LockID(key)).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second.Lost():
	case <-time.After(5 * time.Second):
		t.Fatal("lost lock not reported")
	}
	second.Release()
	third, err := a.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	third.Release()
}

func TestListenHotTokens(t *testing.T) {
	s := open(t, "mkt")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token := "storetest-" + suffix() + ":T"
	cleanup(t, s, "token_id", token, "mkt.hot_tokens")
	events, err := s.Listen(ctx, "tank_capture")
	if err != nil {
		t.Fatal(err)
	}
	hot := map[string]any{"token_id": token, "event_id": "EVT-1", "priority": 2, "since": "2026-10-18T00:00:00Z", "since_prec": "EXACT"}
	if _, err := s.PutMarketBatch(ctx, "mkt.hot_tokens", []map[string]any{hot}, []string{token + ":EVT-1"}); err != nil {
		t.Fatal(err)
	}
	expect := func(want string) {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for {
			select {
			case payload := <-events:
				if payload == want {
					return
				}
			case <-deadline:
				t.Fatalf("no %q notification", want)
			}
		}
	}
	expect("hot_tokens")
	rows, err := s.HotTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range rows {
		found = found || r["token_id"] == token && r["priority"] == int64(2)
	}
	if !found {
		t.Fatal("HotTokens misses the row")
	}
	// A dropped listener reconnects and wakes the receiver with an empty payload.
	if _, err := s.Pool.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND datname=current_database() AND pid<>pg_backend_pid()"); err != nil {
		t.Fatal(err)
	}
	expect("")
	cancel()
	for range events {
	}
}

func TestClaimAndFinishRequests(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	id := "storetest-" + suffix()
	cleanup(t, s, "request_id", id+"%", "mkt.capture_requests")
	now := time.Now().UTC().Truncate(time.Microsecond)
	request := func(n, deadline string) map[string]any {
		return map[string]any{"request_id": id + n, "kind": "REPAIR_WINDOWS", "status": "PENDING", "token_id": "BTC",
			"start": "2026-10-18T00:00:00Z", "start_prec": "EXACT", "end": "2026-10-18T01:00:00Z", "end_prec": "EXACT",
			"requested_at": "1970-01-01T00:00:0" + n + "Z", "requested_at_prec": "COLLECTION_ONLY",
			"deadline": deadline, "deadline_prec": "COLLECTION_ONLY", "budget_s": "20"}
	}
	past := now.Add(-time.Minute).Format(time.RFC3339Nano)
	future := now.Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := s.PutMarketBatch(ctx, "mkt.capture_requests", []map[string]any{request("1", past), request("2", future)}, []string{"key-" + id + "1", "key-" + id + "2"}); err != nil {
		t.Fatal(err)
	}
	// ClaimRequest acts on every PENDING request: never on ones another test owns.
	var foreign int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM mkt.capture_requests WHERE status='PENDING' AND request_id NOT LIKE $1", id+"%").Scan(&foreign)
	if foreign > 0 {
		t.Skip("foreign PENDING requests in the test database")
	}
	claimed, err := s.ClaimRequest(ctx, "tank-capture/test", now)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed["request_id"] != id+"2" || claimed["status"] != "RUNNING" || claimed["writer"] != "tank-capture/test" || claimed["started_at_prec"] != "COLLECTION_ONLY" {
		t.Fatalf("claimed %v", claimed)
	}
	expired, _ := s.Get(ctx, "mkt.capture_requests", "key-"+id+"1")
	if expired["status"] != "EXPIRED" || expired["reason"] != "REQUEST_DEADLINE_PASSED" || expired["finished_at"] == nil {
		t.Fatalf("expired %v", expired)
	}
	stored, _ := s.Get(ctx, "mkt.capture_requests", "key-"+id+"2")
	if stored["status"] != "RUNNING" {
		t.Fatalf("stored %v", stored)
	}
	claimed["status"], claimed["result"] = "COMPLETE", map[string]any{"x": map[string]any{"incomplete_before": 3, "incomplete_after": 0}}
	if err := s.FinishRequest(ctx, with(claimed, "finished_at", now.Format(time.RFC3339Nano), "finished_at_prec", "COLLECTION_ONLY")); err != nil {
		t.Fatal(err)
	}
	done, _ := s.Get(ctx, "mkt.capture_requests", "key-"+id+"2")
	if done == nil || done["status"] != "COMPLETE" || pyjson.Canonical(done["result"]) != `{"x":{"incomplete_after":0,"incomplete_before":3}}` {
		t.Fatalf("finished (under its own storage key) %v", done)
	}
}

func TestLatestTradesRangeAndProjection(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	iid := "storetest-" + suffix() + ":BTC"
	cleanup(t, s, "instrument_id", iid, "mkt.flow_bars_1m", "mkt.funding_obs")
	var rows []map[string]any
	var keys []string
	for i, at := range []string{"2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:02:00Z"} {
		row := flowRow(iid, at)
		if i < 2 {
			row["last_trade"] = map[string]any{"trade_id": string(rune('a' + i)), "ts": at, "price": "1.5", "n": 1}
		}
		rows, keys = append(rows, row), append(keys, iid+":"+at)
	}
	if _, err := s.PutMarketBatch(ctx, "mkt.flow_bars_1m", rows, keys); err != nil {
		t.Fatal(err)
	}
	latest, err := s.LatestTrades(ctx, []string{iid, iid, "storetest-none"}, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC))
	if err != nil || len(latest) != 1 || latest[iid]["trade_id"] != "b" || latest[iid]["n"] != int64(1) {
		t.Fatalf("latest %v %v", latest, err)
	}
	ranged, err := s.Range(ctx, "mkt.flow_bars_1m", "minute_start", time.Date(2026, 10, 18, 0, 1, 0, 0, time.UTC), time.Time{}, map[string]any{"instrument_id": iid, "complete": true})
	if err != nil || len(ranged) != 2 || ranged[0]["minute_start"] != "2026-10-18T00:01:00Z" {
		t.Fatalf("range %v %v", ranged, err)
	}
	projected, err := s.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start", "buy_notional", "storage_key", "n_buy"}, "minute_start",
		time.Time{}, time.Date(2026, 10, 18, 0, 2, 0, 0, time.UTC), []string{iid})
	if err != nil || len(projected) != 2 || projected[1]["buy_notional"] != "10" || projected[1]["storage_key"] != keys[1] || projected[1]["n_buy"] != int64(1) {
		t.Fatalf("projection %v %v", projected, err)
	}
	if none, err := s.Projection(ctx, "mkt.flow_bars_1m", []string{"minute_start"}, "minute_start", time.Time{}, time.Time{}, []string{}); err != nil || none != nil {
		t.Fatalf("empty scope %v %v", none, err)
	}
	for _, packet := range pointPackets(iid)["mkt.funding_obs"] {
		if _, err := s.PutMarketBatch(ctx, "mkt.funding_obs", []map[string]any{packet}, []string{iid + ":f"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, scope := range [][]string{nil, {iid}} {
		records, err := s.LatestRecords(ctx, "mkt.funding_obs", []string{"instrument_id", "kind"}, "as_of", scope)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range records {
			found = found || r["instrument_id"] == iid && r["kind"] == "CURRENT_ESTIMATE"
		}
		if !found {
			t.Fatalf("latest records (scope %v) miss the point", scope)
		}
	}
	single, err := s.LatestRecords(ctx, "mkt.funding_obs", []string{"instrument_id"}, "as_of", []string{iid})
	if err != nil || len(single) != 1 || single[0]["instrument_id"] != iid {
		t.Fatalf("lateral latest %v %v", single, err)
	}
}

func TestLedger(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	bucket := "storetest:" + suffix() + "|1.0|2.0"
	host := "storetest-" + suffix() + ".example"
	t.Cleanup(func() {
		s.Pool.Exec(context.Background(), "DELETE FROM ops.request_budget_ledger WHERE bucket=$1", bucket)
		s.Pool.Exec(context.Background(), "DELETE FROM ops.request_budget_blocks WHERE host=$1", host)
	})
	ledger := s.Ledger()
	for i := 0; i < 2; i++ {
		if wait, err := ledger.Acquire(ctx, bucket, 2, 1, 1, "test", ""); err != nil || wait != 0 {
			t.Fatalf("admission %d: %v %v", i, wait, err)
		}
	}
	wait, err := ledger.Acquire(ctx, bucket, 2, 1, 1, "test", "")
	if err != nil || wait <= 0 || wait > time.Second {
		t.Fatalf("a full window waits for its oldest entry: %v %v", wait, err)
	}
	if _, err := ledger.Acquire(ctx, bucket, 2, 1, 3, "test", ""); err == nil {
		t.Fatal("a weight above capacity is invalid")
	}
	until := time.Now().Add(30 * time.Second)
	if err := ledger.Block(ctx, host, until, "HTTP 429", "test"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Block(ctx, host, time.Now().Add(time.Second), "HTTP 429", "test"); err != nil {
		t.Fatal(err)
	}
	wait, err = ledger.Acquire(ctx, "storetest:"+suffix()+"|1.0|5.0", 5, 1, 1, "test", host)
	if err != nil || wait < 25*time.Second {
		t.Fatalf("a blocked host waits for its latest block: %v %v", wait, err)
	}
}

func TestWriterFlush(t *testing.T) {
	s := open(t, "mkt")
	ctx := context.Background()
	iid := "storetest-" + suffix() + ":BTC"
	cleanup(t, s, "instrument_id", iid, "mkt.flow_bars_1m", "mkt.tickers_24h", "mkt.klines_1m")
	w := NewWriter(s)
	kline := map[string]any{"instrument_id": iid, "token_id": "BTC", "open_time": "2026-10-18T00:00:00Z", "o": "1", "h": "2", "l": "1", "c": "2", "volume_native": "3", "complete": true}
	w.Sink(points.Row{Collection: "mkt.klines_1m", Key: iid + ":k", Record: kline})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":f", Record: with(flowRow(iid, "2026-10-18T00:00:00Z"), "sell_qty", "5")})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":f", Record: flowRow(iid, "2026-10-18T00:00:00Z")})
	if w.Pending() != 2 {
		t.Fatalf("pending %d (the newest row per key)", w.Pending())
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Pending() != 0 || w.LastFlush().IsZero() {
		t.Fatal("flushed")
	}
	bar, _ := s.Get(ctx, "mkt.flow_bars_1m", iid+":f")
	stored, _ := s.Get(ctx, "mkt.klines_1m", iid+":k")
	if bar == nil || bar["sell_qty"] != "0" || stored == nil {
		t.Fatalf("written %v %v", bar, stored)
	}
	// A failing write returns its rows to the queue.
	w.Sink(points.Row{Collection: "mkt.klines_1m", Key: iid + ":k2", Record: with(kline, "open_time", "2026-10-18T00:01:00Z")})
	w.Sink(points.Row{Collection: "mkt.flow_bars_1m", Key: iid + ":bad", Record: with(flowRow(iid, "2026-10-18T00:03:00Z"), "unregistered", "x")})
	if err := w.Flush(ctx); err == nil {
		t.Fatal("an invalid row must fail the flush")
	}
	if w.Pending() != 2 {
		t.Fatalf("requeued %d", w.Pending())
	}
}
