package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/freQuensy23-coder/Tank/capture/internal/requeststate"
	"os"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

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
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func open(t *testing.T) *Store {
	t.Helper()
	url := databaseURL(t)
	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func cleanup(t *testing.T, s *Store, column, value string, collections ...string) {
	t.Cleanup(func() {
		for _, c := range collections {
			if _, err := s.Pool.Exec(context.Background(), "DELETE FROM "+liveTable(c)+" WHERE "+quote(column)+" LIKE $1", value); err != nil {
				t.Error(err)
			}
		}
	})
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

func databaseLockTiming(t *testing.T) {
	t.Helper()
	poll, ping, retry, max := lockPoll, lockPing, lockRetry, lockRetryMax
	lockPoll, lockPing, lockRetry, lockRetryMax = 100*time.Millisecond, time.Second, 50*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { lockPoll, lockPing, lockRetry, lockRetryMax = poll, ping, retry, max })
}

func TestLockExclusion(t *testing.T) {
	a, b := open(t), open(t)
	ctx := context.Background()
	key := "tank:capture:test:" + suffix()
	databaseLockTiming(t)
	first, err := a.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
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
	defer second.Release()

	var pid int
	if err := b.Pool.QueryRow(ctx, "SELECT pid FROM pg_locks WHERE locktype='advisory' AND ((classid::bigint<<32)|objid::bigint)=$1 AND granted", LockID(key)).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	eventually(t, "lock not re-acquired", func() bool {
		second.mu.Lock()
		defer second.mu.Unlock()
		return second.Held() && int(second.conn.PID()) != pid
	})
	if lost(second) {
		t.Fatal("a re-acquired lock must not report Lost")
	}
	second.Release()
	third, err := a.Lock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	third.Release()
}

func TestListenHotTokens(t *testing.T) {
	s := open(t)
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
		found = found || r.TokenID == token && r.Priority != nil && *r.Priority == int64(2)
	}
	if !found {
		t.Fatal("HotTokens misses the row")
	}

	if _, err := s.Pool.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND datname=current_database() AND pid<>pg_backend_pid()"); err != nil {
		t.Fatal(err)
	}
	expect("")
	cancel()
	for range events {
	}
}

func TestClaimAndFinishRequests(t *testing.T) {
	s := open(t)
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

	var foreign int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM mkt.capture_requests WHERE status='PENDING' AND request_id NOT LIKE $1", id+"%").Scan(&foreign); err != nil {
		t.Fatal(err)
	}
	if foreign > 0 {
		t.Skip("foreign PENDING requests in the test database")
	}
	claimed, err := s.ClaimRequest(ctx, "tank-capture/test", now)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != id+"2" || claimed.Writer != "tank-capture/test" || !claimed.StartedAt.Equal(now) {
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
	result := requeststate.RepairResult{Instruments: map[string]requeststate.RepairCoverage{"x": {IncompleteBefore: 3, IncompleteAfter: 0}}}
	if err := s.FinishRequest(ctx, requeststate.Completion{ID: claimed.ID, Outcome: requeststate.Completed(result), FinishedAt: now}); err != nil {
		t.Fatal(err)
	}
	done, _ := s.Get(ctx, "mkt.capture_requests", "key-"+id+"2")
	if done == nil || done["status"] != "COMPLETE" || pyjson.Canonical(done["result"]) != `{"instruments":{"x":{"incomplete_after":0,"incomplete_before":3}}}` {
		t.Fatalf("finished (under its own storage key) %v", done)
	}
}

func TestLedger(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	bucket := "storetest:" + suffix()
	host := "storetest-" + suffix() + ".example"
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(context.Background(), "DELETE FROM ops.request_budget_ledger WHERE bucket=ops.request_budget_scope_key($1::text,1::float8)", bucket); err != nil {
			t.Error(err)
		}
		if _, err := s.Pool.Exec(context.Background(), "DELETE FROM ops.request_budget_blocks WHERE host=$1", host); err != nil {
			t.Error(err)
		}
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
	wait, err = ledger.Acquire(ctx, "storetest:"+suffix(), 5, 1, 1, "test", host)
	if err != nil || wait < 25*time.Second {
		t.Fatalf("a blocked host waits for its latest block: %v %v", wait, err)
	}
}

func TestLockSurvivesTerminatedBackend(t *testing.T) {
	databaseLockTiming(t)
	s := open(t)
	ctx := context.Background()
	l, err := s.Lock(ctx, "tank:capture:test:"+suffix())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	l.mu.Lock()
	first := l.conn.PID()
	l.mu.Unlock()
	if _, err := s.Pool.Exec(ctx, "SELECT pg_terminate_backend($1)", int64(first)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "lock not re-acquired after its backend was terminated", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.Held() && l.conn.PID() != first
	})
	if lost(l) {
		t.Fatal("a re-acquired lock must not report Lost")
	}
}
