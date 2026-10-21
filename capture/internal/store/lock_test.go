package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeServer is the advisory-lock side of PostgreSQL: one holder backend at a time.
type fakeServer struct {
	mu        sync.Mutex
	holder    uint32
	next      uint32
	down      int  // connects that fail before one succeeds
	halfOpen  bool // a closed client connection keeps its backend (and lock) alive
	conns     map[uint32]*fakeConn
	connects  int
	terminate []uint32
}

func newFakeServer() *fakeServer { return &fakeServer{next: 100, conns: map[uint32]*fakeConn{}} }

func (f *fakeServer) connect(context.Context) (lockConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	if f.down > 0 {
		f.down--
		return nil, errors.New("connection refused")
	}
	f.next++
	c := &fakeConn{server: f, pid: f.next}
	f.conns[c.pid] = c
	return c, nil
}

// kill drops a backend as the server does: its session lock goes with it.
func (f *fakeServer) kill(pid uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.conns[pid]; c != nil {
		c.dead = true
	}
	if f.holder == pid {
		f.holder = 0
	}
}

// sever breaks a client connection while the server keeps its backend and lock.
func (f *fakeServer) sever(pid uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.conns[pid]; c != nil {
		c.dead = true
	}
}

func (f *fakeServer) holderPID() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holder
}

type fakeRow struct {
	value any
	err   error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case *bool:
		*d = r.value.(bool)
	case *int64:
		*d = r.value.(int64)
	}
	return nil
}

type fakeConn struct {
	server *fakeServer
	pid    uint32
	dead   bool
}

func (c *fakeConn) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f := c.server
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.dead {
		return fakeRow{err: errors.New("conn closed")}
	}
	switch {
	case strings.Contains(sql, "pg_try_advisory_lock"):
		if f.holder == 0 || f.holder == c.pid {
			f.holder = c.pid
			return fakeRow{value: true}
		}
		return fakeRow{value: false}
	case strings.Contains(sql, "pg_locks"):
		if f.holder == 0 {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeRow{value: int64(f.holder)}
	}
	return fakeRow{err: errors.New("unexpected query " + sql)}
}

func (c *fakeConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f := c.server
	f.mu.Lock()
	if c.dead {
		f.mu.Unlock()
		return pgconn.CommandTag{}, errors.New("conn closed")
	}
	f.mu.Unlock()
	switch {
	case strings.Contains(sql, "pg_terminate_backend"):
		pid := uint32(args[0].(int64))
		f.mu.Lock()
		f.terminate = append(f.terminate, pid)
		f.mu.Unlock()
		f.kill(pid)
	case strings.Contains(sql, "pg_advisory_unlock"):
		f.mu.Lock()
		if f.holder == c.pid {
			f.holder = 0
		}
		f.mu.Unlock()
	}
	return pgconn.CommandTag{}, nil
}

func (c *fakeConn) Ping(context.Context) error {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	if c.dead {
		return errors.New("conn closed")
	}
	return nil
}

func (c *fakeConn) Close(context.Context) error {
	f := c.server
	f.mu.Lock()
	defer f.mu.Unlock()
	c.dead = true
	if !f.halfOpen && f.holder == c.pid {
		f.holder = 0
	}
	return nil
}

func (c *fakeConn) PID() uint32 { return c.pid }

func fastLock(t *testing.T) {
	t.Helper()
	poll, ping, retry, max := lockPoll, lockPing, lockRetry, lockRetryMax
	lockPoll, lockPing, lockRetry, lockRetryMax = 5*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { lockPoll, lockPing, lockRetry, lockRetryMax = poll, ping, retry, max })
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(what)
}

func lost(l *Lock) bool {
	select {
	case <-l.Lost():
		return true
	default:
		return false
	}
}

func TestLockReacquiresAfterConnectionLoss(t *testing.T) {
	fastLock(t)
	f := newFakeServer()
	l, err := acquireLock(context.Background(), "tank:capture:writer", f.connect)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	first := f.holderPID()
	f.mu.Lock()
	f.down = 3 // the database is unreachable for a while
	f.mu.Unlock()
	f.kill(first)
	eventually(t, "lock not re-acquired", func() bool { return l.Held() && f.holderPID() != 0 && f.holderPID() != first })
	if lost(l) {
		t.Fatal("a re-acquired lock must not report Lost")
	}
}

func TestLockTerminatesItsOwnStaleSession(t *testing.T) {
	fastLock(t)
	f := newFakeServer()
	f.halfOpen = true
	l, err := acquireLock(context.Background(), "tank:capture:writer", f.connect)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	stale := f.holderPID()
	f.sever(stale) // the client sees a dead connection, the server still holds the lock
	eventually(t, "stale session not replaced", func() bool { return l.Held() && f.holderPID() != stale && f.holderPID() != 0 })
	f.mu.Lock()
	terminated := append([]uint32(nil), f.terminate...)
	f.mu.Unlock()
	if len(terminated) != 1 || terminated[0] != stale || lost(l) {
		t.Fatalf("terminated %v lost %v", terminated, lost(l))
	}
}

func TestLockLostToAnotherWriter(t *testing.T) {
	fastLock(t)
	f := newFakeServer()
	l, err := acquireLock(context.Background(), "tank:capture:writer", f.connect)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	first := f.holderPID()
	f.mu.Lock()
	f.down = 1 // the other writer gets in first
	f.mu.Unlock()
	f.kill(first)
	other, err := f.connect(context.Background())
	if err != nil {
		other, _ = f.connect(context.Background())
	}
	if held, err := tryLock(context.Background(), other, l.id); err != nil || !held {
		t.Fatalf("other writer: %v %v", held, err)
	}
	eventually(t, "Lost not reported", func() bool { return lost(l) })
	if l.Held() || f.holderPID() != other.PID() {
		t.Fatal("the other writer's lock must stay with it")
	}
}

func TestLockReleaseUnlocks(t *testing.T) {
	fastLock(t)
	f := newFakeServer()
	l, err := acquireLock(context.Background(), "tank:capture:writer", f.connect)
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	l.Release()
	if f.holderPID() != 0 || l.Held() {
		t.Fatal("release leaves the lock free")
	}
}
