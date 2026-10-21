package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// LockID is tank.storage's advisory key: the first 8 bytes of SHA-256(key), big-endian
// and signed.
func LockID(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// lockConn is the dedicated connection a session advisory lock lives on.
type lockConn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	PID() uint32
}

// pgLockConn is a pgx connection as a lockConn.
type pgLockConn struct{ *pgx.Conn }

func (c pgLockConn) PID() uint32 { return c.PgConn().PID() }

type connector func(ctx context.Context) (lockConn, error)

// Lock is the single writer's session advisory lock, held on a dedicated connection.
// When that connection fails the lock is re-acquired on a new one: Held is false
// meanwhile, and Lost closes only when another process holds the lock (or Release).
type Lock struct {
	key     string
	id      int64
	connect connector
	// timing copied at creation: the watcher never reads the package variables
	ping, retry, retryMax, connectMax time.Duration

	mu       sync.Mutex
	conn     lockConn
	held     atomic.Bool
	lost     chan struct{}
	lostOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	released bool
}

// Lock timing: the acquisition retry, the liveness check, and the re-acquire backoff.
var (
	lockPoll       = 2 * time.Second
	lockPing       = 5 * time.Second
	lockRetry      = time.Second
	lockRetryMax   = 30 * time.Second
	lockConnectMax = 10 * time.Second
)

// Lock waits until it holds the session advisory lock of key (pg_try_advisory_lock on
// a dedicated connection) or ctx ends.
func (s *Store) Lock(ctx context.Context, key string) (*Lock, error) {
	return acquireLock(ctx, key, func(ctx context.Context) (lockConn, error) {
		conn, err := pgx.Connect(ctx, s.url)
		if err != nil {
			return nil, err
		}
		return pgLockConn{conn}, nil
	})
}

func acquireLock(ctx context.Context, key string, connect connector) (*Lock, error) {
	conn, err := connect(ctx)
	if err != nil {
		return nil, err
	}
	id := LockID(key)
	logged := time.Time{}
	for {
		held, err := tryLock(ctx, conn, id)
		if err != nil {
			conn.Close(context.Background())
			if ctx.Err() != nil {
				return nil, ctx.Err() // the wait ended mid-query
			}
			return nil, err
		}
		if held {
			break
		}
		if time.Since(logged) >= time.Minute {
			slog.Warn("waiting for the writer lock: another process holds it", "key", key)
			logged = time.Now()
		}
		select {
		case <-ctx.Done():
			conn.Close(context.Background())
			return nil, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
	l := &Lock{key: key, id: id, connect: connect, conn: conn, lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{}),
		ping: lockPing, retry: lockRetry, retryMax: lockRetryMax, connectMax: lockConnectMax}
	l.held.Store(true)
	go l.watch()
	return l, nil
}

func tryLock(ctx context.Context, conn lockConn, id int64) (bool, error) {
	var held bool
	err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&held)
	return held, err
}

// holder is the backend holding the advisory lock id (0 when none). A bigint key is
// stored as classid (high 32 bits) and objid (low 32 bits) with objsubid 1.
func holder(ctx context.Context, conn lockConn, id int64) (uint32, error) {
	var pid int64
	err := conn.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1 LIMIT 1`,
		int64(uint32(uint64(id)>>32)), int64(uint32(uint64(id)))).Scan(&pid)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return uint32(pid), err
}

func (l *Lock) watch() {
	defer close(l.done)
	ticker := time.NewTicker(l.ping)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
		}
		l.mu.Lock()
		conn := l.conn
		l.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), l.ping)
		err := conn.Ping(ctx)
		cancel()
		if err == nil {
			continue
		}
		l.held.Store(false)
		stale := conn.PID()
		conn.Close(context.Background())
		slog.Error("writer lock connection lost: writes pause while it is re-acquired", "key", l.key, "error", err)
		if !l.reacquire(stale) {
			return
		}
	}
}

// reacquire takes the lock again on a new connection, retrying with backoff while the
// database is unreachable. Its own earlier session may still hold the lock (a half-open
// connection the server has not noticed): that backend is terminated. Any other holder
// is another writer: Lost closes and false returns.
func (l *Lock) reacquire(stale uint32) bool {
	backoff := l.retry
	for {
		select {
		case <-l.stop:
			return false
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), l.connectMax)
		other, err := l.attempt(ctx, stale)
		cancel()
		if err == nil && other == 0 {
			slog.Info("writer lock re-acquired", "key", l.key)
			return true
		}
		if err == nil {
			slog.Error("writer lock held by another writer: stopping", "key", l.key, "pid", other)
			l.markLost()
			return false
		}
		slog.Warn("writer lock not re-acquired yet", "key", l.key, "error", err, "retry_in", backoff.String())
		select {
		case <-l.stop:
			return false
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, l.retryMax)
	}
}

// attempt is one re-acquisition: (0, nil) when the lock is held again on a new
// connection, (pid, nil) when another backend holds it, an error to retry otherwise.
func (l *Lock) attempt(ctx context.Context, stale uint32) (uint32, error) {
	conn, err := l.connect(ctx)
	if err != nil {
		return 0, err
	}
	for tries := 0; tries < 2; tries++ {
		held, err := tryLock(ctx, conn, l.id)
		if err != nil {
			conn.Close(context.Background())
			return 0, err
		}
		if held {
			l.mu.Lock()
			l.conn = conn
			l.mu.Unlock()
			l.held.Store(true)
			return 0, nil
		}
		pid, err := holder(ctx, conn, l.id)
		if err != nil {
			conn.Close(context.Background())
			return 0, err
		}
		if pid == 0 {
			continue // released between the two queries
		}
		if pid != stale {
			conn.Close(context.Background())
			return pid, nil
		}
		// Our own earlier session still holds it: end that backend, then try again.
		if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend($1)", int64(pid)); err != nil {
			conn.Close(context.Background())
			return 0, fmt.Errorf("terminate the stale lock session %d: %w", pid, err)
		}
	}
	conn.Close(context.Background())
	return 0, errors.New("writer lock still held by the stale session")
}

func (l *Lock) markLost() { l.lostOnce.Do(func() { close(l.lost) }) }

// Held reports whether the lock is held right now: writes must wait while it is not.
func (l *Lock) Held() bool { return l.held.Load() }

// Lost is closed once another process holds the lock: the caller must stop writing.
func (l *Lock) Lost() <-chan struct{} { return l.lost }

// Release unlocks and closes the connection.
func (l *Lock) Release() {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	l.mu.Unlock()
	close(l.stop)
	<-l.done
	if !l.held.Swap(false) {
		return // the connection is gone and the lock with it
	}
	l.mu.Lock()
	conn := l.conn
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", l.id)
	conn.Close(ctx)
}

// Listen delivers the payloads of NOTIFY on channel from a dedicated connection. A
// broken connection reconnects with backoff and then delivers an empty payload, since
// notifications sent meanwhile are lost (the receiver re-reads its tables). The channel
// closes when ctx ends.
func (s *Store) Listen(ctx context.Context, channel string) (<-chan string, error) {
	conn, err := s.listen(ctx, channel)
	if err != nil {
		return nil, err
	}
	out := make(chan string, 64)
	go func(conn *pgx.Conn) {
		defer close(out)
		backoff := time.Second
		for {
			if conn == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				next, err := s.listen(ctx, channel)
				if err != nil {
					backoff = min(2*backoff, 30*time.Second)
					continue
				}
				conn, backoff = next, time.Second
				deliver(out, "")
			}
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				conn.Close(context.Background())
				conn = nil
				if ctx.Err() != nil {
					return
				}
				slog.Warn("notification connection lost; reconnecting", "channel", channel, "error", err)
				continue
			}
			deliver(out, n.Payload)
		}
	}(conn)
	return out, nil
}

// deliver never blocks the listener: a wake-up queued behind 64 others is redundant.
func deliver(out chan<- string, payload string) {
	select {
	case out <- payload:
	default:
	}
}

func (s *Store) listen(ctx context.Context, channel string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, s.url)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "LISTEN "+quote(channel)); err != nil {
		conn.Close(context.Background())
		return nil, fmt.Errorf("LISTEN %s: %w", channel, err)
	}
	return conn, nil
}
