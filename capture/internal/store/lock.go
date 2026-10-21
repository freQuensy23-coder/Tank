package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// LockID is tank.storage's advisory key: the first 8 bytes of SHA-256(key), big-endian
// and signed.
func LockID(key string) int64 {
	sum := sha256.Sum256([]byte(key))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// Lock is a session advisory lock held on its own connection for the process lifetime.
type Lock struct {
	key  string
	id   int64
	conn *pgx.Conn

	mu       sync.Mutex
	lost     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	released bool
}

// lockPoll and lockPing are the acquisition retry and the liveness check periods.
var (
	lockPoll = 2 * time.Second
	lockPing = 5 * time.Second
)

// Lock waits until it holds the session advisory lock of key (pg_try_advisory_lock on
// a dedicated connection) or ctx ends. The lock lives as long as that connection; Lost
// is closed when the connection fails, after which the caller must stop writing.
func (s *Store) Lock(ctx context.Context, key string) (*Lock, error) {
	conn, err := pgx.Connect(ctx, s.url)
	if err != nil {
		return nil, err
	}
	id := LockID(key)
	logged := time.Time{}
	for {
		var held bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&held); err != nil {
			conn.Close(context.Background())
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
	l := &Lock{key: key, id: id, conn: conn, lost: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	go l.watch()
	return l, nil
}

func (l *Lock) watch() {
	defer close(l.done)
	ticker := time.NewTicker(lockPing)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), lockPing)
			err := l.conn.Ping(ctx)
			cancel()
			if err != nil {
				slog.Error("writer lock connection lost", "key", l.key, "error", err)
				close(l.lost)
				l.conn.Close(context.Background())
				return
			}
		}
	}
}

// Lost is closed once the lock connection failed.
func (l *Lock) Lost() <-chan struct{} { return l.lost }

// Release unlocks and closes the connection.
func (l *Lock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	close(l.stop)
	<-l.done
	select {
	case <-l.lost:
		return // the connection is gone and the lock with it
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", l.id)
	l.conn.Close(ctx)
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
