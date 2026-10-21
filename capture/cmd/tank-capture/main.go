// Command tank-capture is Tank's exchange capture service: the only process that talks
// to the exchanges for market data. It writes the typed market tables Tank core reads
// (capture/README.md).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/capture"
	"github.com/freQuensy23-coder/Tank/capture/internal/config"
	"github.com/freQuensy23-coder/Tank/capture/internal/rawstore"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
	"github.com/freQuensy23-coder/Tank/capture/internal/store"
)

// healthStale is how old the last successful flush may be for the service to be healthy.
const healthStale = 30 * time.Second

// shutdownLimit bounds a stop: the final flush has 20 s of it.
const shutdownLimit = 45 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(log); err != nil {
		log.Error("tank-capture stopped", "error", err)
		os.Exit(1)
	}
}

// healthcheck is the container healthcheck: GET /healthz on the local health address.
func healthcheck() int {
	addr := os.Getenv("TANK_CAPTURE_HEALTH_ADDR")
	if addr == "" {
		addr = ":8791"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	client := http.Client{Timeout: 4 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func run(log *slog.Logger) error {
	settings, err := config.LoadSettings()
	if err != nil {
		return err
	}
	cfg, err := config.Load(settings)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting", "schema", cfg.Schema(), "config_version", cfg.ConfigVersion,
		"code_version", cfg.CodeVersion, "instruments", len(cfg.Instruments))
	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.Schema())
	if err != nil {
		return err
	}
	defer st.Close()
	health := &healthState{}
	server := serveHealth(cfg.HealthAddr, health, log)
	defer server.Close()
	log.Info("waiting for the writer lock", "key", cfg.LockKey())
	lock, err := st.Lock(ctx, cfg.LockKey())
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer lock.Release()
	log.Info("writer lock held", "key", cfg.LockKey())
	owner := config.Writer
	clock := capture.NewSystemClock()
	gate := budget.NewGate(st.Ledger(), owner)
	client := rest.NewClient(cfg.UserAgent, gate, clock.Now)
	writer := store.NewWriter(st)
	svc := capture.New(capture.Options{
		Instruments: cfg.Instruments, Order: cfg.Order, Tokens: cfg.Tokens, Thresholds: cfg.Thresholds,
		ConfigVersion: cfg.ConfigVersion, CodeVersion: cfg.CodeVersion, Writer: config.Writer,
		CanWrite: lock.Held, Fetcher: client, Log: log,
		DataDir: cfg.DataDir,
		Raw:     rawstore.New(cfg.DataDir, cfg.CodeVersion, cfg.ConfigVersion, rawstore.Grace, log),
	}, st, writer, capture.RESTVenues(client), dial(cfg.UserAgent), clock)
	health.set(svc, lock)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	notifications, err := st.Listen(runCtx, "tank_capture")
	if err != nil {
		return err
	}
	go func() {
		for range notifications {
			svc.Wake()
		}
	}()
	go func() {
		select {
		case <-lock.Lost():
			log.Error("another writer holds the writer lock: stopping")
			cancel()
		case <-runCtx.Done():
		}
	}()
	go logReads(runCtx, client, log)
	go func() {
		// A stop that does not finish must not hold the writer lock: exit with the stacks.
		<-runCtx.Done()
		time.Sleep(shutdownLimit)
		buf := make([]byte, 1<<20)
		log.Error("shutdown exceeded its limit", "limit", shutdownLimit.String(), "stacks", string(buf[:runtime.Stack(buf, true)]))
		os.Exit(1)
	}()
	err = svc.Run(runCtx)
	select {
	case <-lock.Lost():
		return errors.New("another writer holds the writer lock")
	default:
	}
	return err
}

// dial opens public WebSocket connections (websockets.connect: 32 MiB frames).
func dial(userAgent string) capture.Dialer {
	dialer := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 30 * time.Second,
		ReadBufferSize: 1 << 16, WriteBufferSize: 1 << 14,
	}
	return func(ctx context.Context, url string) (capture.Conn, error) {
		header := http.Header{"User-Agent": []string{userAgent}}
		conn, resp, err := dialer.DialContext(ctx, url, header)
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			if resp != nil {
				return nil, fmt.Errorf("%s: handshake HTTP %d", strings.SplitN(url, "?", 2)[0], resp.StatusCode)
			}
			return nil, err
		}
		conn.SetReadLimit(32 << 20)
		return conn, nil
	}
}

// logReads logs the REST reads per host once a minute (MarketService's minute log).
func logReads(ctx context.Context, client *rest.Client, log *slog.Logger) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			reads := client.TakeReads()
			parts := make([]string, 0, len(reads))
			for host, counts := range reads {
				parts = append(parts, fmt.Sprintf("%s=%d/%d", host, counts[0], counts[1]))
			}
			if len(parts) > 0 {
				log.Info("rest reads", "hosts", strings.Join(parts, " "))
			}
		}
	}
}

type healthState struct {
	mu   sync.Mutex
	svc  *capture.Service
	lock *store.Lock
}

func (h *healthState) set(svc *capture.Service, lock *store.Lock) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.svc, h.lock = svc, lock
}

func (h *healthState) get() (*capture.Service, *store.Lock) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.svc, h.lock
}

// healthy: the writer lock is held and typed rows were written recently.
func (h *healthState) healthy() (bool, string) {
	svc, lock := h.get()
	if svc == nil || lock == nil {
		return false, "waiting for the writer lock"
	}
	select {
	case <-lock.Lost():
		return false, "another writer holds the writer lock"
	default:
	}
	if !lock.Held() {
		return false, "re-acquiring the writer lock"
	}
	last := svc.LastFlush()
	if last.IsZero() || time.Since(last) > healthStale {
		return false, "no recent market flush"
	}
	return true, "ok"
}

func serveHealth(addr string, h *healthState, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		ok, reason := h.healthy()
		if !ok {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, reason)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		ok, reason := h.healthy()
		status := map[string]any{"healthy": ok, "reason": reason}
		if svc, _ := h.get(); svc != nil {
			status["capture"] = svc.Status()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	// Heap and goroutine profiles (go tool pprof http://<container>:8791/debug/pprof/heap).
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server stopped", "error", err)
		}
	}()
	return server
}
