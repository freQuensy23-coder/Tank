package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/policyfixture"
)

func nativeRetryClient(t *testing.T, server *httptest.Server, ledger budget.Ledger, policies budget.Policies) *Client {
	t.Helper()
	return nativeAddressClient(t, server.Listener.Addr().String(), server.Certificate(), ledger, policies)
}

func nativeAddressClient(t *testing.T, address string, certificate *x509.Certificate, ledger budget.Ledger, policies budget.Policies) *Client {
	t.Helper()
	defaults, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatal("Probe requires the production-default native transport")
	}
	transport := defaults.Clone()
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	owned, err := url.Parse("https://" + address)
	if err != nil {
		t.Fatal(err)
	}
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: owned.Hostname()}
	transport.Proxy = nil
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, target string) (net.Conn, error) {
		if target != "www.binance.com:443" {
			return nil, &net.AddrError{Err: "unexpected probe authority", Addr: target}
		}
		return dialer.DialContext(ctx, network, address)
	}
	t.Cleanup(transport.CloseIdleConnections)
	gate := budget.NewGate(ledger, "native-retry-probe", policies)
	gate.Interval = func(string) time.Duration { return 0 }
	gate.InFlight = func(string) int { return 1 }
	client := NewClient("TankV0/native-retry-probe", gate, time.Now)
	client.HTTP = &http.Client{Transport: transport, Timeout: 2 * time.Second}
	return client
}

func TestDefaultTransportReplaysAreIndividuallyAdmitted(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	remotes := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		number := calls.Add(1)
		mu.Lock()
		remotes = append(remotes, r.RemoteAddr)
		mu.Unlock()
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		if err := r.Body.Close(); err != nil {
			t.Error(err)
		}
		if number == 2 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("Native HTTP/1 probe does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
			return
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer server.Close()
	ledger := budget.NewMemoryLedger(time.Now)
	client := nativeRetryClient(t, server, ledger, fixturePolicies(t))
	var reused atomic.Int32
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused.Add(1)
			}
		},
	})
	if _, err := client.Fetch(ctx, "GET", "https://www.binance.com/warm", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	client.TakeReads()
	if _, err := client.Fetch(ctx, "GET", "https://www.binance.com/replayed", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	reads := client.TakeReads()["www.binance.com"]
	if calls.Load() != 3 || reused.Load() != 1 || reads != [2]int{1, 0} {
		t.Fatalf("Probe did not exercise hidden native retry:calls=%d reused=%d logicalReads=%v", calls.Load(), reused.Load(), reads)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 3 || remotes[0] != remotes[1] || remotes[1] == remotes[2] {
		t.Fatalf("Warm reuse and native redial were not preserved:%v", remotes)
	}
	used := ledger.Used(budget.BucketName("binance.www", 60), 60)
	t.Logf("native physical attempts=%d shared debits=%v logical replay call reads=%v", calls.Load(), used, reads)
	if used != float64(calls.Load()) {
		t.Fatalf("Production-default native replay escaped admission:physical=%d debits=%v", calls.Load(), used)
	}
}

type nativeQuotaClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *nativeQuotaClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *nativeQuotaClock) advance(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(delay)
}

func retryPolicies(t *testing.T, capacity float64) budget.Policies {
	t.Helper()
	input, err := policyfixture.Load("../budget/testdata/policy-input.json")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for index := range input.Public {
		if input.Public[index].Host == "www.binance.com" {
			input.Public[index].Capacity = capacity
			found = true
		}
	}
	if !found {
		t.Fatal("Probe has no configured public-host policy")
	}
	policies, err := budget.NewPolicies(input)
	if err != nil {
		t.Fatal(err)
	}
	return policies
}

func TestDefaultTransportReplayWaitsBeforeDestinationEffectsAndReleasesSlot(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 2 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("Native HTTP/1 probe cannot hijack")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
			return
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer server.Close()
	clock := &nativeQuotaClock{t: time.Now().UTC()}
	ledger := budget.NewMemoryLedger(clock.now)
	waiting := make(chan float64, 1)
	client := nativeRetryClient(t, server, observedWaitingLedger{ledger, waiting}, retryPolicies(t, 2))
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/warm", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Fetch(ctx, "GET", "https://www.binance.com/replay", nil, nil, nil, 0)
		done <- err
	}()
	select {
	case <-waiting:
		cancel()
	case err := <-done:
		t.Fatalf("Native replay did not wait for admission:err=%v calls=%d", err, calls.Load())
	case <-time.After(time.Second):
		t.Fatal("Native replay did not reach its admission boundary")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Admission cancellation lost: %v", err)
	}
	if calls.Load() != 2 || ledger.Used(budget.BucketName("binance.www", 60), 60) != 2 {
		t.Fatalf("Unpaid replay reached server:calls=%d", calls.Load())
	}
	clock.advance(time.Minute)
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/after-cancel", nil, nil, nil, 0); err != nil {
		t.Fatalf("Replay leaked host slot:%v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("Subsequent admitted request did not reach server:%d", calls.Load())
	}
}

func TestDefaultHTTP2RefusedStreamReplayHasItsOwnAdmission(t *testing.T) {
	peer := newNativeH2Peer(t, false)
	ledger := budget.NewMemoryLedger(time.Now)
	client := nativeAddressClient(t, peer.listener.Addr().String(), peer.certificate, ledger, fixturePolicies(t))
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/warm", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	client.TakeReads()
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/replay", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	reads := client.TakeReads()["www.binance.com"]
	if peer.calls.Load() != 3 || peer.connections.Load() != 1 || reads != [2]int{1, 0} {
		t.Fatalf("Native HTTP/2 retry/pooling changed:calls=%d conns=%d reads=%v", peer.calls.Load(), peer.connections.Load(), reads)
	}
	if used := ledger.Used(budget.BucketName("binance.www", 60), 60); used != 3 {
		t.Fatalf("Refused-stream retry escaped shared admission:%v", used)
	}
}

func TestDefaultHTTP2CanceledReplayPreservesOtherStreamAndSharedConnection(t *testing.T) {
	peer := newNativeH2Peer(t, true)
	clock := &nativeQuotaClock{t: time.Now().UTC()}
	ledger := budget.NewMemoryLedger(clock.now)
	waiting := make(chan float64, 1)
	client := nativeAddressClient(t, peer.listener.Addr().String(), peer.certificate, observedWaitingLedger{ledger, waiting}, retryPolicies(t, 3))
	client.Gate.InFlight = func(string) int { return 3 }
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/warm", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	control := make(chan error, 1)
	go func() {
		_, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/hold", nil, nil, nil, 0)
		control <- err
	}()
	select {
	case <-peer.holdSeen:
	case <-time.After(time.Second):
		t.Fatal("Concurrent HTTP/2 stream never arrived")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Fetch(ctx, "GET", "https://www.binance.com/replay", nil, nil, nil, 0)
		done <- err
	}()
	select {
	case <-waiting:
		cancel()
	case err := <-done:
		t.Fatalf("Unpaid native retry did not wait:err=%v calls=%d", err, peer.calls.Load())
	case <-time.After(time.Second):
		t.Fatal("HTTP/2 retry did not reach admission")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Native replay cancellation lost:%v", err)
	}
	if peer.calls.Load() != 3 || peer.connections.Load() != 1 {
		t.Fatalf("Canceled stream escaped or closed shared connection:calls=%d conns=%d", peer.calls.Load(), peer.connections.Load())
	}
	peer.once.Do(func() { close(peer.releaseHold) })
	if err := <-control; err != nil {
		t.Fatalf("Unrelated admitted stream was canceled:%v", err)
	}
	clock.advance(time.Minute)
	if _, err := client.Fetch(context.Background(), "GET", "https://www.binance.com/after", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if peer.calls.Load() != 4 || peer.connections.Load() != 1 {
		t.Fatalf("HTTP/2 stream/host lease leaked:calls=%d conns=%d", peer.calls.Load(), peer.connections.Load())
	}
}
