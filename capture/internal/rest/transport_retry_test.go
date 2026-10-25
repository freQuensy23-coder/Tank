package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
)

func nativeRetryClient(t *testing.T, server *httptest.Server, ledger budget.Ledger, policies budget.Policies) *Client {
	t.Helper()
	defaults, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatal("Probe requires the production-default native transport")
	}
	transport := defaults.Clone()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	owned, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: owned.Hostname()}
	transport.Proxy = nil
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "www.binance.com:443" {
			return nil, &net.AddrError{Err: "Unexpected probe authority", Addr: address}
		}
		return dialer.DialContext(ctx, network, server.Listener.Addr().String())
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
