package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

type ownedRedirectTransport struct {
	targets map[string]*url.URL
	base    *http.Transport
}

func (t *ownedRedirectTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	target, exists := t.targets[strings.ToLower(request.URL.Hostname())]
	if !exists {
		return nil, fmt.Errorf("undeclared owned redirect destination")
	}
	copy := request.Clone(request.Context())
	physical := *request.URL
	physical.Scheme, physical.Host = target.Scheme, target.Host
	copy.URL = &physical
	copy.Host = request.URL.Host
	return t.base.RoundTrip(copy)
}

func redirectClient(t *testing.T, targets map[string]*httptest.Server) (*Client, *budget.MemoryLedger) {
	t.Helper()
	routes := make(map[string]*url.URL)
	for host, server := range targets {
		target, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		routes[host] = target
	}
	base := &http.Transport{}
	t.Cleanup(base.CloseIdleConnections)
	ledger := budget.NewMemoryLedger(time.Now)
	gate := budget.NewGate(ledger, "owned-redirect-test", fixturePolicies(t))
	gate.Interval = func(string) time.Duration { return 0 }
	gate.InFlight = func(string) int { return 1 }
	client := NewClient("TankV0/owned-redirect-test", gate, time.Now)
	client.HTTP = &http.Client{Transport: &ownedRedirectTransport{targets: routes, base: base}, Timeout: time.Second}
	return client, ledger
}

func writeOwnedResponse(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if _, err := io.WriteString(w, body); err != nil {
		t.Error(err)
	}
}

func TestSameHostRedirectsReleaseSingleSlotAndChargeEveryHop(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		number := calls.Add(1)
		if number < 3 {
			w.Header().Set("Location", "/api/v3/depth?limit=100")
			w.WriteHeader(http.StatusFound)
			return
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer server.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": server})
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=100", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 15 {
		t.Fatalf("physical hops escaped admission:calls=%d", calls.Load())
	}
}

func TestConfiguredCrossHostRedirectsStripCredentialsThroughLaterSameHostHops(t *testing.T) {
	var destinationCalls atomic.Int32
	var leaked atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		number := destinationCalls.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Private-Token") != "" || r.Header.Get("Referer") != "" {
			leaked.Store(true)
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Error("safe negotiation header lost")
		}
		if number == 1 {
			w.Header().Set("Location", "/fapi/v1/exchangeInfo?step=2")
			w.WriteHeader(http.StatusFound)
			return
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer destination.Close()
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer FAKE-origin" {
			t.Error("original authority lost its declared credential")
		}
		w.Header().Set("Location", BinanceUSDM+"/fapi/v1/exchangeInfo?step=1")
		w.WriteHeader(http.StatusFound)
	}))
	defer origin.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": origin, "fapi.binance.com": destination})
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	source, err := url.Parse(Binance)
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(source, []*http.Cookie{{Name: "session", Value: "FAKE-jar", Domain: ".binance.com"}})
	client.HTTP.Jar = jar
	headers := map[string]string{"Authorization": "Bearer FAKE-origin", "Cookie": "session=FAKE-header", "X-Private-Token": "FAKE-token", "Accept": "application/json"}
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/exchangeInfo?apikey=FAKE-query", nil, headers, nil, 0); err != nil {
		t.Fatal(err)
	}
	if originCalls.Load() != 1 || destinationCalls.Load() != 2 || leaked.Load() {
		t.Fatalf("authority boundary violated:origin=%d destination=%d leaked=%v", originCalls.Load(), destinationCalls.Load(), leaked.Load())
	}
	if ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 20 || ledger.Used(budget.BucketName("binance.usdm", 60, 1200), 60) != 2 {
		t.Fatal("redirected configured hosts were not independently debited")
	}
}

func TestUnknownRedirectTargetRefusesBeforeDestinationEffects(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1); writeOwnedResponse(t, w, "{}") }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://unconfigured.invalid/api/v3/depth")
		w.WriteHeader(http.StatusFound)
	}))
	defer origin.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": origin, "unconfigured.invalid": destination})
	if result, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/exchangeInfo", nil, nil, nil, 0); err == nil || result != nil {
		t.Fatalf("unknown redirect admitted:result=%+v err=%v", result, err)
	}
	if destinationCalls.Load() != 0 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 20 {
		t.Fatal("unconfigured destination crossed the transport boundary")
	}
}

func TestSameAuthority307ReplaysTheExactAdmittedBody(t *testing.T) {
	payload := []byte("{\"type\":\"meta\",\"fixture\":\"FAKE-public\"}")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if r.Method != "POST" || !bytes.Equal(body, payload) {
			t.Error("redirect changed the admitted method/body")
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Location", "/info?step=2")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer server.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.hyperliquid.xyz": server})
	if _, err := client.Fetch(context.Background(), "POST", Hyperliquid, nil, nil, payload, 0); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || ledger.Used(budget.BucketName("hyperliquid", 60, 600), 60) != 40 {
		t.Fatal("replayed physical body escaped weight admission")
	}
}

func TestRedirectedRateLimitsObserveAndBlockOnlyActualDestination(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusTeapot} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var originCalls, destinationCalls atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				destinationCalls.Add(1)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
			}))
			defer destination.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originCalls.Add(1)
				w.Header().Set("Location", Bybit+"/v5/market/time")
				w.WriteHeader(http.StatusFound)
			}))
			defer origin.Close()
			client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": origin, "api.bybit.com": destination})
			target, err := url.Parse(Binance + "/api/v3/exchangeInfo")
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			buckets, err := client.Gate.PlanFetch("GET", "api.binance.com", target.Path, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Gate.AdmitBuckets(ctx, "api.binance.com", buckets, 0); err != nil {
				t.Fatal(err)
			}
			release, err := client.Gate.Enter(ctx, "api.binance.com", 0)
			if err != nil {
				t.Fatal(err)
			}
			initial := initialAdmission{buckets: buckets, lease: &hostLease{release: release}}
			defer initial.lease.close()
			result, err := client.raw(ctx, "GET", target, nil, nil, nil, 0, initial)
			if status == http.StatusTeapot {
				var banned *budget.BannedError
				if !errors.As(err, &banned) || banned.Host != "api.bybit.com" {
					t.Fatalf("actual destination ban attribution lost:%v", err)
				}
			} else if err != nil || !result.retry || result.Status != status {
				t.Fatalf("actual destination response observation lost:result=%+v err=%v", result, err)
			}
			if _, blocked := ledger.Blocked("api.binance.com"); blocked {
				t.Fatal("redirected response blocked original authority")
			}
			if until, blocked := ledger.Blocked("api.bybit.com"); !blocked || !until.After(time.Now()) {
				t.Fatal("redirected response did not persist destination block")
			}
			if originCalls.Load() != 1 || destinationCalls.Load() != 1 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 20 || ledger.Used(budget.BucketName("bybit", 5, 300), 5) != 1 {
				t.Fatal("actual destination debit/lifetime was not independent")
			}
		})
	}
}

func Test303DropsBodyAndAdmitsTheNewConfiguredMethod(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if r.Method != "GET" || len(data) != 0 || r.Header.Get("Authorization") != "" {
			t.Error("303 delegated original method/body/credential")
		}
		writeOwnedResponse(t, w, "{}")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", Binance+"/api/v3/exchangeInfo")
		w.WriteHeader(http.StatusSeeOther)
	}))
	defer origin.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.hyperliquid.xyz": origin, "api.binance.com": target})
	if _, err := client.Fetch(context.Background(), "POST", Hyperliquid, nil, map[string]string{"Authorization": "Bearer FAKE"}, []byte("{\"type\":\"meta\"}"), 0); err != nil {
		t.Fatal(err)
	}
	if targetCalls.Load() != 1 || ledger.Used(budget.BucketName("hyperliquid", 60, 600), 60) != 20 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 20 {
		t.Fatal("303 target method escaped its own policy")
	}
}

func TestUnsafeRedirectSemanticsRefuseBeforeTargetEffects(t *testing.T) {
	for _, test := range []struct {
		name, initial, target, method, body string
		status                              int
	}{
		{"security downgrade", Binance + "/api/v3/exchangeInfo", "http://fapi.binance.com/fapi/v1/exchangeInfo", "GET", "", http.StatusFound},
		{"introduced credentials", Binance + "/api/v3/exchangeInfo", "https://FAKE:FAKE@fapi.binance.com/fapi/v1/exchangeInfo", "GET", "", http.StatusFound},
		{"unconfigured target path", Binance + "/api/v3/exchangeInfo", BinanceUSDM + "/unconfigured-path", "GET", "", http.StatusFound},
		{"opaque cross-authority body", "https://www.binance.com/cms", "https://data.binance.vision/archive", "POST", "{\"fixture\":\"FAKE\"}", http.StatusTemporaryRedirect},
	} {
		t.Run(test.name, func(t *testing.T) {
			var destinationCalls atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1); writeOwnedResponse(t, w, "{}") }))
			defer destination.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", test.target)
				w.WriteHeader(test.status)
			}))
			defer origin.Close()
			initialURL, err := url.Parse(test.initial)
			if err != nil {
				t.Fatal(err)
			}
			targetURL, err := url.Parse(test.target)
			if err != nil {
				t.Fatal(err)
			}
			client, _ := redirectClient(t, map[string]*httptest.Server{initialURL.Hostname(): origin, targetURL.Hostname(): destination})
			if result, err := client.Fetch(context.Background(), test.method, test.initial, nil, nil, []byte(test.body), 0); err == nil || result != nil {
				t.Fatalf("unsafe redirect admitted:result=%+v err=%v", result, err)
			}
			if destinationCalls.Load() != 0 {
				t.Fatal("rejected redirect reached destination")
			}
		})
	}
}

func TestRedirectLimitReleasesTheSingleHostLease(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("done") == "1" {
			writeOwnedResponse(t, w, "{}")
			return
		}
		w.Header().Set("Location", "/api/v3/depth?limit=100")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": server})
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=100", nil, nil, nil, 0); err == nil {
		t.Fatal("unbounded redirect cycle accepted")
	}
	if calls.Load() != 10 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 50 {
		t.Fatalf("redirect bound hid physical attempts:%d", calls.Load())
	}
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=100&done=1", nil, nil, nil, 0); err != nil {
		t.Fatalf("redirect failure leaked host lease:%v", err)
	}
}

func TestExplicitLastResponsePolicyDoesNotReachRedirectTarget(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "/api/v3/depth?limit=100")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()
	client, ledger := redirectClient(t, map[string]*httptest.Server{"api.binance.com": server})
	client.HTTP.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	result, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/exchangeInfo", nil, nil, nil, 0)
	if err != nil || result == nil || result.Status != http.StatusFound {
		t.Fatalf("explicit last-response policy lost:result=%+v err=%v", result, err)
	}
	if calls.Load() != 1 || ledger.Used(budget.BucketName("binance.spot", 60, 3000), 60) != 20 {
		t.Fatal("last-response policy followed or double-charged a redirect")
	}
}
