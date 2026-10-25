package rest

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
)

func TestFetchChargesActualURLQueryAndOverride(t *testing.T) {
	for _, test := range []struct {
		name   string
		params map[string]string
		weight float64
	}{
		{name: "url", weight: 250},
		{name: "explicit override", params: map[string]string{"limit": "100"}, weight: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger := budget.NewMemoryLedger(time.Now)
			gate := budget.NewGate(ledger, "test", fixturePolicies(t))
			gate.Interval = func(string) time.Duration { return 0 }
			client := NewClient("TankV0/test", gate, time.Now)
			exchange := &exchange{responses: []canned{ok("{}")}}
			client.HTTP = &http.Client{Transport: exchange}
			if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=5000", test.params, nil, nil, 0); err != nil {
				t.Fatal(err)
			}
			if used := ledger.Used(budget.BucketName("binance.spot", 60), 60); used != test.weight {
				t.Fatalf("actual outgoing query charged %v, expected %v", used, test.weight)
			}
		})
	}
}

func TestAmbiguousFetchQueryCannotSpendOrSend(t *testing.T) {
	ledger := budget.NewMemoryLedger(time.Now)
	client := NewClient("TankV0/test", budget.NewGate(ledger, "test", fixturePolicies(t)), time.Now)
	exchange := &exchange{responses: []canned{ok("{}")}}
	client.HTTP = &http.Client{Transport: exchange}
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=100&limit=5000", nil, nil, nil, 0); err == nil {
		t.Fatal("ambiguous request accepted")
	}
	if len(exchange.requests) != 0 || ledger.Used(budget.BucketName("binance.spot", 60), 60) != 0 {
		t.Fatal("ambiguous query crossed admission boundary")
	}
}

func TestFetchPreservesRepeatedNonBudgetQueryFields(t *testing.T) {
	for _, rawURL := range []string{
		"https://www.binance.com/announcement?tag=first&tag=second",
		Binance + "/api/v3/depth?limit=100&filter=first&filter=second",
	} {
		t.Run(rawURL, func(t *testing.T) {
			client, _, _ := newClient(t)
			transport := &queryTransport{}
			client.HTTP = &http.Client{Transport: transport}
			if _, err := client.Fetch(context.Background(), "GET", rawURL, nil, nil, nil, 0); err != nil {
				t.Fatal(err)
			}
			if transport.rawURL != rawURL {
				t.Fatalf("non-budget query multiplicity changed: %s", transport.rawURL)
			}
		})
	}
}

func TestFetchOverrideResolvesRepeatedBudgetFieldBeforeAdmission(t *testing.T) {
	client, _, ledger := newClient(t)
	transport := &queryTransport{}
	client.HTTP = &http.Client{Transport: transport}
	if _, err := client.Fetch(context.Background(), "GET", Binance+"/api/v3/depth?limit=100&limit=5000", map[string]string{"limit": "100"}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if used := ledger.Used(budget.BucketName("binance.spot", 60), 60); used != 5 {
		t.Fatalf("resolved override charged %v", used)
	}
	if transport.rawURL != Binance+"/api/v3/depth?limit=100" {
		t.Fatalf("override did not match admitted query: %s", transport.rawURL)
	}
}

type queryTransport struct {
	rawURL string
}

func (t *queryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.rawURL = request.URL.String()
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}
