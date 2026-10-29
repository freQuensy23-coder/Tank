package rest

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestFetchWithManualRedirectsReturnsTheRedirect(t *testing.T) {
	c, ex, _ := newClient(t,
		canned{status: http.StatusFound, header: http.Header{"Location": {"http://evil.example/page"}}},
	)
	got, err := c.Fetch(WithManualRedirects(context.Background()), "GET", "https://www.okx.com/help/x", nil,
		map[string]string{"User-Agent": "Mozilla/5.0 test"}, nil, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != http.StatusFound || got.Location != "http://evil.example/page" {
		t.Fatalf("got %d %q", got.Status, got.Location)
	}
	if len(ex.requests) != 1 || ex.requests[0].UserAgent != "Mozilla/5.0 test" {
		t.Fatalf("requests %+v", ex.requests)
	}
}

func TestFetchFollowsRedirectsByDefault(t *testing.T) {
	c, ex, _ := newClient(t,
		canned{status: http.StatusFound, header: http.Header{"Location": {"https://www.okx.com/help/final"}}},
		ok("final"),
	)
	got, err := c.Fetch(context.Background(), "GET", "https://www.okx.com/help/x", nil, nil, nil, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != http.StatusOK || string(got.Body) != "final" || got.Location != "" || len(ex.requests) != 2 {
		t.Fatalf("got %d %q %q after %d requests", got.Status, got.Body, got.Location, len(ex.requests))
	}
}

func TestFetchReportsRateLimitWithRetryAfterWhenBudgetEnds(t *testing.T) {
	c, _, _ := newClient(t, canned{status: http.StatusTooManyRequests, header: http.Header{"Retry-After": {"30"}}})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := c.Fetch(ctx, "GET", "https://www.okx.com/help/x", nil, nil, nil, -1)
	var limited *RateLimitedError
	if !errors.As(err, &limited) || limited.RetryAfter != "30" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}
