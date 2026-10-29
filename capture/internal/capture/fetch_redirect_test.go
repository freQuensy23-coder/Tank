package capture

import (
	"context"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

type redirectFetcher struct {
	manual  []bool
	headers map[string]string
}

func (f *redirectFetcher) Fetch(ctx context.Context, _, _ string, _, headers map[string]string, _ any, _ int) (*rest.Fetched, error) {
	f.manual = append(f.manual, rest.ManualRedirects(ctx))
	f.headers = headers
	if rest.ManualRedirects(ctx) {
		return &rest.Fetched{Status: 302, Location: "https://www.okx.com/next"}, nil
	}
	return &rest.Fetched{Status: 200, ContentType: "text/html", Body: []byte("page")}, nil
}

func TestHTTPFetchManualRedirectsReturnLocation(t *testing.T) {
	fetcher := &redirectFetcher{}
	s := &Service{opt: Options{Fetcher: fetcher}, clock: &testClock{time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)}}
	results := []map[string]any{}
	finish := func(status string, _ any, result any) {
		if status != "COMPLETE" {
			t.Fatalf("status %s", status)
		}
		results = append(results, result.(map[string]any))
	}
	request := map[string]any{"url": "https://www.okx.com/help/x", "redirects": "manual", "headers": map[string]any{"User-Agent": "Mozilla/5.0 test"}}
	s.runFetch(context.Background(), map[string]any{"request": request}, 0, finish)
	s.runFetch(context.Background(), map[string]any{"request": map[string]any{"url": "https://www.okx.com/help/x"}}, 0, finish)
	if len(fetcher.manual) != 2 || !fetcher.manual[0] || fetcher.manual[1] {
		t.Fatalf("manual flags %v", fetcher.manual)
	}
	if results[0]["status_code"] != int64(302) || results[0]["location"] != "https://www.okx.com/next" {
		t.Fatalf("manual result %v", results[0])
	}
	if _, ok := results[1]["location"]; ok || results[1]["status_code"] != int64(200) {
		t.Fatalf("default result %v", results[1])
	}
}
