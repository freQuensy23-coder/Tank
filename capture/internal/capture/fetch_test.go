package capture

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
)

type recordedFetch struct {
	calls    int
	method   string
	body     json.RawMessage
	response *rest.Fetched
}

func (f *recordedFetch) Fetch(_ context.Context, method, _ string, _, _ map[string]string, body json.RawMessage, _ int) (*rest.Fetched, error) {
	f.calls++
	f.method = method
	f.body = append(json.RawMessage(nil), body...)
	return f.response, nil
}

func TestFetchRejectsMalformedRequestsBeforeNetworkEffects(t *testing.T) {
	for _, input := range []map[string]any{
		{"url": "https://api.binance.com/data", "source_id": nil},
		{"method": "GET", "url": "https://api.binance.com/data"},
		{"method": "GET", "url": "https://api.binance.com/data", "source_id": 4},
		{"method": "GET", "url": "https://api.binance.com/data", "source_id": nil, "headers": map[string]any{"count": 1}},
		{"method": "GET", "url": "https://api.binance.com/data", "source_id": nil, "unknown": true},
	} {
		f := &recordedFetch{}
		s := &Service{opt: Options{Fetcher: f}}
		var status, reason string
		s.runFetch(context.Background(), map[string]any{"request": input}, time.Second, func(state string, why, _ any) {
			status = state
			reason, _ = why.(string)
		})
		if status != "FAILED" || reason != "FETCH_REQUEST_INVALID" || f.calls != 0 {
			t.Fatalf("malformed request reached network or lost explicit failure: status=%s reason=%s calls=%d input=%v", status, reason, f.calls, input)
		}
	}
}

func TestFetchPreservesBinaryAndEmptyTextWireBodies(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("hello"), {0xff, 0x00, 0x01}} {
		f := &recordedFetch{response: &rest.Fetched{Status: 200, ContentType: "application/octet-stream", Body: data}}
		s := &Service{opt: Options{Fetcher: f}, clock: &testClock{t: time.Date(2026, 10, 24, 1, 2, 3, 0, time.UTC)}}
		request := map[string]any{"method": "POST", "url": "https://api.binance.com/data", "source_id": nil, "body": map[string]any{"value": 0}}
		var status string
		var result FetchResult
		s.runFetch(context.Background(), map[string]any{"request": request}, time.Second, func(state string, why, value any) {
			status = state
			data, err := pyjson.Dumps(value, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			if why != nil {
				t.Fatalf("successful fetch returned failure: %v", why)
			}
		})
		if status != "COMPLETE" || f.calls != 1 || f.method != "POST" || string(f.body) != "{\"value\":0}" {
			t.Fatalf("request semantics changed: status=%s calls=%d method=%s body=%s", status, f.calls, f.method, f.body)
		}
		sum := sha256.Sum256(data)
		if result.SHA256 != hex.EncodeToString(sum[:]) || result.Bytes != int64(len(data)) || result.FetchedAt != "2026-10-24T01:02:03Z" {
			t.Fatalf("response provenance did not describe actual response bytes: %+v", result)
		}
		wire, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(wire, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["source_id"]) != "null" || (fields["body"] == nil) == (fields["body_b64"] == nil) {
			t.Fatalf("wire body must have exactly one representation and nullable source evidence: %s", wire)
		}
		if result.BodyB64 != nil && *result.BodyB64 != base64.StdEncoding.EncodeToString(data) {
			t.Fatalf("binary fetch bytes were not preserved: %s", wire)
		}
		if result.Body != nil && *result.Body != string(data) {
			t.Fatalf("text fetch bytes were not preserved: %s", wire)
		}
	}
}
