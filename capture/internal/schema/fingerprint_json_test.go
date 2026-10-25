package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestFingerprintDocumentBindsExactOpaqueNumericValues(t *testing.T) {
	input := json.RawMessage(`{"request":{"integer":123456789012345678901234567890,"decimal":0.123456789012345678901,"text":"0.123456789012345678901","nullable":null},"budget_s":"NaN","code_version":"excluded","started_at":"2026-10-25T00:00:00Z","started_at_prec":"COLLECTION_ONLY"}`)
	expected := `{"budget_s":"NaN","request":{"decimal":0.123456789012345678901,"integer":123456789012345678901234567890,"text":"0.123456789012345678901"},"started_at":"2026-10-25T00:00:00Z","started_at_prec":"COLLECTION_ONLY"}`
	sum := sha256.Sum256([]byte(expected))
	actual, err := FingerprintDocument(input, "mkt.capture_requests")
	if err != nil || actual != hex.EncodeToString(sum[:]) {
		t.Fatalf("Exact physical evidence fingerprint changed: %s, %v", actual, err)
	}
	rounded := json.RawMessage(`{"request":{"integer":123456789012345678901234567890,"decimal":0.12345678901234568,"text":"0.123456789012345678901"},"budget_s":"NaN","started_at":"2026-10-25T00:00:00Z","started_at_prec":"COLLECTION_ONLY"}`)
	other, err := FingerprintDocument(rounded, "mkt.capture_requests")
	if err != nil {
		t.Fatal(err)
	}
	if actual == other {
		t.Fatal("Distinct stored numeric evidence collapsed through floating-point rounding")
	}
}

func TestFingerprintDocumentRefusesNonRecordRoots(t *testing.T) {
	for _, raw := range []string{"null", "[]", "3", `"record"`, ""} {
		if _, err := FingerprintDocument(json.RawMessage(raw), "mkt.capture_requests"); err == nil {
			t.Fatalf("Non-record root accepted: %q", raw)
		}
	}
}
