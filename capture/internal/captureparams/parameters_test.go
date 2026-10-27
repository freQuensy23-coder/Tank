package captureparams

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestDefaultsRetainActualCapturePolicy(t *testing.T) {
	p, err := Decode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.HeadCheckDelay() != 45*time.Second || p.BookSaveSeconds() != 8 || p.HotPollSeconds() != 30 || p.HotTickerSeconds() != 20 || p.QuietSocket() != 30*time.Second || p.KlineAuditSeconds() != 3600 || p.BaseOIFunding() != 300*time.Second || p.BaseTicker() != 300*time.Second || p.BaseKline() != 60*time.Second || p.HistoricalDays() != 90 {
		t.Fatalf("unexpected omitted-override policy: %+v", p)
	}
}

func TestSuppliedCaptureIntervalsRejectMalformedOrUnrepresentableValues(t *testing.T) {
	for _, name := range []string{"head_check_delay_s", "book_save_interval_s", "hot_poll_interval_s", "hot_ticker_interval_s", "quiet_socket_s", "kline_audit_interval_s", "base_oi_funding_interval_s", "base_ticker_interval_s", "base_kline_interval_s"} {
		t.Run(name, func(t *testing.T) {
			for _, raw := range []any{nil, true, false, "invalid", math.NaN(), math.Inf(1), math.Inf(-1), 0, -1, "-0", "1e-20", "1e40", []any{}, map[string]any{}} {
				if _, err := Decode(map[string]any{name: raw}); err == nil {
					t.Fatalf("admitted invalid interval %T %v", raw, raw)
				}
			}
		})
	}
}

func TestDecimalIntervalOverridesPreserveExistingNumericForms(t *testing.T) {
	for _, raw := range []any{0.125, "0.125", json.Number("0.125")} {
		p, err := Decode(map[string]any{"base_kline_interval_s": raw, "historical_lookback_days": json.Number("14")})
		if err != nil {
			t.Fatal(err)
		}
		if p.BaseKline() != 125*time.Millisecond || p.HistoricalDays() != 14 {
			t.Fatalf("actual overrides lost: %+v", p)
		}
	}
}

func TestLookbackRejectsFractionalDaysAndDurationOverflow(t *testing.T) {
	for _, raw := range []any{nil, true, "invalid", 1.5, "1.5", 0, -1, json.Number("9000000000000000000")} {
		if _, err := Decode(map[string]any{"historical_lookback_days": raw}); err == nil {
			t.Fatalf("admitted invalid lookback %T %v", raw, raw)
		}
	}
}

func TestUnconsumedThresholdFactsRemainOutsideCaptureProjection(t *testing.T) {
	p, err := Decode(map[string]any{"filter_cutoff": nil, "routing_window_days": "unestimated", "base_retention_days": 17})
	if err != nil {
		t.Fatal(err)
	}
	if p != Defaults() {
		t.Fatal("unconsumed facts changed capture policy")
	}
}

func TestUninitializedParametersAreNotRunnableDefaults(t *testing.T) {
	if err := (Parameters{}).Validate(); err == nil {
		t.Fatal("zero initialized parameters admitted")
	}
}

func TestIntegralIntervalOverridesRetainActualNumbers(t *testing.T) {
	for _, raw := range []any{60, int64(60), json.Number("60"), "60"} {
		p, err := Decode(map[string]any{"base_kline_interval_s": raw})
		if err != nil {
			t.Fatal(err)
		}
		if p.BaseKline() != time.Minute {
			t.Fatalf("integral interval lost: %v", p.BaseKline())
		}
	}
}
