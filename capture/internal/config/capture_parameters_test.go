package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func captureConfiguration(t *testing.T, thresholds string) string {
	t.Helper()
	dir := t.TempDir()
	write(t, dir, "registry.yaml", "tokens: [{token_id: 'test:BTC', symbol: BTC}]\ninstruments: []\n")
	write(t, dir, "semantics.yaml", "metrics: []\n")
	policy, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "api_limits.yaml", string(policy))
	if thresholds != "" {
		write(t, dir, "thresholds.yaml", thresholds)
	}
	return dir
}

func TestCaptureConfigurationRefusesMalformedConsumedOverrides(t *testing.T) {
	for _, value := range []string{"null", "true", "not-a-duration", ".nan", ".inf"} {
		t.Run(value, func(t *testing.T) {
			dir := captureConfiguration(t, "base_kline_interval_s: "+value+"\n")
			if _, err := Load(Settings{ConfigDir: dir, UserAgent: DefaultUserAgent}); err == nil {
				t.Fatal("malformed consumed capture interval admitted as runnable configuration")
			}
		})
	}
}

func TestCaptureConfigurationPreservesDeclaredOverrideAbsence(t *testing.T) {
	dir := captureConfiguration(t, "")
	if _, err := Load(Settings{ConfigDir: dir, UserAgent: DefaultUserAgent}); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryCaptureParametersUseOnlyTheirActualDeclaredKeys(t *testing.T) {
	c, err := Load(Settings{ConfigDir: repoConfig(), UserAgent: DefaultUserAgent})
	if err != nil {
		t.Fatal(err)
	}
	p := c.CaptureParameters
	if p.BaseKline() != time.Minute || p.BaseOIFunding() != 300*time.Second || p.HotTickerSeconds() != 300 || p.QuietSocket() != 30*time.Second {
		t.Fatal("declared capture configuration was not projected")
	}
	if p.BookSaveSeconds() != 8 || p.HotPollSeconds() != 30 || p.HistoricalDays() != 90 {
		t.Fatal("unrelated configuration keys changed the actual omitted-override policy")
	}
}
