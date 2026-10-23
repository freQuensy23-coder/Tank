package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

func repoConfig() string { return filepath.Join(testutil.Root(), "..", "config") }

func repoRoot() string { return filepath.Join(testutil.Root(), "..") }

func python(t *testing.T, script string, args ...string) string {
	t.Helper()
	out, err := exec.Command("python3", append([]string{"-c", script}, args...)...).Output()
	if err != nil {
		t.Skipf("python3 with PyYAML unavailable: %v", err)
	}
	return string(out)
}

func TestSafeLoadMatchesPyYAML(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(repoConfig(), "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no configuration files: %v", err)
	}
	for _, file := range files {
		want := python(t, `import json, sys, yaml
print(json.dumps(yaml.safe_load(open(sys.argv[1], "rb")), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str), end="")`, file)
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		value, err := SafeLoad(data)
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		if got := pyjson.Canonical(value); got != want {
			t.Errorf("%s differs from yaml.safe_load", filepath.Base(file))
		}
	}
}

func TestYAML11Resolution(t *testing.T) {
	doc := `
bools: [yes, No, on, OFF, true, y, n]
ints: [010, 0x1F, 0b101, 1_000, "1_000", 1:30, -0, +7, 08]
floats: [1.5, .5, 1e3, 1.0e+3, 6.8e-5, -.inf, 1_0.5, 1:30.5]
nulls: [~, null, Null, "", '']
empty:
base: &base {a: 1, b: 2}
merged:
  <<: *base
  b: 3
quoted: ["yes", '1.5', !!str 12, !!int "12"]
`
	value, err := SafeLoad([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	want := python(t, `import json, sys, yaml
print(json.dumps(yaml.safe_load(sys.argv[1]), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str), end="")`, doc)
	if got := pyjson.Canonical(withoutInf(value)); got != strings.ReplaceAll(want, "-Infinity", `"-inf"`) {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func withoutInf(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, item := range x {
			out[k] = withoutInf(item)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = withoutInf(item)
		}
		return out
	case float64:
		if x < -1e308 {
			return "-inf"
		}
	}
	return v
}

func TestLoadRepositoryConfiguration(t *testing.T) {
	c, err := Load(Settings{ConfigDir: repoConfig(), UserAgent: DefaultUserAgent})
	if err != nil {
		t.Fatal(err)
	}
	want := python(t, `import json, sys, yaml
r = yaml.safe_load(open(sys.argv[1] + "/registry.yaml"))
active = [i["instrument_id"] for i in r["instruments"] if i.get("active", True)]
ids = list(dict.fromkeys(active))
src = yaml.safe_load(open(sys.argv[1] + "/sources.yaml"))
print(json.dumps({"ids": ids, "tokens": list({t["token_id"]: t for t in r["tokens"]}),
	"metrics": len(yaml.safe_load(open(sys.argv[1] + "/semantics.yaml"))["metrics"]),
	"baskets": len(r.get("sector_baskets", [])), "contact": src.get("contact_email")}), end="")`, repoConfig())
	got := pyjson.Canonical(map[string]any{
		"ids": toAny(c.Order), "tokens": toAny(c.TokenOrder), "metrics": len(c.Semantics),
		"baskets": len(c.SectorBaskets), "contact": strings.TrimSuffix(strings.TrimPrefix(c.UserAgent, "TankV0/1.0 ("), ")"),
	})
	if got != canonical(t, want) {
		t.Errorf("Load differs from MarketService:\n got %s\nwant %s", got, want)
	}
	if len(c.Instruments) != len(c.Order) || len(c.Registry) != len(c.Order) {
		t.Fatal("instrument maps and order disagree")
	}
	btc := c.Instruments["binance:spot:BTCUSDT"]
	if btc == nil || btc.Key() != "binance.spot" || btc.Symbol != "BTCUSDT" || btc.Quote() != "USDT" {
		t.Fatalf("binance spot BTC: %+v", btc)
	}
	if c.Threshold("base_kline_interval_s", -1) <= 0 || c.Threshold("no_such_threshold", 7) != 7 {
		t.Error("thresholds")
	}
	if len(c.APILimits) == 0 {
		t.Error("api limits")
	}
	if c.LockKey() != "tank:capture:writer" {
		t.Error("the service writes mkt under the single-writer lock")
	}
}

func toAny(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func canonical(t *testing.T, text string) string {
	value, err := SafeLoad([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return pyjson.Canonical(value)
}

func TestThresholdForms(t *testing.T) {
	c := &Config{Thresholds: map[string]any{"i": 60, "f": 0.5, "s": "12.5", "n": nil, "b": true}}
	for name, want := range map[string]float64{"i": 60, "f": 0.5, "s": 12.5, "n": 3, "b": 1, "missing": 3} {
		if got := c.Threshold(name, 3); got != want {
			t.Errorf("%s: %v, want %v", name, got, want)
		}
	}
}

func TestInactiveInstrumentsAreSkipped(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "registry.yaml", `tokens: [{token_id: "a:A"}]
instruments:
- {instrument_id: one, token_id: "a:A", venue: binance, market_type: SPOT, venue_symbol: AUSDT}
- {instrument_id: two, token_id: "a:A", venue: okx, market_type: SPOT, venue_symbol: A-USDT, active: false}
- {instrument_id: three, token_id: "a:A", venue: okx, market_type: SPOT, venue_symbol: A-USDC, active: null}
- {instrument_id: four, token_id: "a:A", venue: okx, market_type: PERP_LINEAR, venue_symbol: A-USDT-SWAP, funding_interval_h: 8}
`)
	write(t, dir, "semantics.yaml", "metrics: []\n")
	c, err := Load(Settings{ConfigDir: dir, UserAgent: DefaultUserAgent})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Order, []string{"one", "four"}) {
		t.Fatalf("active instruments %v", c.Order)
	}
	if c.Instruments["four"].FundingIntervalH != 8 || c.UserAgent != DefaultUserAgent || len(c.Thresholds) != 0 {
		t.Fatalf("instrument values, user agent or thresholds: %+v %q", c.Instruments["four"], c.UserAgent)
	}
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSettingsFromEnvironmentAndSecret(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "registry.yaml", "tokens: []\n")
	t.Setenv("TANK_CONFIG_DIR", dir)
	t.Setenv("TANK_DATABASE_URL", "")
	t.Setenv("TANK_CAPTURE_MODE", "")
	t.Setenv("TANK_CAPTURE_HEALTH_ADDR", "")
	t.Setenv("TANK_DB_PASSWORD", "")
	if _, err := LoadSettings(); err == nil {
		t.Fatal("no URL and no password must fail")
	}
	t.Setenv("TANK_DB_PASSWORD", "  \n")
	if _, err := LoadSettings(); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("an empty password must fail: %v", err)
	}
	t.Setenv("TANK_DB_PASSWORD", "p@ss:w/rd +%é~_.-\n")
	s, err := LoadSettings()
	if err != nil {
		t.Fatal(err)
	}

	if s.DatabaseURL != "postgresql://tank:p%40ss%3Aw%2Frd%20%2B%25%C3%A9~_.-@postgres:5432/tank" {
		t.Fatalf("secret URL %q", s.DatabaseURL)
	}
	if s.HealthAddr != ":8791" || s.ConfigDir != dir || !strings.HasPrefix(s.CodeVersion, "tank-capture/") {
		t.Fatalf("defaults %+v", s)
	}
	if !strings.HasPrefix(s.ConfigVersion, "dev.") {
		t.Fatalf("a build without its commit names the config by content: %q", s.ConfigVersion)
	}
	t.Setenv("TANK_DATABASE_URL", "postgresql://u@h/db")
	if s, err = LoadSettings(); err != nil || s.DatabaseURL != "postgresql://u@h/db" {
		t.Fatalf("environment wins: %+v %v", s, err)
	}
}
