// Package config is the part of tank.settings and the MarketService constructor the
// capture service needs: settings from the environment and Docker secrets, the versioned
// configuration (registry, semantics, thresholds, API limits) read as yaml.safe_load
// reads it, and the config and code versions every row carries.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pydec"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// DefaultUserAgent is Settings.user_agent before configured_values applies the contact.
const DefaultUserAgent = "TankV0/1.0 (operator contact not configured)"

// Settings are the process settings (TANK_* environment, Docker secrets).
type Settings struct {
	DatabaseURL   string
	ConfigDir     string
	Mode          string // "live" writes mkt.*, "shadow" writes mkt_shadow.*
	HealthAddr    string
	UserAgent     string
	ConfigVersion string
	CodeVersion   string
}

// Modes of the service.
const (
	Live   = "live"
	Shadow = "shadow"
)

func env(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return fallback
}

// escape is urllib.parse.quote(value, safe=""): everything but unreserved characters is
// percent-encoded, with upper-case hex.
func escape(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c == '.' || c == '-' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// databaseURL is TANK_DATABASE_URL, else the URL tank.settings builds from the
// tank_db_password Docker secret (the Compose role "tank" at postgres:5432).
func databaseURL() (string, error) {
	if url := env("TANK_DATABASE_URL", ""); url != "" {
		return url, nil
	}
	path := filepath.Join(env("TANK_SECRETS_DIR", "/run/secrets"), "tank_db_password")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", errors.New("TANK_DATABASE_URL is unset and no tank_db_password Docker secret is mounted")
	}
	if err != nil {
		return "", err
	}
	password := strings.TrimSpace(string(data))
	if password == "" {
		return "", errors.New("Docker secret tank_db_password is empty")
	}
	return "postgresql://tank:" + escape(password) + "@postgres:5432/tank", nil
}

// LoadSettings reads the settings and resolves the config and code versions.
func LoadSettings() (Settings, error) {
	s := Settings{
		ConfigDir:  env("TANK_CONFIG_DIR", "/config"),
		Mode:       env("TANK_CAPTURE_MODE", Shadow),
		HealthAddr: env("TANK_CAPTURE_HEALTH_ADDR", ":8791"),
		UserAgent:  DefaultUserAgent,
	}
	if s.Mode != Live && s.Mode != Shadow {
		return s, fmt.Errorf("TANK_CAPTURE_MODE must be %q or %q, not %q", Live, Shadow, s.Mode)
	}
	url, err := databaseURL()
	if err != nil {
		return s, err
	}
	s.DatabaseURL = url
	if s.ConfigVersion, err = ConfigVersion(s.ConfigDir); err != nil {
		return s, err
	}
	s.CodeVersion = CodeVersion()
	return s, nil
}

// Config is the loaded configuration the capture service runs with.
type Config struct {
	Settings
	Tokens        map[string]map[string]any // registry tokens by token_id
	TokenOrder    []string
	Instruments   map[string]*venue.Instrument // active instruments by id
	Order         []string                     // active instrument ids in registry order
	Registry      map[string]map[string]any    // the registry dict of each active instrument
	SectorBaskets []map[string]any
	Semantics     []map[string]any // semantics.yaml metrics
	Thresholds    map[string]any
	APILimits     []map[string]any // api_limits.yaml limits
}

// ReadYAML is Settings.load_config: a configuration file that must hold a mapping.
func ReadYAML(dir, name string) (map[string]any, error) {
	if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
		name += ".yaml"
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	value, err := SafeLoad(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Configuration must be a mapping: %s", name)
	}
	return m, nil
}

func mappings(v any, what string) ([]map[string]any, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list", what)
	}
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s must hold mappings", what)
		}
		out = append(out, m)
	}
	return out, nil
}

// Load reads the registry, semantics, thresholds, API limits and the sources contact as
// MarketService.__init__ and configured_values do.
func Load(s Settings) (*Config, error) {
	c := &Config{Settings: s, Tokens: map[string]map[string]any{}, Instruments: map[string]*venue.Instrument{}, Registry: map[string]map[string]any{}}
	registry, err := ReadYAML(s.ConfigDir, "registry")
	if err != nil {
		return nil, err
	}
	tokens, err := mappings(registry["tokens"], "registry tokens")
	if err != nil {
		return nil, err
	}
	for _, t := range tokens {
		id, _ := t["token_id"].(string)
		if id == "" {
			return nil, errors.New("registry token without token_id")
		}
		if _, seen := c.Tokens[id]; !seen {
			c.TokenOrder = append(c.TokenOrder, id)
		}
		c.Tokens[id] = t
	}
	instruments, err := mappings(registry["instruments"], "registry instruments")
	if err != nil {
		return nil, err
	}
	for _, r := range instruments {
		// r.get("active", True): a missing flag is active, an explicit null is not.
		if active, ok := r["active"]; ok && !pyval.Truthy(active) {
			continue
		}
		inst := venue.FromRegistry(r)
		if inst.ID == "" {
			return nil, errors.New("registry instrument without instrument_id")
		}
		if _, seen := c.Instruments[inst.ID]; !seen {
			c.Order = append(c.Order, inst.ID)
		}
		c.Instruments[inst.ID], c.Registry[inst.ID] = inst, r
	}
	if c.SectorBaskets, err = mappings(registry["sector_baskets"], "registry sector_baskets"); err != nil {
		return nil, err
	}
	semantics, err := ReadYAML(s.ConfigDir, "semantics")
	if err != nil {
		return nil, err
	}
	if c.Semantics, err = mappings(semantics["metrics"], "semantics metrics"); err != nil {
		return nil, err
	}
	if c.Thresholds, err = ReadYAML(s.ConfigDir, "thresholds"); errors.Is(err, os.ErrNotExist) {
		c.Thresholds = map[string]any{}
	} else if err != nil {
		return nil, err
	}
	if limits, err := ReadYAML(s.ConfigDir, "api_limits"); err == nil {
		if c.APILimits, err = mappings(limits["limits"], "api_limits limits"); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if sources, err := ReadYAML(s.ConfigDir, "sources"); err == nil {
		if contact := sources["contact_email"]; pyval.Truthy(contact) {
			c.UserAgent = "TankV0/1.0 (" + pyval.Str(contact) + ")"
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return c, nil
}

// Threshold is float(thresholds.get(name, def)); a missing or null value is def.
func (c *Config) Threshold(name string, def float64) float64 {
	v, ok := c.Thresholds[name]
	if !ok || v == nil {
		return def
	}
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return f
		}
	}
	if d, err := pydec.Parse(pyval.Str(v)); err == nil {
		return d.Float64()
	}
	return def
}

// Schema is the schema the service writes: mkt (live) or mkt_shadow (shadow).
func (c *Config) Schema() string {
	if c.Mode == Live {
		return "mkt"
	}
	return "mkt_shadow"
}

// LockKey is the advisory lock of the single writer of that schema. Core's in-process
// capture takes the live key too.
func (c *Config) LockKey() string {
	if c.Mode == Live {
		return "tank:capture:writer"
	}
	return "tank:capture:shadow"
}
