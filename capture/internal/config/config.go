package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/captureparams"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const DefaultUserAgent = "TankV0/1.0 (operator contact not configured)"

type Settings struct {
	DatabaseURL   string
	ConfigDir     string
	HealthAddr    string
	UserAgent     string
	ConfigVersion string
	CodeVersion   string
}

const Writer = "tank-capture"

func env(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return fallback
}

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

func databaseURL() (string, error) {
	if url := env("TANK_DATABASE_URL", ""); url != "" {
		return url, nil
	}
	password := strings.TrimSpace(os.Getenv("TANK_DB_PASSWORD"))
	if password == "" {
		return "", errors.New("TANK_DB_PASSWORD is unset or empty")
	}
	return "postgresql://tank:" + escape(password) + "@postgres:5432/tank", nil
}

func LoadSettings() (Settings, error) {
	s := Settings{
		ConfigDir:  env("TANK_CONFIG_DIR", "/config"),
		HealthAddr: env("TANK_CAPTURE_HEALTH_ADDR", ":8791"),
		UserAgent:  DefaultUserAgent,
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

type Config struct {
	Settings
	Tokens            map[string]map[string]any
	TokenOrder        []string
	Instruments       map[string]*venue.Instrument
	Order             []string
	Registry          map[string]map[string]any
	SectorBaskets     []map[string]any
	Semantics         []map[string]any
	CaptureParameters captureparams.Parameters
	APILimits         validatedBudgetDocument
}

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
	thresholds, err := ReadYAML(s.ConfigDir, "thresholds")
	if errors.Is(err, os.ErrNotExist) {
		thresholds = map[string]any{}
	} else if err != nil {
		return nil, err
	}
	if c.CaptureParameters, err = captureparams.Decode(thresholds); err != nil {
		return nil, err
	}
	limitBytes, err := os.ReadFile(filepath.Join(s.ConfigDir, "api_limits.yaml"))
	if err != nil {
		return nil, err
	}
	if c.APILimits, err = parseAPILimits(limitBytes); err != nil {
		return nil, fmt.Errorf("invalid configured API request budgets: %w", err)
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

func (c *Config) LockKey() string { return "tank:capture:writer" }
