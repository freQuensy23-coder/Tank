package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguredAPIPolicyRequiresDocumentationAndDailyCoordinates(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseAPILimits(data); err != nil {
		t.Fatalf("actual configured policy rejected: %v", err)
	}
	for _, field := range []string{
		"  documented_requests_per_s: 10\n",
		"  documented_weight_per_min: 6000\n",
		"  tank_max_requests_per_day: 50000\n",
		"  documented_requests_per_day: 100000\n",
		"  documented_requests: null\n",
	} {
		t.Run(strings.TrimSpace(field), func(t *testing.T) {
			without := strings.Replace(string(data), field, "", 1)
			if without == string(data) {
				t.Fatal("omission recipe did not match current configuration")
			}
			if _, err := parseAPILimits([]byte(without)); err == nil {
				t.Fatal("incomplete configured policy accepted")
			}
		})
	}
}

func TestConfiguredAPIPolicyRejectsMalformedProviderBoundaries(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, before, after string }{
		{"cross-provider field", "- provider: sec.gov\n", "- provider: sec.gov\n  tier: FREE\n"},
		{"nonfinite rate", "tank_max_requests_per_s: 5", "tank_max_requests_per_s: .inf"},
		{"undocumented scope", "scope: IP_INSTRUMENT\n", "scope: GLOBAL\n"},
		{"invented client documentation", "documented_requests: null", "documented_requests: 1000"},
		{"overassigned rate", "tank_max_requests_per_s: 5", "tank_max_requests_per_s: 9"},
		{"invalid captured SHA", "source_content_sha256: 4f64", "source_content_sha256: zf64"},
		{"unknown provider", "provider: sec.gov", "provider: unknown.gov"},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := strings.Replace(string(data), test.before, test.after, 1)
			if malformed == string(data) {
				t.Fatal("malformed-policy recipe did not match current configuration")
			}
			if _, err := parseAPILimits([]byte(malformed)); err == nil {
				t.Fatal("malformed provider policy accepted")
			}
		})
	}
}

func TestChangedAssignedPolicySurvivesValidatedConfiguration(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoConfig(), "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(data), "tank_max_weight_per_min: 3000", "tank_max_weight_per_min: 2000", 1)
	policy, err := parseAPILimits([]byte(changed))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Spot == nil || policy.Spot.Assigned != 2000 {
		t.Fatal("validated decoder replaced the declared assigned capacity")
	}
}
