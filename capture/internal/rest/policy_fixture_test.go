package rest

import (
	"path/filepath"
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/budget"
	"github.com/freQuensy23-coder/Tank/capture/internal/config"
	"github.com/freQuensy23-coder/Tank/capture/internal/testutil"
)

func fixturePolicies(t *testing.T) budget.Policies {
	t.Helper()
	policies, err := config.LoadRequestPolicies(filepath.Join(testutil.Root(), "..", "config", "api_limits.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return policies
}
