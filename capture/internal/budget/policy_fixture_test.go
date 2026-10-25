package budget

import (
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/policyfixture"
)

func fixturePolicies(t *testing.T) Policies {
	t.Helper()
	input, err := policyfixture.Load("testdata/policy-input.json")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := NewPolicies(input)
	if err != nil {
		t.Fatal(err)
	}
	return policies
}
