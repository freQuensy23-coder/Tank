package policyfixture

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/freQuensy23-coder/Tank/capture/internal/budgetpolicy"
)

func Load(path string) (budgetpolicy.PolicyInput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return budgetpolicy.PolicyInput{}, err
	}
	var envelope struct {
		SourceConfigSHA256 string
		Input              budgetpolicy.PolicyInput
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return budgetpolicy.PolicyInput{}, err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return budgetpolicy.PolicyInput{}, fmt.Errorf("policy fixture requires one JSON document")
	}
	digest, err := hex.DecodeString(envelope.SourceConfigSHA256)
	if err != nil || len(digest) != 32 {
		return budgetpolicy.PolicyInput{}, fmt.Errorf("policy fixture requires captured source digest")
	}
	return envelope.Input, nil
}
