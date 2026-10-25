package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
)

func FingerprintDocument(document json.RawMessage, collection string) (string, error) {
	if !Known(collection) || !json.Valid(document) || bytes.TrimSpace(document)[0] != '{' {
		return "", errors.New("fingerprint requires a known collection and valid JSON document")
	}
	value, err := semanticJSON(document, collection, true)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:]), nil
}

func skipSemanticKey(key, collection string, root bool) bool {
	if key == "code_version" || key == "config_version" || key == "semantic_fingerprint" {
		return true
	}
	for _, prefix := range strippedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return root && fingerprintExcluded[collection][key]
}

func semanticJSON(document json.RawMessage, collection string, root bool) ([]byte, error) {
	raw := bytes.TrimSpace(document)
	if len(raw) == 0 {
		return nil, errors.New("semantic JSON value is missing")
	}
	switch raw[0] {
	case '{':
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(object))
		for key, value := range object {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || skipSemanticKey(key, collection, root) {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var output bytes.Buffer
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			output.Write(pyjson.EncodeString(key, true))
			output.WriteByte(':')
			encoded, err := semanticJSON(object[key], collection, false)
			if err != nil {
				return nil, err
			}
			output.Write(encoded)
		}
		output.WriteByte('}')
		return output.Bytes(), nil
	case '[':
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		var output bytes.Buffer
		output.WriteByte('[')
		for index, value := range values {
			if index > 0 {
				output.WriteByte(',')
			}
			encoded, err := semanticJSON(value, collection, false)
			if err != nil {
				return nil, err
			}
			output.Write(encoded)
		}
		output.WriteByte(']')
		return output.Bytes(), nil
	case '"':
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, err
		}
		return pyjson.EncodeString(text, true), nil
	case 't', 'f', 'n':
		if bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false")) || bytes.Equal(raw, []byte("null")) {
			return raw, nil
		}
		return nil, errors.New("invalid semantic JSON literal")
	default:
		return pyjson.ExactJSONNumber(raw)
	}
}
