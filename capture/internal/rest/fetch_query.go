package rest

import (
	"fmt"
	"net/url"
)

func effectiveFetchParams(target *url.URL, params map[string]string, fields []string) (map[string]string, error) {
	query, err := url.ParseQuery(target.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("invalid fetch query: %w", err)
	}
	for key, value := range params {
		query.Set(key, value)
	}
	required := make(map[string]bool)
	for _, key := range fields {
		required[key] = true
	}
	effective := make(map[string]string, len(query))
	for key, values := range query {
		if len(values) != 1 {
			if required[key] {
				return nil, fmt.Errorf("ambiguous fetch query parameter: %s", key)
			}
			continue
		}
		effective[key] = values[0]
	}
	return effective, nil
}
