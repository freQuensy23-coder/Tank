package rest

import (
	"fmt"
	"net/url"
)

func effectiveFetchParams(target *url.URL, params map[string]string) (map[string]string, error) {
	query, err := url.ParseQuery(target.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("invalid fetch query: %w", err)
	}
	for key, value := range params {
		query.Set(key, value)
	}
	effective := make(map[string]string, len(query))
	for key, values := range query {
		if len(values) != 1 {
			return nil, fmt.Errorf("ambiguous fetch query parameter: %s", key)
		}
		effective[key] = values[0]
	}
	return effective, nil
}
