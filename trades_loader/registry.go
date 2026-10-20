package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Instrument is the subset of config/registry.yaml the loader needs.
type Instrument struct {
	ID         string `yaml:"instrument_id"`
	TokenID    string `yaml:"token_id"`
	Venue      string `yaml:"venue"`
	MarketType string `yaml:"market_type"`
	Symbol     string `yaml:"venue_symbol"`
	Active     bool   `yaml:"active"`
}

// loadInstruments returns the active instruments of the given venues and market types.
func loadInstruments(path string, venues, markets map[string]bool) ([]Instrument, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Instruments []Instrument `yaml:"instruments"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	var out []Instrument
	for _, in := range doc.Instruments {
		if !in.Active || !venues[strings.ToLower(in.Venue)] || !markets[strings.ToUpper(in.MarketType)] {
			continue
		}
		out = append(out, in)
	}
	return out, nil
}
