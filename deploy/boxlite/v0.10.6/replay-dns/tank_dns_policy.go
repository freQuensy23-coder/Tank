package main

import (
	"net"
	"strings"
)

func tankIPOnlyAllowlist(rules []string) bool {
	if len(rules) == 0 {
		return false
	}
	for _, raw := range rules {
		rule := strings.TrimSpace(raw)
		if net.ParseIP(rule) != nil {
			continue
		}
		if _, _, err := net.ParseCIDR(rule); err == nil {
			continue
		}
		return false
	}
	return true
}
