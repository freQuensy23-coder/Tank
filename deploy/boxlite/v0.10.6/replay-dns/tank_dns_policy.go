package main

import (
    "net"
    "strings"
)

// An IP-only network policy has no hostname destinations to resolve. Forwarded
// DNS is therefore unnecessary egress. Empty/unrestricted and hostname policies
// keep upstream behavior; local zones and DHCP remain provided by the gateway.
func tankIPOnlyAllowlist(rules []string) bool {
    if len(rules) == 0 { return false }
    for _, raw := range rules {
        rule := strings.TrimSpace(raw)
        if net.ParseIP(rule) != nil { continue }
        if _, _, err := net.ParseCIDR(rule); err == nil { continue }
        return false
    }
    return true
}
