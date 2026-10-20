package main

import (
 "testing"
 "github.com/containers/gvisor-tap-vsock/pkg/types"
)

func TestTankReplayDNSPolicySelection(t *testing.T) {
    cases := []struct {name string; rules []string; disabled bool}{
        {"unrestricted",nil,false}, {"empty",[]string{},false},
        {"database",[]string{"172.29.0.2"},true},
        {"cidr",[]string{"172.29.0.0/24"},true},
        {"ip-and-cidr",[]string{"10.0.0.2","192.168.0.0/16"},true},
        {"hostname",[]string{"postgres"},false},
        {"mixed",[]string{"postgres","172.29.0.2"},false},
        {"wildcard",[]string{"*.example.com"},false},
        {"invalid",[]string{"172.29.0.2/99"},false},
        {"empty-entry",[]string{""},false},
    }
    for _, c := range cases {t.Run(c.name,func(t *testing.T){
        if actual:=tankIPOnlyAllowlist(c.rules);actual!=c.disabled {t.Fatalf("got %v want %v",actual,c.disabled)}
        config:=testGvproxyConfig();config.AllowNet=c.rules
        if actual:=buildTapConfig(config, types.QemuProtocol).DNSForwardingDisabled;actual!=c.disabled {t.Fatalf("native config lost DNS policy: got %v",actual)}
    })}
}
