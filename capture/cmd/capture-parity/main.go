package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/freQuensy23-coder/Tank/capture/internal/config"
	"github.com/freQuensy23-coder/Tank/capture/internal/parity"
	"github.com/freQuensy23-coder/Tank/capture/internal/restparity"
)

func main() {
	var out *parity.Output
	var err error
	switch {
	case len(os.Args) == 5 && os.Args[1] == "rest" && os.Args[3] == "--api-policy":

		var p *restparity.Payload
		if p, err = restparity.Load(os.Args[2]); err == nil {
			policies, policyErr := config.LoadRequestPolicies(os.Args[4])
			if policyErr != nil {
				err = policyErr
			} else {
				out, err = restparity.Run(p, policies)
			}
		}
	case len(os.Args) == 2:
		var fx *parity.Fixture
		if fx, err = parity.Load(os.Args[1]); err == nil {
			out, err = parity.Run(fx)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: capture-parity FIXTURE.jsonl.gz | capture-parity rest FIXTURE.json.gz --api-policy API_LIMITS.yaml")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
