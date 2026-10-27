package capture

import (
	"testing"

	"github.com/freQuensy23-coder/Tank/capture/internal/captureparams"
)

func TestMissingParametersRefuseBeforeDependenciesAreUsed(t *testing.T) {
	service, err := New(Options{}, nil, nil, nil, nil, nil)
	if err == nil || service != nil {
		t.Fatal("missing capture parameters did not refuse before construction")
	}
}

func TestNamedParametersReachActualSealerAndBookPipeline(t *testing.T) {
	r := newRESTRig(t, "coinbase")
	parameters, err := captureparams.Decode(map[string]any{"head_check_delay_s": "42.5", "book_save_interval_s": 0.25})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Options{Parameters: parameters}, r.mem, r.mem, fakeVenues{r.v}, nil, r.clock)
	if err != nil {
		t.Fatal(err)
	}
	if service.seal.headDelay != parameters.HeadCheckDelay() || service.pipe.BookSaveS != 0.25 {
		t.Fatal("actual construction ignored admitted capture parameters")
	}
}
