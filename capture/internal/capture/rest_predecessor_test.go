package capture

import (
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
)

func normalizedPredecessorPrint(t *testing.T, r *restRig, id int64, offset time.Duration) *normalize.Trade {
	t.Helper()
	rows, err := r.s.normalizeREST(r.inst, []any{r.v.raw(fakeTrade{id: id, ts: r.base.Add(offset), buy: true})})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("Provider fixture did not normalize one actual print:%d", len(rows))
	}
	return rows[0]
}

func TestRESTPredecessorsPreserveOrderingAcrossReversePagesAndLateCandidates(t *testing.T) {
	for _, test := range []struct{ venue, want string }{{"coinbase", "20"}, {"okx", "10"}} {
		t.Run(test.venue, func(t *testing.T) {
			r := newRESTRig(t, test.venue)
			index := newPredecessorIndex(ConsecutiveIDs(r.inst))
			later := normalizedPredecessorPrint(t, r, 10, 2*time.Minute)
			earlier := normalizedPredecessorPrint(t, r, 20, 0)
			index.add(later)
			index.add(earlier)
			if got := index.before(r.base.Unix()); got != nil {
				t.Fatalf("Index fabricated a predecessor:%+v", got)
			}
			if got := index.before(r.base.Add(2 * time.Minute).Unix()); got == nil || got.TradeID != "20" {
				t.Fatalf("Current/future minute leaked into predecessor:%+v", got)
			}
			if got := index.before(r.base.Add(3 * time.Minute).Unix()); got == nil || got.TradeID != test.want {
				t.Fatalf("Existing ID/time ordering changed:%+v", got)
			}
			late := normalizedPredecessorPrint(t, r, 30, time.Minute)
			index.add(late)
			want := "30"
			if test.venue == "okx" {
				want = "10"
			}
			if got := index.before(r.base.Add(3 * time.Minute).Unix()); got == nil || got.TradeID != want {
				t.Fatalf("Prefix cache ignored a newly observed earlier minute:%+v", got)
			}
			if got := index.before(r.base.Add(time.Minute).Unix()); got == nil || got.TradeID != "20" {
				t.Fatalf("Exact boundary included the newly observed minute:%+v", got)
			}
		})
	}
}

func TestRESTBuilderCarriesHeldAndOutsideRangeEvidenceIntoEmptyMinutes(t *testing.T) {
	t.Run("held print", func(t *testing.T) {
		r := newRESTRig(t, "coinbase")
		held := normalizedPredecessorPrint(t, r, 1, 20*time.Second)
		r.s.seal.Trade(r.inst, held, 0)
		flow := newRESTFlow(r.s, r.inst, r.base, r.base.Add(2*time.Minute))
		flow.build(r.base.Unix())
		flow.build(r.base.Add(time.Minute).Unix())
		empty := flow.built[r.base.Add(time.Minute).Unix()].bar
		if empty.NBuy != 0 || empty.NSell != 0 || empty.Close != nil || empty.PriorTrade == nil || empty.PriorTrade.Price != "10.5" || empty.PriorTrade.TradeID != "1" {
			t.Fatalf("Empty minute lost actual held-price evidence:%+v", empty)
		}
		if got := flow.previousOf(r.base.Add(time.Minute)); got == nil || got.TradeID != "1" {
			t.Fatalf("Held print was not indexed as predecessor:%+v", got)
		}
	})
	t.Run("outside-range print", func(t *testing.T) {
		r := newRESTRig(t, "coinbase")
		prior := normalizedPredecessorPrint(t, r, 1, -20*time.Second)
		flow := newRESTFlow(r.s, r.inst, r.base, r.base.Add(time.Minute))
		flow.add(prior)
		flow.build(r.base.Unix())
		empty := flow.built[r.base.Unix()].bar
		if empty.Close != nil || empty.PriorTrade == nil || empty.PriorTrade.Price != "10.5" || empty.PriorTrade.TradeID != "1" {
			t.Fatalf("Outside-range predecessor was discarded:%+v", empty)
		}
	})
}
