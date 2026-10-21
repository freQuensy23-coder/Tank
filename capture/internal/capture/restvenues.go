package capture

import (
	"context"

	"github.com/freQuensy23-coder/Tank/capture/internal/rest"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

// RESTVenues is the runtime's REST surface over internal/rest.
func RESTVenues(client *rest.Client) Venues { return restVenues{client} }

type restVenues struct{ client *rest.Client }

func (v restVenues) Venue(inst *venue.Instrument, priority int) Venue {
	return restAdapter{rest.NewAdapter(inst, v.client, priority)}
}

func (v restVenues) FXRate(ctx context.Context, quote string) (string, error) {
	return rest.FXRate(ctx, v.client, quote)
}

type restAdapter struct{ *rest.Adapter }

func (a restAdapter) Derivatives(ctx context.Context) (*Derivatives, error) {
	d, err := a.Adapter.Derivatives(ctx)
	if err != nil {
		return nil, err
	}
	return &Derivatives{OI: d.OI, Current: d.Current, Settled: d.Settled, Interval: d.Interval, Mark: d.Mark}, nil
}

func (a restAdapter) IndependentBook(ctx context.Context, stream map[string]any) (map[string]any, error) {
	return a.Adapter.IndependentBook(ctx, stream, nil)
}

// PollOnce is MarketService.poll_once(instrument_id) with every kind: listed metadata,
// the stablecoin USD rate, derivatives, klines and the 24h ticker.
func (s *Service) PollOnce(ctx context.Context, iid string) {
	s.poll(ctx, iid, pollKinds{derivatives: true, klines: true, ticker: true}, 0)
}
