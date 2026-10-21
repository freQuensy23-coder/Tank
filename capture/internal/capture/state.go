package capture

import (
	"crypto/rand"
	"math/big"
	"strings"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func NewID(prefix string, at time.Time) string {
	prefix = strings.ToUpper(strings.TrimRight(prefix, "-"))
	ms := big.NewInt(at.UnixMilli())
	entropy, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 80))
	value := new(big.Int).Or(new(big.Int).Lsh(ms, 80), entropy)
	chars := make([]byte, 26)
	mask := big.NewInt(31)
	for i := 25; i >= 0; i-- {
		chars[i] = ulidAlphabet[new(big.Int).And(value, mask).Int64()]
		value.Rsh(value, 5)
	}
	return prefix + "-" + string(chars)
}

func (s *Service) gap(inst *venue.Instrument, kind, reason string, start, end *time.Time) {
	now := s.clock.Now()
	row := map[string]any{
		"gap_id": NewID("GAP", now), "instrument_id": inst.ID, "token_id": inst.TokenID,
		"kind": kind, "reason": reason, "status": "OPEN",
	}
	for field, value := range map[string]*time.Time{"start": start, "end": end} {
		if value != nil {
			for k, v := range pytime.Fields(field, pytime.ISO(*value), "") {
				row[k] = v
			}
		} else {
			for k, v := range pytime.Fields(field, pytime.ISO(now), pytime.CollectionOnly) {
				row[k] = v
			}
		}
	}
	pytime.CollectionClock(now).Into(row, "observed_at")
	s.points.Put("mkt.capture_gaps", row["gap_id"].(string), row)
}

type streamRow struct {
	inst      *venue.Instrument
	stream    string
	endpoint  string
	state     string
	connected time.Time
	message   time.Time
	watermark time.Time
	polls     []string
	detail    map[string]any
}

func tsFields(row map[string]any, field string, t time.Time, precision string) {
	if t.IsZero() {
		row[field] = nil
		return
	}
	for k, v := range pytime.Fields(field, pytime.ISO(t), precision) {
		row[k] = v
	}
}

func (s *Service) publishStreams() {
	now := s.clock.Now()
	for _, r := range s.streamRows() {
		row := map[string]any{
			"instrument_id": r.inst.ID, "token_id": r.inst.TokenID, "venue": r.inst.Venue,
			"market_type": r.inst.MarketType, "stream": r.stream, "endpoint": nilIfEmpty(r.endpoint),
			"state": r.state, "writer": s.opt.Writer,
		}

		detail := map[string]any{"heartbeat": pytime.ISO(now)}
		for k, v := range r.detail {
			detail[k] = v
		}
		row["detail"] = detail
		tsFields(row, "connected_since", r.connected, pytime.CollectionOnly)
		tsFields(row, "last_message_at", r.message, pytime.CollectionOnly)
		tsFields(row, "watermark_ts", r.watermark, pytime.Exact)
		tsFields(row, "updated_at", now, pytime.CollectionOnly)
		if r.stream == "WS" {
			if st, ok := s.seal.State(r.inst.ID); ok {
				if st.Last != nil {
					row["last_trade_id"] = st.Last.TradeID
					for k, v := range pytime.Fields("last_trade_ts", pytime.ISO(st.Last.TS), st.Last.TSPrec) {
						row[k] = v
					}
				}
				tsFields(row, "sealed_through", st.Sealed, pytime.Exact)
			}
		}
		if r.polls != nil {
			polls := make([]any, len(r.polls))
			for i, p := range r.polls {
				polls[i] = p
			}
			row["recent_polls"] = polls
		}
		s.points.Put("mkt.capture_streams", r.inst.ID+":"+r.stream, row)
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
