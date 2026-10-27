package captureparams

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
)

type Parameters struct {
	headCheckDelay float64
	bookSave       float64
	hotPoll        float64
	hotTicker      float64
	quietSocket    float64
	klineAudit     float64
	baseOIFunding  float64
	baseTicker     float64
	baseKline      float64
	historicalDays int
}

func Defaults() Parameters {
	return Parameters{
		headCheckDelay: 45, bookSave: 8, hotPoll: 30, hotTicker: 20, quietSocket: 30,
		klineAudit: 3600, baseOIFunding: 300, baseTicker: 300, baseKline: 60, historicalDays: 90,
	}
}

func (p Parameters) HeadCheckDelay() time.Duration {
	return time.Duration(p.headCheckDelay * float64(time.Second))
}
func (p Parameters) BookSaveSeconds() float64  { return p.bookSave }
func (p Parameters) HotPollSeconds() float64   { return p.hotPoll }
func (p Parameters) HotTickerSeconds() float64 { return p.hotTicker }
func (p Parameters) QuietSocket() time.Duration {
	return time.Duration(p.quietSocket * float64(time.Second))
}
func (p Parameters) KlineAuditSeconds() float64 { return p.klineAudit }
func (p Parameters) BaseOIFunding() time.Duration {
	return time.Duration(p.baseOIFunding * float64(time.Second))
}
func (p Parameters) BaseTicker() time.Duration {
	return time.Duration(p.baseTicker * float64(time.Second))
}
func (p Parameters) BaseKline() time.Duration {
	return time.Duration(p.baseKline * float64(time.Second))
}
func (p Parameters) HistoricalDays() int { return p.historicalDays }

func validSeconds(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0 && value*float64(time.Second) < float64(math.MaxInt64) && value*float64(time.Second) >= 1
}

func (p Parameters) Validate() error {
	for _, value := range [...]float64{p.headCheckDelay, p.bookSave, p.hotPoll, p.hotTicker, p.quietSocket, p.klineAudit, p.baseOIFunding, p.baseTicker, p.baseKline} {
		if !validSeconds(value) {
			return errors.New("capture timing parameters require positive finite representable intervals")
		}
	}
	if p.historicalDays <= 0 || int64(p.historicalDays) > math.MaxInt64/int64(24*time.Hour) {
		return errors.New("capture historical lookback requires positive integral representable days")
	}
	return nil
}

func Decode(raw map[string]any) (Parameters, error) {
	p := Defaults()
	for _, field := range []struct {
		name   string
		target *float64
	}{
		{"head_check_delay_s", &p.headCheckDelay},
		{"book_save_interval_s", &p.bookSave},
		{"hot_poll_interval_s", &p.hotPoll},
		{"hot_ticker_interval_s", &p.hotTicker},
		{"quiet_socket_s", &p.quietSocket},
		{"kline_audit_interval_s", &p.klineAudit},
		{"base_oi_funding_interval_s", &p.baseOIFunding},
		{"base_ticker_interval_s", &p.baseTicker},
		{"base_kline_interval_s", &p.baseKline},
	} {
		value, exists := raw[field.name]
		if !exists {
			continue
		}
		d, err := pyval.Decimal(value)
		if err != nil || !validSeconds(d.Float64()) {
			return Parameters{}, fmt.Errorf("capture parameter %s requires a positive finite representable interval", field.name)
		}
		*field.target = d.Float64()
	}
	if value, exists := raw["historical_lookback_days"]; exists {
		d, err := pyval.Decimal(value)
		if err != nil {
			return Parameters{}, errors.New("capture historical lookback requires positive integral representable days")
		}
		days, exact := d.Int64()
		if !exact || !d.IsInteger() || days <= 0 || days > math.MaxInt64/int64(24*time.Hour) {
			return Parameters{}, errors.New("capture historical lookback requires positive integral representable days")
		}
		p.historicalDays = int(days)
	}
	return p, p.Validate()
}
