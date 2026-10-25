package capture

import (
	"sort"

	"github.com/freQuensy23-coder/Tank/capture/internal/normalize"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type predecessorPoint struct {
	minute int64
	best   *normalize.Trade
}

type predecessorIndex struct {
	ids     bool
	minutes map[int64]*normalize.Trade
	prefix  []predecessorPoint
	dirty   bool
}

func newPredecessorIndex(ids bool) *predecessorIndex {
	return &predecessorIndex{ids: ids, minutes: make(map[int64]*normalize.Trade)}
}

func (p *predecessorIndex) add(trade *normalize.Trade) {
	minute := pytime.MinuteFloor(trade.TS).Unix()
	previous := p.minutes[minute]
	if previous == nil || lessPrint(previous, trade, p.ids) {
		p.minutes[minute] = trade
		p.dirty = true
	}
}

func (p *predecessorIndex) before(minute int64) *normalize.Trade {
	if p.dirty {
		keys := make([]int64, 0, len(p.minutes))
		for key := range p.minutes {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		prefix := make([]predecessorPoint, len(keys))
		var best *normalize.Trade
		for index, key := range keys {
			trade := p.minutes[key]
			if best == nil || lessPrint(best, trade, p.ids) {
				best = trade
			}
			prefix[index] = predecessorPoint{minute: key, best: best}
		}
		p.prefix = prefix
		p.dirty = false
	}
	position := sort.Search(len(p.prefix), func(index int) bool { return p.prefix[index].minute >= minute })
	if position == 0 {
		return nil
	}
	return p.prefix[position-1].best
}
