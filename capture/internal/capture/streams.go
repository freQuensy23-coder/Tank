package capture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/books"
	"github.com/freQuensy23-coder/Tank/capture/internal/pipeline"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyjson"
	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
	"github.com/freQuensy23-coder/Tank/capture/internal/pyval"
	"github.com/freQuensy23-coder/Tank/capture/internal/stream"
	"github.com/freQuensy23-coder/Tank/capture/internal/venue"
)

const (
	StreamPingInterval = 20 * time.Second

	StreamPingTimeout = 60 * time.Second

	StreamBacklogFrames = 20000
	pendingFrameLimit   = 1024
	textMessage         = 1
	pingMessage         = 9
)

type group struct {
	s        *Service
	endpoint string
	depth    bool
	control  chan map[string]bool
	cancel   context.CancelFunc
	done     chan struct{}
}

func copySet(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		if v {
			out[k] = true
		}
	}
	return out
}

func (s *Service) startGroup(ctx context.Context, endpoint string, members map[string]bool) *group {
	gctx, cancel := context.WithCancel(ctx)
	g := &group{
		s: s, endpoint: endpoint, depth: endpoint == stream.BinanceUSDMDepth,
		control: make(chan map[string]bool, 1), cancel: cancel, done: make(chan struct{}),
	}
	g.control <- copySet(members)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(g.done)
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("stream group panicked", "endpoint", endpoint, "panic", r)
			}
		}()
		g.run(gctx)
	}()
	return g
}

func (g *group) update(members map[string]bool) {
	select {
	case <-g.control:
	default:
	}
	g.control <- copySet(members)
}

func (g *group) finished() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *group) run(ctx context.Context) {
	desired := <-g.control
	retry := time.Second
	for ctx.Err() == nil && len(desired) > 0 {
		connected, err := g.session(ctx, &desired)
		if connected {
			retry = time.Second
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			g.s.log.Warn("stream disconnected", "endpoint", g.endpoint, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
		retry = min(retry*2, 60*time.Second)
		select {
		case d := <-g.control:
			desired = d
		default:
		}
	}
}

type prepared struct {
	iid    string
	member *stream.Member
	err    error
}

type session struct {
	g            *group
	s            *Service
	conn         Conn
	id           int64
	venueName    string
	opened       time.Time
	lastObserved time.Time
	active       map[string]bool
	preparing    map[string]context.CancelFunc
	pending      map[string][]any
	index        map[string]*venue.Instrument
	ready        chan prepared
	sentAt       time.Time
	globalLiqs   bool
}

func (g *group) session(ctx context.Context, desired *map[string]bool) (connected bool, err error) {
	s := g.s
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := s.dial(ctx, g.endpoint)
	if err != nil {
		return false, err
	}
	sess := &session{
		g: g, s: s, conn: conn, id: s.connSeq.Add(1), opened: s.clock.Now(),
		active: map[string]bool{}, preparing: map[string]context.CancelFunc{}, pending: map[string][]any{},
		index: map[string]*venue.Instrument{}, ready: make(chan prepared, 64),
	}
	sess.lastObserved = sess.opened
	for iid := range *desired {
		sess.venueName = s.opt.Instruments[iid].VenueName()
		break
	}
	inbox := make(chan any, StreamBacklogFrames+1)
	var lastPong, lastPing atomic.Int64
	lastPong.Store(time.Now().UnixNano())
	conn.SetPongHandler(func(string) error { lastPong.Store(time.Now().UnixNano()); return nil })
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				inbox <- err
				return
			}
			select {
			case inbox <- string(data):
			default:
				inbox <- pyval.Semantic("STREAM_BACKLOG_EXCEEDED: over %d unprocessed frames", StreamBacklogFrames)
				return
			}
		}
	}()
	go func() {
		t := time.NewTicker(StreamPingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := time.Now()
				if ping := lastPing.Load(); ping > lastPong.Load() && now.Sub(time.Unix(0, ping)) > StreamPingTimeout {
					_ = conn.Close()
					return
				}
				if lastPing.Load() <= lastPong.Load() {
					lastPing.Store(now.UnixNano())
				}
				if err := conn.WriteControl(pingMessage, nil, now.Add(10*time.Second)); err != nil {
					return
				}
			}
		}
	}()
	defer func() {
		_ = conn.Close()
		for _, cancelPrepare := range sess.preparing {
			cancelPrepare()
		}
		sess.ended(ctx, err)
	}()
	if err = sess.membership(ctx, *desired); err != nil {
		return false, err
	}
	connected = true
	quiet := time.Duration(s.opt.Threshold("quiet_socket_s", 30) * float64(time.Second))
	lastMessage := s.clock.Monotonic()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for len(sess.active) > 0 {
		if s.clock.Monotonic()-lastMessage > quiet {
			return connected, errors.New("NO_MESSAGE_FOR_30_SECONDS")
		}
		select {
		case <-ctx.Done():
			return connected, nil
		case p := <-sess.ready:
			sess.prepared(ctx, p)
		case d := <-g.control:
			*desired = d
			if err = sess.membership(ctx, d); err != nil {
				return connected, err
			}
		case <-tick.C:
		case item := <-inbox:
			if e, ok := item.(error); ok {
				return connected, e
			}
			lastMessage = s.clock.Monotonic()
			sess.lastObserved = s.clock.Now()
			if err = sess.frame(ctx, item.(string)); err != nil {
				return connected, err
			}
		}
	}
	return connected, nil
}

func (ss *session) send(ctx context.Context, message map[string]any) error {
	if ss.venueName == "binance" {
		if wait := time.Until(ss.sentAt.Add(250 * time.Millisecond)); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
		ss.sentAt = time.Now()
	}
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return ss.conn.WriteMessage(textMessage, data)
}

func (ss *session) instruments(ids map[string]bool) []*venue.Instrument {
	var out []*venue.Instrument
	for _, iid := range ss.s.opt.Order {
		if ids[iid] {
			out = append(out, ss.s.opt.Instruments[iid])
		}
	}
	return out
}

func (ss *session) membership(ctx context.Context, desired map[string]bool) error {
	s := ss.s
	removed, added := map[string]bool{}, map[string]bool{}
	for iid := range ss.active {
		if !desired[iid] {
			removed[iid] = true
		}
	}
	for iid := range desired {
		if !ss.active[iid] {
			added[iid] = true
		}
	}
	for _, message := range stream.SubscriptionMessages(ss.instruments(removed), false, ss.g.endpoint) {
		if err := ss.send(ctx, message); err != nil {
			return err
		}
	}
	for iid := range removed {
		if cancel := ss.preparing[iid]; cancel != nil {
			cancel()
			delete(ss.preparing, iid)
		}
		delete(ss.pending, iid)
		inst := s.opt.Instruments[iid]
		s.mu.Lock()
		if ss.g.depth {
			delete(s.depth, iid)
		} else {
			delete(s.members, iid)
		}
		if ss.g.depth || inst.Key() != "binance.usdm" {
			delete(s.books, iid)
		}
		s.mu.Unlock()
		if !ss.g.depth {
			s.seal.Disconnected(inst, ss.id, "MEMBER_REMOVED")
			s.marks.Drop(iid)
		}
	}
	for _, message := range stream.SubscriptionMessages(ss.instruments(added), true, ss.g.endpoint) {
		if err := ss.send(ctx, message); err != nil {
			return err
		}
	}
	for iid := range added {
		ss.pending[iid] = []any{}
		ss.startPrepare(ctx, iid, 0)
	}
	for iid := range removed {
		delete(ss.active, iid)
	}
	for iid := range added {
		ss.active[iid] = true
	}
	ss.index = map[string]*venue.Instrument{}
	for iid := range ss.active {
		inst := s.opt.Instruments[iid]
		ss.index[inst.RoutingSymbol()] = inst
	}
	want := false
	if strings.HasPrefix(ss.g.endpoint, "wss://ws.okx.com/") {
		for iid := range ss.active {
			if s.opt.Instruments[iid].Key() == "okx.swap" {
				want = true
			}
		}
	}
	if want != ss.globalLiqs {
		op := "unsubscribe"
		if want {
			op = "subscribe"
		}
		message := map[string]any{"op": op, "args": []any{map[string]any{"channel": "liquidation-orders", "instType": "SWAP"}}}
		if err := ss.conn.WriteMessage(textMessage, mustJSON(message)); err != nil {
			return err
		}
		ss.globalLiqs = want
	}
	return nil
}

func mustJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

func (ss *session) startPrepare(ctx context.Context, iid string, delay time.Duration) {
	s := ss.s
	pctx, cancel := context.WithCancel(ctx)
	ss.preparing[iid] = cancel
	s.mu.Lock()
	state := &memberState{conn: ss.id, endpoint: ss.g.endpoint, state: "PREPARING"}
	if ss.g.depth {
		s.depth[iid] = state
	} else {
		if old := s.members[iid]; old != nil {
			state.message = old.message
		}
		s.members[iid] = state
	}
	s.mu.Unlock()
	go func() {
		if delay > 0 {
			select {
			case <-pctx.Done():
				return
			case <-time.After(delay):
			}
		}
		member, err := s.prepareMember(pctx, iid, ss.g.depth)
		if pctx.Err() != nil {
			return
		}
		select {
		case ss.ready <- prepared{iid, member, err}:
		case <-pctx.Done():
		}
	}()
}

func (s *Service) prepareMember(ctx context.Context, iid string, depth bool) (*stream.Member, error) {
	if !s.discovered(iid) {
		if err := s.discover(ctx, s.opt.Instruments[iid]); err != nil {
			return nil, err
		}
	}
	inst := s.instrument(iid)
	key := inst.Key()
	regularUSDM := key == "binance.usdm" && !depth
	s.mu.Lock()
	book := s.books[iid]
	if !regularUSDM || book == nil {
		book = books.New(inst, s.clock, s.clock.Now)
		s.books[iid] = book
	}
	s.mu.Unlock()
	member := &stream.Member{Inst: inst, Book: book, Bridging: !regularUSDM}
	if strings.HasPrefix(key, "binance") && !regularUSDM {
		snap, err := s.venues.Venue(inst, 0).DepthSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		prec := pytime.CollectionOnly
		if snap["E"] != nil {
			prec = pytime.Exact
		}
		if err := book.Snapshot(pyval.List(snap["bids"]), pyval.List(snap["asks"]), snap["lastUpdateId"], snap["E"], nil, prec); err != nil {
			return nil, err
		}
	}
	return member, nil
}

func (ss *session) prepared(ctx context.Context, p prepared) {
	s := ss.s
	if _, ok := ss.preparing[p.iid]; !ok || !ss.active[p.iid] {
		return
	}
	delete(ss.preparing, p.iid)
	if p.err != nil {
		ss.reset(ctx, p.iid, p.err.Error())
		return
	}
	now := s.clock.Now()
	s.mu.Lock()
	states := s.members
	if ss.g.depth {
		states = s.depth
	}
	state := states[p.iid]
	if state == nil {
		state = &memberState{endpoint: ss.g.endpoint}
		states[p.iid] = state
	}
	state.member, state.conn, state.state, state.connected = p.member, ss.id, "CONNECTED", now
	since, hot := s.hotSince[p.member.Inst.TokenID]
	s.mu.Unlock()
	if !ss.g.depth {
		var hotSince *time.Time
		if hot {
			hotSince = &since
		}
		if bridge := s.seal.Connected(p.member.Inst, ss.id, now, hotSince); bridge != nil {
			s.queueRepair(p.member.Inst, *bridge, "RECONNECT", nil)
		}
	}
	queued := ss.pending[p.iid]
	delete(ss.pending, p.iid)
	for _, payload := range queued {
		if err := ss.process(p.iid, payload); err != nil {
			ss.reset(ctx, p.iid, err.Error())
			return
		}
	}
}

func (ss *session) reset(ctx context.Context, iid, reason string) {
	s := ss.s
	inst := s.opt.Instruments[iid]
	now := s.clock.Now()
	s.mu.Lock()
	var state *memberState
	if ss.g.depth {
		state = s.depth[iid]
	} else {
		state = s.members[iid]
	}
	since := ss.opened
	if state != nil && !state.connected.IsZero() {
		since = state.connected
	}
	if ss.g.depth {
		if b := s.books[iid]; b != nil {
			b.IntegrityOK, b.GapReason = false, "ORDER_BOOK_STREAM_MEMBER_GAP"
		}
	}
	s.mu.Unlock()
	if ss.g.depth {
		s.gap(inst, "ORDER_BOOK_STREAM", "STREAM_MEMBER_GAP: "+reason, &ss.opened, &now)
	} else {
		s.seal.Disconnected(inst, ss.id, "STREAM_MEMBER_GAP: "+reason)
		s.gap(inst, "STREAM", "STREAM_MEMBER_GAP: "+reason, &since, &now)
	}
	if cancel := ss.preparing[iid]; cancel != nil {
		cancel()
	}
	one := map[string]bool{iid: true}
	for _, message := range stream.SubscriptionMessages(ss.instruments(one), false, ss.g.endpoint) {
		_ = ss.send(ctx, message)
	}
	for _, message := range stream.SubscriptionMessages(ss.instruments(one), true, ss.g.endpoint) {
		_ = ss.send(ctx, message)
	}
	ss.pending[iid] = []any{}
	ss.startPrepare(ctx, iid, time.Second)
}

func (ss *session) frame(ctx context.Context, text string) error {
	s := ss.s
	s.mu.Lock()
	s.frames[ss.g.endpoint]++
	s.mu.Unlock()
	if text == "pong" {
		return nil
	}
	raw, err := pyval.Decode([]byte(text))
	if err != nil {
		return err
	}
	if m := pyval.Map(raw); m != nil && (m["event"] == "error" || m["success"] == false || m["type"] == "error") {
		return pyval.Semantic("subscription rejected %s", pyjson.Canonical(raw))
	}
	for _, routed := range stream.RouteIndexed(ss.index, ss.venueName, raw) {
		iid := routed.Inst.ID
		if _, preparing := ss.preparing[iid]; preparing {
			ss.pending[iid] = append(ss.pending[iid], routed.Payload)
			if len(ss.pending[iid]) > pendingFrameLimit {
				ss.reset(ctx, iid, "initial book buffer exhausted")
			}
			continue
		}
		if err := ss.process(iid, routed.Payload); err != nil {

			ss.reset(ctx, iid, err.Error())
		}
	}
	return nil
}

func (ss *session) process(iid string, payload any) error {
	s := ss.s
	s.mu.Lock()
	var state *memberState
	if ss.g.depth {
		state = s.depth[iid]
	} else {
		state = s.members[iid]
	}
	s.mu.Unlock()
	if state == nil || state.member == nil {
		return nil
	}
	err := stream.Process(s.pipe, state.member, payload)
	if errors.Is(err, stream.ErrSkip) {
		return nil
	}
	if err != nil {
		return err
	}
	now := s.clock.Now()
	s.marks.Record(state.member.Inst, payload, pipeline.Seconds(s.clock.Monotonic()), now)
	if !ss.g.depth && state.member.Inst.VenueName() == "hyperliquid" {
		if w, ok := s.marks.Get(iid); ok {
			s.seal.Watermark(iid, ss.id, w.TS)
		}
	}
	s.mu.Lock()
	state.message = now
	s.mu.Unlock()
	return nil
}

func (ss *session) ended(ctx context.Context, cause error) {
	s := ss.s
	now := s.clock.Now()
	reason := "STREAM_DISCONNECTED: "
	if cause != nil {
		reason += cause.Error()
	}
	for iid := range ss.active {
		inst := s.opt.Instruments[iid]
		s.mu.Lock()
		if ss.g.depth {
			if m := s.depth[iid]; m != nil && m.conn == ss.id {
				m.state, m.connected = "DISCONNECTED", time.Time{}
			}
			if b := s.books[iid]; b != nil && ctx.Err() == nil {
				b.IntegrityOK, b.GapReason = false, "ORDER_BOOK_STREAM_DISCONNECTED"
			}
		} else if m := s.members[iid]; m != nil && m.conn == ss.id {
			m.state, m.connected = "DISCONNECTED", time.Time{}
		}
		s.mu.Unlock()
		if ctx.Err() != nil {
			continue
		}
		last := ss.lastObserved
		if ss.g.depth {
			s.gap(inst, "ORDER_BOOK_STREAM", reason, &last, &now)
		} else {
			s.seal.Disconnected(inst, ss.id, reason)
			s.gap(inst, "STREAM", reason, &last, &now)
		}
	}
}

func (ss *session) String() string { return fmt.Sprintf("%s#%d", ss.g.endpoint, ss.id) }
