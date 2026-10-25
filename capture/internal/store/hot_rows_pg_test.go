package store

import (
	"context"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/hotstate"
)

func TestHotPhysicalProjectionAndPairScopedAnchorLookup(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	prefix := "hot-typed-" + suffix()
	cleanup(t, s, "token_id", prefix+"%", "mkt.hot_tokens", "mkt.backfill_runs")
	first := hotstate.Scope{TokenID: prefix + "a|b", EventID: "c"}
	second := hotstate.Scope{TokenID: prefix + "a", EventID: "b|c"}
	since := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	expired := since.Add(-time.Hour)
	for index, scope := range []hotstate.Scope{first, second} {
		_, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable("mkt.hot_tokens")+
			" (storage_key,token_id,event_id,since,since_prec_reason,expires_at,priority,code_version,semantic_fingerprint) VALUES($1,$2,$3,$4,'LEGACY_PRECISION_UNREPORTED',$5,$6,'retained-code','retained-fingerprint')",
			prefix+string(rune('0'+index)), scope.TokenID, scope.EventID, since, expired, int64(9223372036854775807))
		if err != nil {
			t.Fatal(err)
		}
	}
	raw, err := s.HotTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var owned []hotstate.Record
	for _, record := range raw {
		if record.TokenID != first.TokenID && record.TokenID != second.TokenID {
			continue
		}
		owned = append(owned, record)
		if record.Priority == nil || *record.Priority != int64(9223372036854775807) || record.SincePrec != nil || record.SinceReason == nil ||
			*record.SinceReason != "LEGACY_PRECISION_UNREPORTED" || record.ExpiresAt == nil || !record.ExpiresAt.Equal(expired) ||
			record.CodeVersion == nil || *record.CodeVersion != "retained-code" || record.ConfigVersion != nil ||
			record.SemanticFingerprint == nil || *record.SemanticFingerprint != "retained-fingerprint" {
			t.Fatalf("Physical HOT reader inferred or lost historical evidence: %+v", record)
		}
	}
	if len(owned) != 2 {
		t.Fatal("Stored finite-expired membership was silently filtered or omitted")
	}
	for _, entry := range []struct {
		id      string
		scope   hotstate.Scope
		anchor  *time.Time
		started *time.Time
		status  string
	}{
		{"old", first, &since, &since, "COMPLETE"},
		{"new", first, timePointer(since.Add(time.Minute)), timePointer(since.Add(time.Hour)), "PARTIAL"},
		{"rejected", first, nil, nil, "FAILED"},
		{"malformed", second, &since, &since, "UNKNOWN"},
		{"cross-pair", hotstate.Scope{TokenID: first.TokenID, EventID: second.EventID}, &since, &since, "COMPLETE"},
	} {
		_, err := s.Pool.Exec(ctx, "INSERT INTO "+liveTable("mkt.backfill_runs")+
			" (storage_key,backfill_id,token_id,event_id,anchor,anchor_prec,started_at,status,result) VALUES($1,$1,$2,$3,$4,'BUCKET',$5,$6,$7::jsonb)",
			prefix+"-"+entry.id, entry.scope.TokenID, entry.scope.EventID, entry.anchor, entry.started, entry.status, `{"not_part_of_origin":0.123456789012345678901}`)
		if err != nil {
			t.Fatal(err)
		}
	}
	candidates, err := s.BackfillAnchors(ctx, []hotstate.Scope{first, second, first})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 4 {
		t.Fatalf("Anchor query leaked Cartesian pairs or duplicated scopes: %d", len(candidates))
	}
	selections, err := hotstate.ResolveOrigins(owned, candidates)
	if err != nil {
		t.Fatal(err)
	}
	origin, ok := selections[first].Origin()
	if !ok || origin.BackfillID == nil || *origin.BackfillID != prefix+"-new" || !origin.Clock.Value.Equal(since.Add(time.Minute)) ||
		origin.Clock.Precision == nil || *origin.Clock.Precision != "BUCKET" || origin.StartedClock == nil || origin.StartedClock.Precision != nil {
		t.Fatalf("Pair-scoped physical anchor facts changed: %+v", origin)
	}
	refused, ok := selections[second].Refusal()
	if _, usable := selections[second].Origin(); usable || !ok || refused.Reason != hotstate.BackfillMalformed {
		t.Fatal("Malformed matching lifecycle state disappeared into successful absence")
	}
	if ids := selections[first].UnavailableBackfillIDs(); len(ids) != 1 || ids[0] != prefix+"-rejected" {
		t.Fatal("Actual failed-without-anchor evidence was discarded")
	}
	_, err = s.Pool.Exec(ctx, "INSERT INTO "+liveTable("mkt.backfill_runs")+
		" (storage_key,backfill_id,token_id,event_id,anchor,anchor_prec,started_at,status) VALUES($1,$1,$2,$3,$4,'BUCKET',NULL,'COMPLETE')",
		prefix+"-unobserved", first.TokenID, first.EventID, since.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	candidates, err = s.BackfillAnchors(ctx, []hotstate.Scope{first})
	if err != nil {
		t.Fatal(err)
	}
	selections = nil
	for _, record := range owned {
		if record.TokenID == first.TokenID {
			selections, err = hotstate.ResolveOrigins([]hotstate.Record{record}, candidates)
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	refused, ok = selections[first].Refusal()
	if _, usable := selections[first].Origin(); usable || !ok || refused.Reason != hotstate.BackfillAmbiguous {
		t.Fatal("SQL NULL worker start was treated as an older conflicting origin")
	}
	if _, err := s.BackfillAnchors(ctx, []hotstate.Scope{{TokenID: first.TokenID}}); err == nil {
		t.Fatal("Unscoped anchor lookup was accepted")
	}
}

func timePointer(value time.Time) *time.Time { return &value }
