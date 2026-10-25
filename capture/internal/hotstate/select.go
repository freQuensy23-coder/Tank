package hotstate

import (
	"errors"
	"fmt"
	"time"
)

type RefusalReason string

const (
	SinceUnavailable    RefusalReason = "HOT_SINCE_UNAVAILABLE"
	BackfillUnavailable RefusalReason = "MATCHING_BACKFILL_ANCHOR_UNAVAILABLE"
	BackfillMalformed   RefusalReason = "MATCHING_BACKFILL_ANCHOR_MALFORMED"
	BackfillAmbiguous   RefusalReason = "MATCHING_BACKFILL_ANCHOR_AMBIGUOUS"
)

type Refusal struct {
	Reason RefusalReason
	Detail string
}
type Selection struct {
	origin         *WindowOrigin
	refusal        *Refusal
	unavailableIDs []string
}

func (s Selection) Origin() (WindowOrigin, bool) {
	if s.origin == nil {
		return WindowOrigin{}, false
	}
	return *s.origin, true
}
func (s Selection) Refusal() (Refusal, bool) {
	if s.refusal == nil {
		return Refusal{}, false
	}
	return *s.refusal, true
}
func (s Selection) UnavailableBackfillIDs() []string {
	return append([]string(nil), s.unavailableIDs...)
}

func refusal(reason RefusalReason, detail string, ids []string) Selection {
	return Selection{refusal: &Refusal{Reason: reason, Detail: detail}, unavailableIDs: ids}
}
func sameText(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
func sameAnchor(a, b AnchorRecord) bool {
	return a.Anchor != nil && b.Anchor != nil && a.Anchor.Equal(*b.Anchor) &&
		sameText(a.AnchorPrec, b.AnchorPrec) && sameText(a.AnchorReason, b.AnchorReason)
}
func knownStatus(status *string) bool {
	if status == nil {
		return false
	}
	switch *status {
	case "PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED":
		return true
	default:
		return false
	}
}

func choose(records []AnchorRecord) Selection {
	var valid []AnchorRecord
	var unavailable []string
	for _, record := range records {
		if record.BackfillID == "" || !knownStatus(record.Status) {
			return refusal(BackfillMalformed, "matching backfill lacks an identity or supported lifecycle state", unavailable)
		}
		if record.Anchor == nil {
			if *record.Status == "FAILED" {
				unavailable = append(unavailable, record.BackfillID)
				continue
			}
			return refusal(BackfillMalformed, "matching active backfill lacks its declared anchor", unavailable)
		}
		valid = append(valid, record)
	}
	if len(valid) == 0 {
		return refusal(BackfillUnavailable, "matching failed backfills contain no declared anchor", unavailable)
	}
	var latest *time.Time
	for _, record := range valid {
		if record.StartedAt != nil && (latest == nil || record.StartedAt.After(*latest)) {
			latest = record.StartedAt
		}
	}
	var ordered []AnchorRecord
	for _, record := range valid {
		if latest == nil || (record.StartedAt != nil && record.StartedAt.Equal(*latest)) {
			ordered = append(ordered, record)
		}
	}
	chosen := ordered[0]
	for _, record := range ordered[1:] {
		if !sameAnchor(chosen, record) {
			return refusal(BackfillAmbiguous, "matching backfills have different anchors without a distinct recorded start order", unavailable)
		}
		if record.StorageKey > chosen.StorageKey {
			chosen = record
		}
	}
	origin, err := chosen.RecordedAnchor()
	if err != nil {
		return refusal(BackfillMalformed, err.Error(), unavailable)
	}
	origin.StartedClock = &OptionalClockEvidence{Value: chosen.StartedAt, Precision: chosen.StartedAtPrec, Reason: chosen.StartedAtReason}
	return Selection{origin: &origin, unavailableIDs: unavailable}
}

func ResolveOrigins(hot []Record, records []AnchorRecord) (map[Scope]Selection, error) {
	wanted := make(map[Scope]Record, len(hot))
	for _, record := range hot {
		scope, err := record.Scope()
		if err != nil {
			return nil, err
		}
		wanted[scope] = record
	}
	candidates := make(map[Scope][]AnchorRecord)
	for _, record := range records {
		scope, err := record.Scope()
		if err != nil {
			return nil, fmt.Errorf("backfill anchor scope unavailable: %w", err)
		}
		if _, ok := wanted[scope]; !ok {
			return nil, errors.New("backfill anchor projection contains an unrequested scope")
		}
		candidates[scope] = append(candidates[scope], record)
	}
	result := make(map[Scope]Selection, len(wanted))
	for scope, record := range wanted {
		if matching := candidates[scope]; len(matching) > 0 {
			result[scope] = choose(matching)
			continue
		}
		origin, err := record.RecordedSince()
		if err != nil {
			result[scope] = refusal(SinceUnavailable, err.Error(), nil)
			continue
		}
		result[scope] = Selection{origin: &origin}
	}
	return result, nil
}
