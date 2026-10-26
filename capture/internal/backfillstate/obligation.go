package backfillstate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/freQuensy23-coder/Tank/capture/internal/pytime"
)

type Reference struct {
	key, revision string
	observed      bool
}

func NewReference(storageKey, revision string) (Reference, error) {
	value := Reference{key: storageKey, revision: revision, observed: true}
	if err := value.Validate(); err != nil {
		return Reference{}, err
	}
	return value, nil
}

func (r Reference) Validate() error {
	if !r.observed || strings.TrimSpace(r.revision) == "" || !utf8.ValidString(r.key) || !utf8.ValidString(r.revision) {
		return errors.New("backfill reference requires its actual storage key and observed revision")
	}
	return nil
}

func (r Reference) StorageKey() string { return r.key }
func (r Reference) Revision() string   { return r.revision }

type Phase uint8

const (
	Pending Phase = iota + 1
	Running
)

type Selection interface{ backfillSelection() }

type Obligation struct {
	backfillID, eventID, tokenID string
	anchor                       time.Time
	phase                        Phase
}

func (Obligation) backfillSelection()   {}
func (o Obligation) BackfillID() string { return o.backfillID }
func (o Obligation) EventID() string    { return o.eventID }
func (o Obligation) TokenID() string    { return o.tokenID }
func (o Obligation) Anchor() time.Time  { return o.anchor }
func (o Obligation) Phase() Phase       { return o.phase }

func (o Obligation) Validate() error {
	if !validIdentity(o.backfillID) || !validIdentity(o.eventID) || !validIdentity(o.tokenID) || (o.phase != Pending && o.phase != Running) {
		return errors.New("backfill command requires its admitted identities and observed pending or running phase")
	}
	return nil
}

func validIdentity(value string) bool {
	return strings.TrimSpace(value) != "" && utf8.ValidString(value)
}

type refusalKind uint8

const (
	anchorMissing refusalKind = iota + 1
	anchorUnreadable
	backfillIdentityInvalid
	tokenIdentityInvalid
	eventIdentityInvalid
	statusInvalid
)

type Refused struct {
	kind   refusalKind
	detail string
}

func (Refused) backfillSelection() {}
func (r Refused) Validate() error {
	switch r.kind {
	case anchorUnreadable:
		if r.detail == "" || !utf8.ValidString(r.detail) {
			return errors.New("backfill anchor refusal requires its actual parse failure")
		}
		prefix := "anchor unreadable: " + pytime.ErrTime.Error() + ": "
		if !strings.HasPrefix(r.detail, prefix) {
			return errors.New("backfill anchor refusal cannot borrow another failure reason")
		}
		raw, err := strconv.Unquote(strings.TrimPrefix(r.detail, prefix))
		if err != nil {
			return errors.New("backfill anchor refusal requires its quoted failed timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return errors.New("backfill anchor refusal cannot reject an admitted aware timestamp")
		}
	case anchorMissing, backfillIdentityInvalid, tokenIdentityInvalid, eventIdentityInvalid, statusInvalid:
		if r.detail != "" {
			return errors.New("backfill fixed refusal cannot carry another refusal's detail")
		}
	default:
		return errors.New("backfill refusal kind is not supported")
	}
	return nil
}
func (r Refused) Reason() string {
	switch r.kind {
	case anchorMissing:
		return "anchor unreadable: " + pytime.ErrTime.Error() + ": missing"
	case anchorUnreadable:
		return r.detail
	case backfillIdentityInvalid:
		return "BACKFILL_ID_INVALID"
	case tokenIdentityInvalid:
		return "TOKEN_ID_INVALID"
	case eventIdentityInvalid:
		return "EVENT_ID_INVALID"
	case statusInvalid:
		return "BACKFILL_STATUS_INVALID"
	}
	return ""
}

type Observation struct {
	reference Reference
	selection Selection
}

func (o Observation) Reference() Reference { return o.reference }
func (o Observation) Selection() Selection { return o.selection }

func Observe(reference Reference, record Record) (Observation, error) {
	if err := reference.Validate(); err != nil {
		return Observation{}, err
	}
	if reference.StorageKey() != record.StorageKey {
		return Observation{}, errors.New("backfill observation differs from its actual storage key")
	}
	refuse := func(kind refusalKind, detail string) (Observation, error) {
		return Observation{reference: reference, selection: Refused{kind: kind, detail: detail}}, nil
	}
	if record.Anchor == nil {
		return refuse(anchorMissing, "")
	}
	anchor, err := time.Parse(time.RFC3339Nano, string(*record.Anchor))
	if err != nil {
		return refuse(anchorUnreadable, fmt.Sprintf("anchor unreadable: %s: %q", pytime.ErrTime, *record.Anchor))
	}
	if !validIdentity(record.BackfillID) || record.BackfillID != record.StorageKey {
		return refuse(backfillIdentityInvalid, "")
	}
	if record.TokenID == nil || !validIdentity(*record.TokenID) {
		return refuse(tokenIdentityInvalid, "")
	}
	if record.EventID == nil || !validIdentity(*record.EventID) {
		return refuse(eventIdentityInvalid, "")
	}
	var phase Phase
	if record.Status != nil {
		switch *record.Status {
		case "PENDING":
			phase = Pending
		case "RUNNING":
			phase = Running
		}
	}
	if phase == 0 {
		return refuse(statusInvalid, "")
	}
	return Observation{reference: reference, selection: Obligation{
		backfillID: record.BackfillID, eventID: *record.EventID, tokenID: *record.TokenID,
		anchor: anchor.UTC(), phase: phase,
	}}, nil
}
