package store

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/freQuensy23-coder/Tank/capture/internal/backfillstate"
	"github.com/freQuensy23-coder/Tank/capture/internal/schema"
	"github.com/jackc/pgx/v5"
)

func actualBackfill(t *testing.T, s *Store, id string) (backfillstate.Observation, backfillstate.Record) {
	t.Helper()
	row, err := decodeBackfillRow(s.Pool.QueryRow(context.Background(), "SELECT "+backfillProjection+",backfill_row.xmin::text FROM "+liveTable(backfillRuns)+" backfill_row WHERE storage_key=$1", id))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := backfillstate.NewReference(row.record.StorageKey, row.transaction)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := backfillstate.Observe(reference, row.record)
	if err != nil {
		t.Fatal(err)
	}
	return observation, row.record
}

func backfillWriter(t *testing.T) backfillstate.WriterIdentity {
	t.Helper()
	writer, err := backfillstate.NewWriterIdentity("observed-config", "observed-code")
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func requireBackfillFingerprint(t *testing.T, row backfillstate.Record) {
	t.Helper()
	document, err := row.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := schema.FingerprintDocument(document, backfillRuns)
	if err != nil || row.SemanticFingerprint == nil || *row.SemanticFingerprint != fingerprint {
		t.Fatalf("fingerprint does not bind actual persisted exact evidence: %v", err)
	}
}

func seedBackfill(t *testing.T, s *Store, id string, anchor *time.Time, token, event *string) {
	t.Helper()
	_, err := s.Pool.Exec(context.Background(), "INSERT INTO "+liveTable(backfillRuns)+` (
 storage_key,backfill_id,event_id,token_id,anchor,anchor_prec_reason,status,reason,
 started_at_prec_reason,finished_at_prec_reason,result,code_version,config_version,semantic_fingerprint)
 VALUES($1,$1,$2,$3,$4,'RETAINED_ORIGIN_UNREPORTED','PENDING','RETAINED_PENDING_REASON',
 'RETAINED_START_UNREPORTED','RETAINED_FINISH_UNREPORTED',
 '{"integer":9007199254740993,"fraction":0.123456789012345678901,"text":"000.100"}'::jsonb,
 'retained-code','retained-config','old-rounded-fingerprint')`, id, event, token, anchor)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBackfillSparseRefusalPreservesMalformedNullableEvidenceAndExactPriorResult(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-refusal-" + suffix()
	cleanup(t, s, "backfill_id", id, backfillRuns)
	seedBackfill(t, s, id, nil, nil, nil)
	legacy, err := s.Get(ctx, backfillRuns, id)
	if err != nil {
		t.Fatal(err)
	}
	_, original := actualBackfill(t, s, id)
	document, err := original.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	exactFingerprint, err := schema.FingerprintDocument(document, backfillRuns)
	if err != nil {
		t.Fatal(err)
	}
	legacyFingerprint := schema.Fingerprint(legacy, backfillRuns)
	if exactFingerprint == legacyFingerprint {
		t.Fatal("fixture did not establish actual legacy rounded-number fingerprint evidence")
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE "+liveTable(backfillRuns)+" SET semantic_fingerprint=$2 WHERE storage_key=$1", id, legacyFingerprint); err != nil {
		t.Fatal(err)
	}
	observation, before := actualBackfill(t, s, id)
	refusal, ok := observation.Selection().(backfillstate.Refused)
	if !ok {
		t.Fatal("missing actual anchor was not refused")
	}
	outcome, err := backfillstate.RefusalOutcome(refusal)
	if err != nil {
		t.Fatal(err)
	}
	finished := time.Date(2026, 10, 26, 1, 2, 3, 123456000, time.UTC)
	completion, err := backfillstate.Finish(observation, outcome, finished, finished.Add(time.Microsecond), backfillWriter(t))
	if err != nil {
		t.Fatal(err)
	}
	acknowledgement, err := s.FinishBackfill(ctx, completion)
	if err != nil || acknowledgement.Disposition() != backfillstate.WriteApplied {
		t.Fatalf("explicit refusal was not durably acknowledged: %v", err)
	}
	_, after := actualBackfill(t, s, id)
	if after.TokenID != nil || after.EventID != nil || after.Anchor != nil || after.StartedAt != nil || after.ReceivedTS != nil || after.StartedAtPrecReason == nil || *after.StartedAtPrecReason != "RETAINED_START_UNREPORTED" {
		t.Fatal("terminal refusal fabricated identities, origin or worker-start facts")
	}
	if after.ResultJSON == nil || before.ResultJSON == nil || *after.ResultJSON != *before.ResultJSON || !strings.Contains(*after.ResultJSON, "0.123456789012345678901") {
		t.Fatal("terminal refusal rewrote exact opaque prior result")
	}
	if after.Status == nil || *after.Status != "FAILED" || after.Reason == nil || *after.Reason != "anchor unreadable: unparseable timestamp: missing" || after.FinishedAt == nil || after.FinishedAtPrec == nil || *after.FinishedAtPrec != "COLLECTION_ONLY" || after.FinishedAtPrecReason != nil {
		t.Fatal("actual terminal failure or collection clock changed")
	}
	if after.ConfigVersion == nil || *after.ConfigVersion != "observed-config" || after.CodeVersion == nil || *after.CodeVersion != "observed-code" {
		t.Fatal("live refusal did not use actual writer identities")
	}
	requireBackfillFingerprint(t, after)
	prior, err := after.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	acknowledgement, err = s.FinishBackfill(ctx, completion)
	if err != nil || acknowledgement.Disposition() != backfillstate.WriteSuperseded {
		t.Fatalf("old observation acquired another completion acknowledgement: %v", err)
	}
	_, unchanged := actualBackfill(t, s, id)
	current, err := unchanged.SemanticDocument()
	if err != nil || !bytes.Equal(prior, current) {
		t.Fatal("replayed refusal rewrote immutable prior evidence")
	}
}

func TestBackfillStartCASHasOneWinnerAndCannotFinishOverARequeuedRevision(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-cas-" + suffix()
	cleanup(t, s, "backfill_id", id, backfillRuns)
	anchor := time.Date(2026, 10, 25, 1, 2, 0, 0, time.UTC)
	token, event := "observed-token", "observed-event"
	seedBackfill(t, s, id, &anchor, &token, &event)
	observation, prior := actualBackfill(t, s, id)
	clock := anchor.Add(10 * time.Minute)
	start, err := backfillstate.Begin(observation, clock, clock.Add(time.Microsecond), backfillWriter(t))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan backfillstate.Acknowledgement, 2)
	errors := make(chan error, 2)
	var callers sync.WaitGroup
	for range 2 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			acknowledgement, err := s.StartBackfill(ctx, start)
			results <- acknowledgement
			errors <- err
		}()
	}
	callers.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var winners, superseded int
	var applied backfillstate.Acknowledgement
	for acknowledgement := range results {
		switch acknowledgement.Disposition() {
		case backfillstate.WriteApplied:
			winners++
			applied = acknowledgement
		case backfillstate.WriteSuperseded:
			superseded++
		default:
			t.Fatal("existing actual row was reported missing")
		}
	}
	if winners != 1 || superseded != 1 {
		t.Fatalf("concurrent start did not have one actual winner: %d/%d", winners, superseded)
	}
	reference, err := applied.UpdatedReference()
	if err != nil {
		t.Fatal(err)
	}
	running, err := observation.AcknowledgeStart(reference)
	if err != nil {
		t.Fatal(err)
	}
	_, started := actualBackfill(t, s, id)
	if started.StartedAt == nil || started.StartedAtPrec == nil || *started.StartedAtPrec != "COLLECTION_ONLY" || started.StartedAtPrecReason != nil || started.FinishedAt != nil || started.FinishedAtPrecReason == nil || *started.FinishedAtPrecReason != "RETAINED_FINISH_UNREPORTED" || started.ResultJSON == nil || *started.ResultJSON != *prior.ResultJSON || started.AnchorPrec != nil || *started.AnchorPrecReason != "RETAINED_ORIGIN_UNREPORTED" {
		t.Fatal("start rewrote unrelated retained facts")
	}
	requireBackfillFingerprint(t, started)
	failure, err := backfillstate.Failed(backfillstate.CoverageReadFailed)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := backfillstate.Finish(running, failure, clock.Add(time.Second), clock.Add(time.Second), backfillWriter(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE "+liveTable(backfillRuns)+" SET status='PENDING',anchor=$2,reason='ACTUAL_REQUEUE' WHERE storage_key=$1", id, anchor.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, requeued := actualBackfill(t, s, id)
	acknowledgement, err := s.FinishBackfill(ctx, completion)
	if err != nil || acknowledgement.Disposition() != backfillstate.WriteSuperseded {
		t.Fatalf("old worker overwrote corrected queued input: %v", err)
	}
	_, after := actualBackfill(t, s, id)
	beforeDocument, err := requeued.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	afterDocument, err := after.SemanticDocument()
	if err != nil || !bytes.Equal(beforeDocument, afterDocument) {
		t.Fatal("superseded completion changed actual requeued evidence")
	}
	if _, err := s.Pool.Exec(ctx, "DELETE FROM "+liveTable(backfillRuns)+" WHERE storage_key=$1", id); err != nil {
		t.Fatal(err)
	}
	acknowledgement, err = s.FinishBackfill(ctx, completion)
	if err != nil || acknowledgement.Disposition() != backfillstate.WriteMissing {
		t.Fatalf("deleted row acquired a replacement write: %v", err)
	}
	var count int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+liveTable(backfillRuns)+" WHERE storage_key=$1", id).Scan(&count); err != nil || count != 0 {
		t.Fatal("old worker resurrected a deleted backfill row")
	}
}

func TestBackfillStoreBindsActualLockedFactsBeforeAnyWrite(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-binding-" + suffix()
	cleanup(t, s, "backfill_id", id, backfillRuns)
	anchor := time.Date(2026, 10, 25, 1, 2, 0, 0, time.UTC)
	token, event := "observed-token", "observed-event"
	seedBackfill(t, s, id, &anchor, &token, &event)
	observation, before := actualBackfill(t, s, id)
	other := "another-token"
	forged := before
	forged.TokenID = &other
	unbound, err := backfillstate.Observe(observation.Reference(), forged)
	if err != nil {
		t.Fatal(err)
	}
	start, err := backfillstate.Begin(unbound, anchor.Add(time.Minute), anchor.Add(time.Minute), backfillWriter(t))
	if err != nil {
		t.Fatal(err)
	}
	acknowledgement, err := s.StartBackfill(ctx, start)
	if err == nil || acknowledgement.Disposition() == backfillstate.WriteApplied {
		t.Fatal("caller-provided facts replaced the actual locked command")
	}
	_, after := actualBackfill(t, s, id)
	prior, err := before.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	current, err := after.SemanticDocument()
	if err != nil || !bytes.Equal(prior, current) {
		t.Fatal("unbound command produced physical effects")
	}
}

func TestBackfillCapturedOutcomePersistsNamedCompleteAndPartialWire(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	prefix := "backfill-result-" + suffix()
	cleanup(t, s, "backfill_id", prefix+"%", backfillRuns)
	anchor := time.Date(2026, 10, 25, 1, 2, 0, 0, time.UTC)
	token, event := "observed-token", "observed-event"
	for _, complete := range []bool{false, true} {
		id := prefix + "partial"
		if complete {
			id = prefix + "complete"
		}
		seedBackfill(t, s, id, &anchor, &token, &event)
		observation, _ := actualBackfill(t, s, id)
		clock := anchor.Add(time.Minute)
		start, err := backfillstate.Begin(observation, clock, clock, backfillWriter(t))
		if err != nil {
			t.Fatal(err)
		}
		acknowledgement, err := s.StartBackfill(ctx, start)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := acknowledgement.UpdatedReference()
		if err != nil {
			t.Fatal(err)
		}
		running, err := observation.AcknowledgeStart(reference)
		if err != nil {
			t.Fatal(err)
		}
		var instruments []backfillstate.InstrumentCoverage
		if complete {
			var windows [3]backfillstate.WindowCoverage
			for index, hours := range []int{1, 4, 24} {
				start := anchor.Add(-time.Duration(hours) * time.Hour)
				minutes, err := backfillstate.NewMinuteCoverage(int64(hours*60), int64(hours*60))
				if err != nil {
					t.Fatal(err)
				}
				windows[index], err = backfillstate.NewWindowCoverage(start, anchor, start, anchor, 0, minutes, minutes)
				if err != nil {
					t.Fatal(err)
				}
			}
			coverage, err := backfillstate.NewCaptureWindows(windows[0], windows[1], windows[2])
			if err != nil {
				t.Fatal(err)
			}
			instrument, err := backfillstate.NonSpotInstrumentCoverage("INSTR-observed", coverage)
			if err != nil {
				t.Fatal(err)
			}
			instruments = []backfillstate.InstrumentCoverage{instrument}
		}
		result, err := backfillstate.NewResult(token, anchor, instruments, nil)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := backfillstate.Captured(result)
		if err != nil {
			t.Fatal(err)
		}
		completion, err := backfillstate.Finish(running, outcome, clock.Add(time.Second), clock.Add(time.Second), backfillWriter(t))
		if err != nil {
			t.Fatal(err)
		}
		acknowledgement, err = s.FinishBackfill(ctx, completion)
		if err != nil || acknowledgement.Disposition() != backfillstate.WriteApplied {
			t.Fatalf("named capture outcome was not committed: %v", err)
		}
		_, stored := actualBackfill(t, s, id)
		if stored.Status == nil || *stored.Status != outcome.Status() || stored.ResultJSON == nil {
			t.Fatal("actual terminal status or result was discarded")
		}
		var wire struct {
			Instruments []struct {
				InstrumentID string `json:"instrument_id"`
				Complete     bool   `json:"complete"`
			} `json:"instruments"`
			Benchmarks []json.RawMessage `json:"benchmark_klines"`
		}
		if err := json.Unmarshal([]byte(*stored.ResultJSON), &wire); err != nil || wire.Instruments == nil || wire.Benchmarks == nil {
			t.Fatal("observed empty lists became null or malformed wire")
		}
		if complete && (len(wire.Instruments) != 1 || wire.Instruments[0].InstrumentID != "INSTR-observed" || !wire.Instruments[0].Complete || stored.Reason != nil || !strings.Contains(*stored.ResultJSON, `"unresolved_boundary_seconds": 0.0`)) {
			t.Fatal("named complete coverage or float-kind evidence changed")
		}
		if !complete && (len(wire.Instruments) != 0 || stored.Reason == nil || *stored.Reason != "INCOMPLETE_VENUE_WINDOWS_EXCLUDED") {
			t.Fatal("actual empty coverage acquired a complete result")
		}
		requireBackfillFingerprint(t, stored)
	}
}

func TestBackfillExplicitEmptyKeyRefusesWithoutBlockingValidPendingWork(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-valid-batch-" + suffix()
	cleanup(t, s, "backfill_id", id, backfillRuns)
	anchor := time.Date(2026, 10, 25, 1, 2, 0, 0, time.UTC)
	token, event := "observed-token", "observed-event"
	seedBackfill(t, s, id, &anchor, &token, &event)
	seedBackfill(t, s, "", &anchor, &token, &event)
	cleanup(t, s, "backfill_id", "", backfillRuns)
	validObservation, validBefore := actualBackfill(t, s, id)
	observations, err := s.BackfillObligations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var refused, ready int
	for _, observation := range observations {
		switch observation.Reference().StorageKey() {
		case "":
			refusal, ok := observation.Selection().(backfillstate.Refused)
			if !ok || refusal.Reason() != "BACKFILL_ID_INVALID" {
				t.Fatal("explicit empty stored identity became valid or acquired a replacement")
			}
			failure, err := backfillstate.RefusalOutcome(refusal)
			if err != nil {
				t.Fatal(err)
			}
			clock := anchor.Add(time.Minute)
			completion, err := backfillstate.Finish(observation, failure, clock, clock, backfillWriter(t))
			if err != nil {
				t.Fatal(err)
			}
			acknowledgement, err := s.FinishBackfill(ctx, completion)
			if err != nil || acknowledgement.Disposition() != backfillstate.WriteApplied {
				t.Fatalf("actual malformed row did not reach explicit terminal refusal: %v", err)
			}
			refused++
		case id:
			if _, ok := observation.Selection().(backfillstate.Obligation); !ok {
				t.Fatal("valid sibling was refused with the malformed row")
			}
			ready++
		}
	}
	if refused != 1 || ready != 1 {
		t.Fatalf("one malformed stored key blocked the remaining batch: %d/%d", refused, ready)
	}
	_, invalid := actualBackfill(t, s, "")
	if invalid.StorageKey != "" || invalid.BackfillID != "" || invalid.Status == nil || *invalid.Status != "FAILED" {
		t.Fatal("malformed stored identity was fabricated or left pending")
	}
	currentObservation, current := actualBackfill(t, s, id)
	beforeDocument, err := validBefore.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	afterDocument, err := current.SemanticDocument()
	if err != nil || !bytes.Equal(beforeDocument, afterDocument) || currentObservation.Reference() != validObservation.Reference() {
		t.Fatal("malformed sibling changed valid pending work")
	}
}

func TestBackfillWriteFailureRollsBackWithoutAnAppliedAcknowledgement(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	id := "backfill-rollback-" + suffix()
	cleanup(t, s, "backfill_id", id, backfillRuns)
	anchor := time.Date(2026, 10, 25, 1, 2, 0, 0, time.UTC)
	token, event := "observed-token", "observed-event"
	seedBackfill(t, s, id, &anchor, &token, &event)
	observation, before := actualBackfill(t, s, id)
	name := "owned_backfill_refusal_" + suffix()
	function := pgx.Identifier{"ops", name}.Sanitize()
	trigger := pgx.Identifier{name}.Sanitize()
	_, err := s.Pool.Exec(ctx, "CREATE FUNCTION "+function+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.storage_key = '"+id+"' THEN RAISE EXCEPTION 'owned write refusal'; END IF; RETURN NEW; END $$")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(context.Background(), "DROP TRIGGER IF EXISTS "+trigger+" ON "+liveTable(backfillRuns)+"; DROP FUNCTION IF EXISTS "+function+"()"); err != nil {
			t.Error(err)
		}
	})
	if _, err := s.Pool.Exec(ctx, "CREATE TRIGGER "+trigger+" BEFORE UPDATE ON "+liveTable(backfillRuns)+" FOR EACH ROW EXECUTE FUNCTION "+function+"()"); err != nil {
		t.Fatal(err)
	}
	start, err := backfillstate.Begin(observation, anchor.Add(time.Minute), anchor.Add(time.Minute), backfillWriter(t))
	if err != nil {
		t.Fatal(err)
	}
	acknowledgement, err := s.StartBackfill(ctx, start)
	if err == nil || acknowledgement.Disposition() == backfillstate.WriteApplied {
		t.Fatal("failed transaction acquired a durable write acknowledgement")
	}
	_, after := actualBackfill(t, s, id)
	prior, err := before.SemanticDocument()
	if err != nil {
		t.Fatal(err)
	}
	current, err := after.SemanticDocument()
	if err != nil || !bytes.Equal(prior, current) {
		t.Fatal("failed transition left partial physical effects")
	}
}
