package requeststate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

func text(value string) *string { return &value }

func physicalFixture() Record {
	started := time.Date(2026, 10, 25, 1, 2, 3, 0, time.UTC)
	deadline := started.Add(time.Minute)
	return Record{
		StorageKey: "physical-storage-key", RequestID: "request-one", Status: text("RUNNING"),
		Writer: text("capture-peer"), StartedAt: &started, StartedAtPrec: text("COLLECTION_ONLY"),
		Deadline: &deadline, BudgetS: text("NaN"), ConfigVersion: text("retained-config"),
		RequestJSON: text("{\"method\":\"POST\",\"body\":{\"exact\":0.123456789012345678901,\"nullable\":null}}"),
		ResultJSON:  text("null"),
	}
}

func TestPhysicalReadPreservesPoisonExactJSONAndDeclaredNulls(t *testing.T) {
	original := physicalFixture()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if record.StorageKey != "physical-storage-key" || record.BudgetS == nil || *record.BudgetS != "NaN" || record.DeadlinePrec != nil || record.ResultJSON == nil || *record.ResultJSON != "null" {
		t.Fatalf("Physical read collapsed retained evidence:%+v", record)
	}
	if record.RequestJSON == nil || *record.RequestJSON != *original.RequestJSON {
		t.Fatal("Opaque request numeric/text evidence changed")
	}
	claimed, err := record.Claimed()
	if err != nil {
		t.Fatal(err)
	}
	if _, invalid := DecodeLimits(claimed.Deadline, claimed.BudgetS, claimed.StartedAt); invalid == nil || invalid.Reason != BudgetInvalid {
		t.Fatalf("Poison should become an explicit execution failure:%v", invalid)
	}
	if _, err := record.SemanticDocument(); err != nil {
		t.Fatal(err)
	}
	if *record.BudgetS != "NaN" || *record.RequestJSON != *original.RequestJSON {
		t.Fatal("Fingerprint serialization mutated raw input")
	}
}

func TestReadRejectsMissingPhysicalFieldsAndNeverInfersStartMetadata(t *testing.T) {
	original := physicalFixture()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	missing := bytes.Replace(data, []byte(`"deadline_prec":null,`), nil, 1)
	if _, err := DecodeRecord(missing); err == nil {
		t.Fatal("Missing physical metadata was silently replaced by null")
	}
	original.StartedAtPrec = nil
	if _, err := original.Claimed(); err == nil {
		t.Fatal("Actual collection start precision was inferred")
	}
	original.StartedAtPrec = text("COLLECTION_ONLY")
	original.Writer = nil
	if _, err := original.Claimed(); err == nil {
		t.Fatal("Missing claim writer was guessed")
	}
}

func TestExpiryPrecedesBudgetValidationAndLegacyDeadlineRemainsBounded(t *testing.T) {
	now := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	limits, invalid := DecodeLimits(&past, text("NaN"), now)
	if invalid != nil || limits.Budget != -time.Second {
		t.Fatalf("Expired poison was reinterpreted as executable budget:%+v %v", limits, invalid)
	}
	future := now.Add(7 * time.Second)
	limits, invalid = DecodeLimits(&future, nil, now)
	if invalid != nil || limits.Budget != 7*time.Second {
		t.Fatalf("Declared deadline did not bound legacy missing budget:%+v %v", limits, invalid)
	}
	limits, invalid = DecodeLimits(&future, text("3.5"), now)
	if invalid != nil || limits.Budget != 3500*time.Millisecond {
		t.Fatalf("Declared numeric text did not narrow deadline:%+v %v", limits, invalid)
	}
}

func TestLiveFetchOutcomeBindsActualBytesAndRejectsContradictoryRepresentations(t *testing.T) {
	body := "a'b\x00"
	sum := sha256.Sum256([]byte(body))
	result := FetchResult{StatusCode: 200, ContentType: "application/octet-stream", SHA256: hex.EncodeToString(sum[:]), FetchedAt: "2026-10-25T00:00:00Z", Bytes: int64(len(body)), Body: &body}
	outcome := Completed(result)
	if _, err := outcome.JSONResult(); err != nil {
		t.Fatal(err)
	}
	result.SHA256 = hex.EncodeToString(make([]byte, 32))
	if _, err := Completed(result).JSONResult(); err == nil {
		t.Fatal("Declared hash replaced actual response-byte evidence")
	}
	result.SHA256 = hex.EncodeToString(sum[:])
	result.BodyB64 = text("YQ==")
	if _, err := Completed(result).JSONResult(); err == nil {
		t.Fatal("Contradictory body variants were admitted")
	}
	var pointer *FetchResult
	if _, err := Completed(pointer).JSONResult(); err == nil {
		t.Fatal("Typed nil result escaped complete-outcome validation")
	}
}

func TestFailedRepairRetainsOnlyActuallyObservedTypedCounts(t *testing.T) {
	observed := RepairResult{Instruments: map[string]RepairCoverage{"one": {IncompleteBefore: 3, IncompleteAfter: 1}}}
	completion := Completion{ID: "request-one", Outcome: Failure(RepairFailed, observed), FinishedAt: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)}
	if err := completion.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := completion.Outcome.JSONResult()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"instruments":{"one":{"incomplete_before":3,"incomplete_after":1}}}` {
		t.Fatalf("Partial repair evidence changed:%s", data)
	}
	observed.Instruments["one"] = RepairCoverage{IncompleteBefore: -1}
	if _, err := Completed(observed).JSONResult(); err == nil {
		t.Fatal("Negative invented coverage was admitted")
	}
}

func TestEveryPhysicalProjectionKeyIsRequiredEvenWhenNullable(t *testing.T) {
	data, err := json.Marshal(physicalFixture())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 35 {
		t.Fatalf("Physical schema unexpectedly changed: %d", len(fields))
	}
	for name, original := range fields {
		t.Run(name, func(t *testing.T) {
			delete(fields, name)
			missing, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			fields[name] = original
			if _, err := DecodeRecord(missing); err == nil {
				t.Fatalf("Missing physical projection key %s was accepted", name)
			}
		})
	}
	record, err := DecodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if record.ResultJSON == nil || *record.ResultJSON != "null" {
		t.Fatal("JSON null text collapsed into SQL NULL")
	}
	record.ResultJSON = nil
	sqlNull, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRecord(sqlNull)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ResultJSON != nil {
		t.Fatal("Explicit SQL NULL became JSON null text")
	}
}

func TestPhysicalIdentityColumnsRejectSQLNull(t *testing.T) {
	data, err := json.Marshal(physicalFixture())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"storage_key", "request_id"} {
		original := fields[name]
		fields[name] = json.RawMessage("null")
		invalid, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		fields[name] = original
		if _, err := DecodeRecord(invalid); err == nil {
			t.Fatalf("Nonnullable physical identity %s accepted SQLNULL", name)
		}
	}
}

func TestPhysicalProjectionRejectsWrongTimestampAndBudgetWireTypes(t *testing.T) {
	data, err := json.Marshal(physicalFixture())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []struct {
		name  string
		value json.RawMessage
	}{
		{"deadline", json.RawMessage(`"not-a-timestamp"`)},
		{"budget_s", json.RawMessage("true")},
		{"budget_s", json.RawMessage("0")},
	} {
		original := fields[candidate.name]
		fields[candidate.name] = candidate.value
		malformed, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		fields[candidate.name] = original
		if _, err := DecodeRecord(malformed); err == nil {
			t.Fatalf("Wrong physical wire type accepted for %s: %s", candidate.name, candidate.value)
		}
	}
}

func TestLiveFailureReasonAndResultVariantsFormClosedContract(t *testing.T) {
	variants := []Result{
		nil,
		RepairResult{Instruments: map[string]RepairCoverage{}},
		FetchFailure{Error: "observed transport error"},
		RejectedURL{URL: "https://refused.invalid/path"},
		RejectedMethod{Method: "DELETE"},
	}
	reasons := []struct {
		reason  Reason
		variant int
	}{
		{DeadlineInvalid, 0}, {BudgetInvalid, 0}, {TokenInvalid, 0}, {Unsupported, 0}, {FetchUnavailable, 0},
		{RepairFailed, 1}, {FetchRequestInvalid, 2}, {FetchFailed, 2}, {ResponseTooLarge, 2}, {HostBanned, 2}, {BudgetExhausted, 2},
		{FetchHostNotAllowed, 3}, {FetchMethodNotAllowed, 4},
	}
	for _, reason := range reasons {
		for index, result := range variants {
			_, err := Failure(reason.reason, result).JSONResult()
			if (err == nil) != (index == reason.variant) {
				t.Errorf("Reason %s variant %d accepted=%v, expected variant %d", reason.reason, index, err == nil, reason.variant)
			}
		}
	}
	for _, reason := range []Reason{"", "UNKNOWN", "REPAIR_FAIL", DeadlinePassed} {
		for _, result := range variants {
			if _, err := Failure(reason, result).JSONResult(); err == nil {
				t.Fatalf("Undeclared live failure reason accepted: %q", reason)
			}
		}
	}
	for _, candidate := range []Outcome{
		Failure(FetchFailed, FetchFailure{}), Failure(FetchHostNotAllowed, RejectedURL{}), Failure(FetchMethodNotAllowed, RejectedMethod{}),
	} {
		if _, err := candidate.JSONResult(); err == nil {
			t.Fatal("Empty refusal evidence accepted")
		}
	}
	historical := physicalFixture()
	historical.Reason = text("UNKNOWN_RETAINED_PEER_REASON")
	raw, err := json.Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	preserved, err := DecodeRecord(raw)
	if err != nil || preserved.Reason == nil || *preserved.Reason != "UNKNOWN_RETAINED_PEER_REASON" {
		t.Fatalf("Live reason validation tightened raw historical evidence: %+v %v", preserved, err)
	}
}

func TestTextBodyCannotReplaceInvalidUTF8AfterDigestValidation(t *testing.T) {
	body := string([]byte{0xff, 0x00, 0x61})
	digest := sha256.Sum256([]byte(body))
	result := FetchResult{StatusCode: 200, ContentType: "application/octet-stream", SHA256: hex.EncodeToString(digest[:]), FetchedAt: "2026-10-25T00:00:00Z", Bytes: int64(len(body)), Body: &body}
	if _, err := Completed(result).JSONResult(); err == nil {
		t.Fatal("Invalid UTF-8 passed as text and could be rewritten after its digest check")
	}
	result.Body = nil
	result.BodyB64 = text("/wBh")
	if _, err := Completed(result).JSONResult(); err != nil {
		t.Fatalf("Actual binary bytes lost their valid explicit base64 representation: %v", err)
	}
}
