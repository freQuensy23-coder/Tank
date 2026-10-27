"""Operation-bound source report intent and observed durable notice outcomes."""

import re
from collections.abc import Sequence
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scalars import OneInt
from tank.contracts.source_errors import EntryErrorMarker, EntryErrorNotice, SourceEntryError
from tank.contracts.source_items import SourceContextId
from tank.time_precision import Timestamp
from tank.util import parse_iso_time, sha256

type SourceReportMode = Literal["ENABLED", "DISABLED", "REPLAY"]
type ReportDigest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class SourceReportingPolicy(DomainModel):
    ops_progress_enabled: bool
    replay: bool

    @property
    def mode(self) -> SourceReportMode:
        return "REPLAY" if self.replay else "ENABLED" if self.ops_progress_enabled else "DISABLED"


class SourceReportScope(DomainModel):
    source_id: SourceContextId
    feed_url: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_feed(self) -> Self:
        if not self.feed_url.strip():
            raise ValueError("Source report scope requires its actual logical feed")
        return self


class SourceReportSubject(SourceReportScope):
    source_name: str = Field(min_length=1)

    @model_validator(mode="after")
    def actual_name(self) -> Self:
        if not self.source_name.strip():
            raise ValueError("Source report subject requires its actual source name")
        return self

    @property
    def scope(self) -> SourceReportScope:
        return SourceReportScope(source_id=self.source_id, feed_url=self.feed_url)


def source_error_key(source_id: str, feed_url: str, error: SourceEntryError) -> str:
    return sha256(
        {
            "source_id": source_id,
            "feed": feed_url,
            "reason": re.sub(r"\d+", "#", error.reason),
            **error.identity().model_dump(mode="json"),
        }
    )


def source_report_notice_id(scope: SourceReportScope, keys: Sequence[str]) -> str:
    record: dict[str, JsonValue] = {
        "source_id": scope.source_id,
        "feed": scope.feed_url,
        "keys": [key for key in sorted(keys)],
    }
    return "entry_errors_" + sha256(record)


class SourceReportRequest(DomainModel):
    subject: SourceReportSubject
    seen_at: str
    errors: list[SourceEntryError] = Field(min_length=1)

    @model_validator(mode="after")
    def collection_observation(self) -> Self:
        parse_iso_time(self.seen_at)
        return self

    @property
    def error_keys(self) -> list[str]:
        return sorted(
            {source_error_key(self.subject.source_id, self.subject.feed_url, error) for error in self.errors}
        )


class SourceReportIntent(SourceReportRequest):
    intent_id: SourceContextId
    versions: ResolvedVersionIdentity
    created_at: Timestamp

    @model_validator(mode="after")
    def actual_intent_clock(self) -> Self:
        if self.created_at.value is None or self.created_at.precision != "COLLECTION_ONLY":
            raise ValueError("Source report intent requires its actual collection clock")
        return self


class SourceReportMarkerReference(DomainModel):
    error_key: ReportDigest
    marker_id: SourceContextId
    notice_id: SourceContextId
    marker_sha256: ReportDigest
    notice_sha256: ReportDigest

    @model_validator(mode="after")
    def marker_identity(self) -> Self:
        if self.marker_id != "entry_error_" + self.error_key:
            raise ValueError("Source report marker identity conflicts with its error key")
        return self


class SourceReportNoticeWitness(DomainModel):
    scope: SourceReportScope
    notice_id: SourceContextId
    error_keys: list[ReportDigest] = Field(min_length=1)
    notice_sha256: ReportDigest


def source_report_notice_witness(
    scope: SourceReportScope, raw: dict[str, JsonValue]
) -> SourceReportNoticeWitness:
    notice = EntryErrorNotice.model_validate(raw)
    keys = sorted({source_error_key(scope.source_id, scope.feed_url, error) for error in notice.entry_errors})
    if (
        notice.notice_id != source_report_notice_id(scope, keys)
        or notice.subject_id != scope.source_id
        or notice.source_id != scope.source_id
        or notice.feed_url != scope.feed_url
        or sorted(notice.entry_error_keys) != keys
    ):
        raise ValueError("Stored source report notice conflicts with its actual scope")
    return SourceReportNoticeWitness(
        scope=scope, notice_id=notice.notice_id, error_keys=keys, notice_sha256=sha256(raw)
    )


def source_report_marker_reference(
    scope: SourceReportScope, key: str, raw: dict[str, JsonValue], notice: SourceReportNoticeWitness
) -> SourceReportMarkerReference:
    marker = EntryErrorMarker.model_validate(raw)
    if (
        marker.notice_id != "entry_error_" + key
        or marker.subject_id != scope.source_id
        or marker.feed_url != scope.feed_url
        or source_error_key(scope.source_id, scope.feed_url, marker.entry_error) != key
        or marker.group_notice_id != notice.notice_id
        or notice.scope != scope
        or key not in notice.error_keys
    ):
        raise ValueError("Stored source report marker conflicts with its actual scope")
    return SourceReportMarkerReference(
        error_key=key,
        marker_id=marker.notice_id,
        notice_id=notice.notice_id,
        marker_sha256=sha256(raw),
        notice_sha256=notice.notice_sha256,
    )


class SourceReportConfirmation(DomainModel):
    scope: SourceReportScope
    markers: list[SourceReportMarkerReference] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_error_keys(self) -> Self:
        if len({marker.error_key for marker in self.markers}) != len(self.markers):
            raise ValueError("Source report confirmation repeats an error identity")
        notices: dict[str, str] = {}
        for marker in self.markers:
            if marker.notice_id in notices and notices[marker.notice_id] != marker.notice_sha256:
                raise ValueError("Source report confirmation contradicts its stored notice revision")
            notices[marker.notice_id] = marker.notice_sha256
        return self


class SourceReportRecorded(SourceReportConfirmation):
    kind: Literal["RECORDED"]
    notice_id: SourceContextId

    @model_validator(mode="after")
    def actual_notice(self) -> Self:
        if not any(marker.notice_id == self.notice_id for marker in self.markers):
            raise ValueError("Recorded source report does not reference its actual notice")
        return self


class SourceReportDeduplicated(SourceReportConfirmation):
    kind: Literal["DEDUPLICATED"]


class SourceReportNotRequested(DomainModel):
    kind: Literal["NOT_REQUESTED"]
    scope: SourceReportScope
    reason: Literal["DISABLED", "REPLAY"]


type SourceReportOutcome = Annotated[
    SourceReportRecorded | SourceReportDeduplicated | SourceReportNotRequested,
    Field(discriminator="kind"),
]


class SourceReportDisposition(DomainModel):
    intent_id: SourceContextId | None
    intent_revision: ReportDigest | None
    outcome: SourceReportOutcome
    recorded_at: Timestamp
    versions: ResolvedVersionIdentity

    @model_validator(mode="after")
    def actual_observation(self) -> Self:
        if (self.intent_id is None) != (self.intent_revision is None):
            raise ValueError("Source report disposition requires both intent identity and revision")
        if self.outcome.kind != "NOT_REQUESTED" and self.intent_id is None:
            raise ValueError("Source report acknowledgement requires its actual stored intent")
        if self.recorded_at.value is None or self.recorded_at.precision != "COLLECTION_ONLY":
            raise ValueError("Source report disposition requires its actual collection clock")
        return self


class LegacySourceReports(DomainModel):
    field: Literal["entry_error_reports"]
    field_sha256: ReportDigest
    reason: Literal["UNSCOPED_LEGACY_REPORTS", "MALFORMED_LEGACY_REPORTS"]


class SourceReportLedger(DomainModel):
    contract_version: OneInt
    source_id: SourceContextId
    pending: dict[SourceContextId, SourceReportIntent]
    last_outcome: SourceReportDisposition | None
    unresolved_legacy: LegacySourceReports | None

    @model_validator(mode="after")
    def actual_operation_owner(self) -> Self:
        if any(
            key != intent.intent_id or intent.subject.source_id != self.source_id
            for key, intent in self.pending.items()
        ) or (self.last_outcome is not None and self.last_outcome.outcome.scope.source_id != self.source_id):
            raise ValueError("Source report ledger conflicts with its operation identity")
        return self
